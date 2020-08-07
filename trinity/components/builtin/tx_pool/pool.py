import asyncio
from typing import (
    Callable,
    Iterable,
    List,
    Sequence,
    Tuple,
)
import uuid

from async_service import Service
from lahja import EndpointAPI

from eth_utils.toolz import partition_all
from eth.abc import SignedTransactionAPI

from p2p.abc import SessionAPI

from trinity._utils.bloom import RollingBloom
from trinity._utils.logging import get_logger
from trinity.protocol.eth.events import (
    TransactionsEvent,
    GetPooledTransactionsEvent,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeer,
    ETHProxyPeerPool,
)
from trinity.sync.common.events import SendLocalTransaction


# The 'LOW_WATER` mark determines the minimum size at which we'll choose to
# broadcast a chunk of transactions to our peers (even if we have more than
# this locally available and ready).

BATCH_LOW_WATER = 100

# The `HIGH_WATER` mark determines the maximum number of transactions we'll
# send in a batch to any given peer.  This is purely in place to ensure that we
# have a strict upper bound on the total size of a `Transactions` message for
# abnormal cases where we suddenly get a very large batch of transactions all
# at once.
BATCH_HIGH_WATER = 200


class TxPool(Service):
    """
    The :class:`~trinity.tx_pool.pool.TxPool` class is responsible for holding and relaying
    of transactions, represented as :class:`~eth.abc.SignedTransactionAPI` among the
    connected peers.

      .. note::

        This is a minimal viable implementation that only relays transactions but doesn't actually
        hold on to them yet. It's still missing many features of a grown up transaction pool.
    """

    def __init__(self,
                 event_bus: EndpointAPI,
                 peer_pool: ETHProxyPeerPool,
                 tx_validation_fn: Callable[[SignedTransactionAPI], bool],
                 ) -> None:
        self.logger = get_logger('trinity.components.txpool.TxPoolService')
        self._event_bus = event_bus
        self._peer_pool = peer_pool

        if tx_validation_fn is None:
            raise ValueError('Must pass a tx validation function')

        self.tx_validation_fn = tx_validation_fn

        # The effectiveness of the filter is based on the number of peers int the peer pool.
        #
        # Assuming 25 peers:
        # - each transaction will get sent to at most 24 peers resulting in 24 entries in the BF
        # - rough estimate of 100 transactions per block
        # - 2400 BF entries per block-of-transactions
        # - we'll target rotating the bloom filter every 10 minutes -> 40 blocks
        #
        # This gives a target generation size of 24 * 100 * 40 -> 96000 (round up to 100,000)
        #
        # We want our BF to remain effective for at least 24 hours -> 1440 min -> 144 generations
        #
        # Memory size can be computed as:

        #
        # bits_per_bloom = (-1 * generation_size * log(0.1)) / (log(2) ** 2) -> 479252
        # kbytes_per_bloom = bits_per_bloom / 8 / 1024 -> 58
        # kbytes_total = max_generations * kbytes_per_bloom -> 8424
        #
        # We can expect the maximum memory footprint to be about 8.5mb for the bloom filters.
        self._bloom = RollingBloom(generation_size=100000, max_generations=144)
        self._bloom_salt = uuid.uuid4()
        self._internal_queue: 'asyncio.Queue[Sequence[SignedTransactionAPI]]' = asyncio.Queue(2000)

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    async def run(self) -> None:
        self.logger.info("Running Tx Pool")

        # background process which aggregates transactions and relays them to
        # our other peers.
        self.manager.run_daemon_task(self._process_transactions)

        # Process GetPooledTransactions requests
        self.manager.run_daemon_task(self._process_get_pooled_transactions_requests)

        # Process all local transactions coming through the JSON-RPC API
        self.manager.run_daemon_task(self._process_local_transactions)

        async for event in self._event_bus.stream(TransactionsEvent):
            self.manager.run_task(self._handle_tx, event.session, event.command.payload)

    async def _process_get_pooled_transactions_requests(self) -> None:

        async for event in self._event_bus.stream(GetPooledTransactionsEvent):
            asking_peer = await self._peer_pool.ensure_proxy_peer(event.session)
            asking_peer.eth_api.send_pooled_transactions([])

    async def _process_local_transactions(self) -> None:

        async for event in self._event_bus.stream(SendLocalTransaction):
            # We probably want to save the transaction in the database to make sure it survives
            # across reboots and gets rebroadcasted if needed. It should probably also be monitored
            # for inclusion and removed when it's either included or past the maximum age.
            # See: https://github.com/ethereum/trinity/issues/29
            await self._internal_queue.put((event.transaction,))

    async def _handle_tx(self, sender: SessionAPI, txs: Sequence[SignedTransactionAPI]) -> None:

        self.logger.debug2('Received %d transactions from %s', len(txs), sender)

        self._add_txs_to_bloom(sender, txs)
        await self._internal_queue.put(txs)

    async def _process_transactions(self) -> None:
        while self.manager.is_running:
            buffer: List[SignedTransactionAPI] = []

            # wait for there to be items available on the queue.
            transactions = await self._internal_queue.get()
            buffer.extend(transactions)

            # continue to pull items from the queue synchronously until the
            # queue is either empty or we hit a sufficient size to justify
            # sending to our peers.
            while not self._internal_queue.empty():
                if len(buffer) > BATCH_LOW_WATER:
                    break
                buffer.extend(self._internal_queue.get_nowait())

            # Now that the queue is either empty or we have an adequate number
            # to send to our peers, broadcast them to the appropriate peers.
            for batch in partition_all(BATCH_HIGH_WATER, buffer):
                peers = await self._peer_pool.get_peers()
                for receiving_peer in peers:
                    filtered_tx = self._filter_tx_for_peer(receiving_peer, batch)
                    if len(filtered_tx) == 0:
                        self.logger.debug2(
                            '%d TXNS filtered down to ZERO for peer: %s',
                            len(batch),
                            receiving_peer,
                        )
                        continue

                    self.logger.debug2(
                        'Relaying %d transactions to %s',
                        len(filtered_tx),
                        receiving_peer,
                    )
                    receiving_peer.eth_api.send_transactions(filtered_tx)
                    self._add_txs_to_bloom(receiving_peer.session, filtered_tx)
                    # release to the event loop since this loop processes a
                    # lot of data queue up a lot of outbound messages.
                    await asyncio.sleep(0)

    def _filter_tx_for_peer(
            self,
            peer: ETHProxyPeer,
            txs: Sequence[SignedTransactionAPI]) -> Tuple[SignedTransactionAPI, ...]:

        return tuple(
            val for val in txs
            if self._construct_bloom_entry(peer.session, val) not in self._bloom
            if self.tx_validation_fn(val)
        )

    def _construct_bloom_entry(self, session: SessionAPI, tx: SignedTransactionAPI) -> bytes:
        return b':'.join((
            session.id.bytes,
            tx.hash,
            self._bloom_salt.bytes,
        ))

    def _add_txs_to_bloom(self,
                          session: SessionAPI,
                          txs: Iterable[SignedTransactionAPI]) -> None:
        for val in txs:
            key = self._construct_bloom_entry(session, val)
            self._bloom.add(key)
