import asyncio
from typing import (
    cast,
    Callable,
    Iterable,
    List,
    Sequence,
    Tuple,
)
import uuid
import weakref

from eth_utils import to_tuple

from lahja import EndpointAPI

from cancel_token import CancelToken

from eth_utils.toolz import partition_all
from eth.abc import SignedTransactionAPI

from p2p.abc import SessionAPI
from p2p.disconnect import DisconnectReason
from p2p.service import BaseService
from p2p.token_bucket import TokenBucket, NotEnoughTokens

from trinity._utils.bloom import RollingBloom
from trinity.protocol.eth.peer import ETHProxyPeerPool
from trinity.protocol.eth.events import TransactionsEvent


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


# Rate that the duplicate transactions token bucket fills
TWO_PER_MINUTE = 1 / 30


class TxPool(BaseService):
    """
    The :class:`~trinity.tx_pool.pool.TxPool` class is responsible for holding and relaying
    of transactions, represented as :class:`~eth.abc.SignedTransactionAPI` among the
    connected peers.

      .. note::

        This is a minimal viable implementation that only relays transactions but doesn't actually
        hold on to them yet. It's still missing many features of a grown up transaction pool.
    """
    _buckets: 'weakref.WeakKeyDictionary[SessionAPI, TokenBucket]'

    def __init__(self,
                 event_bus: EndpointAPI,
                 peer_pool: ETHProxyPeerPool,
                 tx_validation_fn: Callable[[SignedTransactionAPI], bool],
                 token: CancelToken = None) -> None:
        super().__init__(token)
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
        self._inbound_bloom = RollingBloom(generation_size=100000, max_generations=144)
        self._outbound_bloom = RollingBloom(generation_size=100000, max_generations=144)
        self._bloom_salt = uuid.uuid4()

        # This internal queue is used to pipeline the incoming transactions so
        # that we can batch them before sending them back out.
        self._internal_queue: 'asyncio.Queue[Sequence[SignedTransactionAPI]]' = asyncio.Queue(2000)

        # A set of `TokenBucket` for each peer which are used to determine
        # whether the peer is sending us "too many" duplicate transactions.
        self._buckets = weakref.WeakKeyDictionary()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    async def _run(self) -> None:
        self.logger.info("Running Tx Pool")

        self.run_daemon_task(self._process_transactions())
        async for event in self.wait_iter(self._event_bus.stream(TransactionsEvent)):
            txs = cast(List[SignedTransactionAPI], event.msg)
            self.run_task(self._handle_tx(event.session, txs))

    async def _handle_tx(self, sender: SessionAPI, txs: Sequence[SignedTransactionAPI]) -> None:

        self.logger.debug2('Received %d transactions from %s', len(txs), sender)

        duplicates = self._record_inbound_to_bloom(sender, txs)
        if duplicates:
            self.logger.debug(
                "Received %s duplicate transactions from peer %s",
                len(duplicates),
                sender,
            )
            # We need to use `peer.session` rather than `sender` because we
            # need an object that lives beyond the lifecycle of this function
            # call.
            peer = await self._peer_pool.ensure_proxy_peer(sender)
            if peer.session not in self._buckets:
                # Bucket refils 1 token-per-30-seconds with a maximum of 10
                # tokens.  Receiving more than 10 duplicate transactions in a
                # 10 second period will result in a client being disconnected.
                self._buckets[peer.session] = TokenBucket(rate=TWO_PER_MINUTE, capacity=10)
            bucket = self._buckets[peer.session]
            try:
                bucket.take_nowait(len(duplicates))
            except NotEnoughTokens:
                self.logger.debug(
                    "Received too many duplicate transactions from peer %s, disconnecting",
                    sender,
                )
                await peer.disconnect(DisconnectReason.subprotocol_error)

        await self._internal_queue.put(txs)

    async def _process_transactions(self) -> None:
        while self.is_operational:
            buffer: List[SignedTransactionAPI] = []

            # wait for there to be items available on the queue.
            buffer.extend(await self._internal_queue.get())

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
                for receiving_peer in await self._peer_pool.get_peers():
                    filtered_tx = self._filter_tx_for_peer(receiving_peer.session, batch)
                    if len(filtered_tx) == 0:
                        continue

                    self.logger.debug2(
                        'Sending %d transactions to %s',
                        len(filtered_tx),
                        receiving_peer,
                    )
                    receiving_peer.sub_proto.send_transactions(filtered_tx)
                    self._record_outbound_to_bloom(receiving_peer.session, filtered_tx)

    def _filter_tx_for_peer(
            self,
            session: SessionAPI,
            txs: Sequence[SignedTransactionAPI]) -> Tuple[SignedTransactionAPI, ...]:

        return tuple(
            val
            for key, val
            in ((self._construct_bloom_entry(session, val), val) for val in txs)
            if key not in self._inbound_bloom
            if key not in self._outbound_bloom
            if self.tx_validation_fn(val)
        )

    def _construct_bloom_entry(self, session: SessionAPI, tx: SignedTransactionAPI) -> bytes:
        return b':'.join((
            session.id.bytes,
            tx.hash,
            self._bloom_salt.bytes,
        ))

    @to_tuple
    def _record_inbound_to_bloom(self,
                                 session: SessionAPI,
                                 txs: Iterable[SignedTransactionAPI],
                                 ) -> Iterable[SignedTransactionAPI]:
        for val in txs:
            key = self._construct_bloom_entry(session, val)
            if key in self._inbound_bloom:
                yield val
            self._inbound_bloom.add(key)

    def _record_outbound_to_bloom(self,
                                  session: SessionAPI,
                                  txs: Iterable[SignedTransactionAPI],
                                  ) -> None:
        for val in txs:
            self._outbound_bloom.add(self._construct_bloom_entry(session, val))

    async def do_cleanup(self) -> None:
        self.logger.info("Stopping Tx Pool...")
