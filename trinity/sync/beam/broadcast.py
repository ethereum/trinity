import asyncio
from collections import Counter
from concurrent.futures import CancelledError
import itertools
import typing
from typing import (
    FrozenSet,
    Iterable,
    Set,
    Tuple,
    Type,
)

from lahja import EndpointAPI

from eth_hash.auto import keccak
from eth_utils import (
    to_checksum_address,
    ValidationError,
)
from eth_typing import (
    Address,
    Hash32,
)

from eth.abc import AtomicDatabaseAPI

from cancel_token import CancelToken, OperationCancelled

from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trie import HexaryTrie
from trie.exceptions import MissingTrieNode

from trinity._utils.datastructures import TaskQueue
from trinity._utils.timer import Timer
from trinity.protocol.common.typing import (
    NodeDataBundles,
)
from trinity.protocol.eth.commands import (
    NodeData,
)
from trinity.protocol.eth.constants import (
    MAX_STATE_FETCH,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.eth import (
    constants as eth_constants,
)
from trinity.sync.beam.backfill import (
    QueenTrackerAPI,
)
from trinity.sync.beam.constants import (
    DELAY_BEFORE_NON_URGENT_REQUEST,
    REQUEST_BUFFER_MULTIPLIER,
)

from trinity.sync.common.event import StatelessBlockImportDone
from trinity.sync.common.peers import WaitingPeers


def slice_hashes(hash_list: bytes) -> Iterable[Hash32]:
    for index in range(0, len(hash_list), 32):
        next_hash = hash_list[index:index + 32]
        if len(next_hash) != 32:
            raise TypeError(
                f"Value is not a hash, because it's only {len(next_hash)} long: {next_hash}"
            )
        yield next_hash


class WitnessBroadcaster(BaseService, PeerSubscriber):
    """
    Broadcast block witness node hashes to all Firehose-compatible peers.
    """

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _firehose_peers: Set[FirehosePeer]

    def __init__(
            self,
            db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._db = db
        self._peer_pool = peer_pool
        self._event_bus = event_bus

        self._firehose_peers = set()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # This service is only interested in peers that implement firehose
        if peer.connection.has_protocol(FirehoseProtocol):
            self._firehose_peers.add(peer)

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self._firehose_peers.remove(peer)

    async def _broadcast_new_witnesses(self) -> None:
        async for event in self.wait_iter(self._event_bus.stream(StatelessBlockImportDone)):
            self._broadcast_witness_metadata(event.block.hash)

    def _broadcast_witness_metadata(self, header_hash: Hash32) -> None:
        # TODO: merge py-evm changes that save these witness hashes, and use a proper API
        # Load witness from DB
        witness_key = b'witnesshashes:' + header_hash
        # TODO: clean up old witnesses out of database

        if witness_key not in self._db:
            self.logger.warning("Witness data for block %s not found", humanize_hash(header_hash))
            return
        else:
            witness_hashes_concat = self._db[witness_key]
            witness_hashes = tuple(slice_hashes(witness_hashes_concat))

            for peer in self._firehose_peers:
                peer.fh_api.send_new_block_witness_hashes(header_hash, witness_hashes)

    async def _run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self.logger.info("Starting witness metadata broadcaster")
        with self.subscribe(self._peer_pool):
            await self._broadcast_new_witnesses()
