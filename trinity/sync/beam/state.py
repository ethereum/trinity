import asyncio
from concurrent.futures import CancelledError
import itertools
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
    encode_hex,
    to_checksum_address,
    ValidationError,
)
from eth_typing import (
    Address,
    Hash32,
)

from cancel_token import CancelToken, OperationCancelled

from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trie import HexaryTrie
from trie.exceptions import MissingTrieNode

from trinity._utils.datastructures import TaskQueue
from trinity._utils.timer import Timer
from trinity.db.base import BaseAsyncDB
from trinity.protocol.common.types import (
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
from trinity.sync.beam.constants import (
    DELAY_BEFORE_NON_URGENT_REQUEST,
    REQUEST_BUFFER_MULTIPLIER,
    EMPTY_PEER_RESPONSE_PENALTY,
)

from trinity.sync.common.peers import WaitingPeers


def _is_hash(maybe_hash: bytes) -> bool:
    return isinstance(maybe_hash, bytes) and len(maybe_hash) == 32


class BeamDownloader(BaseService, PeerSubscriber):
    """
    Coordinate the request of needed state data: accounts, storage, bytecodes, and
    other arbitrary intermediate nodes in the trie.
    """
    do_predictive_downloads = False
    _total_processed_nodes = 0
    _urgent_processed_nodes = 0
    _predictive_processed_nodes = 0
    _total_timeouts = 0
    _predictive_only_requests = 0
    _total_requests = 0
    _timer = Timer(auto_start=False)
    _report_interval = 10  # Number of seconds between progress reports.
    _reply_timeout = 20  # seconds

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    def __init__(
            self,
            db: BaseAsyncDB,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._db = db
        self._trie_db = HexaryTrie(db)
        self._node_data_peers = WaitingPeers[ETHPeer](NodeData)
        self._event_bus = event_bus

        # Track the needed node data that is urgent and important:
        buffer_size = MAX_STATE_FETCH * REQUEST_BUFFER_MULTIPLIER
        self._node_tasks = TaskQueue[Hash32](buffer_size, lambda task: 0)

        # list of events waiting on new data
        self._new_data_events: Set[asyncio.Event] = set()

        self._peer_pool = peer_pool

        # Track node data that might be useful: hashes we bumped into while getting urgent nodes
        self._maybe_useful_nodes = TaskQueue[Hash32](
            buffer_size,
            # Everything is the same priority, for now
            lambda node_hash: 0,
        )

        self._peers_without_full_trie: Set[ETHPeer] = set()

        # It's possible that you are connected to a peer that doesn't have a full state DB
        # In that case, we may get stuck requesting predictive nodes from them over and over
        #   because they don't have anything but the nodes required to prove recent block
        #   executions. If we get stuck in that scenario, turn off allow_predictive_only.
        #   For now, we just turn it off for all peers, for simplicity.
        self._allow_predictive_only = True

    async def ensure_nodes_present(
            self,
            node_hashes: Iterable[Hash32],
            urgent: bool=True) -> int:
        """
        Wait until the nodes that are the preimages of `node_hashes` are available in the database.
        If one is not available in the first check, request it from peers.

        :param urgent: Should this node be downloaded urgently? If False, download as backfill

        Note that if your ultimate goal is an account or storage data, it's probably better to use
        download_account or download_storage. This method is useful for other
        scenarios, like bytecode lookups or intermediate node lookups.

        :return: how many nodes had to be downloaded
        """
        if urgent:
            queue = self._node_tasks
        else:
            queue = self._maybe_useful_nodes

        return await self._wait_for_nodes(node_hashes, queue)

    async def _wait_for_nodes(
            self,
            node_hashes: Iterable[Hash32],
            queue: TaskQueue[Hash32]) -> int:
        missing_nodes = set(
            node_hash for node_hash in node_hashes if self._is_node_missing(node_hash)
        )
        unrequested_nodes = tuple(
            node_hash for node_hash in missing_nodes if node_hash not in queue
        )
        if unrequested_nodes:
            await queue.add(unrequested_nodes)
        if missing_nodes:
            await self._node_hashes_present(missing_nodes)
        return len(unrequested_nodes)

    def _is_node_missing(self, node_hash: Hash32) -> bool:
        if len(node_hash) != 32:
            raise ValidationError(f"Must request node by its 32-byte hash: 0x{node_hash.hex()}")

        self.logger.debug2("checking if node 0x%s is present", node_hash.hex())

        return node_hash not in self._db

    async def download_accounts(
            self,
            account_addresses: Iterable[Hash32],
            root_hash: Hash32,
            urgent: bool=True) -> int:
        """
        Like :meth:`download_account`, but waits for multiple addresses to be available.

        :return: total number of trie node downloads that were required to locally prove
        """
        missing_account_hashes = set(keccak(address) for address in account_addresses)
        completed_account_hashes = set()
        nodes_downloaded = 0
        # will never take more than 64 attempts to get a full account
        for _ in range(64):
            need_nodes = set()
            with self._trie_db.at_root(root_hash) as snapshot:
                for account_hash in missing_account_hashes:
                    try:
                        snapshot[account_hash]
                    except MissingTrieNode as exc:
                        need_nodes.add(exc.missing_node_hash)
                    else:
                        completed_account_hashes.add(account_hash)

            await self.ensure_nodes_present(need_nodes, urgent)
            nodes_downloaded += len(need_nodes)
            missing_account_hashes -= completed_account_hashes

            if not missing_account_hashes:
                return nodes_downloaded
        else:
            raise Exception(
                f"State Downloader failed to download {account_addresses!r} at "
                f"state root 0x{root_hash.hex} in 64 runs"
            )

    async def download_account(
            self,
            account_hash: Hash32,
            root_hash: Hash32,
            urgent: bool=True) -> Tuple[bytes, int]:
        """
        Check the given account address for presence in the state database.
        Wait until we have the state proof for the given address.
        If the account is not available in the first check, then request any trie nodes
        that we need to determine and prove the account rlp.

        Mark these nodes as urgent and important, which increases request priority.

        :return: The downloaded account rlp, and how many state trie node downloads were required
        """
        # will never take more than 64 attempts to get a full account
        for num_downloads_required in range(64):
            try:
                with self._trie_db.at_root(root_hash) as snapshot:
                    account_rlp = snapshot[account_hash]
            except MissingTrieNode as exc:
                await self.ensure_nodes_present({exc.missing_node_hash}, urgent)
            else:
                # Account is fully available within the trie
                return account_rlp, num_downloads_required
        else:
            raise Exception(
                f"State Downloader failed to download 0x{account_hash.hex()} at "
                f"state root 0x{root_hash.hex} in 64 runs"
            )

    async def download_storage(
            self,
            storage_key: Hash32,
            storage_root_hash: Hash32,
            account: Address,
            urgent: bool=True) -> int:
        """
        Check the given storage key for presence in the account's storage database.
        Wait until we have a trie proof for the given storage key.
        If the storage key value is not available in the first check, then request any trie nodes
        that we need to determine and prove the storage value.

        Mark these nodes as urgent and important, which increases request priority.

        :return: how many storage trie node downloads were required
        """
        # should never take more than 64 attempts to get a full account
        for num_downloads_required in range(64):
            try:
                with self._trie_db.at_root(storage_root_hash) as snapshot:
                    # request the data just to see which part is missing
                    snapshot[storage_key]
            except MissingTrieNode as exc:
                await self.ensure_nodes_present({exc.missing_node_hash}, urgent)
            else:
                # Account is fully available within the trie
                return num_downloads_required
        else:
            raise Exception(
                f"State Downloader failed to download storage 0x{storage_key.hex()} in "
                f"{to_checksum_address(account)} at storage root 0x{storage_root_hash} "
                f"in 64 runs."
            )

    async def _match_node_requests_to_peers(self) -> None:
        """
        Monitor TaskQueue for needed trie nodes, and request them from peers. Repeat as necessary.
        Prefer urgent nodes over predictive ones.
        """
        while self.is_operational:
            urgent_batch_id, urgent_hashes = await self._get_waiting_urgent_hashes()

            predictive_batch_id, predictive_hashes = self._maybe_add_predictive_nodes(urgent_hashes)

            # combine to single tuple of unique hashes
            node_hashes = self._append_unique_hashes(urgent_hashes, predictive_hashes)

            if not node_hashes:
                self.logger.warning("restarting because empty node hashes")
                await self.sleep(0.02)
                continue

            # Get an available peer, preferring the one that gives us the most node data throughput
            peer = await self._node_data_peers.get_fastest()

            if urgent_batch_id is None:
                # We will make a request of all-predictive nodes
                if peer in self._peers_without_full_trie:
                    self.logger.warning("Skipping all-predictive loading on %s", peer)
                    self._node_data_peers.put_nowait(peer)
                    self._maybe_useful_nodes.complete(predictive_batch_id, ())
                    self._allow_predictive_only = False
                    continue

            if any(len(h) != 32 for h in node_hashes):
                # This was inserted to identify and resolve a buggy situation
                short_node_urgent_hashes = tuple(h for h in node_hashes if len(h) != 32)
                raise ValidationError(
                    f"Some of the requested node hashes are too short! {short_node_urgent_hashes!r}"
                )

            if urgent_batch_id is None:
                self._predictive_only_requests += 1
            self._total_requests += 1

            # Request all the nodes from the given peer, and immediately move on to
            #   try to request other nodes from another peer.
            self.run_task(self._get_nodes_from_peer(
                peer,
                node_hashes,
                urgent_batch_id,
                urgent_hashes,
                predictive_hashes,
                predictive_batch_id,
            ))

    async def _get_waiting_urgent_hashes(self) -> Tuple[int, Tuple[Hash32, ...]]:
        # if any predictive nodes are waiting, then time out after a short pause to grab them
        if self._allow_predictive_only and self._maybe_useful_nodes.num_pending():
            timeout = DELAY_BEFORE_NON_URGENT_REQUEST
        else:
            timeout = None
        try:
            return await self.wait(
                self._node_tasks.get(eth_constants.MAX_STATE_FETCH),
                timeout=timeout,
            )
        except TimeoutError:
            return None, ()

    def _maybe_add_predictive_nodes(
            self,
            urgent_hashes: Tuple[Hash32, ...]) -> Tuple[int, Tuple[Hash32, ...]]:
        # how many predictive nodes should we request?
        num_predictive_backfills = min(
            eth_constants.MAX_STATE_FETCH - len(urgent_hashes),
            self._maybe_useful_nodes.num_pending(),
        )
        if num_predictive_backfills:
            return self._maybe_useful_nodes.get_nowait(
                num_predictive_backfills,
            )
        else:
            return None, ()

    @staticmethod
    def _append_unique_hashes(
            first_hashes: Tuple[Hash32, ...],
            non_unique_hashes: Tuple[Hash32, ...]) -> Tuple[Hash32, ...]:
        unique_hashes_to_add = tuple(set(non_unique_hashes).difference(first_hashes))
        return first_hashes + unique_hashes_to_add

    async def _get_nodes_from_peer(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...],
            urgent_batch_id: int,
            urgent_node_hashes: Tuple[Hash32, ...],
            predictive_node_hashes: Tuple[Hash32, ...],
            predictive_batch_id: int) -> None:

        nodes = await self._request_nodes(peer, node_hashes)

        if len(nodes) == 0 and urgent_batch_id is None:
            self.logger.debug("Shutting off all-predictive loading on %s", peer)
            self._peers_without_full_trie.add(peer)

        urgent_nodes = {
            node_hash: node for node_hash, node in nodes
            if node_hash in urgent_node_hashes
        }
        predictive_nodes = {
            node_hash: node for node_hash, node in nodes
            if node_hash in predictive_node_hashes
        }
        if len(urgent_nodes) + len(predictive_nodes) < len(nodes):
            raise ValidationError(f"All nodes must be either urgent or predictive")

        if len(urgent_nodes) == 0 and urgent_batch_id is not None:
            self.logger.info("%s returned no urgent nodes from %r", peer, urgent_node_hashes)

        # batch all DB writes into one, for performance
        with self._db.atomic_batch() as batch:
            for node_hash, node in nodes:
                batch[node_hash] = node

        if urgent_batch_id is not None:
            self._node_tasks.complete(urgent_batch_id, tuple(urgent_nodes.keys()))

        if predictive_batch_id is not None:
            # retire all predictions, if the responding node doesn't have them, then we don't
            # want to keep asking
            self._maybe_useful_nodes.complete(predictive_batch_id, predictive_node_hashes)

        self._urgent_processed_nodes += len(urgent_nodes)
        for node_hash in predictive_nodes.keys():
            if node_hash not in urgent_node_hashes:
                self._predictive_processed_nodes += 1
        self._total_processed_nodes += len(nodes)

        if len(nodes):
            for new_data in self._new_data_events:
                new_data.set()

    def _is_node_present(self, node_hash: Hash32) -> bool:
        """
        Check if node_hash has data in the database or in the predicted node set.
        """
        return node_hash in self._db

    async def _node_hashes_present(self, node_hashes: Set[Hash32]) -> None:
        remaining_hashes = node_hashes.copy()

        # save an event that gets triggered when new data comes in
        new_data = asyncio.Event()
        self._new_data_events.add(new_data)

        iterations = itertools.count()

        while remaining_hashes and next(iterations) < 1000:
            await new_data.wait()

            found_hashes = set(found for found in remaining_hashes if self._is_node_present(found))
            remaining_hashes -= found_hashes

            new_data.clear()

        if remaining_hashes:
            self.logger.error("Never collected node data for hashes %r", remaining_hashes)

        self._new_data_events.remove(new_data)

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # when a new peer is added to the pool, add it to the idle peer list
        self._node_data_peers.put_nowait(peer)  # type: ignore

    async def _request_nodes(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...]) -> NodeDataBundles:
        try:
            completed_nodes = await self._make_node_request(peer, node_hashes)
        except BaseP2PError as exc:
            self.logger.warning("Unexpected p2p err while downloading nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading nodes from peer, dropping...", exc_info=True)
            return tuple()
        except OperationCancelled:
            self.logger.debug(
                "Service cancellation while fetching segment, dropping %s from queue",
                peer,
                exc_info=True,
            )
            return tuple()
        except PeerConnectionLost:
            self.logger.debug("%s went away, cancelling the nodes request and moving on...", peer)
            return tuple()
        except CancelledError:
            self.logger.debug("Pending nodes call to %r future cancelled", peer)
            return tuple()
        except Exception as exc:
            self.logger.info("Unexpected err while downloading nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading nodes from peer, dropping...", exc_info=True)
            return tuple()
        else:
            if len(completed_nodes) > 0:
                # peer completed successfully, so have it get back in line for processing
                self._node_data_peers.put_nowait(peer)
            else:
                # peer didn't return enough results, wait a while before trying again
                delay = EMPTY_PEER_RESPONSE_PENALTY
                self.logger.debug(
                    "Pausing %s for %.1fs, for replying with no node data "
                    "to request for: %r",
                    peer,
                    delay,
                    [encode_hex(h) for h in node_hashes],
                )
                self.call_later(delay, self._node_data_peers.put_nowait, peer)
            return completed_nodes

    async def _make_node_request(
            self,
            peer: ETHPeer,
            original_node_hashes: Tuple[Hash32, ...]) -> NodeDataBundles:
        node_hashes = tuple(set(original_node_hashes))
        num_nodes = len(node_hashes)
        self.logger.debug2("Requesting %d nodes from %s", num_nodes, peer)
        try:
            return await peer.requests.get_node_data(node_hashes, timeout=self._reply_timeout)
        except TimeoutError as err:
            # This kind of exception shouldn't necessarily *drop* the peer,
            # so capture error, log and swallow
            self.logger.debug("Timed out requesting %d nodes from %s", num_nodes, peer)
            self._total_timeouts += 1
            return tuple()

    async def _run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self._timer.start()
        self.logger.info("Starting beam state sync")
        self.run_task(self._periodically_report_progress())
        with self.subscribe(self._peer_pool):
            await self.wait(self._match_node_requests_to_peers())

    async def _periodically_report_progress(self) -> None:
        while self.is_operational:
            msg = "all=%d  " % self._total_processed_nodes
            msg += "urgent=%d  " % self._urgent_processed_nodes
            msg += "pred=%d  " % self._predictive_processed_nodes
            msg += "all/sec=%d  " % (self._total_processed_nodes / self._timer.elapsed)
            msg += "urgent/sec=%d  " % (self._urgent_processed_nodes / self._timer.elapsed)
            msg += "reqs=%d  " % (self._total_requests)
            msg += "pred_reqs=%d  " % (self._predictive_only_requests)
            msg += "timeouts=%d" % self._total_timeouts
            self.logger.debug("Beam-Sync: %s", msg)
            await self.sleep(self._report_interval)
