from __future__ import annotations
import asyncio
from collections import Counter
from functools import partial
import itertools
import typing
from typing import (
    Dict,
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from async_service import Service

from eth.abc import (
    AtomicDatabaseAPI,
    BlockHeaderAPI,
)
from eth.constants import EMPTY_SHA3
from eth.rlp.accounts import Account
from eth_typing import Hash32
from eth_utils.toolz import take
import rlp
from trie import (
    HexaryTrie,
    exceptions as trie_exceptions,
    fog,
)
from trie.constants import (
    BLANK_NODE_HASH,
)
from trie.utils.nibbles import (
    bytes_to_nibbles,
)
from trie.utils.nodes import (
    key_starts_with,
)
from trie.typing import (
    HexaryTrieNode,
    Nibbles,
)

from p2p.exceptions import BaseP2PError, PeerConnectionLost

from trinity.protocol.eth.constants import (
    MAX_STATE_FETCH,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.sync.beam.constants import (
    EPOCH_BLOCK_LENGTH,
    GAP_BETWEEN_TESTS,
    NON_IDEAL_RESPONSE_PENALTY,
    PAUSE_SECONDS_IF_STATE_BACKFILL_STARVED,
)
from trinity._utils.logging import get_logger
from trinity._utils.priority import SilenceObserver
from trinity._utils.timer import Timer

from .queen import (
    QueeningQueue,
    QueenTrackerAPI,
)

# How many trie nodes to request in each backfill message to peers:
REQUEST_SIZE = MAX_STATE_FETCH


class BeamStateBackfill(Service, QueenTrackerAPI):
    """
    Use a very simple strategy to fill in state in the background.

    Ask each peer in sequence for some nodes, ignoring the lowest RTT node.
    Reduce memory pressure by using a depth-first strategy.

    An intended side-effect is to build & maintain an accurate measurement of
    the round-trip-time that peers take to respond to GetNodeData commands.
    """

    _total_added_nodes = 0
    _num_added = 0
    _num_missed = 0
    _num_accounts_completed = 0
    _num_storage_completed = 0
    _report_interval = 10

    _num_requests_by_peer: typing.Counter[ETHPeer]

    def __init__(self, db: AtomicDatabaseAPI, peer_pool: ETHPeerPool) -> None:
        self.logger = get_logger('trinity.sync.beam.backfill.BeamStateBackfill')
        self._db = db

        self._peer_pool = peer_pool

        self._is_missing: Set[Hash32] = set()

        self._num_requests_by_peer = Counter()

        self._queening_queue = QueeningQueue(peer_pool)

        # Track the nodes that we are requesting in the account trie
        self._account_tracker = TrieNodeRequestTracker()

        self._storage_trackers: Dict[Hash32, TrieNodeRequestTracker] = {}
        self._bytecode_trackers: Dict[Hash32, TrieNodeRequestTracker] = {}

        # The most recent root hash to use to navigate the trie
        self._next_trie_root_hash: Optional[Hash32] = None
        self._begin_backfill = asyncio.Event()

        # Only acquire peasant peers for backfill if there are no other coros
        #   waiting for a peasant. Any other waiter is assumed to be higher priority.
        self._external_peasant_usage = SilenceObserver(minimum_silence_duration=GAP_BETWEEN_TESTS)

    async def get_queen_peer(self) -> ETHPeer:
        return await self._queening_queue.get_queen_peer()

    def penalize_queen(self, peer: ETHPeer, delay: float = NON_IDEAL_RESPONSE_PENALTY) -> None:
        self._queening_queue.penalize_queen(peer, delay=delay)

    def insert_peer(self, peer: ETHPeer, delay: float = 0) -> None:
        self._queening_queue.insert_peer(peer, delay=delay)

    async def pop_fastest_peasant(self) -> ETHPeer:
        async with self._external_peasant_usage.make_noise():
            return await self._queening_queue.pop_fastest_peasant()

    def pop_knights(self) -> Iterable[ETHPeer]:
        return self._queening_queue.pop_knights()

    def set_desired_knight_count(self, desired_knights: int) -> None:
        self._queening_queue.set_desired_knight_count(desired_knights)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodically_report_progress)

        queening_manager = self.manager.run_daemon_child_service(self._queening_queue)
        await queening_manager.wait_started()
        await self._run_backfill()
        self.manager.cancel()

    def _batch_of_missing_hashes(self) -> Tuple[TrackedRequest, ...]:
        """
        Take a batch of missing trie hashes, sized for a single peer request
        """
        return tuple(take(
            REQUEST_SIZE,
            self._missing_trie_hashes(),
        ))

    async def _run_backfill(self) -> None:
        await self._begin_backfill.wait()
        if self._next_trie_root_hash is None:
            raise RuntimeError("Cannot start backfill when a recent trie root hash is unknown")

        loop = asyncio.get_event_loop()
        while self.manager.is_running:
            # Collect node hashes that might be missing; enough for a single request.
            # Collect batch before asking for peer, because we don't want to hold the
            #   peer idle, for a long time.
            required_data = await loop.run_in_executor(None, self._batch_of_missing_hashes)

            if len(required_data) == 0:
                # Nothing available to request, for one of two reasons:
                if self._check_complete():
                    self.logger.info("Downloaded all accounts, storage and bytecode state")
                    return
                else:
                    # There are active requests to peers, and we don't have enough information to
                    #   ask for any more trie nodes (for example, near the beginning, when the top
                    #   of the trie isn't available).
                    self.logger.debug("Backfill is waiting for more hashes to arrive")
                    await asyncio.sleep(PAUSE_SECONDS_IF_STATE_BACKFILL_STARVED)
                    continue

            await asyncio.wait(
                (
                    self._external_peasant_usage.until_silence(),
                    self.manager.wait_finished(),
                ),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not self.manager.is_running:
                break

            peer = await self._queening_queue.pop_fastest_peasant()

            # skip over peer if it has an active data request
            while peer.eth_api.get_node_data.is_requesting:
                self.logger.debug(
                    "Want backfill nodes from %s, but it has an active request, skipping...",
                    peer,
                )
                # Put this peer back
                self._queening_queue.insert_peer(peer, NON_IDEAL_RESPONSE_PENALTY)
                # Ask for the next peer
                peer = await self._queening_queue.pop_fastest_peasant()

            self.manager.run_task(self._make_request, peer, required_data)

    def _check_complete(self) -> bool:
        if self._account_tracker.is_complete:
            storage_complete = all(
                storage_tracker.is_complete
                for storage_tracker in self._storage_trackers.values()
            )
            if storage_complete:
                bytecode_complete = all(
                    bytecode_tracker.is_complete
                    for bytecode_tracker in self._bytecode_trackers.values()
                )
                # All backfill is complete only if the account and storage and bytecodes are present
                return bytecode_complete
            else:
                # At least one account is missing a storage trie node
                return False
        else:
            # At least one account trie node is missing
            return False

    def _missing_trie_hashes(self) -> Iterator[TrackedRequest]:
        """
        Walks through the full state trie, yielding one missing node hash/prefix
        at a time.

        The yielded node info is wrapped in a TrackedRequest. The hash is
        marked as active until it is explicitly marked for review again. The
        hash/prefix will be marked for review asking a peer for the data.

        Will exit when all known node hashes are already actively being
        requested, or if there are no more missing nodes.
        """
        # For each account, when we have asked for all known storage and bytecode
        #   hashes, but some are still not present, we "pause" the account so we can look
        #   for neighboring nodes.
        # This is a list of paused accounts, using the path to the leaf node,
        #   because that's how the account tracker is indexed.
        exhausted_account_leaves: Tuple[Nibbles, ...] = ()

        starting_root_hash = self._next_trie_root_hash

        try:
            while self.manager.is_running:
                # Get the next account

                # We have to rebuild the account iterator every time because...
                #   something about an exception during a manual __anext__()?
                account_iterator = self._request_tracking_trie_items(
                    self._account_tracker,
                    starting_root_hash,
                )
                try:
                    next_account_info = next(account_iterator)
                except trie_exceptions.MissingTraversalNode as exc:
                    # Found a missing trie node while looking for the next account
                    yield self._account_tracker.generate_request(
                        exc.missing_node_hash,
                        exc.nibbles_traversed,
                    )
                    continue
                except StopIteration:
                    # Finished iterating over all available accounts
                    break

                # Decode account
                path_to_leaf, address_hash_nibbles, encoded_account = next_account_info
                account = rlp.decode(encoded_account, sedes=Account)

                # Iterate over all missing hashes of subcomponents (storage & bytecode)
                subcomponent_hashes_iterator = self._missing_subcomponent_hashes(
                    address_hash_nibbles,
                    account,
                    starting_root_hash,
                )
                for node_request in subcomponent_hashes_iterator:
                    yield node_request

                # Check if account is fully downloaded
                account_components_complete = self._are_account_components_complete(
                    address_hash_nibbles,
                    account,
                )
                if account_components_complete:
                    # Mark fully downloaded accounts as complete, and do some cleanup
                    self._mark_account_complete(path_to_leaf, address_hash_nibbles)
                else:
                    # Pause accounts that are not fully downloaded, and track the account
                    #   to resume when the generator exits.
                    self._account_tracker.pause_review(path_to_leaf)
                    exhausted_account_leaves += (path_to_leaf, )

        except GeneratorExit:
            # As the generator is exiting, we want to resume any paused accounts. This
            #   allows us to find missing storage/bytecode on the next iteration.
            for path_to_leaf in exhausted_account_leaves:
                self._account_tracker.mark_for_review(path_to_leaf)
            raise
        else:
            # If we pause a few accounts and then run out of nodes to ask for, then we
            #   still need to resume the paused accounts to prepare for the next iteration.
            for path_to_leaf in exhausted_account_leaves:
                self._account_tracker.mark_for_review(path_to_leaf)

            # Possible scenarios:
            #   1. We have completed backfill
            #   2. We have iterated the available nodes, and all known hashes are being requested.
            #       For example: if 0 nodes are available, and we walk to the root and request
            #       the root from a peer, we do not have any available information to ask for
            #       more nodes, and exit cleanly.
            #
            # In response to these situations, we might like to:
            #   1. Log and celebrate that the full state has been downloaded
            #   2. Exit this search and sleep a bit, waiting for new trie nodes to arrive
            #
            # 1 and 2 are a little more cleanly handled outside this iterator, so we just
            #   exit and let the caller deal with it, using a _check_complete() check.
            return

    def _request_tracking_trie_items(
            self,
            request_tracker: TrieNodeRequestTracker,
            root_hash: Hash32) -> Iterator[Tuple[Nibbles, Nibbles, bytes]]:
        """
        Walk through the supplied trie, yielding the request tracker and node
        request for any missing trie nodes.

        :yield: path to leaf node, a key (as nibbles), and the value found in the trie
        :raise: MissingTraversalNode if a node is missing while walking the trie
        """
        if self._next_trie_root_hash is None:
            # We haven't started beam syncing, so don't know which root to start at
            return
        trie = HexaryTrie(self._db, root_hash)

        starting_index = bytes_to_nibbles(root_hash)

        while self.manager.is_running:
            try:
                path_to_node = request_tracker.next_path_to_explore(starting_index)
            except trie_exceptions.PerfectVisibility:
                # This doesn't necessarily mean we are finished.
                # Any active prefixes might still be hiding some significant portion of the trie
                # But it's all we're able to explore for now, until more node data arrives
                return

            try:
                cached_node, uncached_key = request_tracker.get_cached_parent(path_to_node)
            except KeyError:
                cached_node = None
                node_getter = partial(trie.traverse, path_to_node)
            else:
                node_getter = partial(trie.traverse_from, cached_node, uncached_key)

            try:
                node = node_getter()
            except trie_exceptions.MissingTraversalNode as exc:
                # Found missing account trie node
                if path_to_node == exc.nibbles_traversed:
                    raise
                elif cached_node is None:
                    # The path and nibbles traversed should always match in a non-cached traversal
                    raise RuntimeError(
                        f"Unexpected: on a non-cached traversal to {path_to_node}, the"
                        f" exception only claimed to traverse {exc.nibbles_traversed} -- {exc}"
                    ) from exc
                else:
                    # We need to re-raise a version of the exception that includes the whole path
                    #   from the root node (when using cached nodes, we only have the path from
                    #   the parent node to the child node)
                    # We could always raise this re-wrapped version, but skipping it (probably?)
                    #   improves performance.
                    missing_hash = exc.missing_node_hash
                    raise trie_exceptions.MissingTraversalNode(missing_hash, path_to_node) from exc
            except trie_exceptions.TraversedPartialPath as exc:
                node = exc.simulated_node

            if node.value:
                full_key_nibbles = path_to_node + node.suffix

                if len(node.sub_segments):
                    # It shouldn't be a problem to skip handling this case, because all keys are
                    #   hashed 32 bytes.
                    raise NotImplementedError(
                        "The state backfiller doesn't handle keys of different lengths, where"
                        f" one key is a prefix of another. But found {node} in trie with"
                        f" {root_hash!r}"
                    )

                yield path_to_node, full_key_nibbles, node.value
                # Note that we do not mark value nodes as completed. It is up to the caller
                #   to do that when it is ready. For example, the storage iterator will
                #   immediately treat the key as completed. The account iterator will
                #   not treat the key as completed until all of its storage and bytecode
                #   are also marked as complete.
            else:
                # If this is just an intermediate node, then we can mark it as confirmed.
                request_tracker.confirm_prefix(path_to_node, node)

    def _missing_subcomponent_hashes(
            self,
            address_hash_nibbles: Nibbles,
            account: Account,
            starting_main_root: Hash32) -> Iterator[TrackedRequest]:

        storage_node_iterator = self._missing_storage_hashes(
            address_hash_nibbles,
            account.storage_root,
            starting_main_root,
        )
        for node_request in storage_node_iterator:
            yield node_request

        bytecode_node_iterator = self._missing_bytecode_hashes(
            address_hash_nibbles,
            account.code_hash,
            starting_main_root,
        )
        for node_request in bytecode_node_iterator:
            yield node_request

        # Note that completing this iterator does NOT mean we're done with the
        #   account. It just means that all known missing hashes are actively
        #   being requested.

    def _missing_storage_hashes(
            self,
            address_hash_nibbles: Nibbles,
            storage_root: Hash32,
            starting_main_root: Hash32) -> Iterator[TrackedRequest]:
        """
        Walks through the storage trie at the given root, yielding one missing
        storage node hash/prefix at a time.

        The yielded node info is wrapped in a ``TrackedRequest``. The hash is
        marked as active until it is explicitly marked for review again. The
        hash/prefix will be marked for review asking a peer for the data.

        Will exit when all known node hashes are already actively being
        requested, or if there are no more missing nodes.
        """

        if storage_root == BLANK_NODE_HASH:
            # Nothing to do if the storage has an empty root
            return

        storage_tracker = self._get_storage_tracker(address_hash_nibbles)
        while self.manager.is_running:
            storage_iterator = self._request_tracking_trie_items(
                storage_tracker,
                storage_root,
            )
            try:
                for path_to_leaf, _hashed_key, _storage_value in storage_iterator:
                    # We don't actually care to look at the storage keys/values during backfill
                    storage_tracker.confirm_leaf(path_to_leaf)

            except trie_exceptions.MissingTraversalNode as exc:
                yield storage_tracker.generate_request(
                    exc.missing_node_hash,
                    exc.nibbles_traversed,
                )
            else:
                # Possible scenarios:
                #   1. We have completed backfilling this account's storage
                #   2. We have iterated the available nodes, and only their children are missing,
                #       for example: if 0 nodes are available, and we walk to the root and request
                #       the root from a peer, we do not have any available information to ask for
                #       more nodes.
                #
                # In response to these situations, we might like to:
                #   1. Debug log?
                #   2. Look for more missing nodes in neighboring accounts and their storage, etc.
                #
                # 1 and 2 are a little more cleanly handled outside this iterator, so we just
                #   exit and let the caller deal with it.
                return

    def _missing_bytecode_hashes(
            self,
            address_hash_nibbles: Nibbles,
            code_hash: Hash32,
            starting_main_root: Hash32) -> Iterator[TrackedRequest]:
        """
        Checks if this bytecode is missing. If so, yield it and then exit.
        If not, then exit immediately.

        This may seem like overkill, and it is right now. But...
        Code merkelization is coming (theoretically), and the other account
        and storage trie iterators work similarly to this, so in some ways
        it's easier to do this "over-generalized" solution now. It makes
        request tracking a bit easier too, to have the same TrackedRequest
        result mechanism.
        """

        if code_hash == EMPTY_SHA3:
            # Nothing to do if the bytecode is for the empty hash
            return

        bytecode_tracker = self._get_bytecode_tracker(address_hash_nibbles)
        if bytecode_tracker.is_complete:
            # All bytecode has been collected
            return

        # If there is an active request (for now, there can only be one), then skip
        #   any database checks until the active request is resolved.
        if not bytecode_tracker.has_active_requests:
            if code_hash not in self._db:
                # The bytecode isn't present, so we ask for it.
                # A bit hacky here, since there is no trie, we just treat it as
                #   if it were a leaf node at the root.
                yield bytecode_tracker.generate_request(code_hash, prefix=())
            else:
                # The bytecode is already present, but the tracker isn't marked
                #   as completed yet, so finish it off.
                bytecode_tracker.confirm_leaf(path_to_leaf=())

    def _get_storage_tracker(self, address_hash_nibbles: Nibbles) -> TrieNodeRequestTracker:
        if address_hash_nibbles in self._storage_trackers:
            return self._storage_trackers[address_hash_nibbles]
        else:
            new_tracker = TrieNodeRequestTracker()
            self._storage_trackers[address_hash_nibbles] = new_tracker
            return new_tracker

    def _get_bytecode_tracker(self, address_hash_nibbles: Nibbles) -> TrieNodeRequestTracker:
        if address_hash_nibbles in self._bytecode_trackers:
            return self._bytecode_trackers[address_hash_nibbles]
        else:
            new_tracker = TrieNodeRequestTracker()
            self._bytecode_trackers[address_hash_nibbles] = new_tracker
            return new_tracker

    def _mark_account_complete(self, path_to_leaf: Nibbles, address_hash_nibbles: Nibbles) -> None:
        self._account_tracker.confirm_leaf(path_to_leaf)

        self._num_accounts_completed += 1

        # Clear the storage tracker, to reduce memory usage
        #   and the time to check self._check_complete()
        if address_hash_nibbles in self._storage_trackers:
            self._num_storage_completed += 1
            del self._storage_trackers[address_hash_nibbles]

        # Clear the bytecode tracker, for the same reason
        if address_hash_nibbles in self._bytecode_trackers:
            del self._bytecode_trackers[address_hash_nibbles]

    def _are_account_components_complete(
            self,
            address_hash_nibbles: Nibbles,
            account: Account) -> bool:

        if account.storage_root != BLANK_NODE_HASH:
            # Avoid generating a storage tracker if there is no storage for this account
            storage_tracker = self._get_storage_tracker(address_hash_nibbles)

        if account.storage_root == BLANK_NODE_HASH or storage_tracker.is_complete:
            if account.code_hash == EMPTY_SHA3:
                # All storage is downloaded, and no bytecode to download
                return True
            else:
                bytecode_tracker = self._get_bytecode_tracker(address_hash_nibbles)
                # All storage is downloaded, return True only if bytecode is downloaded
                return bytecode_tracker.is_complete
        else:
            # Missing some storage
            return False

    async def _make_request(
            self,
            peer: ETHPeer,
            request_data: Iterable[TrackedRequest]) -> None:

        self._num_requests_by_peer[peer] += 1
        request_hashes = tuple(set(request.node_hash for request in request_data))
        try:
            nodes = await peer.eth_api.get_node_data(request_hashes)
        except asyncio.TimeoutError:
            self._queening_queue.insert_peer(peer, GAP_BETWEEN_TESTS * 2)
        except PeerConnectionLost:
            # Something unhappy, but we don't really care, peer will be gone by next loop
            pass
        except (BaseP2PError, Exception) as exc:
            self.logger.info("Unexpected err while getting background nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading background nodes from peer...", exc_info=True)
            self._queening_queue.insert_peer(peer, GAP_BETWEEN_TESTS * 2)
        else:
            self._queening_queue.insert_peer(peer, GAP_BETWEEN_TESTS)
            self._insert_results(request_hashes, nodes)
        finally:
            for request in request_data:
                request.tracker.mark_for_review(request.prefix)

    def _insert_results(
            self,
            requested_hashes: Tuple[Hash32, ...],
            nodes: Tuple[Tuple[Hash32, bytes], ...]) -> None:

        returned_nodes = dict(nodes)
        with self._db.atomic_batch() as write_batch:
            for requested_hash in requested_hashes:
                if requested_hash in returned_nodes:
                    self._num_added += 1
                    self._total_added_nodes += 1
                    encoded_node = returned_nodes[requested_hash]
                    write_batch[requested_hash] = encoded_node
                else:
                    self._num_missed += 1

    def set_root_hash(self, header: BlockHeaderAPI, root_hash: Hash32) -> None:
        if self._next_trie_root_hash is None:
            self._next_trie_root_hash = root_hash
            self._begin_backfill.set()
        elif header.block_number % EPOCH_BLOCK_LENGTH == 1:
            # This is the root hash of the *parent* of the header, so use modulus equals 1
            self._next_trie_root_hash = root_hash

    async def _periodically_report_progress(self) -> None:
        for step in itertools.count():
            if not self.manager.is_running:
                break

            self._num_added = 0
            self._num_missed = 0
            timer = Timer()
            await asyncio.sleep(self._report_interval)

            if not self._begin_backfill.is_set():
                self.logger.debug("Beam-Backfill: waiting for new state root")
                continue

            msg = "total=%d" % self._total_added_nodes
            msg += "  new=%d" % self._num_added
            msg += "  miss=%d" % self._num_missed
            self.logger.debug("Beam-Backfill: %s", msg)

            # log peer counts
            show_top_n_peers = 3
            self.logger.debug(
                "Beam-Backfill-Peer-Usage-Top-%d: %s",
                show_top_n_peers,
                self._num_requests_by_peer.most_common(show_top_n_peers),
            )

            # For now, report every 30s (1/3 as often as the debug report above)
            if step % 3 == 0:
                num_storage_trackers = len(self._storage_trackers)
                if num_storage_trackers:
                    # Need a copy to avoid changes to the dict in other threads,
                    #   while the generator loops.
                    trackers = tuple(self._storage_trackers.values())
                    active_storage_completion = sum(
                        self._complete_trie_fraction(store_tracker)
                        for store_tracker in trackers
                    ) / num_storage_trackers
                else:
                    active_storage_completion = 0

                # Log backfill state stats as a progress indicator to the user:
                #   - nodes: the total number of nodes collected during this backfill session
                #   - accts: number of accounts completed, including all storage and bytecode,
                #       if present. This includes accounts downloaded and ones already present.
                #   - prog: the progress to completion, measured as a percentage of accounts
                #       completed, using trie structure. Ignores imbalances caused by storage.
                #   - stores: number of non-trivial complete storages downloaded
                #   - storing: the percentage complete and number of storage tries being
                #       downloaded actively
                #   - walked: the part of the account trie walked from this
                #       epoch's index, as parts per million (a fraction of the
                #       total account trie)
                #   - tnps: trie nodes collected per second, since the last debug log (in the
                #       last 10 seconds, at comment time)
                num_requests = sum(self._num_requests_by_peer.values())
                if num_requests == 0:
                    log = self.logger.debug
                else:
                    log = self.logger.info

                log(
                    (
                        "State Stats: nodes=%d accts=%d prog=%.2f%% stores=%d"
                        " storing=%.1f%% of %d walked=%.1fppm tnps=%.0f req=%d"
                    ),
                    self._total_added_nodes,
                    self._num_accounts_completed,
                    self._complete_trie_fraction(self._account_tracker) * 100,
                    self._num_storage_completed,
                    active_storage_completion * 100,
                    num_storage_trackers,
                    self._contiguous_accounts_complete_fraction() * 1e6,
                    self._num_added / timer.elapsed,
                    num_requests,
                )

            self._num_requests_by_peer.clear()

    def _complete_trie_fraction(self, tracker: TrieNodeRequestTracker) -> float:
        """
        Calculate stats for logging: estimate what percent of the trie is completed,
        by looking at unexplored prefixes in the account trie.

        :return: a number in the range [0, 1] (+/- rounding error) estimating
            trie completion

        One awkward thing: there will be no apparent progress while filling in
        the storage of a single large account. Progress is slow enough anyway
        that this is probably immaterial.
        """
        # Move this logic into HexaryTrieFog someday

        unknown_prefixes = tracker._trie_fog._unexplored_prefixes

        # Basic estimation logic:
        # - An unknown prefix 0xf means that we are missing 1/16 of the trie
        # - An unknown prefix 0x12 means that we are missing 1/(16^2) of the trie
        # - Add up all the unknown prefixes to estimate the total collected fraction.

        unknown_fraction = sum(
            (1 / 16) ** len(prefix)
            for prefix in unknown_prefixes
        )

        return 1 - unknown_fraction

    def _contiguous_accounts_complete_fraction(self) -> float:
        """
        Estimate the completed fraction of the trie that is contiguous with
        the current index (which rotates every 32 blocks)

        It will be probably be quite noticeable that it will get "stuck" when
        downloading a lot of storage, because we'll have to blow it up to more
        than a percentage to see any significant change within 32 blocks. (when
        the index will change again anyway)

        :return: a number in the range [0, 1] (+/- rounding error) estimating
            trie completion contiguous with the current backfill index key
        """
        starting_index = bytes_to_nibbles(self._next_trie_root_hash)
        unknown_prefixes = self._account_tracker._trie_fog._unexplored_prefixes
        if len(unknown_prefixes) == 0:
            return 1

        # find the nearest unknown prefix (typically, on the right)
        nearest_index = unknown_prefixes.bisect(starting_index)

        # Get the nearest unknown prefix to the left
        if nearest_index == 0:
            left_prefix = (0, ) * 64
        else:
            left_prefix = unknown_prefixes[nearest_index - 1]
            if key_starts_with(starting_index, left_prefix):
                # The prefix of the starting index is unknown, so the index
                #   itself is unknown.
                return 0

        # Get the nearest unknown prefix to the right
        if len(unknown_prefixes) == nearest_index:
            right_prefix = (0xf, ) * 64
        else:
            right_prefix = unknown_prefixes[nearest_index]

        # Use the space between the unknown prefixes to estimate the completed contiguous fraction

        # At the base, every gap in the first nibble is a full 1/16th of the state complete
        known_first_nibbles = right_prefix[0] - left_prefix[0] - 1
        completed_fraction_base = (1 / 16) * known_first_nibbles

        # Underneath, you can count completed subtrees on the right, each child 1/16 of the parent
        right_side_completed = sum(
            nibble * (1 / 16) ** nibble_depth
            for nibble_depth, nibble
            in enumerate(right_prefix[1:], 2)
        )
        # Do the same on the left
        left_side_completed = sum(
            (0xf - nibble) * (1 / 16) ** nibble_depth
            for nibble_depth, nibble
            in enumerate(left_prefix[1:], 2)
        )

        # Add up all completed areas
        return left_side_completed + completed_fraction_base + right_side_completed


class TrieNodeRequestTracker:
    def __init__(self) -> None:
        self._trie_fog = fog.HexaryTrieFog()
        self._active_prefixes: Set[Nibbles] = set()

        # cache of nodes used to speed up trie walking
        self._node_frontier_cache = fog.TrieFrontierCache()

    def mark_for_review(self, prefix: Nibbles) -> None:
        # Calling this does not mean that the nodes were returned, only that they are eligible again
        #   for review (either they were returned or we can ask a different peer for them)
        self._active_prefixes.remove(prefix)

    def pause_review(self, prefix: Nibbles) -> None:
        """
        Stop iterating this node, until mark_for_review() is called
        """
        self._active_prefixes.add(prefix)

    def _get_eligible_fog(self) -> fog.HexaryTrieFog:
        """
        Return the Trie Fog that can be searched, ignoring any nodes that are currently
        being requested.
        """
        # Must pass in a copy of prefixes, so the set doesn't get modified while
        #   mark_all_complete is iterating over it.
        return self._trie_fog.mark_all_complete(self._active_prefixes.copy())

    def next_path_to_explore(self, starting_index: Nibbles) -> Nibbles:
        return self._get_eligible_fog().nearest_unknown(starting_index)

    def confirm_prefix(
            self,
            confirmed_prefix: Nibbles,
            node: fog.HexaryTrieFog) -> None:

        if node.sub_segments:
            # No nodes have both value and sub_segments, so we can wait to update the cache
            self.add_cache(confirmed_prefix, node, node.sub_segments)
        elif node.value:
            # If we are confirming a leaf, use confirm_leaf(). We do not attempt to handle a
            #   situation where one key is a prefix of another key, and simply error out.
            raise ValueError("Do not handle case where prefix of another key has a value")
        else:
            # We don't have to look up this node anymore, so can delete it from our cache
            self.delete_cache(confirmed_prefix)

        self._trie_fog = self._trie_fog.explore(confirmed_prefix, node.sub_segments)

    def confirm_leaf(self, path_to_leaf: Nibbles) -> None:
        # We don't handle keys that are subkeys of other keys (because
        #   all keys are 32 bytes), so we can just hard-code that there
        #   are no children of this address.
        self.delete_cache(path_to_leaf)
        self._trie_fog = self._trie_fog.explore(path_to_leaf, ())

    def generate_request(
            self,
            node_hash: Hash32,
            prefix: Nibbles) -> TrackedRequest:

        self.pause_review(prefix)
        return TrackedRequest(self, node_hash, prefix)

    @property
    def has_active_requests(self) -> bool:
        return len(self._active_prefixes) > 0

    def get_cached_parent(self, prefix: Nibbles) -> Tuple[HexaryTrieNode, Nibbles]:
        return self._node_frontier_cache.get(prefix)

    def add_cache(
            self,
            prefix: Nibbles,
            node: HexaryTrieNode,
            sub_segments: Iterable[Nibbles]) -> None:
        self._node_frontier_cache.add(prefix, node, sub_segments)

    def delete_cache(self, prefix: Nibbles) -> None:
        self._node_frontier_cache.delete(prefix)

    @property
    def is_complete(self) -> bool:
        return self._trie_fog.is_complete

    def __repr__(self) -> str:
        return (
            f"TrieNodeRequestTracker(trie_fog={self._trie_fog!r},"
            f" active_prefixes={self._active_prefixes!r})"
        )


class TrackedRequest(NamedTuple):
    tracker: TrieNodeRequestTracker
    node_hash: Hash32
    prefix: Nibbles
