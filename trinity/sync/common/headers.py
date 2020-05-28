from abc import ABC, abstractmethod
import asyncio
from concurrent.futures import CancelledError
import contextlib
from functools import partial
from operator import attrgetter, itemgetter
from random import randrange
from typing import (
    Any,
    AsyncIterator,
    Callable,
    FrozenSet,
    Generic,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from async_service import Service, background_asyncio_service

from eth_typing import (
    BlockIdentifier,
    BlockNumber,
    Hash32,
)
from eth_utils import (
    humanize_hash,
    ValidationError,
)
from eth_utils.toolz import (
    compose,
    concatv,
    drop,
    sliding_window,
    take,
)

from eth.exceptions import (
    HeaderNotFound,
)
from eth.rlp.headers import BlockHeader
from lahja import EndpointAPI

from p2p.abc import CommandAPI
from p2p.constants import SEAL_CHECK_RANDOM_SAMPLE_RATE
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.eth.commands import BlockHeadersV65 as ETHBlockHeaders
from trinity.protocol.les.commands import BlockHeaders as LESBlockHEaders
from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.common.peer import BaseChainPeer, BaseChainPeerPool
from trinity.protocol.eth.constants import (
    MAX_HEADERS_FETCH,
)
from trinity.sync.common.constants import (
    EMPTY_PEER_RESPONSE_PENALTY,
    MAX_SKELETON_REORG_DEPTH,
)
from trinity.sync.common.events import SyncingRequest, SyncingResponse
from trinity.sync.common.peers import TChainPeer, WaitingPeers
from trinity.sync.common.strategies import (
    FromGenesisLaunchStrategy,
    SyncLaunchStrategyAPI,
)
from trinity.sync.common.types import SyncProgress
from trinity._utils.datastructures import (
    DuplicateTasks,
    NoPrerequisites,
    OrderedTaskPreparation,
    TaskQueue,
)
from trinity._utils.headers import (
    skip_complete_headers,
)
from trinity._utils.humanize import (
    humanize_integer_sequence,
)
from trinity._utils.logging import get_logger


# NOTE: This service should not be cancelled externally as doing so may cause queued headers
# to get dropped on the floor.
class SkeletonSyncer(Service, Generic[TChainPeer]):
    # header skip: long enough that the pairs leave a gap of 192, the max header request length
    _skip_length = MAX_HEADERS_FETCH + 1

    max_reorg_depth = MAX_SKELETON_REORG_DEPTH

    _fetched_headers: 'asyncio.Queue[Tuple[BlockHeader, ...]]'

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncHeaderDB,
                 peer: TChainPeer,
                 launch_strategy: SyncLaunchStrategyAPI = None) -> None:
        self.logger = get_logger('trinity.sync.common.headers.SkeletonSyncer')
        self._chain = chain
        self._db = db
        if launch_strategy is None:
            launch_strategy = FromGenesisLaunchStrategy(db, chain)

        self._launch_strategy = launch_strategy
        self.peer = peer
        max_pending_headers = peer.max_headers_fetch * 8
        self._fetched_headers = asyncio.Queue(max_pending_headers)

    async def next_skeleton_segment(self) -> AsyncIterator[Tuple[BlockHeader, ...]]:
        while self.manager.is_running:
            yield await self._fetched_headers.get()
            self._fetched_headers.task_done()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._display_stats)
        try:
            await self._quietly_fetch_full_skeleton()
            self.logger.debug2(
                "Skeleton %s stopped responding, pausing for headers to emit", self.peer)
            await self._fetched_headers.join()
        except asyncio.CancelledError:
            self.logger.debug(
                "Skeleton syncer had %d pending headers when it was cancelled",
                self._fetched_headers.qsize())
            raise
        self.logger.debug2("Skeleton %s emitted all headers", self.peer)
        self.manager.cancel()

    async def _display_stats(self) -> None:
        queue = self._fetched_headers
        while self.manager.is_running:
            await asyncio.sleep(5)
            self.logger.debug("Skeleton header queue is %d/%d full", queue.qsize(), queue.maxsize)

    async def _quietly_fetch_full_skeleton(self) -> None:
        try:
            await self._fetch_full_skeleton()
        except ValidationError as exc:
            self.logger.debug(
                "Exiting sync and booting %s due to validation error: %s",
                self.peer,
                exc,
            )
        except asyncio.TimeoutError:
            self.logger.warning("Timeout waiting for header batch from %s, halting sync", self.peer)

    async def _fetch_full_skeleton(self) -> None:
        """
        Request a skeleton of headers.  In other words, return headers with gaps like so:
        parent -> child -> [skip] ... [skip] -> parent -> child -> [skip] ... [skip] -> ...

        There are some exceptions where more than two headers are returned consecutively.
        """
        peer = self.peer

        # launch the skeleton sync by finding a segment that has a parent header in the DB
        launch_headers = await self._find_launch_headers(peer)
        self._fetched_headers.put_nowait(launch_headers)
        previous_tail_header = launch_headers[-1]
        start_num = BlockNumber(previous_tail_header.block_number + self._skip_length)

        while self.manager.is_running:
            # get parents
            parents = await self._fetch_headers_from(peer, start_num)
            if not parents:
                break

            # get children
            children = await self._fetch_headers_from(peer, BlockNumber(start_num + 1))
            if not children:
                break

            # validate that parents and children match
            pairs = tuple(zip(parents, children))
            try:
                validate_pair_coros = (
                    self._chain.coro_validate_chain(parent, (child, ))
                    for parent, child in pairs
                )
                await asyncio.gather(*validate_pair_coros)
            except ValidationError as e:
                self.logger.warning(
                    "Received an invalid header pair from %s: %s",
                    peer,
                    e,
                )
                raise

            # select and validate a single random gap, to test that skeleton peer has meat headers
            if len(pairs) >= 2:
                # choose random gap to fill
                gap_index = randrange(0, len(pairs) - 1)
                segments = await self._fill_in_gap(peer, pairs, gap_index)
                if len(segments) == 0:
                    raise ValidationError(
                        "Unexpected - filling in gap silently returned no headers"
                    )
            else:
                segments = pairs

            previous_lead_header = segments[0][0]
            previous_tail_header = segments[-1][-1]
            self.logger.debug(
                "Got new header bones from %s: %s-%s",
                peer,
                previous_lead_header,
                previous_tail_header,
            )
            # load all headers, pausing when buffer is full
            for segment in segments:
                if len(segment) > 0:
                    await self._fetched_headers.put(segment)
                else:
                    raise ValidationError(f"Found empty header segment in {segments}")

            # prepare for the next request
            start_num = BlockNumber(previous_tail_header.block_number + self._skip_length - 1)

        await self._get_final_headers(peer, previous_tail_header)

    async def _get_final_headers(self, peer: TChainPeer, previous_tail_header: BlockHeader) -> None:
        while self.manager.is_running:
            final_headers = await self._fetch_headers_from(
                peer,
                BlockNumber(previous_tail_header.block_number + 1),
                skip=0,
            )
            if len(final_headers) == 0:
                break

            await self._chain.coro_validate_chain(
                previous_tail_header,
                final_headers,
                SEAL_CHECK_RANDOM_SAMPLE_RATE,
            )
            await self._fetched_headers.put(final_headers)
            previous_tail_header = final_headers[-1]

    async def _find_newest_matching_skeleton_header(self, peer: TChainPeer) -> BlockHeader:
        start_num = await self._launch_strategy.get_starting_block_number()

        # after returning this header, we request the next gap, and prefer that one header
        # is new to us, which may be the next header in this mini-skeleton. (hence the -1 below)
        skip = MAX_HEADERS_FETCH - 1
        skeleton_launch_headers = await self._fetch_headers_from(peer, start_num, skip=skip)

        if len(skeleton_launch_headers) == 0:
            raise ValidationError(
                f"{peer} gave 0 headers when seeking common skeleton ancestors from {start_num}"
            )

        # check the first returned value
        first = skeleton_launch_headers[0]

        first_is_present = await self._is_header_imported(first)

        if not first_is_present:
            await self._log_ancester_failure(peer, first)
            raise ValidationError(f"No common ancestor with {peer}, who started with {first}")
        elif len(skeleton_launch_headers) == 1:
            return skeleton_launch_headers[0]
        else:
            for parent, child in sliding_window(2, skeleton_launch_headers):
                is_present = await self._is_header_imported(child)
                if not is_present:
                    return parent
            else:
                # All headers are present, probably the canonical head updated recently
                # Return the newest one
                return skeleton_launch_headers[-1]

    async def _is_header_imported(self, header: BlockHeader) -> bool:
        """
        Typically used to decide if we should skip trying to import this header.
        Return True if the syncing of header appears to be complete.
        """
        if not await self._db.coro_header_exists(header.hash):
            return False
        else:
            try:
                # This is a somewhat slow mechanism, since it triggers an RLP encode on
                #   the other side. Once AsyncHeaderDB can do a coro_exists(), check
                #   the score directly, instead.
                await self._db.coro_get_score(header.hash)
            except HeaderNotFound:
                # The header rlp is saved, but a score isn't so it was just preloaded in the DB
                return False
            else:
                # The RLP and score are available, so this seems to have been properly imported
                # Skip it during sync
                return True

    async def _find_launch_headers(self, peer: TChainPeer) -> Tuple[BlockHeader, ...]:
        """
        When getting started with a peer, find exactly where the headers start differing from the
        current database of headers by requesting contiguous headers from peer. Return the first
        headers returned that are missing from the local db.

        It is possible that it will be unreasonable to find the exact starting header. For example,
        the canonical head may update while waiting for a response from the skeleton peer. In
        that case, return a *stale* header that we already know about, and there will be some
        duplicate header downloads.
        """
        newest_matching_header = await self._find_newest_matching_skeleton_header(peer)

        # This next gap will have at least one header that's new to us, because it overlaps
        # with the skeleton header that is next in the previous skeleton request, and
        # we chose the starting skeleton header so it goes past our canonical head
        start_num = BlockNumber(newest_matching_header.block_number + 1)
        launch_headers = await self._fetch_headers_from(peer, start_num, skip=0)

        if len(launch_headers) == 0:
            raise ValidationError(
                f"{peer} gave 0 headers when seeking common meat ancestors from {start_num}"
            )

        # identify headers that are not already stored locally
        completed_headers, new_headers = await skip_complete_headers(
            launch_headers, self._is_header_imported)

        if completed_headers:
            self.logger.debug(
                "During header sync launch, skipping over (%d) already stored headers %s: %s..%s",
                len(completed_headers),
                humanize_integer_sequence(h.block_number for h in completed_headers),
                completed_headers[0],
                completed_headers[-1],
            )

        if len(new_headers) == 0:
            self.logger.debug(
                "Canonical head updated while finding new head from %s, returning old %s instead",
                peer,
                launch_headers[-1],
            )
            return (launch_headers[-1], )
        else:
            try:
                launch_parent = await self._db.coro_get_block_header_by_hash(
                    new_headers[0].parent_hash)
            except HeaderNotFound as exc:
                raise ValidationError(
                    f"First header {new_headers[0]} did not have parent in DB"
                ) from exc
            # validate new headers against the parent in the database
            await self._chain.coro_validate_chain(
                launch_parent,
                new_headers,
                SEAL_CHECK_RANDOM_SAMPLE_RATE,
            )
            return new_headers

    async def _fill_in_gap(
            self,
            peer: TChainPeer,
            pairs: Tuple[Tuple[BlockHeader, ...], ...],
            gap_index: int) -> Tuple[Tuple[BlockHeader, ...], ...]:
        """
        Fill headers into the specified gap in the middle of the header pairs using supplied peer.
        Validate the returned segment of headers against the surrounding header pairs.
        :param peer: to make the request to
        :param pairs: header pairs with gaps in between
        :param gap_index: 0-indexed gap number that should be filled in
        :return: segments just like the pairs input, but with one long segment that was filled in

        For example, if four pairs were input, and the gap_index set to 1, then the
        returned value would have three segments, like:

        ::

            segment 0: (parent, child)
            --formerly gap 0--
            segment 1: (parent, child, ... all headers between ..., parent, child)
            --formerly gap 2--
            segment 2: (parent, child)
        """
        # validate gap value
        if not (0 <= gap_index < len(pairs) - 1):
            raise ValidationError(
                f"Tried to fill gap #{gap_index} in skeleton, with only {len(pairs) - 1} gaps"
            )

        # find the headers just before and after the gap
        gap_parent = pairs[gap_index][-1]
        gap_child = pairs[gap_index + 1][0]
        # request the gap's headers from the skeleton peer
        start_num = BlockNumber(gap_parent.block_number + 1)
        max_headers = gap_child.block_number - gap_parent.block_number - 1
        gap_headers = await self._fetch_headers_from(peer, start_num, max_headers, skip=0)

        if len(gap_headers) == 0:
            self.logger.warning(
                "Skeleton %s could not fill header gap with headers at %s",
                peer,
                start_num,
            )
            raise ValidationError(f"Skeleton {peer} could not return headers at {start_num}")

        # validate the filled headers
        filled_gap_children = tuple(concatv(gap_headers, pairs[gap_index + 1]))
        try:
            await self._chain.coro_validate_chain(
                gap_parent,
                filled_gap_children,
                SEAL_CHECK_RANDOM_SAMPLE_RATE,
            )
        except ValidationError:
            self.logger.warning(
                "%s returned an invalid gap for index %s, with pairs %s, filler %s",
                peer,
                gap_index,
                pairs,
                gap_headers,
            )
            raise
        else:
            return tuple(concatv(
                # include all the leading pairs, through the pair that marks the start of the gap
                pairs[:gap_index + 1],
                # include the gap that has been filled in, which includes the pair after the gap
                # must convert to tuple of tuple of headers to match the other types
                (filled_gap_children, ),
                # skip the pair following the gap, include all the following pairs
                pairs[gap_index + 2:],
            ))

    async def _fetch_headers_from(
            self,
            peer: TChainPeer,
            start_at: BlockNumber,
            max_headers: int = None,
            skip: int = None) -> Tuple[BlockHeader, ...]:

        if not peer.manager.is_running:
            self.logger.info("%s disconnected while fetching headers", peer)
            return tuple()

        if skip is not None:
            derived_skip = skip
        else:
            derived_skip = self._skip_length

        if max_headers is None:
            header_limit = peer.max_headers_fetch
        else:
            header_limit = min(max_headers, peer.max_headers_fetch)

        try:
            self.logger.debug("Requsting chain of headers from %s starting at #%d", peer, start_at)

            headers = await peer.chain_api.get_block_headers(
                start_at,
                header_limit,
                derived_skip,
                reverse=False,
            )

            self.logger.debug2('sync received new headers: %s', headers)
        except PeerConnectionLost:
            self.logger.debug("Lost connection to %s while retrieving headers", peer)
            return tuple()
        except asyncio.TimeoutError:
            self.logger.debug("Timeout waiting for headers (skip=%s) from %s", skip, peer)
            return tuple()
        except ValidationError as err:
            self.logger.warning(
                "Invalid header response sent by peer %s: %s",
                peer, err,
            )
            return tuple()

        if not headers:
            self.logger.debug2("Got no new headers from %s, exiting skeleton sync", peer)
            return tuple()
        else:
            return headers

    async def _log_ancester_failure(self, peer: TChainPeer, first_header: BlockHeader) -> None:
        self.logger.info("Unable to find common ancestor betwen our chain and %s", peer)
        block_num = first_header.block_number
        try:
            local_header = await self._db.coro_get_canonical_block_header_by_number(block_num)
        except HeaderNotFound as exc:
            self.logger.debug("Could not find any header at #%d: %s", block_num, exc)
            local_header = None

        # Canonical header at same number may or may not be in the database. Either way log an error
        self.logger.debug(
            "%s returned starting header %s, which is not in our DB. "
            "Instead at #%d, our is header %s",
            peer,
            first_header,
            block_num,
            local_header,
        )


class HeaderSyncerAPI(ABC):
    @abstractmethod
    async def new_sync_headers(
            self,
            max_batch_size: int = None) -> AsyncIterator[Tuple[BlockHeader, ...]]:
        # hack to get python & mypy to recognize that this is an async generator
        if False:
            yield

    @abstractmethod
    def get_target_header_hash(self) -> Hash32:
        ...


class ManualHeaderSyncer(HeaderSyncerAPI):
    def __init__(self) -> None:
        self._headers_to_emit: Tuple[BlockHeader, ...] = ()
        self._final_header_hash: Hash32 = None
        self._new_data = asyncio.Event()

    async def new_sync_headers(
            self,
            max_batch_size: int = None) -> AsyncIterator[Tuple[BlockHeader, ...]]:
        while True:
            next_batch = tuple(take(max_batch_size, self._headers_to_emit))
            if not next_batch:
                self._new_data.clear()
                await self._new_data.wait()
                continue
            yield next_batch
            self._headers_to_emit = tuple(drop(max_batch_size, self._headers_to_emit))

    def get_target_header_hash(self) -> Hash32:
        return self._final_header_hash

    def emit(self, headers: Iterable[BlockHeader]) -> None:
        self._headers_to_emit = self._headers_to_emit + tuple(headers)
        self._final_header_hash = self._headers_to_emit[-1].hash
        self._new_data.set()


class _PeerBehind(Exception):
    """
    Raised when a candidate for skeleton sync has lower total difficulty than the local chain.
    """
    pass


HeaderStitcher = OrderedTaskPreparation[BlockHeader, Hash32, NoPrerequisites]


class HeaderMeatSyncer(Service, PeerSubscriber, Generic[TChainPeer]):
    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()
    msg_queue_maxsize = 2000

    _filler_header_tasks: TaskQueue[Tuple[BlockHeader, int, TChainPeer]]

    def __init__(
            self,
            chain: AsyncChainAPI,
            peer_pool: BaseChainPeerPool,
            stitcher: HeaderStitcher) -> None:
        self.logger = get_logger('trinity.sync.common.headers.SkeletonSyncer')
        self._chain = chain
        self._stitcher = stitcher
        max_pending_fillers = 50
        self._filler_header_tasks = TaskQueue(
            max_pending_fillers,
            # order by block number of the parent header
            compose(attrgetter('block_number'), itemgetter(0)),
        )

        # queue up idle peers, ordered by speed that they return block bodies
        self._waiting_peers: WaitingPeers[TChainPeer] = WaitingPeers(
            (ETHBlockHeaders, LESBlockHEaders),
        )
        self._peer_pool = peer_pool
        self.sync_progress: SyncProgress = None

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # when a new peer is added to the pool, add it to the idle peer list
        self._waiting_peers.put_nowait(peer)  # type: ignore

    async def schedule_segment(
            self,
            parent_header: BlockHeader,
            gap_length: int,
            skeleton_peer: TChainPeer) -> None:
        """
        :param parent_header: the parent of the gap to fill
        :param gap_length: how long is the header gap
        :param skeleton_peer: the peer that provided the parent_header - will not use to fill gaps
        """
        try:
            await self._filler_header_tasks.add((
                (parent_header, gap_length, skeleton_peer),
            ))
        except ValidationError as exc:
            self.logger.debug(
                "Tried to re-add a duplicate list of headers to the download queue: %s",
                exc,
            )
            # Since the task is already queued up, there is no value in
            # re-adding it, so it is safe to drop the exception after logging
            # it. One example of a time it happens is when the skeleton sync
            # restarts, and happens to choose the same skeleton structure. It
            # tries to reinsert the same duplicate meat filler tasks.

    async def run(self) -> None:
        self.manager.run_daemon_task(self._display_stats)
        with self.subscribe(self._peer_pool):
            await self._match_header_dls_to_peers()

    async def _display_stats(self) -> None:
        q = self._filler_header_tasks
        while self.manager.is_running:
            await asyncio.sleep(5)
            self.logger.debug(
                "Header Skeleton Gaps: active=%d queued=%d max=%d",
                q.num_in_progress(),
                len(q),
                q._maxsize,
            )

    async def _match_header_dls_to_peers(self) -> None:
        while self.manager.is_running:
            batch_id, (
                (parent_header, gap, skeleton_peer),
            ) = await self._filler_header_tasks.get(1)

            await self._match_dl_to_peer(batch_id, parent_header, gap, skeleton_peer)

    async def _match_dl_to_peer(
            self,
            batch_id: int,
            parent_header: BlockHeader,
            gap: int,
            skeleton_peer: TChainPeer) -> None:
        def fail_task() -> None:
            self._filler_header_tasks.complete(batch_id, tuple())

        peer = await self._waiting_peers.get_fastest()
        if not self.sync_progress:
            await self._init_sync_progress(parent_header, peer)

        def complete_task() -> None:
            self._filler_header_tasks.complete(batch_id, (
                (parent_header, gap, skeleton_peer),
            ))
        peer.manager.run_task(
            self._run_fetch_segment, peer, parent_header, gap, complete_task, fail_task)

    async def _run_fetch_segment(
            self,
            peer: TChainPeer,
            parent_header: BlockHeader,
            length: int,
            complete_task_fn: Callable[[], None],
            fail_task_fn: Callable[[], None]) -> None:
        try:
            completed_headers = await self._fetch_segment(peer, parent_header, length)
        except BaseP2PError as exc:
            self.logger.info("Unexpected p2p err while downloading headers from %s: %s", peer, exc)
            self.logger.debug("Problem downloading headers from peer, dropping...", exc_info=True)
            fail_task_fn()
        except Exception as exc:
            self.logger.info("Unexpected err while downloading headers from %s: %s", peer, exc)
            self.logger.debug("Problem downloading headers from peer, dropping...", exc_info=True)
            fail_task_fn()
        else:
            if len(completed_headers) == length:
                # peer completed successfully, so have it get back in line for processing
                self._waiting_peers.put_nowait(peer)
                complete_task_fn()
            else:
                # peer didn't return enough results, wait a while before trying again
                delay = EMPTY_PEER_RESPONSE_PENALTY
                self.logger.debug(
                    "Pausing %s for %.1fs, for sending %d headers",
                    peer,
                    delay,
                    len(completed_headers),
                )
                loop = asyncio.get_event_loop()
                loop.call_later(delay, partial(self._waiting_peers.put_nowait, peer))
                fail_task_fn()

    async def _fetch_segment(
            self,
            peer: TChainPeer,
            parent_header: BlockHeader,
            length: int) -> Tuple[BlockHeader, ...]:
        if length > peer.max_headers_fetch:
            raise ValidationError(
                f"Can't request {length} headers, because peer maximum is {peer.max_headers_fetch}"
            )

        headers = await self._request_headers(
            peer,
            BlockNumber(parent_header.block_number + 1),
            length,
        )
        if not headers:
            return tuple()
        elif headers[0].parent_hash != parent_header.hash:
            # Segment doesn't match leading peer, drop this peer
            # Eventually, we'll do something smarter, in case the leading peer is the divergent one
            self.logger.warning(
                "%s returned segment starting %s & parent %s, doesn't match %s, ignoring result...",
                peer,
                headers[0],
                humanize_hash(headers[0].parent_hash),
                parent_header,
            )
            return tuple()
        elif len(headers) != length:
            self.logger.debug(
                "Ignoring %d headers from %s, because wanted %d",
                len(headers),
                peer,
                length,
            )
            return tuple()
        else:
            try:
                await self._chain.coro_validate_chain(
                    parent_header,
                    headers,
                    SEAL_CHECK_RANDOM_SAMPLE_RATE,
                )
            except ValidationError as e:
                self.logger.warning(
                    "Received invalid header segment from %s against known parent %s, "
                    ": %s",
                    peer,
                    parent_header,
                    e,
                )
                return tuple()
            else:
                # stitch headers together in order, ignoring duplicates
                self._stitcher.register_tasks(headers, ignore_duplicates=True)
                if self.sync_progress:
                    last_received_header = headers[-1]
                    self.sync_progress = self.sync_progress.update_current_block(
                        last_received_header.block_number,
                    )
                return headers

    async def _request_headers(
            self, peer: TChainPeer, start_at: BlockIdentifier, length: int
    ) -> Tuple[BlockHeader, ...]:
        self.logger.debug("Requesting %d headers from %s", length, peer)
        try:
            return await peer.chain_api.get_block_headers(start_at, length, skip=0, reverse=False)
        except asyncio.TimeoutError:
            self.logger.debug("Timed out requesting %d headers from %s", length, peer)
            return tuple()
        except CancelledError:
            self.logger.debug("Pending headers call to %r future cancelled", peer)
            return tuple()
        except PeerConnectionLost:
            self.logger.debug("Peer went away, cancelling the headers request and moving on...")
            return tuple()
        except Exception:
            self.logger.exception("Unknown error when getting headers")
            raise

    async def _init_sync_progress(self, parent_header: BlockHeader, peer: TChainPeer) -> None:
        try:
            latest_block_number = peer.head_info.head_number
        except AttributeError:
            headers = await self._request_headers(peer, peer.head_info.head_hash, 1)
            if headers:
                latest_block_number = headers[0].block_number
            else:
                return

        self.sync_progress = SyncProgress(
            parent_header.block_number,
            parent_header.block_number,
            latest_block_number,
        )


def first_nonconsecutive_header(headers: Sequence[BlockHeader]) -> int:
    """
    :return: index of first child that does not match parent header, or a number
        past the end if all are consecutive
    """
    for index, (parent, child) in enumerate(sliding_window(2, headers)):
        if child.parent_hash != parent.hash:
            return index + 1

    # return an index off the end to indicate that all headers are consecutive
    return len(headers)


class BaseHeaderChainSyncer(Service, HeaderSyncerAPI, Generic[TChainPeer]):
    """
    Generate a skeleton header, then use all peers to fill in the headers
    returned by the skeleton syncer.
    """
    _meat: HeaderMeatSyncer[TChainPeer]

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncHeaderDB,
                 peer_pool: BaseChainPeerPool,
                 launch_strategy: SyncLaunchStrategyAPI = None) -> None:
        self.logger = get_logger('trinity.sync.common.headers.SkeletonSyncer')
        self._db = db
        self._chain = chain
        self._peer_pool = peer_pool
        self._tip_monitor = self.tip_monitor_class(peer_pool)
        self._last_target_header_hash: Hash32 = None
        self._skeleton: SkeletonSyncer[TChainPeer] = None

        if launch_strategy is None:
            launch_strategy = FromGenesisLaunchStrategy(self._db, self._chain)

        self._launch_strategy = launch_strategy

        # Track if there is capacity for syncing more headers
        self._buffer_capacity = asyncio.Event()

        self._reset_buffer()

    def _reset_buffer(self) -> None:
        # stitch together headers as they come in
        self._stitcher = OrderedTaskPreparation(
            # we don't have to do any prep work on the headers, just linearize them, so empty enum
            OrderedTaskPreparation.NoPrerequisites,
            id_extractor=attrgetter('hash'),
            # make sure that a header is not returned in new_sync_headers until its parent has been
            dependency_extractor=attrgetter('parent_hash'),
            # headers will come in out of order
            accept_dangling_tasks=True,
        )
        # When downloading the headers into the gaps left by the syncer, they must be linearized
        # by the stitcher
        self._meat = HeaderMeatSyncer(
            self._chain,
            self._peer_pool,
            self._stitcher,
        )

        # Queue has reset, so always start with capacity
        self._buffer_capacity.set()

    async def new_sync_headers(
            self,
            max_batch_size: int = None) -> AsyncIterator[Tuple[BlockHeader, ...]]:

        while self.manager.is_running:
            headers = await self._stitcher.ready_tasks(max_batch_size)
            if self._stitcher.has_ready_tasks():
                # Even after clearing out a big batch, there is no available capacity, so
                # pause any coroutines that might wait for capacity
                self._buffer_capacity.clear()

            while headers:
                split_idx = first_nonconsecutive_header(headers)
                consecutive_batch, headers = headers[:split_idx], headers[split_idx:]
                if headers:
                    # Note lack of capacity if the headers are non-consecutive
                    self._buffer_capacity.clear()
                yield consecutive_batch

            if not self._stitcher.has_ready_tasks():
                # There is available capacity, let any waiting coroutines continue
                self._buffer_capacity.set()

    def get_target_header_hash(self) -> Hash32:
        if not self._is_syncing_skeleton and self._last_target_header_hash is None:
            raise ValidationError("Cannot check the target hash before the first sync has started")
        elif self._is_syncing_skeleton:
            return self._skeleton.peer.head_info.head_hash
        else:
            return self._last_target_header_hash

    @property
    @abstractmethod
    def tip_monitor_class(self) -> Type[BaseChainTipMonitor]:
        ...

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._tip_monitor)
        self._run_handle_sync_status_requests()
        self.manager.run_daemon_child_service(self._meat)
        await self._build_skeleton()

    async def _build_skeleton(self) -> None:
        """
        Find best peer to build a skeleton, and build it immediately
        """
        # iterator yields the peer with the highest TD in our pool
        async for peer in self._tip_monitor.wait_tip_info():
            try:
                await self._validate_peer_is_ahead(peer)
            except _PeerBehind:
                self.logger.debug("At or behind peer %s, skipping skeleton sync", peer)
            else:
                async with self._get_skeleton_syncer(peer) as syncer:
                    await self._full_skeleton_sync(syncer)

    @contextlib.asynccontextmanager
    async def _get_skeleton_syncer(
            self, peer: TChainPeer) -> AsyncIterator[SkeletonSyncer[TChainPeer]]:
        if self._is_syncing_skeleton:
            raise ValidationError("Cannot sync skeleton headers from two peers at the same time")

        self._skeleton = SkeletonSyncer(
            self._chain,
            self._db,
            peer,
            self._launch_strategy,
        )
        async with background_asyncio_service(self._skeleton):
            try:
                yield self._skeleton
            finally:
                self.logger.debug("Skeleton sync with %s ended", peer)
                self._last_target_header_hash = peer.head_info.head_hash
                self._skeleton = None

    @property
    def _is_syncing_skeleton(self) -> bool:
        return self._skeleton is not None

    async def _full_skeleton_sync(self, skeleton_syncer: SkeletonSyncer[TChainPeer]) -> None:
        skeleton_generator = skeleton_syncer.next_skeleton_segment()
        try:
            first_segment = await skeleton_generator.__anext__()
        except StopAsyncIteration:
            self.logger.debug(
                "Skeleton %s was cancelled before first header was returned",
                skeleton_syncer.peer,
            )
            return

        self.logger.debug(
            "Skeleton syncer asserts that parent (%s) of the first header (%s) is already present",
            humanize_hash(first_segment[0].parent_hash),
            first_segment[0],
        )
        first_parent = await self._db.coro_get_block_header_by_hash(first_segment[0].parent_hash)
        try:
            self._stitcher.set_finished_dependency(first_parent)
        except DuplicateTasks:
            # the first header of this segment was already registered: no problem, carry on
            pass

        self._stitcher.register_tasks(first_segment, ignore_duplicates=True)

        previous_segment = first_segment
        async for segment in skeleton_generator:
            self._stitcher.register_tasks(segment, ignore_duplicates=True)

            gap_length = segment[0].block_number - previous_segment[-1].block_number - 1
            if gap_length > MAX_HEADERS_FETCH:
                raise ValidationError(f"Header skeleton gap of {gap_length} > {MAX_HEADERS_FETCH}")
            elif gap_length == 0:
                # no need to fill in when there is no gap, just verify against previous header
                await self._chain.coro_validate_chain(
                    previous_segment[-1],
                    segment,
                    SEAL_CHECK_RANDOM_SAMPLE_RATE,
                )
            elif gap_length < 0:
                raise ValidationError(
                    f"Invalid headers: {gap_length} gap from {previous_segment} to {segment}"
                )
            else:
                # if the header filler is overloaded, this will pause
                await self._meat.schedule_segment(
                    previous_segment[-1],
                    gap_length,
                    skeleton_syncer.peer,
                )
            previous_segment = segment

            # Don't race ahead if the consumer is lagging
            await self._buffer_capacity.wait()

    async def _validate_peer_is_ahead(self, peer: BaseChainPeer) -> None:
        head = await self._db.coro_get_canonical_head()
        head_td = await self._db.coro_get_score(head.hash)
        if peer.head_info.head_td <= head_td:
            self.logger.debug(
                "Head TD (%d) announced by %s not higher than ours (%d), not syncing",
                peer.head_info.head_td, peer, head_td)
            raise _PeerBehind(f"{peer} is behind us, not a valid target for sync")
        else:
            self.logger.debug(
                "%s announced Head TD %d, which is higher than ours (%d), starting sync",
                peer, peer.head_info.head_td, head_td)

    def _run_handle_sync_status_requests(self) -> None:
        if self._peer_pool.has_event_bus:
            event_bus = self._peer_pool.get_event_bus()
            self.manager.run_daemon_task(self._handle_sync_status_requests, event_bus)
        else:
            self.logger.warning(
                "Cannot start task for handling eth_syncing requests "
                "as peer pool doesn't have an event_bus"
            )

    def _get_sync_status(self) -> Tuple[bool, Optional[SyncProgress]]:
        if not self._is_syncing_skeleton or not self._meat.sync_progress:
            return False, None
        return True, self._meat.sync_progress

    async def _handle_sync_status_requests(self, event_bus: EndpointAPI) -> None:
        async for req in event_bus.stream(SyncingRequest):
            await event_bus.broadcast(
                SyncingResponse(*self._get_sync_status()),
                req.broadcast_config()
            )
