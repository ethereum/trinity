from abc import abstractmethod
import asyncio
import functools
from typing import Any, FrozenSet, Iterable, Optional, Type

from async_service import Service, ServiceAPI

from p2p.abc import CommandAPI
from p2p.exchange import PerformanceAPI
from p2p.peer import BasePeer, PeerSubscriber
from trinity.protocol.eth.commands import NodeDataV65
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.sync.beam.constants import (
    NON_IDEAL_RESPONSE_PENALTY,
    WARN_AFTER_QUEEN_STARVED,
)
from trinity.sync.common.peers import WaitingPeers
from trinity._utils.logging import get_logger
from trinity._utils.timer import Timer


def queen_peer_performance_sort(tracker: PerformanceAPI) -> float:
    return -1 * tracker.items_per_second_ema.value


def _peer_sort_key(peer: ETHPeer) -> float:
    # FIXME: This should not be hard-coded to get_node_data as we could, in theory, be used to
    # request other types of data.
    return queen_peer_performance_sort(peer.eth_api.get_node_data.tracker)


class QueenTrackerAPI(ServiceAPI):
    """
    Keep track of the single best peer
    """
    @abstractmethod
    async def get_queen_peer(self) -> ETHPeer:
        ...

    @abstractmethod
    def penalize_queen(self, peer: ETHPeer, delay: float = NON_IDEAL_RESPONSE_PENALTY) -> None:
        ...

    @abstractmethod
    def insert_peer(self, peer: ETHPeer, delay: float = 0) -> None:
        ...

    @abstractmethod
    async def pop_fastest_peasant(self) -> ETHPeer:
        ...

    def pop_knights(self) -> Iterable[ETHPeer]:
        ...

    @abstractmethod
    def set_desired_knight_count(self, desired_knights: int) -> None:
        ...


class QueeningQueue(Service, PeerSubscriber, QueenTrackerAPI):
    # The best peer gets skipped for backfill, because we prefer to use it for
    #   urgent beam sync nodes. _queen_peer should only ever be set to a new peer
    #   in _insert_peer(). It may be set to None anywhere.
    _queen_peer: ETHPeer = None
    _queen_updated: asyncio.Event
    _knights: WaitingPeers[ETHPeer]
    _peasants: WaitingPeers[ETHPeer]

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _report_interval = 30

    def __init__(self, peer_pool: ETHPeerPool) -> None:
        self.logger = get_logger('trinity.sync.beam.queen.QueeningQueue')
        self._peer_pool = peer_pool
        self._knights = WaitingPeers(NodeDataV65)
        self._peasants = WaitingPeers(NodeDataV65)
        self._queen_updated = asyncio.Event()
        self._desired_knights = 0
        self._num_peers = 0

    async def run(self) -> None:
        with self.subscribe(self._peer_pool):
            self.manager.run_daemon_task(self._report_statistics)
            await self.manager.wait_finished()

    async def _report_statistics(self) -> None:
        while self.manager.is_running:
            await asyncio.sleep(self._report_interval)
            self.logger.debug(
                "queen-stats: free_knights=%d/%d free_peasants=%d/%d queen=%s",
                len(self._knights),
                self._desired_knights,
                len(self._peasants),
                self._num_peers - self._desired_knights - 1,
                self._queen_peer,
            )

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)

        self._insert_peer(peer)  # type: ignore
        self._num_peers += 1

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if self._queen_peer == peer:
            self._queen_peer = None
        # If it's not the queen, we will catch the peer as cancelled when we try to draw it
        #   as a peasant. (We can't drop an element from the middle of a Queue)
        self._num_peers -= 1

    async def get_queen_peer(self) -> ETHPeer:
        """
        Wait until a queen peer is designated, then return it.
        """
        t = Timer()
        while self._queen_peer is None:
            try:
                promote_knight = self._knights.pop_nowait()
            except asyncio.QueueEmpty:
                # There are no knights available. Wait for a new queen to appear.
                await self._queen_updated.wait()
                self._queen_updated.clear()
            else:
                # There is a knight who can be promoted to queen immediately.
                self._insert_peer(promote_knight)

        queen_starve_time = t.elapsed
        if queen_starve_time > WARN_AFTER_QUEEN_STARVED:
            self.logger.debug(
                "Blocked for %.2fs waiting for queen=%s",
                queen_starve_time,
                self._queen_peer,
            )

        return self._queen_peer

    @property
    def queen(self) -> Optional[ETHPeer]:
        """
        Might be None. If None is unacceptable, use :meth:`get_queen_peer`
        """
        return self._queen_peer

    def set_desired_knight_count(self, desired_knights: int) -> None:
        self._desired_knights = desired_knights

        # Promote knights if there are not enough
        while len(self._knights) < self._desired_knights:
            try:
                promoted_knight = self._peasants.pop_nowait()
            except asyncio.QueueEmpty:
                # no peasants available to promote
                break
            else:
                self._knights.put_nowait(promoted_knight)

    def pop_knights(self) -> Iterable[ETHPeer]:
        for _ in range(self._desired_knights):
            try:
                yield self._knights.pop_nowait()
            except asyncio.QueueEmpty:
                try:
                    yield self._peasants.pop_nowait()
                except asyncio.QueueEmpty:
                    break

        # Push all remaining knights down to peasants
        while len(self._knights):
            try:
                demoted_knight = self._knights.pop_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._peasants.put_nowait(demoted_knight)

    async def pop_fastest_peasant(self) -> ETHPeer:
        """
        Get the fastest peer that is not the queen.
        """
        # NOTE: We don't use the common `while self.is_running` idiom here because this method
        # runs in a task belonging to the service that runs this service as a child, so that task
        # has to monitor the child service (us) and stop calling this method when we're no longer
        # running.
        while True:
            peer = await self._peasants.get_fastest()
            if not peer.is_alive:
                # drop any peers that aren't alive anymore
                self.logger.info("Dropping %s from beam peers, as no longer active", peer)
                if peer == self._queen_peer:
                    self._queen_peer = None
                continue

            if self._should_be_queen(peer):
                self.logger.debug("About to draw peasant %s, but realized it should be queen", peer)
                self._insert_peer(peer)
                continue

            # FIXME: This should not be hard-coded to get_node_data as we could, in theory, be
            # used to request other types of data.
            peer_is_requesting = peer.eth_api.get_node_data.is_requesting

            if peer_is_requesting:
                # skip the peer if there's an active request
                self.logger.debug("QueenQueuer is skipping active peer %s", peer)
                self.insert_peer(peer, delay=NON_IDEAL_RESPONSE_PENALTY)
                continue

            return peer

    def insert_peer(self, peer: ETHPeer, delay: float = 0) -> None:
        if not peer.is_alive:
            # Peer exited, dropping it...
            return
        elif self._should_be_queen(peer):
            self.logger.debug("Fast-tracking peasant to promote to queen: %s", peer)
            self._insert_peer(peer)
        elif delay > 0:
            loop = asyncio.get_event_loop()
            loop.call_later(delay, functools.partial(self._insert_peer, peer))
        else:
            self._insert_peer(peer)

    def penalize_queen(self, peer: ETHPeer, delay: float = NON_IDEAL_RESPONSE_PENALTY) -> None:
        if peer == self._queen_peer:
            self._queen_peer = None

            self.logger.debug(
                "Penalizing %s for %.2fs, for minor infraction",
                peer,
                delay,
            )
            loop = asyncio.get_event_loop()
            loop.call_later(delay, functools.partial(self._insert_peer, peer))

    def _should_be_queen(self, peer: ETHPeer) -> bool:
        if not peer.is_alive:
            raise ValueError(f"{peer} is no longer alive")

        if self._queen_peer is None:
            return True
        elif not self._queen_peer.is_alive:
            return True
        elif peer == self._queen_peer:
            return True
        else:
            new_peer_quality = _peer_sort_key(peer)
            current_queen_quality = _peer_sort_key(self._queen_peer)
            # Quality is designed so that an ascending sort puts the best quality at the front
            #   of a sequence. So, a lower value means a better quality.
            return new_peer_quality < current_queen_quality

    def _insert_peer(self, peer: ETHPeer) -> None:
        """
        Add peer as ready to receive requests. Check if it should be queen, and promote if
        appropriate. Otherwise, insert to be drawn as a peasant.

        If the given peer is no longer running, do nothing. This is needed because we're sometimes
        called via loop.call_later().
        """
        if not peer.is_alive:
            self.logger.debug("Peer %s is no longer alive, not adding to queue", peer)
            return

        if self._should_be_queen(peer):
            old_queen, self._queen_peer = self._queen_peer, peer
            if peer != old_queen:
                # We only need to log the change if there was an actual change in queen
                self.logger.debug("Switching queen peer from %s to %s", old_queen, peer)
                self._queen_updated.set()

                if old_queen is not None:
                    self._insert_peer(old_queen)
        else:
            self._peasants.put_nowait(peer)

            # If the number of knights is too low, add one.
            # Insert the peasant before promoting a knight, in case a former peasant is
            #   now faster than the peer being inserted now.
            if len(self._knights) < self._desired_knights:
                try:
                    promoted_knight = self._peasants.pop_nowait()
                except asyncio.QueueEmpty:
                    # no peasants available to promote, exit cleanly
                    return
                else:
                    self._knights.put_nowait(promoted_knight)
