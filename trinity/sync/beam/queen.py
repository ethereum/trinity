from abc import ABC, abstractmethod
import asyncio
import functools
from typing import Any, FrozenSet, Optional, Type

from async_service import Service

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


class QueenTrackerAPI(ABC):
    """
    Keep track of the single best peer
    """
    @abstractmethod
    async def get_queen_peer(self) -> ETHPeer:
        ...

    @abstractmethod
    def penalize_queen(self, peer: ETHPeer, delay: float = NON_IDEAL_RESPONSE_PENALTY) -> None:
        ...


class QueeningQueue(Service, PeerSubscriber, QueenTrackerAPI):
    # The best peer gets skipped for backfill, because we prefer to use it for
    #   urgent beam sync nodes. _queen_peer should only ever be set to a new peer
    #   in _insert_peer(). It may be set to None anywhere.
    _queen_peer: ETHPeer = None
    _queen_updated: asyncio.Event
    _waiting_peers: WaitingPeers[ETHPeer]

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    def __init__(self, peer_pool: ETHPeerPool) -> None:
        self.logger = get_logger('trinity.sync.beam.queen.QueeningQueue')
        self._peer_pool = peer_pool
        self._waiting_peers = WaitingPeers(NodeDataV65)
        self._queen_updated = asyncio.Event()

    async def run(self) -> None:
        with self.subscribe(self._peer_pool):
            await self.manager.wait_finished()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)

        self._insert_peer(peer)  # type: ignore

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if self._queen_peer == peer:
            self._queen_peer = None
        # If it's not the queen, we will catch the peer as cancelled when we try to draw it
        #   as a peasant. (We can't drop an element from the middle of a Queue)

    async def get_queen_peer(self) -> ETHPeer:
        """
        Wait until a queen peer is designated, then return it.
        """
        t = Timer()
        while self._queen_peer is None:
            await self._queen_updated.wait()
            self._queen_updated.clear()

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

    async def pop_fastest_peasant(self) -> ETHPeer:
        """
        Get the fastest peer that is not the queen.
        """
        while self.manager.is_running:
            peer = await self._waiting_peers.get_fastest()
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
                loop = asyncio.get_event_loop()
                loop.call_later(10, functools.partial(self._insert_peer, peer))
                continue

            return peer
        # This should never happen as we run as a daemon and if we return before our caller it'd
        # raise a DaemonTaskExit, but just in case we raise a CancelledError() to ensure our
        # caller realizes we've stopped.
        self.logger.error("Service ended before a queen peer could be elected")
        raise asyncio.CancelledError()

    def readd_peasant(self, peer: ETHPeer, delay: float = 0) -> None:
        if delay > 0:
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
                    self._waiting_peers.put_nowait(old_queen)
        else:
            self._waiting_peers.put_nowait(peer)
