from abc import ABC, abstractmethod
import asyncio
import functools
from typing import Any, FrozenSet, Optional, Type

from async_service import Service

from eth_utils import ValidationError

from p2p.abc import CommandAPI
from p2p.exceptions import (
    PeerConnectionLost,
    UnknownAPI,
)
from p2p.exchange import PerformanceAPI
from p2p.peer import BasePeer, PeerSubscriber
from trinity.protocol.eth.commands import NodeDataV65
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.sync.beam.constants import NON_IDEAL_RESPONSE_PENALTY
from trinity.sync.common.peers import WaitingPeers
from trinity._utils.logging import get_logger


def queen_peer_performance_sort(tracker: PerformanceAPI) -> float:
    return -1 * tracker.items_per_second_ema.value


def _peer_sort_key(peer: ETHPeer) -> float:
    return queen_peer_performance_sort(peer.eth_api.get_node_data.tracker)


class QueenTrackerAPI(ABC):
    """
    Keep track of the single best peer
    """
    @abstractmethod
    async def get_queen_peer(self) -> ETHPeer:
        ...

    @abstractmethod
    def penalize_queen(self, peer: ETHPeer) -> None:
        ...


class QueeningQueue(Service, PeerSubscriber, QueenTrackerAPI):
    # The best peer gets skipped for backfill, because we prefer to use it for
    #   urgent beam sync nodes
    _queen_peer: ETHPeer = None
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

    async def run(self) -> None:
        with self.subscribe(self._peer_pool):
            await self.manager.wait_finished()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # when a new peer is added to the pool, add it to the idle peer list
        self._waiting_peers.put_nowait(peer)  # type: ignore

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if self._queen_peer == peer:
            self._queen_peer = None

    async def get_queen_peer(self) -> ETHPeer:
        """
        Wait until a queen peer is designated, then return it.
        """
        while self._queen_peer is None:
            peer = await self._waiting_peers.get_fastest()
            self._update_queen(peer)

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
            if not peer.manager.is_running:
                # drop any peers that aren't alive anymore
                self.logger.info("Dropping %s from beam peers, as no longer active", peer)
                if peer == self._queen_peer:
                    self._queen_peer = None
                continue

            old_queen = self._queen_peer
            self._update_queen(peer)
            if peer == self._queen_peer:
                self.logger.debug("Switching queen peer from %s to %s", old_queen, peer)
                continue

            try:
                peer_is_requesting_nodes = peer.eth_api.get_node_data.is_requesting
            except PeerConnectionLost:
                self.logger.debug("QueenQueuer is skipping disconnecting peer %s", peer)
                # Don't bother re-adding to _waiting_peers, since the peer is disconnected
            else:
                if peer_is_requesting_nodes:
                    # skip the peer if there's an active request
                    self.logger.debug("QueenQueuer is skipping active peer %s", peer)
                    loop = asyncio.get_event_loop()
                    loop.call_later(10, functools.partial(self._waiting_peers.put_nowait, peer))
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
            loop.call_later(delay, functools.partial(self._waiting_peers.put_nowait, peer))
        else:
            self._waiting_peers.put_nowait(peer)

    def penalize_queen(self, peer: ETHPeer) -> None:
        if peer == self._queen_peer:
            self._queen_peer = None

            delay = NON_IDEAL_RESPONSE_PENALTY
            self.logger.debug(
                "Penalizing %s for %.2fs, for minor infraction",
                peer,
                delay,
            )
            loop = asyncio.get_event_loop()
            loop.call_later(delay, functools.partial(self._waiting_peers.put_nowait, peer))

    def _update_queen(self, peer: ETHPeer) -> None:
        '''
        @return peer that is no longer queen
        '''
        if self._queen_peer is None:
            self._queen_peer = peer
            return
        elif peer == self._queen_peer:
            # nothing to do, peer is already the queen
            return
        else:
            try:
                new_peer_quality = _peer_sort_key(peer)
            except (UnknownAPI, PeerConnectionLost) as exc:
                self.logger.debug("Ignoring %s, because we can't get speed stats: %r", peer, exc)
                return

            try:
                old_queen_quality = _peer_sort_key(self._queen_peer)
                force_drop_queen = False
            except (UnknownAPI, PeerConnectionLost) as exc:
                self.logger.debug(
                    "Dropping queen %s, because we can't get speed stats: %r",
                    self._queen_peer,
                    exc,
                )
                force_drop_queen = True

            if force_drop_queen or new_peer_quality < old_queen_quality:
                old_queen, self._queen_peer = self._queen_peer, peer
                self._waiting_peers.put_nowait(old_queen)
                return
            else:
                # nothing to do, peer is slower than the queen
                return

        raise ValidationError(
            "Unreachable: every queen peer check should have finished and returned. "
            f"Was checking {peer} against queen {self._queen_peer}."
        )
