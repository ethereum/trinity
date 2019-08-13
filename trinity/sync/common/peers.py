from asyncio import (
    PriorityQueue,
)
from typing import (
    Generic,
    Type,
    TypeVar,
)

from eth_utils import (
    ValidationError,
)

from p2p.protocol import Command

from trinity.protocol.common.peer import BaseChainPeer
from trinity._utils.datastructures import (
    SortableTask,
)

TChainPeer = TypeVar('TChainPeer', bound=BaseChainPeer)


class WaitingPeers(Generic[TChainPeer]):
    """
    Peers waiting to perform some action. When getting a peer from this queue,
    prefer the peer with the best throughput for the given command.
    """
    _waiting_peers: 'PriorityQueue[SortableTask[TChainPeer]]'

    def __init__(self, response_command_type: Type[Command], latency_focused: bool=False) -> None:
        """
        :param latency_focused: should we prefer peers that respond quickly over total bandwidth?
        """
        self._waiting_peers = PriorityQueue()
        self._response_command_type = response_command_type
        self._peer_wrapper = SortableTask.orderable_by_func(self._get_peer_rank)
        self._latency_focused = latency_focused

    def _get_peer_rank(self, peer: TChainPeer) -> float:
        relevant_trackers = [
            exchange.tracker
            for exchange in peer.requests
            if issubclass(exchange.response_cmd_type, self._response_command_type)
        ]

        if self._latency_focused:
            # Low latency should pop out of queue first, so rank as positive
            scores = [
                tracker.latency for tracker in relevant_trackers
            ]
        else:
            # High speed should pop out of queue first, so rank as negative
            scores = [
                -1 * tracker.items_per_second_ema.value for tracker in relevant_trackers
            ]

        if len(scores) == 0:
            raise ValidationError(
                f"Could not find any exchanges on {peer} "
                f"with response {self._response_command_type!r}"
            )

        return sum(scores) / len(scores)

    def put_nowait(self, peer: TChainPeer) -> None:
        self._waiting_peers.put_nowait(self._peer_wrapper(peer))

    async def get_fastest(self) -> TChainPeer:
        wrapped_peer = await self._waiting_peers.get()
        peer = wrapped_peer.original

        # make sure the peer has not gone offline while waiting in the queue
        while not peer.is_operational:
            # if so, look for the next best peer
            wrapped_peer = await self._waiting_peers.get()
            peer = wrapped_peer.original

        return peer
