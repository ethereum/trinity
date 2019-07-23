from asyncio import (
    PriorityQueue,
)
from typing import (
    Generic,
    Type,
    TypeVar,
    Union,
)

from eth_utils import (
    ValidationError,
)

from p2p.protocol import Command

from trinity.protocol.eth.peer import ETHProxyPeer
from trinity.protocol.les.peer import LESProxyPeer
from trinity._utils.datastructures import (
    SortableTask,
)

TChainPeer = TypeVar('TChainPeer', bound=Union[ETHProxyPeer, LESProxyPeer])


class WaitingPeers(Generic[TChainPeer]):
    """
    Peers waiting to perform some action. When getting a peer from this queue,
    prefer the peer with the best throughput for the given command.
    """
    _waiting_peers: 'PriorityQueue[SortableTask[TChainPeer]]'

    def __init__(self, response_command_type: Type[Command]) -> None:
        self._waiting_peers = PriorityQueue()
        self._response_command_type = response_command_type
        self._peer_wrapper = SortableTask.orderable_by_func(self._get_peer_rank)

    def _get_peer_rank(self, peer: TChainPeer) -> float:
        relevant_throughputs = [
            items_per_second
            for response_cmd_type, items_per_second in peer.perf_metrics.items()
            if issubclass(response_cmd_type, self._response_command_type)
        ]

        if len(relevant_throughputs) == 0:
            raise ValidationError(
                f"Could not find any exchanges on {peer} "
                f"with response {self._response_command_type!r}"
            )

        avg_throughput = sum(relevant_throughputs) / len(relevant_throughputs)

        # high throughput peers should pop out of the queue first, so ranked as negative
        return -1 * avg_throughput

    async def put(self, peer: TChainPeer) -> None:
        await peer.get_perf_metrics()
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
