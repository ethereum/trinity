import logging
from typing import (
    DefaultDict,
    Dict,
    Generator,
    NamedTuple,
    Optional,
)
import collections
import operator

from trio.abc import (
    ReceiveChannel,
)

from eth_utils import (
    encode_hex,
    to_tuple,
)
from eth_utils.toolz import (
    merge,
)

from async_service import (
    Service,
)

from p2p.discv5.abc import (
    NodeDBAPI,
)
from p2p.discv5.channel_services import (
    Endpoint,
)
from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.enr import (
    UnsignedENR,
)
from p2p.discv5.identity_schemes import (
    IdentitySchemeRegistry,
)
from p2p.discv5.typing import (
    NodeID,
)
from p2p.kademlia import Node


class EndpointVote(NamedTuple):
    endpoint: Endpoint
    node_id: NodeID
    timestamp: float


class EndpointVoteBallotBox:
    def __init__(self, quorum: int, majority_fraction: float, expiry_time: float) -> None:
        self.quorum = quorum
        self.majority_fraction = majority_fraction
        self.expiry_time = expiry_time

        self.votes_by_sender: Dict[NodeID, EndpointVote] = {}
        self.num_votes_by_endpoint: DefaultDict[Endpoint, int] = collections.defaultdict(int)
        self.num_total_votes = 0

    def add_vote(self, vote: EndpointVote) -> None:
        try:
            self.remove_vote_by_node_id(vote.node_id)
        except KeyError:
            pass
        self.votes_by_sender[vote.node_id] = vote
        self.num_votes_by_endpoint[vote.endpoint] += 1
        self.num_total_votes += 1

    def remove_vote_by_node_id(self, node_id: NodeID) -> None:
        removed_vote = self.votes_by_sender.pop(node_id)
        endpoint = removed_vote.endpoint
        self.num_votes_by_endpoint[endpoint] -= 1
        self.num_total_votes -= 1
        # prevent num_votes_by_endpoint from growing over time
        if self.num_votes_by_endpoint[endpoint] <= 0:
            self.num_votes_by_endpoint.pop(endpoint)

    @to_tuple
    def get_expired_votes(self, current_time: float) -> Generator[EndpointVote, None, None]:
        for vote in self.votes_by_sender.values():
            if vote.timestamp <= current_time - self.expiry_time:
                yield vote

    def remove_expired_votes(self, current_time: float) -> None:
        votes_to_remove = self.get_expired_votes(current_time)
        for vote in votes_to_remove:
            self.remove_vote_by_node_id(vote.node_id)

    @property
    def result(self) -> Optional[Endpoint]:
        if self.num_total_votes < self.quorum:
            return None  # quorum not reached

        vote, num_votes = max(self.num_votes_by_endpoint.items(), key=operator.itemgetter(1))
        if num_votes / self.num_total_votes < self.majority_fraction:
            return None  # majority not big enough

        return vote


class EndpointTracker(Service):

    logger = logging.getLogger("p2p.discv5.endpoint_tracker.EndpointTracker")

    def __init__(self,
                 local_private_key: bytes,
                 local_node_id: NodeID,
                 node_db: NodeDBAPI,
                 identity_scheme_registry: IdentitySchemeRegistry,
                 vote_receive_channel: ReceiveChannel[EndpointVote],
                 ) -> None:
        self.local_private_key = local_private_key
        self.local_node_id = local_node_id
        self.node_db = node_db
        self.identity_scheme_registry = identity_scheme_registry

        self.vote_receive_channel = vote_receive_channel

    async def run(self) -> None:
        async with self.vote_receive_channel:
            async for vote in self.vote_receive_channel:
                await self.handle_vote(vote)

    async def handle_vote(self, vote: EndpointVote) -> None:
        self.logger.debug(
            "Received vote for %s from %s",
            vote.endpoint,
            encode_hex(vote.node_id),
        )

        current_node = await self.node_db.get(self.local_node_id)
        current_enr = current_node.enr

        # TODO: majority voting, discard old votes
        are_endpoint_keys_present = (
            IP_V4_ADDRESS_ENR_KEY in current_enr and
            UDP_PORT_ENR_KEY in current_enr
        )
        enr_needs_update = not are_endpoint_keys_present or (
            vote.endpoint.ip_address != current_enr[IP_V4_ADDRESS_ENR_KEY] or
            vote.endpoint.port != current_enr[UDP_PORT_ENR_KEY]
        )
        if enr_needs_update:
            kv_pairs = merge(
                current_enr,
                {
                    IP_V4_ADDRESS_ENR_KEY: vote.endpoint.ip_address,
                    UDP_PORT_ENR_KEY: vote.endpoint.port,
                }
            )
            new_unsigned_enr = UnsignedENR(
                kv_pairs=kv_pairs,
                sequence_number=current_enr.sequence_number + 1,
                identity_scheme_registry=self.identity_scheme_registry,
            )
            signed_enr = new_unsigned_enr.to_signed_enr(self.local_private_key)
            self.logger.info(
                f"Updating local endpoint to %s (new ENR sequence number: %d)",
                vote.endpoint,
                signed_enr.sequence_number,
            )
            await self.node_db.update(Node(signed_enr))
