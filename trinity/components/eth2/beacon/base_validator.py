# Add a base class to share the same logic
# It's a temporary stage

from typing import (
    Callable,
    Tuple,
)

from cancel_token import (
    CancelToken,
)
from eth_typing import (
    BlockNumber,
    Hash32,
)
from lahja import EndpointAPI

from eth2._utils.numeric import integer_squareroot
from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.state_machines.base import (
    BaseBeaconStateMachine,
)
from eth2.beacon.types.attestations import (
    Attestation,
)
from eth2.beacon.types.deposits import (
    Deposit,
)
from eth2.beacon.types.eth1_data import (
    Eth1Data,
)
from eth2.beacon.types.states import (
    BeaconState,
)
from eth2.beacon.typing import (
    CommitteeIndex,
    Slot,
    Timestamp,
)
from p2p.service import (
    BaseService,
)
from trinity.components.eth2.eth1_monitor.events import (
    GetDistanceRequest,
    GetDistanceResponse,
    GetDepositRequest,
    GetDepositResponse,
    GetEth1DataRequest,
    GetEth1DataResponse,
)
from trinity.protocol.bcc_libp2p.node import Node


GetReadyAttestationsFn = Callable[[Slot, bool], Tuple[Attestation, ...]]
GetAggregatableAttestationsFn = Callable[[Slot, CommitteeIndex], Tuple[Attestation, ...]]
ImportAttestationFn = Callable[[Attestation, bool], None]


# FIXME: Read this from validator config
ETH1_FOLLOW_DISTANCE = 16


class BaseValidator(BaseService):
    genesis_time: Timestamp
    chain: BaseBeaconChain
    p2p_node: Node
    event_bus: EndpointAPI

    starting_eth1_block_hash: Hash32

    def __init__(
            self,
            chain: BaseBeaconChain,
            p2p_node: Node,
            event_bus: EndpointAPI,
            get_ready_attestations_fn: GetReadyAttestationsFn,
            get_aggregatable_attestations_fn: GetAggregatableAttestationsFn,
            import_attestation_fn: ImportAttestationFn,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.genesis_time = chain.get_head_state().genesis_time
        self.chain = chain
        self.p2p_node = p2p_node
        self.event_bus = event_bus
        self.get_ready_attestations: GetReadyAttestationsFn = get_ready_attestations_fn
        self.get_aggregatable_attestations: GetAggregatableAttestationsFn = get_aggregatable_attestations_fn  # noqa: E501
        self.import_attestation: ImportAttestationFn = import_attestation_fn

        # `state.eth1_data` can be updated in the middle of voting period and thus
        # the starting `eth1_data.block_hash` must be stored separately.
        self.starting_eth1_block_hash = chain.get_head_state().eth1_data.block_hash

    #
    # Proposing block
    #
    async def _get_deposit_data(self,
                                state: BeaconState,
                                state_machine: BaseBeaconStateMachine,
                                eth1_vote: Eth1Data) -> Tuple[Deposit, ...]:
        eth1_data = state.eth1_data
        # Check if the eth1 vote pass the threshold
        slots_per_eth1_voting_period = state_machine.config.SLOTS_PER_ETH1_VOTING_PERIOD
        eth1_data_votes = state.eth1_data_votes.append(eth1_vote)
        if eth1_data_votes.count(eth1_vote) * 2 > slots_per_eth1_voting_period:
            eth1_data = eth1_vote

        deposits: Tuple[Deposit, ...] = ()
        expected_deposit_count = min(
            state_machine.config.MAX_DEPOSITS,
            eth1_data.deposit_count - state.eth1_deposit_index,
        )
        for i in range(expected_deposit_count):
            request_params = {
                "deposit_count": eth1_data.deposit_count,
                "deposit_index": state.eth1_deposit_index + i,
            }
            resp: GetDepositResponse = await self.event_bus.request(
                GetDepositRequest(**request_params)
            )
            if resp.error is None:
                deposits = deposits + (resp.to_data(),)
            else:
                self.logger.error(
                    "Fail to get deposit data with `deposit_count`=%s,"
                    "`deposit_index`=%s",
                    request_params["deposit_count"],
                    request_params["deposit_index"],
                )
        return tuple(deposits)

    async def _request_eth1_data(self,
                                 eth1_voting_period_start_timestamp: Timestamp,
                                 start: int,
                                 end: int) -> Tuple[Eth1Data, ...]:
        eth1_data: Tuple[Eth1Data, ...] = ()
        for distance in range(start, end):
            resp: GetEth1DataResponse = await self.event_bus.request(
                GetEth1DataRequest(
                    distance=BlockNumber(distance),
                    eth1_voting_period_start_timestamp=Timestamp(
                        eth1_voting_period_start_timestamp
                    ),
                )
            )
            if resp.error is None:
                eth1_data = eth1_data + (resp.to_data(),)
            else:
                self.logger.error(
                    "Fail to get eth1 data with `distance`=%s,"
                    "`eth1_voting_period_start_timestamp`=%s",
                    distance,
                    eth1_voting_period_start_timestamp,
                )
        return eth1_data

    async def _get_eth1_vote(self,
                             slot: Slot,
                             state: BeaconState,
                             state_machine: BaseBeaconStateMachine) -> Eth1Data:
        slots_per_eth1_voting_period = state_machine.config.SLOTS_PER_ETH1_VOTING_PERIOD
        seconds_per_slot = state_machine.config.SECONDS_PER_SLOT
        eth1_follow_distance = ETH1_FOLLOW_DISTANCE
        eth1_voting_period_start_timestamp = (
            self.genesis_time +
            (slot - slot % slots_per_eth1_voting_period) * seconds_per_slot
        )

        new_eth1_data = await self._request_eth1_data(
            Timestamp(eth1_voting_period_start_timestamp),
            eth1_follow_distance,
            2 * eth1_follow_distance,
        )
        # Default is the `Eth1Data` at `ETH1_FOLLOW_DISTANCE`
        default_eth1_data = new_eth1_data[0]

        # Compute `previous_eth1_distance` which is the distance between current block and
        # `state.eth1_data`.
        resp: GetDistanceResponse = await self.event_bus.request(
            GetDistanceRequest(
                block_hash=self.starting_eth1_block_hash,
                eth1_voting_period_start_timestamp=Timestamp(eth1_voting_period_start_timestamp),
            )
        )
        if resp.error is not None:
            return default_eth1_data

        previous_eth1_distance = resp.distance

        # Request all eth1 data within `previous_eth1_distance`
        all_eth1_data: Tuple[Eth1Data, ...] = ()
        # Copy overlapped eth1 data from `new_eth1_data`
        if 2 * eth1_follow_distance >= previous_eth1_distance:
            all_eth1_data = new_eth1_data[
                : (previous_eth1_distance - eth1_follow_distance)
            ]
        else:
            all_eth1_data = new_eth1_data[:]
            all_eth1_data += await self._request_eth1_data(
                Timestamp(eth1_voting_period_start_timestamp),
                2 * eth1_follow_distance,
                previous_eth1_distance,
            )

        # Filter out invalid votes
        voting_period_int_sqroot = integer_squareroot(slots_per_eth1_voting_period)
        period_tail = slot % slots_per_eth1_voting_period >= voting_period_int_sqroot
        if period_tail:
            votes_to_consider = all_eth1_data
        else:
            votes_to_consider = new_eth1_data

        valid_votes: Tuple[Eth1Data, ...] = tuple(
            vote for vote in state.eth1_data_votes if vote in votes_to_consider
        )

        # Vote with most count wins. Otherwise vote for defaute eth1 data.
        win_vote = max(
            valid_votes,
            key=lambda v: (
                valid_votes.count(v),
                -all_eth1_data.index(v),
            ),  # Tiebreak by smallest distance
            default=default_eth1_data,
        )
        return win_vote
