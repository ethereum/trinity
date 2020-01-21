from itertools import (
    groupby,
)
from typing import (
    Callable,
    Dict,
    Iterable,
    Set,
    Tuple,
)

from lahja import EndpointAPI

from cancel_token import (
    CancelToken,
)
from eth_typing import (
    BlockNumber,
    BLSSignature,
    Hash32,
)
from eth_utils import (
    humanize_hash,
    to_tuple,
    ValidationError,
)

from eth2._utils.numeric import integer_squareroot
from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.helpers import (
    compute_epoch_at_slot,
)
from eth2.beacon.state_machines.base import (
    BaseBeaconStateMachine,
)
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenitySignedBeaconBlock,
)
from eth2.beacon.tools.builder.aggregator import (
    get_aggregate_from_valid_committee_attestations,
    get_slot_signature,
    is_aggregator,
)
from eth2.beacon.tools.builder.committee_assignment import (
    CommitteeAssignment,
)
from eth2.beacon.tools.builder.proposer import (
    create_block_on_state,
    get_beacon_proposer_index,
)
from eth2.beacon.tools.builder.committee_assignment import (
    get_committee_assignment,
)
from eth2.beacon.tools.builder.validator import (
    create_signed_attestations_at_slot,
)
from eth2.beacon.types.aggregate_and_proof import (
    AggregateAndProof,
)
from eth2.beacon.types.attestations import (
    Attestation,
)
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
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
    CommitteeValidatorIndex,
    Epoch,
    Slot,
    SubnetId,
    Timestamp,
    ValidatorIndex,
)
from eth2.configs import CommitteeConfig
from p2p.service import (
    BaseService,
)
from trinity._utils.shellart import (
    bold_green,
)
from trinity.components.eth2.eth1_monitor.events import (
    GetDistanceRequest,
    GetDistanceResponse,
    GetDepositRequest,
    GetDepositResponse,
    GetEth1DataRequest,
    GetEth1DataResponse,
)
from trinity.components.eth2.metrics.events import (
    Libp2pPeersRequest,
    Libp2pPeersResponse,
)
from trinity.components.eth2.beacon.slot_ticker import (
    SlotTickEvent,
)
from trinity.components.eth2.metrics.registry import metrics
from trinity.protocol.bcc_libp2p.node import Node
from trinity.protocol.bcc_libp2p.configs import ATTESTATION_SUBNET_COUNT


GetReadyAttestationsFn = Callable[[Slot, bool], Tuple[Attestation, ...]]
GetAggregatableAttestationsFn = Callable[[Slot, CommitteeIndex], Tuple[Attestation, ...]]
ImportAttestationFn = Callable[[Attestation, bool], None]


# FIXME: Read this from validator config
ETH1_FOLLOW_DISTANCE = 16


class Validator(BaseService):
    genesis_time: Timestamp
    chain: BaseBeaconChain
    p2p_node: Node
    validator_privkeys: Dict[ValidatorIndex, int]
    event_bus: EndpointAPI
    slots_per_epoch: int
    latest_proposed_epoch: Dict[ValidatorIndex, Epoch]
    latest_attested_epoch: Dict[ValidatorIndex, Epoch]
    local_validator_epoch_assignment: Dict[ValidatorIndex, Tuple[Epoch, CommitteeAssignment]]

    starting_eth1_block_hash: Hash32

    def __init__(
            self,
            chain: BaseBeaconChain,
            p2p_node: Node,
            validator_privkeys: Dict[ValidatorIndex, int],
            event_bus: EndpointAPI,
            get_ready_attestations_fn: GetReadyAttestationsFn,
            get_aggregatable_attestations_fn: GetAggregatableAttestationsFn,
            import_attestation_fn: ImportAttestationFn,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.genesis_time = chain.get_head_state().genesis_time
        self.chain = chain
        self.p2p_node = p2p_node
        self.validator_privkeys = validator_privkeys
        self.event_bus = event_bus
        config = self.chain.get_state_machine().config
        self.slots_per_epoch = config.SLOTS_PER_EPOCH
        # TODO: `latest_proposed_epoch` and `latest_attested_epoch` should be written
        # into/read from validator's own db.
        self.latest_proposed_epoch = {}
        self.latest_attested_epoch = {}
        self.local_validator_epoch_assignment = {}
        for validator_index in validator_privkeys:
            self.latest_proposed_epoch[validator_index] = Epoch(-1)
            self.latest_attested_epoch[validator_index] = Epoch(-1)
            self.local_validator_epoch_assignment[validator_index] = (
                Epoch(-1),
                CommitteeAssignment((), CommitteeIndex(-1), Slot(-1)),
            )
        self.get_ready_attestations: GetReadyAttestationsFn = get_ready_attestations_fn
        self.get_aggregatable_attestations: GetAggregatableAttestationsFn = get_aggregatable_attestations_fn  # noqa: E501
        self.import_attestation: ImportAttestationFn = import_attestation_fn

        # `state.eth1_data` can be updated in the middle of voting period and thus
        # the starting `eth1_data.block_hash` must be stored separately.
        self.starting_eth1_block_hash = chain.get_head_state().eth1_data.block_hash

    async def _run(self) -> None:
        self.logger.info(
            bold_green("validating with indices %s"),
            sorted(tuple(self.validator_privkeys.keys()))
        )
        self.run_daemon_task(self.handle_slot_tick())

        # Metrics
        self.run_daemon_task(self.handle_libp2p_peers_requests())

        await self.cancellation()

    def _check_and_update_data_per_slot(self, slot: Slot) -> None:
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        slots_per_eth1_voting_period = state_machine.config.SLOTS_PER_ETH1_VOTING_PERIOD
        # Update eth1 block_hash in the beginning of each voting period
        if (
            slot % slots_per_eth1_voting_period == 0 and
            self.starting_eth1_block_hash != state.eth1_data.block_hash
        ):
            self.starting_eth1_block_hash = state.eth1_data.block_hash

    async def handle_slot_tick(self) -> None:
        """
        The callback for `SlotTicker` and it's expected to be called twice for one slot.
        """
        async for event in self.event_bus.stream(SlotTickEvent):
            try:
                self._check_and_update_data_per_slot(event.slot)
                if event.tick_type.is_start:
                    await self.handle_first_tick(event.slot)
                elif event.tick_type.is_one_third:
                    await self.handle_second_tick(event.slot)
                elif event.tick_type.is_two_third:
                    await self.handle_third_tick(event.slot)
            except ValidationError as e:
                self.logger.warn("%s", e)
                self.logger.warn(
                    "SHOULD NOT GET A VALIDATION ERROR"
                    " HERE AS IT IS INTERNAL TO OUR OWN CODE"
                )

    async def handle_first_tick(self, slot: Slot) -> None:
        head = self.chain.get_canonical_head()
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        self.logger.debug(
            bold_green(
                "status at slot %s in epoch %s: state_root %s, finalized_checkpoint %s"
            ),
            state.slot,
            state.current_epoch(self.slots_per_epoch),
            humanize_hash(head.message.state_root),
            state.finalized_checkpoint,
        )
        self.logger.debug(
            (
                "status at slot %s in epoch %s:"
                " previous_justified_checkpoint %s, current_justified_checkpoint %s"
            ),
            state.slot,
            state.current_epoch(self.slots_per_epoch),
            state.previous_justified_checkpoint,
            state.current_justified_checkpoint,
        )
        self.logger.debug(
            (
                "status at slot %s in epoch %s:"
                " previous_epoch_attestations %s, current_epoch_attestations %s"
            ),
            state.slot,
            state.current_epoch(self.slots_per_epoch),
            state.previous_epoch_attestations,
            state.current_epoch_attestations,
        )

        # To see if a validator is assigned to propose during the slot, the beacon state must
        # be in the epoch in question. At the epoch boundaries, the validator must run an
        # epoch transition into the epoch to successfully check the proposal assignment of the
        # first slot.
        temp_state = state_machine.state_transition.apply_state_transition(
            state,
            future_slot=slot,
        )
        proposer_index = get_beacon_proposer_index(
            temp_state,
            CommitteeConfig(state_machine.config),
        )

        # `latest_proposed_epoch` is used to prevent validator from erraneously proposing twice
        # in the same epoch due to service crashing.
        epoch = compute_epoch_at_slot(slot, self.slots_per_epoch)
        if proposer_index in self.validator_privkeys:
            has_proposed = epoch <= self.latest_proposed_epoch[proposer_index]
            if not has_proposed:
                await self.propose_block(
                    proposer_index=proposer_index,
                    slot=slot,
                    state=state,
                    state_machine=state_machine,
                    head_block=head,
                )
                self.latest_proposed_epoch[proposer_index] = epoch

    async def handle_second_tick(self, slot: Slot) -> None:
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        if state.slot < slot:
            self.skip_block(
                slot=slot,
                state=state,
                state_machine=state_machine,
            )

        await self.attest(slot)

    async def handle_third_tick(self, slot: Slot) -> None:
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        if state.slot < slot:
            self.skip_block(
                slot=slot,
                state=state,
                state_machine=state_machine,
            )

        await self.aggregate(slot)

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

    async def propose_block(self,
                            proposer_index: ValidatorIndex,
                            slot: Slot,
                            state: BeaconState,
                            state_machine: BaseBeaconStateMachine,
                            head_block: BaseBeaconBlock) -> BaseBeaconBlock:
        """
        Propose a block and broadcast it.
        """
        eth1_vote = await self._get_eth1_vote(slot, state, state_machine)
        deposits = await self._get_deposit_data(state, state_machine, eth1_vote)
        # TODO(hwwhww): Check if need to aggregate and if they are overlapping.
        aggregated_attestations = self.get_ready_attestations(slot, True)
        unaggregated_attestations = self.get_ready_attestations(slot, False)
        ready_attestations = aggregated_attestations + unaggregated_attestations

        block = create_block_on_state(
            state=state,
            config=state_machine.config,
            state_machine=state_machine,
            signed_block_class=SerenitySignedBeaconBlock,  # TODO: Should get block class from slot
            parent_block=head_block,
            slot=slot,
            validator_index=proposer_index,
            privkey=self.validator_privkeys[proposer_index],
            attestations=ready_attestations,
            eth1_data=eth1_vote,
            deposits=deposits,
            check_proposer_index=False,
        )
        self.logger.debug(
            bold_green("validator %s is proposing a block %s with attestations %s"),
            proposer_index,
            block,
            block.body.attestations,
        )
        self.chain.import_block(block)
        self.logger.debug("broadcasting block %s", block)
        await self.p2p_node.broadcast_beacon_block(block)
        metrics.validator_proposed_blocks.inc()
        return block

    def skip_block(self,
                   slot: Slot,
                   state: BeaconState,
                   state_machine: BaseBeaconStateMachine) -> BeaconState:
        """
        Forward state to the target ``slot`` and persist the state.
        """
        post_state = state_machine.state_transition.apply_state_transition(
            state,
            future_slot=slot,
        )
        self.logger.debug(
            bold_green("Skip block at slot=%s  post_state=%s"),
            slot,
            repr(post_state),
        )
        # FIXME: We might not need to persist state for skip slots since `create_block_on_state`
        # will run the state transition which also includes the state transition for skipped slots.
        self.chain.chaindb.persist_state(post_state)
        self.chain.chaindb.update_head_state(post_state.slot, post_state.hash_tree_root)
        return post_state

    #
    # Attesting attestation
    #
    def _get_local_current_epoch_assignment(self,
                                            validator_index: ValidatorIndex,
                                            epoch: Epoch) -> CommitteeAssignment:
        """
        Return the validator's epoch assignment at the given epoch.

        Note that ``epoch`` <= next_epoch.
        """
        is_new_local_validator = validator_index not in self.local_validator_epoch_assignment
        should_update = (
            is_new_local_validator or (
                not is_new_local_validator and (
                    epoch > self.local_validator_epoch_assignment[validator_index][0]
                )
            )
        )
        if should_update:
            state_machine = self.chain.get_state_machine()
            state = self.chain.get_head_state()
            self.local_validator_epoch_assignment[validator_index] = (
                epoch,
                get_committee_assignment(
                    state,
                    state_machine.config,
                    epoch,
                    validator_index,
                ),
            )
        return self.local_validator_epoch_assignment[validator_index][1]

    def _get_local_current_epoch_assignments(
        self, epoch: Epoch
    ) -> Dict[ValidatorIndex, CommitteeAssignment]:
        """
        Return the validator assignments of all the local validators.
        """
        validator_assignments = {
            validator_index: self._get_local_current_epoch_assignment(
                validator_index,
                epoch,
            )
            for validator_index in self.validator_privkeys
        }
        return validator_assignments

    def _get_attesting_assignments_at_slot(self, slot: Slot) -> Set[CommitteeAssignment]:
        """
        Return the set of ``CommitteeAssignment``s of the given ``slot``
        """
        epoch = compute_epoch_at_slot(slot, self.slots_per_epoch)
        validator_assignments = self._get_local_current_epoch_assignments(epoch)
        committee_assignments = set(validator_assignments.values())
        committee_assignments_at_slot = set(
            filter(
                lambda committee_assignment: committee_assignment.slot == slot,
                committee_assignments
            )
        )
        return committee_assignments_at_slot

    @to_tuple
    def _get_local_attesters_at_assignment(
        self, target_assignment: CommitteeAssignment
    ) -> Iterable[ValidatorIndex]:
        """
        Return the local attesters that in the committee of the given assignment
        """
        for validator_index, (_, assignment) in self.local_validator_epoch_assignment.items():
            if (
                assignment.slot == target_assignment.slot and
                assignment.committee_index == target_assignment.committee_index
            ):
                yield validator_index

    async def attest(self, slot: Slot) -> Tuple[Attestation, ...]:
        """
        Attest the block at the given ``slot`` and broadcast them.
        """
        result_attestations: Tuple[Attestation, ...] = ()
        head = self.chain.get_canonical_head()
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        epoch = compute_epoch_at_slot(slot, self.slots_per_epoch)

        attesting_committee_assignments_at_slot = self._get_attesting_assignments_at_slot(slot)

        for committee_assignment in attesting_committee_assignments_at_slot:
            committee_index = committee_assignment.committee_index
            committee = committee_assignment.committee

            attesting_validators_indices = tuple(
                filter(
                    lambda attester: self.latest_attested_epoch[attester] < epoch,
                    self._get_local_attesters_at_assignment(committee_assignment),
                )
            )
            # Get the validator_index -> privkey map of the attesting validators
            attesting_validator_privkeys = {
                index: self.validator_privkeys[index]
                for index in attesting_validators_indices
            }
            attestations = create_signed_attestations_at_slot(
                state,
                state_machine.config,
                state_machine,
                slot,
                head.message.hash_tree_root,
                attesting_validator_privkeys,
                committee,
                committee_index,
                tuple(
                    CommitteeValidatorIndex(committee.index(index))
                    for index in attesting_validators_indices
                ),
            )
            self.logger.debug(
                bold_green("validators %s attesting to block %s with attestation %s"),
                attesting_validators_indices,
                head,
                attestations,
            )

            # await self.p2p_node.broadcast_attestation(attestation)
            subnet_id = SubnetId(committee_index % ATTESTATION_SUBNET_COUNT)

            # Import attestation to pool and then broadcast it
            for attestation in attestations:
                self.import_attestation(attestation, False)
                await self.p2p_node.broadcast_attestation_to_subnet(attestation, subnet_id)

            # Log the last epoch that the validator attested
            for index in attesting_validators_indices:
                self.latest_attested_epoch[index] = epoch

            metrics.validator_sent_attestation.inc()

            result_attestations = result_attestations + attestations
        # TODO: Aggregate attestations

        return result_attestations

    #
    # Aggregating attestation
    #
    @to_tuple
    def _get_aggregates(
        self, slot: Slot, committee_index: CommitteeIndex, config: CommitteeConfig
    ) -> Iterable[Attestation]:
        """
        Return the aggregate attestation of the given committee.
        """
        # TODO: The aggregator should aggregate the late attestations?
        aggregatable_attestations = self.get_aggregatable_attestations(slot, committee_index)
        attestation_groups = groupby(
            aggregatable_attestations,
            key=lambda attestation: attestation.data,
        )
        for _, group in attestation_groups:
            yield get_aggregate_from_valid_committee_attestations(tuple(group))

    async def aggregate(self, slot: Slot) -> Tuple[AggregateAndProof, ...]:
        """
        Aggregate the attestations at ``slot`` and broadcast them.
        """
        # Check the aggregators selection
        aggregate_and_proofs: Tuple[AggregateAndProof, ...] = ()
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        config = state_machine.config

        attesting_committee_assignments_at_slot = self._get_attesting_assignments_at_slot(slot)
        # 1. For each committee_assignment at the given slot
        for committee_assignment in attesting_committee_assignments_at_slot:
            committee_index = committee_assignment.committee_index

            local_attesters = self._get_local_attesters_at_assignment(committee_assignment)
            # Get the validator_index -> privkey map of the attesting validators
            attesting_validator_privkeys = {
                index: self.validator_privkeys[index]
                for index in local_attesters
            }

            selected_proofs: Dict[ValidatorIndex, BLSSignature] = {}
            # 2. For each attester
            for validator_index, privkey in attesting_validator_privkeys.items():
                # Check if the vallidator is one of the aggregators
                signature = get_slot_signature(
                    state, slot, privkey, CommitteeConfig(config)
                )
                is_aggregator_result = is_aggregator(
                    state,
                    slot,
                    committee_index,
                    signature,
                    CommitteeConfig(config),
                )
                if is_aggregator_result:
                    self.logger.debug(
                        f"validator ({validator_index}) is aggregator of"
                        f" committee_index={committee_index} at slot={slot}"
                    )
                    selected_proofs[validator_index] = signature
                else:
                    continue

                aggregates = self._get_aggregates(slot, committee_index, config)
                # 3. For each aggregate
                # (it's possible with same CommitteeIndex and different AttesatationData)
                for aggregate in aggregates:
                    aggregate_and_proof = AggregateAndProof.create(
                        aggregator_index=validator_index,
                        aggregate=aggregate,
                        selection_proof=selected_proofs[validator_index],
                    )
                    self.import_attestation(aggregate_and_proof.aggregate, True)
                    await self.p2p_node.broadcast_beacon_aggregate_and_proof(aggregate_and_proof)
                    aggregate_and_proofs += (aggregate_and_proof,)

        return aggregate_and_proofs

    async def handle_libp2p_peers_requests(self) -> None:
        async for req in self.wait_iter(self.event_bus.stream(Libp2pPeersRequest)):
            peer_count = len(self.p2p_node.handshaked_peers.peers)
            await self.event_bus.broadcast(
                Libp2pPeersResponse(peer_count),
                req.broadcast_config(),
            )
