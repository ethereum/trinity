from typing import (
    Callable,
    Tuple,
)

from cancel_token import (
    CancelToken,
)
from eth_typing import (
    Hash32,
)
from eth_utils import (
    ValidationError,
)
from lahja import EndpointAPI

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.state_machines.base import (
    BaseBeaconStateMachine,
)
from eth2.beacon.types.attestations import (
    Attestation,
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
from trinity._utils.shellart import (
    bold_green,
)
from trinity.components.eth2.beacon.slot_ticker import (
    SlotTickEvent,
)

GetReadyAttestationsFn = Callable[[Slot, bool], Tuple[Attestation, ...]]
GetAggregatableAttestationsFn = Callable[[Slot, CommitteeIndex], Tuple[Attestation, ...]]
ImportAttestationFn = Callable[[Attestation, bool], None]


class ChainMaintainer(BaseService):
    genesis_time: Timestamp
    chain: BaseBeaconChain
    event_bus: EndpointAPI
    slots_per_epoch: int

    starting_eth1_block_hash: Hash32

    def __init__(
            self,
            chain: BaseBeaconChain,
            event_bus: EndpointAPI,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.genesis_time = chain.get_head_state().genesis_time
        self.chain = chain
        self.event_bus = event_bus
        config = self.chain.get_state_machine().config
        self.slots_per_epoch = config.SLOTS_PER_EPOCH

    async def _run(self) -> None:
        self.logger.info(bold_green("ChainMaintainer is running"))
        self.run_daemon_task(self.handle_slot_tick())

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
                    pass
                elif event.tick_type.is_one_third:
                    await self.handle_second_tick(event.slot)
                elif event.tick_type.is_two_third:
                    pass
            except ValidationError as e:
                self.logger.warn("%s", e)
                self.logger.warn(
                    "SHOULD NOT GET A VALIDATION ERROR"
                    " HERE AS IT IS INTERNAL TO OUR OWN CODE"
                )

    async def handle_second_tick(self, slot: Slot) -> None:
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        if state.slot < slot:
            self.skip_block(
                slot=slot,
                state=state,
                state_machine=state_machine,
            )

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
