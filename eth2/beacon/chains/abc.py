from abc import ABC, abstractmethod
from typing import Optional

from eth.abc import AtomicDatabaseAPI

from eth2.beacon.db.abc import BaseBeaconChainDB
from eth2.beacon.fork_choice.abc import BaseForkChoice
from eth2.beacon.state_machines.abc import BaseBeaconStateMachine
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseSignedBeaconBlock, BeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot
from eth2.clock import Tick


class BaseBeaconChain(ABC):
    @abstractmethod
    def __init__(
        self, chain_db: BaseBeaconChainDB, fork_choice: BaseForkChoice
    ) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_genesis(
        cls, base_db: AtomicDatabaseAPI, genesis_state: BeaconState
    ) -> "BaseBeaconChain":
        ...

    @property
    @abstractmethod
    def db(self) -> BaseBeaconChainDB:
        ...

    @abstractmethod
    def get_state_machine(self, slot: Slot) -> BaseBeaconStateMachine:
        ...

    @abstractmethod
    def get_canonical_head(self) -> BeaconBlock:
        ...

    @abstractmethod
    def get_canonical_head_state(self) -> BeaconState:
        ...

    @abstractmethod
    def get_block_by_slot(self, slot: Slot) -> Optional[BaseSignedBeaconBlock]:
        ...

    @abstractmethod
    def on_tick(self, tick: Tick) -> None:
        ...

    @abstractmethod
    def on_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> None:
        ...

    @abstractmethod
    def on_attestation(self, attestation: Attestation) -> None:
        ...


def advance_state_to_slot(
    chain: BaseBeaconChain, target_slot: Slot, state: BeaconState = None
) -> BeaconState:
    if state is None:
        state = chain.get_canonical_head_state()

    current_slot = state.slot
    for slot in range(current_slot, target_slot + 1):
        slot = Slot(slot)
        state_machine = chain.get_state_machine(slot)
        state, _ = state_machine.apply_state_transition(state, future_slot=slot)
    return state
