from abc import ABC, abstractmethod

from eth.abc import AtomicDatabaseAPI

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseSignedBeaconBlock, BeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.clock import Tick


class BaseBeaconChain(ABC):
    @classmethod
    @abstractmethod
    def from_genesis(
        cls, base_db: AtomicDatabaseAPI, genesis_state: BeaconState
    ) -> "BaseBeaconChain":
        ...

    @abstractmethod
    def get_canonical_head(self) -> BeaconBlock:
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
