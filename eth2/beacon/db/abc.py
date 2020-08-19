from abc import ABC, abstractmethod
from typing import Optional, Type

from eth.abc import AtomicDatabaseAPI

from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import BLSSignature, Root, Slot, Timestamp
from eth2.configs import Eth2Config


class BaseBeaconChainDB(ABC):
    """
    Persist data relating to a beacon chain.

    Stores blocks and states with ``persist_{block,state}`` methods.
    Any stored block or state can be subsequently queried by its hash tree root.

    Once a block has been finalized (according to the fork choice) it is marked
    as "canonical" and is available to be queried by "canonical" slot.
    NOTE: Blocks and states are not stored by slot until they have
    been finalized. To get data for non-finalized slots, defer to the fork choice computation.
    """

    genesis_time: Timestamp

    @abstractmethod
    def __init__(self, db: AtomicDatabaseAPI) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_genesis(
        cls,
        db: AtomicDatabaseAPI,
        genesis_state: BeaconState,
        signed_block_class: Type[BaseSignedBeaconBlock],
        config: Eth2Config,
    ) -> "BaseBeaconChainDB":
        ...

    @abstractmethod
    def get_block_by_slot(
        self, slot: Slot, block_class: Type[BaseBeaconBlock]
    ) -> Optional[BaseBeaconBlock]:
        ...

    @abstractmethod
    def get_block_by_root(
        self, block_root: Root, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_block_signature_by_root(self, block_root: Root) -> BLSSignature:
        """
        ``block_root`` is the hash tree root of a beacon block.
        This method provides a way to reconstruct the ``SignedBeaconBlock`` if required.
        """
        ...

    @abstractmethod
    def persist_block(self, block: BaseSignedBeaconBlock) -> None:
        ...

    @abstractmethod
    def mark_canonical_block(self, block: BaseBeaconBlock) -> None:
        """
        Record the ``block`` as part of the canonical ("finalized") chain.
        """
        ...

    @abstractmethod
    def mark_canonical_head(self, block: BaseBeaconBlock) -> None:
        ...

    @abstractmethod
    def get_canonical_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        """
        Prefer to read the canonical head from the fork choice module.

        This method primarily exists to help the fork choice module restore
        context from disk, e.g. in between periods of running a beacon node.
        """
        ...

    @abstractmethod
    def mark_justified_head(self, block: BaseBeaconBlock) -> None:
        ...

    @abstractmethod
    def get_justified_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def mark_finalized_head(self, block: BaseBeaconBlock) -> None:
        ...

    @abstractmethod
    def get_finalized_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_state_by_slot(
        self, slot: Slot, state_class: Type[BeaconState], config: Eth2Config
    ) -> Optional[BeaconState]:
        ...

    @abstractmethod
    def get_state_by_root(
        self, state_root: Root, state_class: Type[BeaconState], config: Eth2Config
    ) -> BeaconState:
        ...

    @abstractmethod
    def persist_state(self, state: BeaconState, config: Eth2Config) -> None:
        ...
