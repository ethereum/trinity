from abc import ABC, abstractmethod
from typing import Iterable, Tuple

from typing_extensions import Protocol

from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Epoch, Root, Slot, ValidatorIndex
from eth2.configs import Eth2Config


class BlockSink(Protocol):
    def on_pruned_block(self, slot: Slot, block_root: Root, canonical: bool) -> None:
        """
        When a block is not part of fork-choice anymore.
        If canonical, it is finalized. If not canonical, it is orphaned.
        """
        ...


class BaseForkChoice(ABC):
    @classmethod
    @abstractmethod
    def from_recent_state(
        cls, recent_state: BeaconState, config: Eth2Config, block_sink: BlockSink
    ) -> "BaseForkChoice":
        ...

    # @abstractmethod
    # def load_context(
    #     cls, chain_db: BaseBeaconChainDB, config: Eth2Config, block_sink: BlockSink
    # ) -> "BaseForkChoice":
    #     ...

    @abstractmethod
    def update_justified(self, state: BeaconState) -> None:
        ...

    @abstractmethod
    def get_canonical_chain(self) -> Iterable[Tuple[Slot, Root]]:
        ...

    @abstractmethod
    def on_block(self, block: BaseBeaconBlock) -> None:
        """
        Assumes the parent of ``block`` has already been supplied to an instance of this class.
        """
        ...

    @abstractmethod
    def on_attestation(
        self, block_root: Root, target_epoch: Epoch, *indices: ValidatorIndex
    ) -> None:
        ...

    @abstractmethod
    def find_head(self) -> Root:
        ...
