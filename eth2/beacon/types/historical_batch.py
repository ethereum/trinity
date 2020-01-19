from typing import Sequence, Type, TypeVar

from eth.constants import ZERO_HASH32
from eth_typing import Hash32
from ssz.hashable_container import HashableContainer
from ssz.sedes import Vector, bytes32

from eth2.beacon.constants import ZERO_ROOT
from eth2.beacon.typing import Root
from eth2.configs import Eth2Config

from .defaults import default_tuple, default_tuple_of_size

THistoricalBatch = TypeVar("THistoricalBatch", bound="HistoricalBatch")


class HistoricalBatch(HashableContainer):

    fields = [("block_roots", Vector(bytes32, 1)), ("state_roots", Vector(bytes32, 1))]

    @classmethod
    def create(
        cls: Type[THistoricalBatch],
        *,
        block_roots: Sequence[Root] = default_tuple,
        state_roots: Sequence[Hash32] = default_tuple,
        config: Eth2Config = None
    ) -> THistoricalBatch:
        if config:
            # try to provide sane defaults
            if block_roots == default_tuple:
                block_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_ROOT
                )
            if state_roots == default_tuple:
                state_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )

        return super().create(block_roots=block_roots, state_roots=state_roots)
