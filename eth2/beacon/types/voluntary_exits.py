from typing import Type, TypeVar

from eth_typing import BLSSignature
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.typing import Epoch, ValidatorIndex

from .defaults import default_epoch, default_validator_index

TVoluntaryExit = TypeVar("TVoluntaryExit", bound="VoluntaryExit")


class VoluntaryExit(HashableContainer):

    fields = [
        # Minimum epoch for processing exit
        ("epoch", uint64),
        ("validator_index", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TVoluntaryExit],
        epoch: Epoch = default_epoch,
        validator_index: ValidatorIndex = default_validator_index,
    ) -> TVoluntaryExit:
        return super().create(
            epoch=epoch, validator_index=validator_index
        )

    def __str__(self) -> str:
        return (
            f"epoch={self.epoch},"
            f" validator_index={self.validator_index},"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


class SignedVoluntaryExit(HashableContainer):
    fields = [
        ('message', VoluntaryExit),
        ('signature', bytes96),
    ]

    @classmethod
    def create(
        cls: Type[VoluntaryExit],
        *,
        message: VoluntaryExit,
        signature: bytes,
    ) -> VoluntaryExit:
        return super().create(
            message=message,
            signature=signature,
        )
