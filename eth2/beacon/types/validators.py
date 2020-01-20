from typing import TYPE_CHECKING, Type, TypeVar

from eth_typing import BLSPubkey, Hash32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import boolean, bytes32, bytes48, uint64

from eth2.beacon.constants import FAR_FUTURE_EPOCH, ZERO_HASH32
from eth2.beacon.typing import Epoch, Gwei
from eth2.configs import Eth2Config

from .defaults import default_bls_pubkey, default_epoch, default_gwei

if TYPE_CHECKING:
    from eth2.beacon.types.states import BeaconState  # noqa: F401


def _round_down_to_previous_multiple(amount: int, increment: int) -> int:
    return amount - amount % increment


def calculate_effective_balance(amount: Gwei, config: Eth2Config) -> Gwei:
    return Gwei(
        min(
            _round_down_to_previous_multiple(
                amount, config.EFFECTIVE_BALANCE_INCREMENT
            ),
            config.MAX_EFFECTIVE_BALANCE,
        )
    )


TValidator = TypeVar("TValidator", bound="Validator")


class Validator(HashableContainer):

    fields = [
        ("pubkey", bytes48),
        ("withdrawal_credentials", bytes32),
        ("effective_balance", uint64),
        ("slashed", boolean),
        # Epoch when validator became eligible for activation
        ("activation_eligibility_epoch", uint64),
        # Epoch when validator activated
        ("activation_epoch", uint64),
        # Epoch when validator exited
        ("exit_epoch", uint64),
        # Epoch when validator withdrew
        ("withdrawable_epoch", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TValidator],
        *,
        pubkey: BLSPubkey = default_bls_pubkey,
        withdrawal_credentials: Hash32 = ZERO_HASH32,
        effective_balance: Gwei = default_gwei,
        slashed: bool = False,
        activation_eligibility_epoch: Epoch = default_epoch,
        activation_epoch: Epoch = default_epoch,
        exit_epoch: Epoch = default_epoch,
        withdrawable_epoch: Epoch = default_epoch,
    ) -> TValidator:
        return super().create(
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            effective_balance=effective_balance,
            slashed=slashed,
            activation_eligibility_epoch=activation_eligibility_epoch,
            activation_epoch=activation_epoch,
            exit_epoch=exit_epoch,
            withdrawable_epoch=withdrawable_epoch,
        )

    def is_active(self, epoch: Epoch) -> bool:
        """
        Return ``True`` if the validator is active during the epoch, ``epoch``.

        From `is_active_validator` in the spec.
        """
        return self.activation_epoch <= epoch < self.exit_epoch

    def is_slashable(self, epoch: Epoch) -> bool:
        """
        From `is_slashable_validator` in the spec.
        """
        not_slashed = self.slashed is False
        active_but_not_withdrawable = (
            self.activation_epoch <= epoch < self.withdrawable_epoch
        )
        return not_slashed and active_but_not_withdrawable

    def is_eligible_for_activation_queue(self, config: Eth2Config) -> bool:
        """
        From `is_eligible_for_activation_queue` in the spec
        """
        return (
            self.activation_eligibility_epoch == FAR_FUTURE_EPOCH
            and self.effective_balance == config.MAX_EFFECTIVE_BALANCE
        )

    def is_eligible_for_activation(self, state: "BeaconState") -> bool:
        """
        Check if ``validator`` is eligible for activation.
        From `is_eligible_for_activation` in the spec
        """
        return (
            # Placement in queue is finalized
            self.activation_eligibility_epoch <= state.finalized_checkpoint.epoch
            # Has not yet been activated
            and self.activation_epoch == FAR_FUTURE_EPOCH
        )

    @classmethod
    def create_pending_validator(
        cls,
        pubkey: BLSPubkey,
        withdrawal_credentials: Hash32,
        amount: Gwei,
        config: Eth2Config,
    ) -> "Validator":
        """
        Return a new pending ``Validator`` with the given fields.
        """
        return cls.create(
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            effective_balance=calculate_effective_balance(amount, config),
            activation_eligibility_epoch=FAR_FUTURE_EPOCH,
            activation_epoch=FAR_FUTURE_EPOCH,
            exit_epoch=FAR_FUTURE_EPOCH,
            withdrawable_epoch=FAR_FUTURE_EPOCH,
        )

    def __str__(self) -> str:
        return (
            f"pubkey={humanize_hash(self.pubkey)},"
            f" withdrawal_credentials={humanize_hash(self.withdrawal_credentials)},"
            f" effective_balance={self.effective_balance},"
            f" slashed={self.slashed},"
            f" activation_eligibility_epoch={self.activation_eligibility_epoch},"
            f" activation_epoch={self.activation_epoch},"
            f" exit_epoch={self.exit_epoch},"
            f" withdrawable_epoch={self.withdrawable_epoch}"
        )
