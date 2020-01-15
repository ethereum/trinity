from typing import Type, TypeVar

from eth.constants import ZERO_HASH32
from eth_typing import BLSPubkey, BLSSignature, Hash32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32, bytes48, bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.typing import Gwei

from .defaults import default_bls_pubkey, default_gwei

TDepositMessage = TypeVar("TDepositMessage", bound="DepositMessage")


class DepositMessage(HashableContainer):
    fields = [
        ("pubkey", bytes48),
        ("withdrawal_credentials", bytes32),
        ("amount", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TDepositMessage],
        pubkey: BLSPubkey = default_bls_pubkey,
        withdrawal_credentials: Hash32 = ZERO_HASH32,
        amount: Gwei = default_gwei,
    ) -> TDepositMessage:
        return super().create(
            pubkey=pubkey, withdrawal_credentials=withdrawal_credentials, amount=amount
        )

    def __str__(self) -> str:
        return (
            f"pubkey={humanize_hash(self.pubkey)},"
            f" withdrawal_credentials={humanize_hash(self.withdrawal_credentials)},"
            f" amount={self.amount}"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


TDepositData = TypeVar("TDepositData", bound="DepositData")


class DepositData(HashableContainer):
    """
    :class:`~eth2.beacon.types.deposit_data.DepositData` corresponds to the data broadcast from the
    Ethereum 1.0 deposit contract after a successful call to the ``deposit`` function on that
    contract.
    """

    fields = [
        ("pubkey", bytes48),
        ("withdrawal_credentials", bytes32),
        ("amount", uint64),
        # Signing over DepositMessage
        ("signature", bytes96),
    ]

    @classmethod
    def create(
        cls: Type[TDepositData],
        pubkey: BLSPubkey = default_bls_pubkey,
        withdrawal_credentials: Hash32 = ZERO_HASH32,
        amount: Gwei = default_gwei,
        signature: BLSSignature = EMPTY_SIGNATURE,
    ) -> TDepositData:
        return super().create(
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            amount=amount,
            signature=signature,
        )

    def __str__(self) -> str:
        return (
            f"pubkey={humanize_hash(self.pubkey)},"
            f" withdrawal_credentials={humanize_hash(self.withdrawal_credentials)},"
            f" amount={self.amount}, signature={humanize_hash(self.signature)}"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {str(self)}>"


default_deposit_data = DepositData.create()
