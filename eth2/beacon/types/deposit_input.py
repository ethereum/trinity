from eth_typing import (
    BLSPubkey,
    BLSSignature,
    Hash32,
)
import ssz
from ssz.sedes import (
    bytes32,
    bytes48,
    bytes96,
)

from eth2.beacon.constants import EMPTY_SIGNATURE


class DepositInput(ssz.SignedSerializable):

    fields = [
        # BLS pubkey
        ('pubkey', bytes48),
        # Withdrawal credentials
        ('withdrawal_credentials', bytes32),
        # BLS proof of possession (a BLS signature)
        ('signature', bytes96),
    ]

    def __init__(self,
                 pubkey: BLSPubkey,
                 withdrawal_credentials: Hash32,
                 signature: BLSSignature=EMPTY_SIGNATURE) -> None:
        super().__init__(
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            signature=signature,
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root

    _signing_root = None

    @property
    def signing_root(self) -> Hash32:
        # Use SSZ built-in function
        if self._signing_root is None:
            self._signing_root = super().signing_root
        return self._signing_root
