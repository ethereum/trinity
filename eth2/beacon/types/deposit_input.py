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

from eth2.beacon._utils.hash import hash_eth2
from eth2.beacon.constants import EMPTY_SIGNATURE


class DepositInput(ssz.Serializable):

    fields = [
        # BLS pubkey
        ('pubkey', bytes48),
        # Withdrawal credentials
        ('withdrawal_credentials', bytes32),
        # BLS proof of possession (a BLS signature)
        ('proof_of_possession', bytes96),
    ]

    def __init__(self,
                 pubkey: BLSPubkey,
                 withdrawal_credentials: Hash32,
                 proof_of_possession: BLSSignature=EMPTY_SIGNATURE) -> None:
        super().__init__(
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            proof_of_possession=proof_of_possession,
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root

    _signed_root = None

    @property
    def signed_root(self) -> Hash32:
        # TODO: Use SSZ built-in function
        if self._signed_root is None:
            self._signed_root = hash_eth2(
                ssz.encode(self.copy(proof_of_possession=EMPTY_SIGNATURE))
            )
        return self._signed_root
