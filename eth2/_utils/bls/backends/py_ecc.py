from typing import Sequence

from eth_typing import BLSPubkey, BLSSignature, Hash32
from py_ecc.bls import G2ProofOfPossession

from eth2._utils.bls.backends.base import BaseBLSBackend
from eth2.beacon.constants import EMPTY_SIGNATURE


class PyECCBackend(BaseBLSBackend):
    @staticmethod
    def SkToPk(k: int) -> BLSPubkey:
        return G2ProofOfPossession.SkToPk(k)

    @staticmethod
    def Sign(SK: int, message: Hash32) -> BLSSignature:
        return G2ProofOfPossession.Sign(SK, message)

    @staticmethod
    def Verify(PK: BLSPubkey, message: Hash32, signature: BLSSignature) -> bool:
        return G2ProofOfPossession.Verify(PK, message, signature)

    @staticmethod
    def Aggregate(signatures: Sequence[BLSSignature]) -> BLSSignature:
        # py_ecc use a different EMPTY_SIGNATURE. Return the Trinity one here:
        if len(signatures) == 0:
            return EMPTY_SIGNATURE
        return G2ProofOfPossession.Aggregate(signatures)

    @staticmethod
    def FastAggregateVerify(
        PKs: Sequence[BLSPubkey], message: Hash32, signature: BLSSignature
    ) -> bool:
        return G2ProofOfPossession.FastAggregateVerify(PKs, message, signature)
