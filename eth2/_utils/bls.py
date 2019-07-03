
from .bls_bindings.chia_network import (
    api as chia_api,
)
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Sequence,
)
from eth_typing import (
    BLSPubkey,
    BLSSignature,
    Hash32,
)


class BaseBLSBackend(ABC):

    @staticmethod
    @abstractmethod
    def privtopub(k: int) -> BLSPubkey:
        pass

    @staticmethod
    @abstractmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        pass

    @staticmethod
    @abstractmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def aggregate_signatures(signatures: Sequence[BLSSignature]) -> BLSSignature:
        pass

    @staticmethod
    @abstractmethod
    def aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        pass

    @staticmethod
    @abstractmethod
    def verify_multiple(pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        pass


class ChiaBackend(BaseBLSBackend):
    privtopub = staticmethod(chia_api.privtopub)
    sign = staticmethod(chia_api.sign)
    verify = staticmethod(chia_api.verify)
    aggregate_signatures = staticmethod(chia_api.aggregate_signatures)
    aggregate_pubkeys = staticmethod(chia_api.aggregate_pubkeys)
    verify_multiple = staticmethod(chia_api.verify_multiple)


eth2_bls = ChiaBackend()
