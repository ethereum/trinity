
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
    @staticmethod
    def privtopub(k: int) -> BLSPubkey:
        return chia_api.privtopub(k)

    @staticmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        return chia_api.sign(message_hash, privkey, domain)

    @staticmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        return chia_api.verify(message_hash, pubkey, signature, domain)

    @staticmethod
    def aggregate_signatures(signatures: Sequence[BLSSignature]) -> BLSSignature:
        return chia_api.aggregate_signatures(signatures)

    @staticmethod
    def aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return chia_api.aggregate_pubkeys(pubkeys)

    @staticmethod
    def verify_multiple(pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        return chia_api.verify_multiple(pubkeys, message_hashes, signature, domain)


eth2_bls = ChiaBackend()
