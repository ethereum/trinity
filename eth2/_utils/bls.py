
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
from eth_utils import (
    ValidationError,
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
    def _verify(message_hash: Hash32,
                pubkey: BLSPubkey,
                signature: BLSSignature,
                domain: int) -> bool:
        pass

    @classmethod
    def verify(cls,
               message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> None:
        if not cls._verify(message_hash, pubkey, signature, domain):
            raise ValidationError((
                "Verification failed:\n"
                f"message_hash {message_hash}\n"
                f"pubkey {pubkey}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            ))

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
    def _verify_multiple(pubkeys: Sequence[BLSPubkey],
                         message_hashes: Sequence[Hash32],
                         signature: BLSSignature,
                         domain: int) -> bool:
        pass

    @classmethod
    def verify_multiple(cls,
                        pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> None:
        if not cls._verify_multiple(pubkeys, message_hashes, signature, domain):
            raise ValidationError((
                "Verification failed:\n"
                f"pubkeys {pubkeys}\n"
                f"message_hashes {message_hashes}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            ))


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
    def _verify(message_hash: Hash32,
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
    def _verify_multiple(pubkeys: Sequence[BLSPubkey],
                         message_hashes: Sequence[Hash32],
                         signature: BLSSignature,
                         domain: int) -> bool:
        return chia_api.verify_multiple(pubkeys, message_hashes, signature, domain)


eth2_bls = ChiaBackend()
