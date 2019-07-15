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

from eth2.beacon.exceptions import (
    SignatureError,
)
from eth2.beacon.constants import (
    EMPTY_PUBKEY,
    EMPTY_SIGNATURE,
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


class NoOpBackend(BaseBLSBackend):
    @staticmethod
    def privtopub(k: int) -> BLSPubkey:
        return EMPTY_PUBKEY

    @staticmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        return True

    @staticmethod
    def aggregate_signatures(signatures: Sequence[BLSSignature]) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return EMPTY_PUBKEY

    @staticmethod
    def verify_multiple(pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        return True


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


class Eth2BLS:
    backend: BaseBLSBackend

    def __init__(self, backend):
        self.backend = backend

    def use_noop_backend(self):
        self.backend = NoOpBackend()

    def privtopub(self,
                  k: int) -> BLSPubkey:
        return self.backend.privtopub(k)

    def sign(self,
             message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        return self.backend.sign(message_hash, privkey, domain)

    def aggregate_signatures(self,
                             signatures: Sequence[BLSSignature]) -> BLSSignature:
        return self.backend.aggregate_signatures(signatures)

    def aggregate_pubkeys(self,
                          pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return self.backend.aggregate_pubkeys(pubkeys)

    def validate(self,
                 message_hash: Hash32,
                 pubkey: BLSPubkey,
                 signature: BLSSignature,
                 domain: int) -> None:
        if not self.backend.verify(message_hash, pubkey, signature, domain):
            raise SignatureError(
                f"message_hash {message_hash}\n"
                f"pubkey {pubkey}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            )

    def validate_multiple(self,
                          pubkeys: Sequence[BLSPubkey],
                          message_hashes: Sequence[Hash32],
                          signature: BLSSignature,
                          domain: int) -> None:
        if not self.backend.verify_multiple(pubkeys, message_hashes, signature, domain):
            raise SignatureError(
                f"pubkeys {pubkeys}\n"
                f"message_hashes {message_hashes}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            )


default_backend = ChiaBackend()
eth2_bls = Eth2BLS(backend=default_backend)
