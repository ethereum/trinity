from typing import (
    Sequence,
    Type,
)

from eth_typing import (
    BLSPubkey,
    BLSSignature,
    Hash32,
)

from eth2.beacon.exceptions import (
    SignatureError,
)

from .backends import (
    DEFAULT_BACKEND,
    NoOpBackend,
)
from .backends.base import (
    BaseBLSBackend,
)


class Eth2BLS:
    backend: Type[BaseBLSBackend]

    @classmethod
    def use_default_backend(cls) -> None:
        cls.backend = DEFAULT_BACKEND

    @classmethod
    def use_noop_backend(cls) -> None:
        cls.backend = NoOpBackend

    @classmethod
    def privtopub(cls,
                  k: int) -> BLSPubkey:
        return cls.backend.privtopub(k)

    @classmethod
    def sign(cls,
             message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        return cls.backend.sign(message_hash, privkey, domain)

    @classmethod
    def aggregate_signatures(cls,
                             signatures: Sequence[BLSSignature]) -> BLSSignature:
        return cls.backend.aggregate_signatures(signatures)

    @classmethod
    def aggregate_pubkeys(cls,
                          pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return cls.backend.aggregate_pubkeys(pubkeys)

    @classmethod
    def verify(cls,
               message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        return cls.backend.verify(message_hash, pubkey, signature, domain)

    @classmethod
    def verify_multiple(cls,
                        pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        return cls.backend.verify_multiple(pubkeys, message_hashes, signature, domain)

    @classmethod
    def validate(cls,
                 message_hash: Hash32,
                 pubkey: BLSPubkey,
                 signature: BLSSignature,
                 domain: int) -> None:
        if not cls.backend.verify(message_hash, pubkey, signature, domain):
            raise SignatureError(
                f"message_hash {message_hash}\n"
                f"pubkey {pubkey}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            )

    @classmethod
    def validate_multiple(cls,
                          pubkeys: Sequence[BLSPubkey],
                          message_hashes: Sequence[Hash32],
                          signature: BLSSignature,
                          domain: int) -> None:
        if not cls.backend.verify_multiple(pubkeys, message_hashes, signature, domain):
            raise SignatureError(
                f"pubkeys {pubkeys}\n"
                f"message_hashes {message_hashes}\n"
                f"signature {signature}\n"
                f"domain {domain}"
            )


eth2_bls = Eth2BLS
eth2_bls.use_default_backend()
