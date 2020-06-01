from typing import Sequence, Type

from eth_typing import BLSPubkey, BLSSignature, Hash32

from eth2.beacon.exceptions import SignatureError

from .backends import DEFAULT_BACKEND, NoOpBackend
from .backends.base import BaseBLSBackend
from .validation import (
    validate_many_public_keys,
    validate_private_key,
    validate_public_key,
    validate_signature,
)


class Eth2BLS:
    backend: Type[BaseBLSBackend]

    def __init__(self) -> None:
        self.use_default_backend()

    @classmethod
    def use(cls, backend: Type[BaseBLSBackend]) -> None:
        cls.backend = backend

    @classmethod
    def use_default_backend(cls) -> None:
        cls.use(DEFAULT_BACKEND)

    @classmethod
    def use_noop_backend(cls) -> None:
        cls.use(NoOpBackend)

    @classmethod
    def SkToPk(cls, SK: int) -> BLSPubkey:
        validate_private_key(SK)
        return cls.backend.SkToPk(SK)

    @classmethod
    def Sign(cls, SK: int, message: Hash32) -> BLSSignature:
        validate_private_key(SK)
        return cls.backend.Sign(SK, message)

    @classmethod
    def Aggregate(cls, signatures: Sequence[BLSSignature]) -> BLSSignature:
        return cls.backend.aggregate_signatures(signatures)

    @classmethod
    def _AggregatePKs(cls, pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return cls.backend.aggregate_pubkeys(pubkeys)

    @classmethod
    def Verify(
        cls,
        PK: BLSPubkey,
        message: Hash32,
        signature: BLSSignature,
    ) -> bool:
        return cls.backend.verify(PK, message, signature)

    @classmethod
    def FastAggregateVerify(
        cls,
        PKs: Sequence[BLSPubkey],
        message: Sequence[Hash32],
        signature: BLSSignature,
    ) -> bool:
        return cls.backend.verify_multiple(PKs, message, signature)

    @classmethod
    def validate(
        cls,
        message_hash: Hash32,
        pubkey: BLSPubkey,
        signature: BLSSignature,
    ) -> None:
        if cls.backend != NoOpBackend:
            validate_signature(signature)
            validate_public_key(pubkey)

        if not cls.Verify(message_hash, pubkey, signature):
            raise SignatureError(
                f"backend {cls.backend.__name__}\n"
                f"message_hash {message_hash.hex()}\n"
                f"pubkey {pubkey.hex()}\n"
                f"signature {signature.hex()}\n"
            )

    @classmethod
    def validate_multiple(
        cls,
        pubkeys: Sequence[BLSPubkey],
        message: Hash32,
        signature: BLSSignature,
    ) -> None:
        if cls.backend != NoOpBackend:
            validate_signature(signature)
            validate_many_public_keys(pubkeys)

        if not cls.FastAggregateVerify():
            raise SignatureError(
                f"backend {cls.backend.__name__}\n"
                f"pubkeys {pubkeys}\n"
                f"message_hashes {message}\n"
                f"signature {signature.hex()}\n"
            )


bls = Eth2BLS()
