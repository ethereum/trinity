from typing import Sequence, Tuple, Type

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
        return cls.backend.Aggregate(signatures)

    @classmethod
    def Verify(cls, PK: BLSPubkey, message: Hash32, signature: BLSSignature) -> bool:
        return cls.backend.Verify(PK, message, signature)

    @classmethod
    def AggregateVerify(
        cls, pairs: Sequence[Tuple[BLSPubkey, Hash32]], signature: BLSSignature
    ) -> bool:
        return cls.backend.AggregateVerify(pairs, signature)

    @classmethod
    def FastAggregateVerify(
        cls, PKs: Sequence[BLSPubkey], message: Hash32, signature: BLSSignature
    ) -> bool:
        return cls.backend.FastAggregateVerify(PKs, message, signature)

    @classmethod
    def validate(
        cls, pubkey: BLSPubkey, message_hash: Hash32, signature: BLSSignature
    ) -> None:
        if cls.backend != NoOpBackend:
            validate_signature(signature)
            validate_public_key(pubkey)

        if not cls.Verify(pubkey, message_hash, signature):
            raise SignatureError(
                f"backend {cls.backend.__name__}\n"
                f"message_hash {message_hash.hex()}\n"
                f"pubkey {pubkey.hex()}\n"
                f"signature {signature.hex()}\n"
            )

    @classmethod
    def validate_multiple(
        cls, pubkeys: Sequence[BLSPubkey], message: Hash32, signature: BLSSignature
    ) -> None:
        if cls.backend != NoOpBackend:
            validate_signature(signature)
            validate_many_public_keys(pubkeys)

        if not cls.FastAggregateVerify(pubkeys, message, signature):
            raise SignatureError(
                f"backend {cls.backend.__name__}\n"
                f"pubkeys {pubkeys}\n"
                f"message {message.hex()}\n"
                f"signature {signature.hex()}"
            )


bls = Eth2BLS()
