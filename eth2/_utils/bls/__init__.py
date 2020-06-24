from typing import Tuple, Type

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
    def sk_to_pk(cls, secret_key: int) -> BLSPubkey:
        validate_private_key(secret_key)
        return cls.backend.SkToPk(secret_key)

    @classmethod
    def sign(cls, secret_key: int, message: Hash32) -> BLSSignature:
        validate_private_key(secret_key)
        return cls.backend.Sign(secret_key, message)

    @classmethod
    def aggregate(cls, *signatures: BLSSignature) -> BLSSignature:
        return cls.backend.Aggregate(signatures)

    @classmethod
    def verify(
        cls, message: Hash32, signature: BLSSignature, public_key: BLSPubkey
    ) -> bool:
        return cls.backend.Verify(public_key, message, signature)

    @classmethod
    def aggregate_verify(
        cls,
        signature: BLSSignature,
        public_keys: Tuple[BLSPubkey, ...],
        messages: Tuple[Hash32, ...],
    ) -> bool:
        return cls.backend.AggregateVerify(signature, public_keys, messages)

    @classmethod
    def fast_aggregate_verify(
        cls, message: Hash32, signature: BLSSignature, *public_keys: BLSPubkey
    ) -> bool:
        return cls.backend.FastAggregateVerify(public_keys, message, signature)

    @classmethod
    def validate(
        cls, message: Hash32, signature: BLSSignature, *public_keys: BLSPubkey
    ) -> None:
        if len(public_keys) == 0:
            raise SignatureError("public_keys is empty")

        is_aggregate = len(public_keys) > 1

        if cls.backend != NoOpBackend:
            validate_signature(signature)
            if is_aggregate:
                validate_many_public_keys(public_keys)
            else:
                validate_public_key(public_keys[0])

        if is_aggregate:
            if not cls.fast_aggregate_verify(message, signature, *public_keys):
                raise SignatureError(
                    f"backend {cls.backend.__name__}\n"
                    f"message {message.hex()}\n"
                    f"public_keys {public_keys}\n"
                    f"signature {signature.hex()}"
                )
        else:
            if not cls.verify(message, signature, public_keys[0]):
                raise SignatureError(
                    f"backend {cls.backend.__name__}\n"
                    f"message {message.hex()}\n"
                    f"public_key {public_keys[0].hex()}\n"
                    f"signature {signature.hex()}\n"
                )


bls = Eth2BLS()
