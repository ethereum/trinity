import secrets

import factory

from eth_utils import (
    int_to_big_endian,
)

from eth_keys import keys


def _mk_private_key_bytes() -> bytes:
    return int_to_big_endian(secrets.randbits(256)).rjust(32, b'\x00')


class PrivateKeyFactory(factory.Factory):
    class Meta:
        model = keys.PrivateKey

    private_key_bytes = factory.LazyFunction(_mk_private_key_bytes)


def _mk_public_key_bytes() -> bytes:
    # type ignored to fix https://github.com/ethereum/trinity/issues/1520
    return PrivateKeyFactory().public_key.to_bytes()  # type: ignore


class PublicKeyFactory(factory.Factory):
    class Meta:
        model = keys.PublicKey

    public_key_bytes = factory.LazyFunction(_mk_public_key_bytes)
