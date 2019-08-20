from py_ecc.optimized_bls12_381 import (
    curve_order,
)
from eth_typing import (
    BLSSignature,
    BLSPubkey,
)
from eth_utils import (
    ValidationError,
)
from eth2.beacon.constants import (
    EMPTY_PUBKEY,
    EMPTY_SIGNATURE,
)
from eth2.beacon.exceptions import (
    SignatureError,
)


def validate_private_key(privkey: int) -> None:
    if privkey <= 0 or privkey >= curve_order:
        raise ValueError(
            f"Invalid private key: Expect integer between 1 and {curve_order - 1}, got {privkey}"
        )


def validate_empty_public_key(pubkey: BLSPubkey) -> None:
    if pubkey == EMPTY_PUBKEY:
        raise ValidationError(f"Empty public key breaks Milagro binding  pubkey={pubkey}")


def validate_signature(signature: BLSSignature) -> None:
    if len(signature) != 96:
        raise SignatureError(
            f"Invalid signaute length, expect 96 got {len(signature)}. Signature: {signature}"
        )
    if signature == EMPTY_SIGNATURE:
        raise SignatureError(
            f"Signature should not be empty. Signature: {signature}"
        )
