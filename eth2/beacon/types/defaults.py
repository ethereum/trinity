"""
This module contains default values to be shared across types in the parent module.
"""
from typing import TYPE_CHECKING, Tuple, TypeVar

from eth2.beacon.constants import EMPTY_PUBKEY
from eth2.beacon.typing import (  # noqa: F401
    default_bitfield,
    default_committee_index,
    default_epoch,
    default_gwei,
    default_root,
    default_second,
    default_slot,
    default_timestamp,
    default_validator_index,
    default_version,
)

if TYPE_CHECKING:
    from typing import Any  # noqa: F401


default_bls_pubkey = EMPTY_PUBKEY

# NOTE: there is a bug in our current version of ``flake8`` (==3.5.0)
# which does not recognize the inline typing:
#     default_tuple: Tuple[SomeElement, ...] = ...
# so we add the type via comment
#
# for more info, see: https://stackoverflow.com/q/51885518
# updating to ``flake8==3.7.7`` fixes this bug but introduces many other breaking changes.
SomeElement = TypeVar("SomeElement")

default_tuple = tuple()  # type: Tuple[Any, ...]


def default_tuple_of_size(
    size: int, default_element: SomeElement
) -> Tuple[SomeElement, ...]:
    return (default_element,) * size
