from typing import Any, NamedTuple, NewType, Sequence, Tuple

from eth_typing import Hash32
from typing_extensions import Protocol

Slot = NewType("Slot", int)  # uint64
Epoch = NewType("Epoch", int)  # uint64


class Bitfield(Tuple[bool, ...]):
    def __new__(self, *args: Sequence[Any]) -> "Bitfield":
        return tuple.__new__(Bitfield, *args)

    def __str__(self) -> str:
        elems = map(lambda elem: "1" if elem else "0", self)
        return f"0b{''.join(elems)}"


CommitteeIndex = NewType("CommitteeIndex", int)  # uint64, a committee index at a slot
ValidatorIndex = NewType("ValidatorIndex", int)  # uint64, a validator registry index
CommitteeValidatorIndex = NewType(
    "CommitteeValidatorIndex", int
)  # uint64, the i-th position in a committee tuple

Gwei = NewType("Gwei", int)  # uint64

Timestamp = NewType("Timestamp", int)
Second = NewType("Second", int)

Version = NewType("Version", bytes)

DomainType = NewType("DomainType", bytes)  # bytes of length 4

Root = NewType("Root", Hash32)  # a Merkle root


#
#  Networkinig
#

# CommitteeIndex % ATTESTATION_SUBNET_COUNT
SubnetId = NewType("SubnetId", int)


#
# Helpers
#


class FromBlockParams(NamedTuple):
    slot: Slot = None


#
# Defaults to emulate "zero types"
#

default_slot = Slot(0)
default_epoch = Epoch(0)
default_committee_index = CommitteeIndex(0)
default_validator_index = ValidatorIndex(0)
default_gwei = Gwei(0)
default_timestamp = Timestamp(0)
default_second = Second(0)
default_bitfield = Bitfield(tuple())
default_version = Version(b"\x00" * 4)


class Operation(Protocol):
    hash_tree_root: Root
