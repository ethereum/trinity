from typing import (
    Any,
    List,
    Union,
)

from rlp import sedes


class AnyRLP:
    @classmethod
    def serialize(cls, obj: Any) -> Union[bytes, List[bytes]]:
        return obj

    @classmethod
    def deserialize(cls, encoded: Union[bytes, List[bytes]]) -> Any:
        return encoded


class HashOrNumber:

    def serialize(self, obj: int) -> bytes:
        if isinstance(obj, int):
            return sedes.big_endian_int.serialize(obj)
        return sedes.binary.serialize(obj)

    def deserialize(self, serial: bytes) -> int:
        if len(serial) == 32:
            return sedes.binary.deserialize(serial)
        return sedes.big_endian_int.deserialize(serial)


hash_sedes = sedes.Binary(min_length=32, max_length=32)

# We often have to rlp-decode without knowing ahead of time how to interpret the values.
#   So we just pass around uninterpreted bytes (or list of bytes, for legacy txns),
#   until the moment that we can use the VM to decode the serialized values.
SerializedTransaction = Union[bytes, List[bytes]]
