from rlp import sedes


class HashOrNumber:

    def serialize(self, obj: int) -> bytes:
        if isinstance(obj, int):
            # type ignored to fix https://github.com/ethereum/trinity/issues/1520
            return sedes.big_endian_int.serialize(obj)  # type: ignore
        return sedes.binary.serialize(obj)

    def deserialize(self, serial: bytes) -> int:
        # returned type ignored to fix https://github.com/ethereum/trinity/issues/1520
        if len(serial) == 32:
            return sedes.binary.deserialize(serial)  # type: ignore
        return sedes.big_endian_int.deserialize(serial)  # type: ignore


hash_sedes = sedes.Binary(min_length=32, max_length=32)
