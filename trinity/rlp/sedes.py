from typing import (
    Any,
    List,
    Sequence,
    Tuple,
    Union,
)

from eth.abc import (
    ReceiptAPI,
)
import rlp
from rlp import sedes

DecodedZeroOrOneLayerRLP = Union[bytes, List[bytes]]


class ZeroOrOneLayerRLP:
    """
    An RLP object of unknown interpretation, with a maximum "depth" of 1.

    It can be either a simple bytes object, or a list of bytes objects.
    """
    @classmethod
    def serialize(cls, obj: Any) -> DecodedZeroOrOneLayerRLP:
        return obj

    @classmethod
    def deserialize(cls, encoded: DecodedZeroOrOneLayerRLP) -> Any:
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


def strip_interpretation(rlp_object: Any) -> DecodedZeroOrOneLayerRLP:
    return rlp.decode(rlp.encode(rlp_object))


# We often have to rlp-decode without knowing ahead of time how to interpret the values.
#   So we just pass around uninterpreted bytes (or list of bytes, for legacy txns),
#   until the moment that we can use the VM to decode the serialized values.
UninterpretedTransaction = DecodedZeroOrOneLayerRLP
UninterpretedReceipt = DecodedZeroOrOneLayerRLP

UninterpretedTransactionRLP = ZeroOrOneLayerRLP
UninterpretedReceiptRLP = ZeroOrOneLayerRLP


def deinterpret_receipt_bundles(
    receipt_bundles: Sequence[Sequence[ReceiptAPI]],
) -> Tuple[Tuple[UninterpretedReceipt, ...], ...]:

    return tuple(
        tuple(
            strip_interpretation(receipt)
            for receipt in receipts
        )
        for receipts in receipt_bundles
    )
