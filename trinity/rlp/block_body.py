from typing import (
    Iterable,
    Union,
)

from eth.abc import (
    BlockHeaderAPI,
    SignedTransactionAPI,
)
from eth.rlp.headers import BlockHeader
import rlp
from rlp import sedes

from .sedes import AnyRLP, SerializedTransaction


class BlockBody(rlp.Serializable):
    fields = [
        ('transactions', sedes.CountableList(AnyRLP)),
        ('uncles', sedes.CountableList(BlockHeader))
    ]

    def __init__(
            self,
            transactions: Iterable[Union[SerializedTransaction, SignedTransactionAPI]],
            uncles: Iterable[BlockHeaderAPI]) -> None:
        if not isinstance(transactions, (list, bytes)):
            transactions = rlp.decode(rlp.encode(transactions))
        super().__init__(transactions, uncles)
