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

from .sedes import (
    UninterpretedTransaction,
    UninterpretedTransactionRLP,
)


class BlockBody(rlp.Serializable):
    fields = [
        ('transactions', sedes.CountableList(UninterpretedTransactionRLP)),
        ('uncles', sedes.CountableList(BlockHeader))
    ]

    def __init__(
            self,
            transactions: Iterable[Union[UninterpretedTransaction, SignedTransactionAPI]],
            uncles: Iterable[BlockHeaderAPI]) -> None:
        if not isinstance(transactions, (list, bytes)):
            transactions = rlp.decode(rlp.encode(transactions))
        super().__init__(transactions, uncles)
