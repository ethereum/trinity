from typing import NamedTuple, Union, Sequence, Generic, Iterator, overload, TypeVar, Tuple

from eth.abc import BlockHeaderAPI, BlockAPI, SignedTransactionAPI, ReceiptAPI
from eth_typing import BlockNumber, Hash32

from p2p.commands import BaseCommand

TData = TypeVar('TData')


# mypy doesn't support generic named tuples yet: https://github.com/python/mypy/issues/685
class Payload(Sequence[Union[int, TData]], Generic[TData]):

    def __init__(self, request_id: int, data: TData):
        self.request_id = request_id
        self.data = data

    def __iter__(self) -> Iterator[Union[int, TData]]:
        yield self.request_id
        yield self.data

    @overload
    def __getitem__(self, idx: int) -> Union[int, TData]:
        ...

    @overload  # noqa: F811
    def __getitem__(self, s: slice) -> Sequence[Union[int, TData]]:
        ...

    def __getitem__(  # noqa: F811
            self, index: Union[int, slice]
    ) -> Union[Union[int, TData], Sequence[Union[int, TData]]]:
        if isinstance(index, slice):
            raise Exception("Subclass disallows slicing")

        if index == 0:
            return self.request_id
        elif index == 1:
            return self.data
        else:
            raise IndexError(f"Index {index} is out of bounds. Can only be 0 or 1")

    def __len__(self) -> int:
        return 2


TQuery = TypeVar('TQuery')


class QueryPayload(Payload[TQuery]):
    def __init__(self, request_id: int, query: TQuery):
        self.request_id = request_id
        self.data = query

    @property
    def query(self) -> TQuery:
        return self.data


TResult = TypeVar('TResult')


class ResultPayload(Payload[TResult]):
    def __init__(self, request_id: int, result: TResult):
        self.request_id = request_id
        self.data = result

    @property
    def result(self) -> TResult:
        return self.data


TPayload = TypeVar('TPayload')


def get_cmd_payload(
    cmd: Union[
        BaseCommand[TPayload],
        BaseCommand[QueryPayload[TQuery]],
        BaseCommand[ResultPayload[TResult]],
    ]
) -> Union[TPayload, TQuery, TResult]:
    """
    Return the actual payload from a given command, meaning, for a ``BaseCommand[TPayload]`` return
    ``TPayload``, for a ``BaseCommand[QueryPayload[TQuery]]`` return ``TQuery`` and for a
    ``BaseCommand[ResultPayload[TResult]]`` return ``TResult``. This helper preserves type
    information.
    """
    if isinstance(cmd.payload, QueryPayload):
        return cmd.payload.query
    if isinstance(cmd.payload, ResultPayload):
        return cmd.payload.result
    else:
        return cmd.payload


class BlockHeadersQuery(NamedTuple):
    block_number_or_hash: Union[BlockNumber, Hash32]
    max_headers: int
    skip: int
    reverse: bool


BlockHeadersQueryPayload = QueryPayload[BlockHeadersQuery]
BytesTupleQueryPayload = QueryPayload[Tuple[bytes, ...]]
Hash32TupleQueryPayload = QueryPayload[Tuple[Hash32, ...]]

BytesTupleResultPayload = ResultPayload[Tuple[bytes, ...]]
BlockHeadersResultPayload = ResultPayload[Tuple[BlockHeaderAPI, ...]]
BlocksResultPayload = ResultPayload[Tuple[BlockAPI, ...]]
TransactionsResultPayload = ResultPayload[Tuple[SignedTransactionAPI, ...]]
ReceiptBundleResultPayload = ResultPayload[Tuple[Tuple[ReceiptAPI, ...], ...]]
