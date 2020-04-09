from typing import Tuple, Union, Any

from eth_typing import Hash32
from eth_utils.curried import (
    apply_formatter_at_index,
    apply_formatter_to_array,
)
from eth_utils.toolz import compose
from rlp import sedes

from eth.abc import BlockHeaderAPI, ReceiptAPI, SignedTransactionAPI
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from eth.rlp.transactions import BaseTransactionFields

from p2p.commands import BaseCommand, RLPCodec

from trinity.protocol.common.payloads import (
    BlockHeadersQuery,
    BlockHeadersQueryPayload,
    BlockHeadersResultPayload,
    BlocksResultPayload,
    BytesTupleQueryPayload,
    BytesTupleResultPayload,
    Hash32TupleQueryPayload,
    ReceiptBundleResultPayload,
    TransactionsResultPayload,
)
from trinity.rlp.block_body import BlockBody
from trinity.rlp.sedes import HashOrNumber, hash_sedes
from .forkid import ForkID

from .payloads import (
    BlockFields,
    NewBlockHash,
    NewBlockPayload,
    StatusPayload,
    StatusV63Payload,
)


# Using a NewType such as Hash32 will throw of pickle. That's why we use `bytes` and ignore
# the mypy warning where this helper is used. That way, we don't leak `bytes` into other APIs.
def process_tuple_bytes_query(args: Any) -> BytesTupleQueryPayload:
    return BytesTupleQueryPayload(*args)


class StatusV63(BaseCommand[StatusV63Payload]):
    protocol_command_id = 0
    serialization_codec: RLPCodec[StatusV63Payload] = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.big_endian_int,
            sedes.big_endian_int,
            hash_sedes,
            hash_sedes,
        )),
        process_inbound_payload_fn=compose(
            lambda args: StatusV63Payload(*args),
        ),
    )


class Status(BaseCommand[StatusPayload]):
    protocol_command_id = 0
    serialization_codec: RLPCodec[StatusPayload] = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.big_endian_int,
            sedes.big_endian_int,
            hash_sedes,
            hash_sedes,
            ForkID
        )),
        process_inbound_payload_fn=compose(
            lambda args: StatusPayload(*args),
        ),
    )


class NewBlockHashes(BaseCommand[Tuple[NewBlockHash, ...]]):
    protocol_command_id = 1
    serialization_codec: RLPCodec[Tuple[NewBlockHash, ...]] = RLPCodec(
        sedes=sedes.CountableList(sedes.List([hash_sedes, sedes.big_endian_int])),
        process_inbound_payload_fn=apply_formatter_to_array(lambda args: NewBlockHash(*args)),
    )


class Transactions(BaseCommand[Tuple[BaseTransactionFields, ...]]):
    protocol_command_id = 2
    serialization_codec: RLPCodec[Tuple[BaseTransactionFields, ...]] = RLPCodec(
        sedes=sedes.CountableList(BaseTransactionFields),
    )


class GetBlockHeadersV65(BaseCommand[BlockHeadersQuery]):
    protocol_command_id = 3
    serialization_codec: RLPCodec[BlockHeadersQuery] = RLPCodec(
        sedes=sedes.List((
            HashOrNumber(),
            sedes.big_endian_int,
            sedes.big_endian_int,
            sedes.boolean,
        )),
        process_inbound_payload_fn=lambda args: BlockHeadersQuery(*args),
    )


class GetBlockHeadersV66(BaseCommand[BlockHeadersQueryPayload]):
    protocol_command_id = 3
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.List((HashOrNumber(), sedes.big_endian_int, sedes.big_endian_int, sedes.boolean)),
        )),
        process_inbound_payload_fn=compose(
            lambda args: BlockHeadersQueryPayload(*args),
            apply_formatter_at_index(lambda args: BlockHeadersQuery(*args), 1)
        ),
    )


AnyGetBlockHeaders = Union[GetBlockHeadersV65, GetBlockHeadersV66]


class BlockHeadersV65(BaseCommand[Tuple[BlockHeaderAPI, ...]]):
    protocol_command_id = 4
    serialization_codec: RLPCodec[Tuple[BlockHeaderAPI, ...]] = RLPCodec(
        sedes=sedes.CountableList(BlockHeader),
    )


class BlockHeadersV66(BaseCommand[BlockHeadersResultPayload]):
    protocol_command_id = 4
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(BlockHeader),
        )),
        process_inbound_payload_fn=lambda args: BlockHeadersResultPayload(*args),
    )


AnyBlockHeaders = Union[BlockHeadersV65, BlockHeadersV66]


class GetBlockBodiesV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 5
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class GetBlockBodiesV66(BaseCommand[Hash32TupleQueryPayload]):
    protocol_command_id = 5
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(hash_sedes),
        )),
        # processing as `bytes` because NewType`s such as Hash32 throw off pickle
        process_inbound_payload_fn=process_tuple_bytes_query  # type: ignore
    )


AnyGetBlockBodies = Union[GetBlockBodiesV65, GetBlockBodiesV66]


class BlockBodiesV65(BaseCommand[Tuple[BlockBody, ...]]):
    protocol_command_id = 6
    serialization_codec: RLPCodec[Tuple[BlockBody, ...]] = RLPCodec(
        sedes=sedes.CountableList(BlockBody),
    )


class BlockBodiesV66(BaseCommand[BlocksResultPayload]):
    protocol_command_id = 6
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(BlockBody),
        )),
        process_inbound_payload_fn=lambda args: BlocksResultPayload(*args),
    )


AnyBlockBodies = Union[BlockBodiesV65, BlockBodiesV66]


class NewBlock(BaseCommand[NewBlockPayload]):
    protocol_command_id = 7
    serialization_codec: RLPCodec[NewBlockPayload] = RLPCodec(
        sedes=sedes.List((
            sedes.List((
                BlockHeader,
                sedes.CountableList(BaseTransactionFields),
                sedes.CountableList(BlockHeader)
            )),
            sedes.big_endian_int
        )),
        process_inbound_payload_fn=compose(
            lambda args: NewBlockPayload(*args),
            apply_formatter_at_index(
                lambda args: BlockFields(*args),
                0,
            )
        )
    )


class NewPooledTransactionHashes(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 8
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class GetPooledTransactionsV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 9
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class GetPooledTransactionsV66(BaseCommand[Hash32TupleQueryPayload]):
    protocol_command_id = 9
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(hash_sedes),
        )),
        # processing as `bytes` because NewType`s such as Hash32 throw off pickle
        process_inbound_payload_fn=process_tuple_bytes_query,  # type: ignore
    )


AnyGetPooledTransactions = Union[GetPooledTransactionsV65, GetPooledTransactionsV66]


class PooledTransactionsV65(BaseCommand[Tuple[SignedTransactionAPI, ...]]):
    protocol_command_id = 10
    serialization_codec: RLPCodec[Tuple[SignedTransactionAPI, ...]] = RLPCodec(
        sedes=sedes.CountableList(SignedTransactionAPI),
    )


class PooledTransactionsV66(BaseCommand[TransactionsResultPayload]):
    protocol_command_id = 10
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(SignedTransactionAPI),
        )),
        process_inbound_payload_fn=lambda args: TransactionsResultPayload(*args),  # noqa: E501
    )


AnyPooledTransactions = Union[PooledTransactionsV65, PooledTransactionsV66]


class GetNodeDataV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 13
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class GetNodeDataV66(BaseCommand[Hash32TupleQueryPayload]):
    protocol_command_id = 13
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(hash_sedes),
        )),
        # processing as `bytes` because NewType`s such as Hash32 throw off pickle
        process_inbound_payload_fn=process_tuple_bytes_query  # type: ignore
    )


AnyGetNodeData = Union[GetNodeDataV65, GetNodeDataV66]


class NodeDataV65(BaseCommand[Tuple[bytes, ...]]):
    protocol_command_id = 14
    serialization_codec: RLPCodec[Tuple[bytes, ...]] = RLPCodec(
        sedes=sedes.CountableList(sedes.binary),
    )


class NodeDataV66(BaseCommand[BytesTupleResultPayload]):
    protocol_command_id = 14
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(sedes.binary),
        )),
        process_inbound_payload_fn=lambda args: BytesTupleResultPayload(*args),
    )


AnyNodeData = Union[NodeDataV65, NodeDataV66]


class GetReceiptsV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 15
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class GetReceiptsV66(BaseCommand[Hash32TupleQueryPayload]):
    protocol_command_id = 15
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(hash_sedes),
        )),
        # processing as `bytes` because NewType`s such as Hash32 throw off pickle
        process_inbound_payload_fn=process_tuple_bytes_query,  # type: ignore
    )


AnyGetReceipts = Union[GetReceiptsV65, GetReceiptsV66]


class ReceiptsV65(BaseCommand[Tuple[Tuple[ReceiptAPI, ...], ...]]):
    protocol_command_id = 16
    serialization_codec: RLPCodec[Tuple[Tuple[ReceiptAPI, ...], ...]] = RLPCodec(
        sedes=sedes.CountableList(sedes.CountableList(Receipt)),
    )


class ReceiptsV66(BaseCommand[ReceiptBundleResultPayload]):
    protocol_command_id = 16
    serialization_codec = RLPCodec(
        sedes=sedes.List((
            sedes.big_endian_int,
            sedes.CountableList(sedes.CountableList(Receipt)),
        )),
        process_inbound_payload_fn=lambda args: ReceiptBundleResultPayload(*args),
    )


AnyReceipts = Union[ReceiptsV65, ReceiptsV66]
