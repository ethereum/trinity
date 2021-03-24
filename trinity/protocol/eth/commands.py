from typing import Tuple

from eth_typing import Hash32
from eth_utils.curried import (
    apply_formatter_at_index,
    apply_formatter_to_array,
)
from eth_utils.toolz import compose
from rlp import sedes

from eth.abc import BlockHeaderAPI, SignedTransactionAPI
from eth.rlp.headers import BlockHeader

from p2p.commands import BaseCommand, RLPCodec

from trinity.protocol.common.payloads import BlockHeadersQuery
from trinity.rlp.block_body import BlockBody
from trinity.rlp.sedes import (
    HashOrNumber,
    hash_sedes,
    UninterpretedTransaction,
    UninterpretedTransactionRLP,
    UninterpretedReceipt,
    UninterpretedReceiptRLP,
)
from .forkid import ForkID

from .payloads import (
    StatusV63Payload,
    NewBlockHash,
    BlockFields,
    NewBlockPayload,
    StatusPayload,
)


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


class Transactions(BaseCommand[Tuple[UninterpretedTransaction, ...]]):
    protocol_command_id = 2
    serialization_codec: RLPCodec[Tuple[UninterpretedTransaction, ...]] = RLPCodec(
        sedes=sedes.CountableList(UninterpretedTransactionRLP),
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


class BlockHeadersV65(BaseCommand[Tuple[BlockHeaderAPI, ...]]):
    protocol_command_id = 4
    serialization_codec: RLPCodec[Tuple[BlockHeaderAPI, ...]] = RLPCodec(
        sedes=sedes.CountableList(BlockHeader),
    )


class GetBlockBodiesV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 5
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class BlockBodiesV65(BaseCommand[Tuple[BlockBody, ...]]):
    protocol_command_id = 6
    serialization_codec: RLPCodec[Tuple[BlockBody, ...]] = RLPCodec(
        sedes=sedes.CountableList(BlockBody),
    )


class NewBlock(BaseCommand[NewBlockPayload]):
    protocol_command_id = 7
    serialization_codec: RLPCodec[NewBlockPayload] = RLPCodec(
        sedes=sedes.List((
            sedes.List((
                BlockHeader,
                sedes.CountableList(UninterpretedTransactionRLP),
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


class PooledTransactionsV65(BaseCommand[Tuple[SignedTransactionAPI, ...]]):
    protocol_command_id = 10
    serialization_codec: RLPCodec[Tuple[SignedTransactionAPI, ...]] = RLPCodec(
        sedes=sedes.CountableList(SignedTransactionAPI),
    )


class GetNodeDataV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 13
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class NodeDataV65(BaseCommand[Tuple[bytes, ...]]):
    protocol_command_id = 14
    serialization_codec: RLPCodec[Tuple[bytes, ...]] = RLPCodec(
        sedes=sedes.CountableList(sedes.binary),
    )


class GetReceiptsV65(BaseCommand[Tuple[Hash32, ...]]):
    protocol_command_id = 15
    serialization_codec: RLPCodec[Tuple[Hash32, ...]] = RLPCodec(
        sedes=sedes.CountableList(hash_sedes),
    )


class ReceiptsV65(BaseCommand[Tuple[Tuple[UninterpretedReceipt, ...], ...]]):
    protocol_command_id = 16
    serialization_codec: RLPCodec[Tuple[Tuple[UninterpretedReceipt, ...], ...]] = RLPCodec(
        sedes=sedes.CountableList(sedes.CountableList(UninterpretedReceiptRLP)),
    )
