from typing import (
    Any,
    cast,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Tuple,
    Union,
)

import rlp
from rlp import sedes

from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt

from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from trinity.protocol.common.commands import BaseBlockHeaders
from trinity.rlp.block_body import BlockBody
from trinity.rlp.sedes import HashOrNumber


class StatusMessage(NamedTuple):
    protocolVersion: int
    networkId: int
    headTd: int
    headHash: bytes
    headNum: int
    genesisHash: bytes
    serveHeaders: Any
    serveChainSince: int
    serveStateSince: int
    txRelay: Any
    flowControl_BL: int = None
    flowControl_MRC: List[List[int]] = None
    flowControl_MRR: int = None


class Status(Command):
    _cmd_id = 0
    decode_strict = False
    # A list of (key, value) pairs is all a Status msg contains, but since the values can be of
    # any type, we need to use the raw sedes here and do the actual deserialization in
    # decode_payload().
    message_class = StatusMessage
    structure = sedes.CountableList(sedes.List([sedes.text, sedes.raw]))
    # The sedes used for each key in the list above. Keys that use None as their sedes are
    # optional and have no value -- IOW, they just need to be present in the msg when appropriate.
    items_sedes = {
        'protocolVersion': sedes.big_endian_int,
        'networkId': sedes.big_endian_int,
        'headTd': sedes.big_endian_int,
        'headHash': sedes.binary,
        'headNum': sedes.big_endian_int,
        'genesisHash': sedes.binary,
        'serveHeaders': None,
        'serveChainSince': sedes.big_endian_int,
        'serveStateSince': sedes.big_endian_int,
        'txRelay': None,
        'flowControl_BL': sedes.big_endian_int,
        'flowControl_MRC': sedes.CountableList(
            sedes.List([sedes.big_endian_int, sedes.big_endian_int, sedes.big_endian_int])),
        'flowControl_MRR': sedes.big_endian_int,
    }

    # T = TypeVar("T")
    #
    # def to_NamedTuple(
    #     tupleType: NamedTuple,
    # ) -> Callable[..., Callable[..., T]]:
    #     def outer(fn):
    #         @functools.wraps(fn)
    #         def inner(*args, **kwargs) -> "T":  # type: ignore
    #             return tupleType(*fn(*args, **kwargs))
    #
    #         return inner
    #
    #     return outer

    # @to_NamedTuple(StatusMessage)
    def decode_payload(self, rlp_data: bytes) -> Iterator[Tuple[str, Any]]:
        data = cast(List[Tuple[str, bytes]], super().decode_payload(rlp_data))
        # The LES/Status msg contains an arbitrary list of (key, value) pairs, where values can
        # have different types and unknown keys should be ignored for forward compatibility
        # reasons, so here we need an extra pass to deserialize each of the key/value pairs we
        # know about.
        message_dict = {}
        for key, value in data:
            if key not in self.items_sedes:
                continue
            message_dict[key] = self._deserialize_item(key, value)

        return self.get_message_class()(**message_dict)

    def encode_payload(self, data: Union[_DecodedMsgType, sedes.CountableList]) -> bytes:
        response = [
            (key, self._serialize_item(key, value))
            for key, value
            in sorted(cast(Dict[str, Any], data).items())
        ]
        return super().encode_payload(response)

    def _deserialize_item(self, key: str, value: bytes) -> Any:
        sedes = self.items_sedes[key]
        if sedes is not None:
            return sedes.deserialize(value)
        else:
            # See comment in the definition of item_sedes as to why we do this.
            return b''

    def _serialize_item(self, key: str, value: bytes) -> bytes:
        sedes = self.items_sedes[key]
        if sedes is not None:
            return sedes.serialize(value)
        else:
            # See comment in the definition of item_sedes as to why we do this.
            return b''


class AnnounceMessage(NamedTuple):
    head_hash: bytes
    head_number: int
    head_td: int
    reorg_depth: int
    # TODO: Need to change below
    params: List[List[Union[str, Any]]]


class Announce(Command):
    _cmd_id = 1
    message_class = AnnounceMessage
    structure = [
        ('head_hash', sedes.binary),
        ('head_number', sedes.big_endian_int),
        ('head_td', sedes.big_endian_int),
        ('reorg_depth', sedes.big_endian_int),
        # TODO: The params CountableList may contain any of the values from the
        # Status msg.  Need to extend this command to process that too.
        ('params', sedes.CountableList(sedes.List([sedes.text, sedes.raw]))),
    ]


class GetBlockHeadersQuery(rlp.Serializable):
    fields = [
        ('block_number_or_hash', HashOrNumber()),
        ('max_headers', sedes.big_endian_int),
        ('skip', sedes.big_endian_int),
        ('reverse', sedes.boolean),
    ]


class GetBlockHeadersMessage(NamedTuple):
    request_id: int
    query: GetBlockHeadersQuery


class GetBlockHeaders(Command):
    _cmd_id = 2
    message_class = GetBlockHeadersMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('query', GetBlockHeadersQuery),
    ]


class BlockHeadersMessage(NamedTuple):
    request_id: int
    buffer_value: int
    headers: List[BlockHeader]


class BlockHeaders(BaseBlockHeaders):
    _cmd_id = 3
    message_class = BlockHeadersMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('headers', sedes.CountableList(BlockHeader)),
    ]

    def extract_headers(self, msg: _DecodedMsgType) -> Tuple[BlockHeader, ...]:
        msg = cast(Dict[str, Any], msg)
        return cast(Tuple[BlockHeader, ...], tuple(msg.headers))


class GetBlockBodiesMessage(NamedTuple):
    request_id: int
    block_hashes: List[bytes]


class GetBlockBodies(Command):
    _cmd_id = 4
    message_class = GetBlockBodiesMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('block_hashes', sedes.CountableList(sedes.binary)),
    ]


class BlockBodiesMessage(NamedTuple):
    request_id: int
    buffer_value: int
    bodies: List[BlockBody]


class BlockBodies(Command):
    _cmd_id = 5
    message_class = BlockBodiesMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('bodies', sedes.CountableList(BlockBody)),
    ]


class GetReceiptsMessage(NamedTuple):
    request_id: int
    block_hashes: List[bytes]


class GetReceipts(Command):
    _cmd_id = 6
    message_class = GetReceiptsMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('block_hashes', sedes.CountableList(sedes.binary)),
    ]


class ReceiptsMessage(NamedTuple):
    request_id: int
    buffer_value: int
    receipts: List[List[Receipt]]


class Receipts(Command):
    _cmd_id = 7
    message_class = ReceiptsMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('receipts', sedes.CountableList(sedes.CountableList(Receipt))),
    ]


class ProofRequest(rlp.Serializable):
    fields = [
        ('block_hash', sedes.binary),
        ('account_key', sedes.binary),
        ('key', sedes.binary),
        ('from_level', sedes.big_endian_int),
    ]


class GetProofsMessage(NamedTuple):
    request_id: int
    proof_requests: List[ProofRequest]


class GetProofs(Command):
    _cmd_id = 8
    message_class = GetProofsMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('proof_requests', sedes.CountableList(ProofRequest)),
    ]


class GetProofsMessage(NamedTuple):
    request_id: int
    buffer_value: int
    # TODO: Need to change below Any
    proofs: List[List[Any]]
    # TODO: Need to tighten below type
    proof: List[Any] = None


class Proofs(Command):
    _cmd_id = 9
    message_class = GetProofsMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('proofs', sedes.CountableList(sedes.CountableList(sedes.raw))),
    ]

    def decode_payload(self, rlp_data: bytes) -> _DecodedMsgType:
        decoded = super().decode_payload(rlp_data)
        decoded = cast(Dict[str, Any], decoded)
        # This is just to make Proofs messages compatible with ProofsV2, so that LightPeerChain
        # doesn't have to special-case them. Soon we should be able to drop support for LES/1
        # anyway, and then all this code will go away.
        if not decoded.proofs:
            proof = []
        else:
            proof = decoded.proofs[0]
        return self.get_message_class()(
            request_id=decoded.request_id,
            buffer_value=decoded.buffer_value,
            proofs=decoded.proofs,
            proof=proof,
        )


class ContractCodeRequest(rlp.Serializable):
    fields = [
        ('block_hash', sedes.binary),
        ('key', sedes.binary),
    ]


class GetContractCodesMessage(NamedTuple):
    request_id: int
    code_requests: List[ContractCodeRequest]


class GetContractCodes(Command):
    _cmd_id = 10
    message_class = GetContractCodesMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('code_requests', sedes.CountableList(ContractCodeRequest)),
    ]


class ContractCodesMessage(NamedTuple):
    request_id: int
    buffer_value: int
    codes: List[bytes]


class ContractCodes(Command):
    _cmd_id = 11
    message_class = ContractCodesMessage
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('codes', sedes.CountableList(sedes.binary)),
    ]


class StatusV2Message(NamedTuple):
    protocolVersion: int
    networkId: int
    headTd: int
    headHash: bytes
    headNum: int
    genesisHash: bytes
    serveHeaders: Any
    serveChainSince: int
    serveStateSince: int = None
    txRelay: Any = None
    flowControl_BL: int = None
    flowControl_MRC: List[List[int]] = None
    flowControl_MRR: int = None
    announceType: int = None


class StatusV2(Status):
    _cmd_id = 0
    message_class = StatusV2Message

    def __init__(self, cmd_id_offset: int) -> None:
        super().__init__(cmd_id_offset)
        self.items_sedes['announceType'] = sedes.big_endian_int


class GetProofsV2(GetProofs):
    _cmd_id = 15


class ProofsV2Message(NamedTuple):
    request_id: int
    buffer_value: int
    # TODO: Change below from Any to Type[Sedes.raw]
    proof: List[Any]


class ProofsV2(Command):
    _cmd_id = 16
    message_class = ProofsV2Message
    structure = [
        ('request_id', sedes.big_endian_int),
        ('buffer_value', sedes.big_endian_int),
        ('proof', sedes.CountableList(sedes.raw)),
    ]
