from enum import Enum, unique
import io
import logging
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from eth_utils import ValidationError
from libp2p.network.stream.exceptions import StreamEOF
from libp2p.network.stream.net_stream_interface import INetStream
from libp2p.peer.id import ID as PeerID
from libp2p.utils import decode_uvarint_from_stream, encode_uvarint
from snappy import StreamCompressor, StreamDecompressor
import ssz
from ssz.hashable_container import HashableContainer

from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.typing import Root, Slot, default_slot
from trinity.nodes.beacon.metadata import MetaData
from trinity.nodes.beacon.metadata import SeqNumber as MetaDataSeqNumber
from trinity.nodes.beacon.status import Status

MAX_REQUEST_BLOCKS = 2 ** 10
MAX_CHUNK_SIZE = 2 ** 20
SUCCESS_CODE = b"\x00"
VALID_BLOCKS_BY_RANGE_RANGE = range(1, MAX_REQUEST_BLOCKS + 1)
ROOT_SEDES = ssz.sedes.bytes32

StatusProvider = Callable[[], Status]
MetadataProvider = Callable[[], MetaData]
BlockProviderBySlot = Callable[[Slot], Optional[SignedBeaconBlock]]
BlockProviderByRoot = Callable[[Root], Optional[SignedBeaconBlock]]
PeerUpdater = Callable[[PeerID, Any], Awaitable[None]]

StreamHandler = Callable[[INetStream], Awaitable[None]]

SHUTTING_DOWN_CODE = 1
IRRELEVANT_NETWORK_CODE = 2
INTERNAL_ERROR_CODE = 3


class GoodbyeReason(int):
    def __repr__(self) -> str:
        if self == SHUTTING_DOWN_CODE:
            return "peer is shutting down"
        elif self == IRRELEVANT_NETWORK_CODE:
            return "peer is on irrelevant network"
        elif self == INTERNAL_ERROR_CODE:
            return "peer has some internal error"
        else:
            return f"unknown goodbye reason: {self}"


TBlocksByRangeRequest = TypeVar("TBlocksByRangeRequest", bound="BlocksByRangeRequest")


class BlocksByRangeRequest(HashableContainer):
    fields = [
        ("start_slot", ssz.sedes.uint64),
        ("count", ssz.sedes.uint64),
        ("step", ssz.sedes.uint64),
    ]

    @classmethod
    def create(
        cls: Type[TBlocksByRangeRequest],
        *,
        start_slot: Slot = default_slot,
        count: int = 0,
        step: int = 1,
    ) -> TBlocksByRangeRequest:
        if count < 0:
            raise ValidationError(
                "range request must ask for a non-negative number of blocks"
            )
        if step < 1:
            raise ValidationError("range request must have step greater than 0")
        return super().create(start_slot=start_slot, count=count, step=step)

    def count_expected_blocks(self) -> int:
        """
        Return the number of blocks we expect the resource to provide.

        Note that the actual response could be less if the resource is missing blocks
        in the range we query.
        """
        return self.count


@unique
class ReqRespProtocol(Enum):
    status = "status"
    goodbye = "goodbye"
    beacon_blocks_by_range = "beacon_blocks_by_range"
    beacon_blocks_by_root = "beacon_blocks_by_root"
    ping = "ping"
    metadata = "metadata"

    def id(self, version: int = 1, encoding: str = "ssz_snappy") -> str:
        return f"/eth2/beacon_chain/req/{self.value}/{version}/{encoding}"


async def _write_with_snappy(data: bytes, stream: INetStream) -> None:
    compressor = StreamCompressor()
    data_reader = io.BytesIO(data)
    chunk = data_reader.read(MAX_CHUNK_SIZE)
    while chunk:
        chunk = compressor.add_chunk(chunk)
        await stream.write(chunk)
        chunk = data_reader.read(MAX_CHUNK_SIZE)


async def _write_request(request_payload: bytes, stream: INetStream) -> None:
    request_len = len(request_payload)
    await stream.write(encode_uvarint(request_len))
    await _write_with_snappy(request_payload, stream)


async def _write_success_response_chunk(
    stream: INetStream, response_payload: bytes
) -> None:
    await stream.write(SUCCESS_CODE)
    await stream.write(encode_uvarint(len(response_payload)))
    await _write_with_snappy(response_payload, stream)


async def _read_with_snappy(stream: INetStream, length: int) -> bytes:
    decompressor = StreamDecompressor()
    data = io.BytesIO()

    chunk_size = min(length, MAX_CHUNK_SIZE)
    chunk = await stream.read(chunk_size)
    remaining = length
    while chunk:
        chunk = decompressor.decompress(chunk)
        data.write(chunk)
        remaining -= len(chunk)

        if not remaining:
            break

        chunk = await stream.read(chunk_size)
    decompressor.flush()
    return data.getvalue()


async def _read_request(stream: INetStream) -> bytes:
    try:
        request_len = await decode_uvarint_from_stream(stream)
        return await _read_with_snappy(stream, request_len)
    except StreamEOF:
        # TODO further handling of EOF?
        return b""


async def _ensure_valid_response(stream: INetStream) -> None:
    response_code = await stream.read(1)
    if not response_code:
        # TODO how to handle this...
        raise StreamEOF()

    if response_code != SUCCESS_CODE:
        # TODO attempt parsing an error message from the client
        # response_code == b"\x01": invalid request
        # response_code == b"\x02": server error
        # each with a payload ssz type: List[byte, 256]
        raise ValidationError("response code was not successful: {response_code}")


async def _read_response(stream: INetStream) -> bytes:
    try:
        await _ensure_valid_response(stream)
    except StreamEOF:
        # TODO further handling of EOF?
        return b""
    return await _read_request(stream)


def _serialize_ssz(message: ssz.Serializable, sedes: ssz.BaseSedes) -> bytes:
    return ssz.encode(message, sedes)


def _deserialize_ssz(data: bytes, sedes: ssz.BaseSedes) -> ssz.Serializable:
    return ssz.decode(data, sedes)


class RequestResponder:
    TTFB_TIMEOUT = 5  # seconds
    RESP_TIMEOUT = 10  # seconds

    logger = logging.getLogger(
        "trinity.nodes.beacon.request_responder.RequestResponder"
    )

    def __init__(
        self,
        peer_updater: PeerUpdater,
        status_provider: StatusProvider,
        block_provider_by_slot: BlockProviderBySlot,
        block_provider_by_root: BlockProviderByRoot,
        metadata_provider: MetadataProvider,
    ) -> None:
        self._peer_updater = peer_updater
        self._status_provider = status_provider
        self._block_provider_by_slot = block_provider_by_slot
        self._block_provider_by_root = block_provider_by_root
        self._metadata_provider = metadata_provider

    def get_protocols(self) -> Iterable[Tuple[str, StreamHandler]]:
        for protocol, handler in {
            ReqRespProtocol.status: self._recv_status,
            ReqRespProtocol.goodbye: self._recv_goodbye,
            ReqRespProtocol.beacon_blocks_by_range: self._recv_beacon_blocks_by_range,
            ReqRespProtocol.beacon_blocks_by_root: self._recv_beacon_blocks_by_root,
            ReqRespProtocol.ping: self._recv_ping,
            ReqRespProtocol.metadata: self._recv_metadata,
        }.items():
            yield protocol.id(), handler

    async def send_status(self, stream: INetStream, status: Status) -> Status:
        request_payload = _serialize_ssz(status, Status)

        await _write_request(request_payload, stream)
        await stream.close()

        response_data = await _read_response(stream)
        return _deserialize_ssz(response_data, Status)

    async def _recv_status(self, stream: INetStream) -> None:
        request_data = await _read_request(stream)

        remote_status = _deserialize_ssz(request_data, Status)

        status = self._status_provider()
        response_payload = _serialize_ssz(status, Status)

        await _write_success_response_chunk(stream, response_payload)
        await stream.close()

        peer_id = stream.muxed_conn.peer_id
        await self._peer_updater(peer_id, remote_status)

    async def send_goodbye(self, stream: INetStream, reason: GoodbyeReason) -> None:
        request_payload = _serialize_ssz(reason, ssz.uint64)

        await _write_request(request_payload, stream)
        await stream.close()

    async def _recv_goodbye(self, stream: INetStream) -> None:
        request_data = await _read_request(stream)

        remote_goodbye_value = _deserialize_ssz(request_data, ssz.uint64)
        remote_goodbye_reason = GoodbyeReason(remote_goodbye_value)

        peer_id = stream.muxed_conn.peer_id
        await self._peer_updater(peer_id, remote_goodbye_reason)

    async def get_blocks_by_range(
        self, stream: INetStream, range_request: BlocksByRangeRequest
    ) -> AsyncIterable[SignedBeaconBlock]:
        if range_request.count_expected_blocks() not in VALID_BLOCKS_BY_RANGE_RANGE:
            return

        request_payload = _serialize_ssz(range_request, BlocksByRangeRequest)

        await _write_request(request_payload, stream)
        await stream.close()

        for _ in range(range_request.count):
            response_data = await _read_response(stream)

            if not response_data:
                break

            block = _deserialize_ssz(response_data, SignedBeaconBlock)
            yield block

    async def _recv_beacon_blocks_by_range(self, stream: INetStream) -> None:
        request_data = await _read_request(stream)

        request = _deserialize_ssz(request_data, BlocksByRangeRequest)

        if request.count_expected_blocks() not in VALID_BLOCKS_BY_RANGE_RANGE:
            # TODO signal error to client and possibly adjust reputation internally
            await stream.reset()
            return

        for slot in range(
            request.start_slot, request.start_slot + request.count, request.step
        ):
            block = self._block_provider_by_slot(Slot(slot))

            if not block:
                continue

            response_payload = _serialize_ssz(block, SignedBeaconBlock)

            await _write_success_response_chunk(stream, response_payload)

        await stream.close()

    async def get_blocks_by_root(
        self, stream: INetStream, *roots: Sequence[Root]
    ) -> AsyncIterable[SignedBeaconBlock]:
        if not roots:
            return

        # TODO ensure ssz error if ``len(roots) > MAX_REQUEST_BLOCKS``
        request_payload = _serialize_ssz(
            roots, ssz.List(ROOT_SEDES, MAX_REQUEST_BLOCKS)
        )

        await _write_request(request_payload, stream)
        await stream.close()

        for _ in range(len(roots)):
            response_data = await _read_response(stream)

            if not response_data:
                break

            block = _deserialize_ssz(response_data, SignedBeaconBlock)
            yield block

    async def _recv_beacon_blocks_by_root(self, stream: INetStream) -> None:
        request_data = await _read_request(stream)
        request = _deserialize_ssz(
            request_data, ssz.List(ROOT_SEDES, MAX_REQUEST_BLOCKS)
        )

        for root in request:
            block = self._block_provider_by_root(root)

            if not block:
                continue

            response_payload = _serialize_ssz(block, SignedBeaconBlock)

            await _write_success_response_chunk(stream, response_payload)

        await stream.close()

    async def send_ping(self, stream: INetStream) -> MetaDataSeqNumber:
        metadata = self._metadata_provider()
        request_payload = _serialize_ssz(metadata.seq_number, ssz.uint64)

        await _write_request(request_payload, stream)
        await stream.close()

        response_data = await _read_response(stream)
        return _deserialize_ssz(response_data, ssz.uint64)

    async def _recv_ping(self, stream: INetStream) -> None:
        request_data = await _read_request(stream)
        remote_metadata_seq_number = _deserialize_ssz(request_data, ssz.uint64)

        metadata = self._metadata_provider()
        response_payload = _serialize_ssz(metadata.seq_number, ssz.uint64)

        await _write_success_response_chunk(stream, response_payload)
        await stream.close()

        peer_id = stream.muxed_conn.peer_id
        await self._peer_updater(peer_id, MetaDataSeqNumber(remote_metadata_seq_number))

    async def send_metadata(self, stream: INetStream) -> MetaData:
        # NOTE: empty request
        await stream.close()

        response_data = await _read_response(stream)
        return _deserialize_ssz(response_data, MetaData)

    async def _recv_metadata(self, stream: INetStream) -> None:
        # NOTE: empty request
        # TODO should check for EOF...
        metadata = self._metadata_provider()
        response_payload = _serialize_ssz(metadata, MetaData)

        await _write_success_response_chunk(stream, response_payload)
        await stream.close()
