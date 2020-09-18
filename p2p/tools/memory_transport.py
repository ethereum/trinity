import asyncio
import struct
from typing import Tuple

from cached_property import cached_property
from eth_keys import datatypes

from p2p.abc import MessageAPI, NodeAPI, TransportAPI
from p2p import constants
from p2p.exceptions import PeerConnectionLost
from p2p.message import Message
from p2p.session import Session
from p2p.tools.asyncio_streams import get_directly_connected_streams
from p2p._utils import get_logger


CONNECTION_LOST_ERRORS = (
    asyncio.IncompleteReadError,
    ConnectionResetError,
    BrokenPipeError,
)


class MemoryTransport(TransportAPI):

    def __init__(self,
                 remote: NodeAPI,
                 private_key: datatypes.PrivateKey,
                 reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.logger = get_logger('p2p.tools.memory_transport.MemoryTransport')
        self.remote = remote
        self.session = Session(remote)
        self._private_key = private_key
        self._reader = reader
        self._writer = writer

    @classmethod
    def connected_pair(cls,
                       alice: Tuple[NodeAPI, datatypes.PrivateKey],
                       bob: Tuple[NodeAPI, datatypes.PrivateKey],
                       ) -> Tuple[TransportAPI, TransportAPI]:
        (
            (alice_reader, alice_writer),
            (bob_reader, bob_writer),
        ) = get_directly_connected_streams()
        alice_remote, alice_private_key = alice
        bob_remote, bob_private_key = bob
        alice_transport = cls(
            alice_remote,
            alice_private_key,
            alice_reader,
            alice_writer,
        )
        bob_transport = cls(bob_remote, bob_private_key, bob_reader, bob_writer)
        return alice_transport, bob_transport

    @cached_property
    def public_key(self) -> datatypes.PublicKey:
        return self._private_key.public_key

    async def read(self, n: int) -> bytes:
        self.logger.debug2("Waiting for %s bytes from %s", n, self.remote)
        try:
            return await asyncio.wait_for(
                self._reader.readexactly(n), timeout=constants.CONN_IDLE_TIMEOUT)
        except CONNECTION_LOST_ERRORS as err:
            raise PeerConnectionLost from err

    def write(self, data: bytes) -> None:
        self._writer.write(data)

    async def recv(self) -> MessageAPI:
        encoded_sizes = await self.read(8)
        header_size, body_size = struct.unpack('>II', encoded_sizes)
        header = await self.read(header_size)
        body = await self.read(body_size)
        return Message(header, body)

    def send(self, message: MessageAPI) -> None:
        header_size = len(message.header)
        body_size = len(message.body)

        encoded_sizes = struct.pack('>II', header_size, body_size)

        if self.is_closing:
            self.logger.error(
                f"Attempted to send msg with cmd id {message.command_id} to "
                f"disconnected peer {self.remote}"
            )
            return
        self.write(encoded_sizes + message.header + message.body)

    async def close(self) -> None:
        """
        Close this transport's writer stream.
        """
        await self._writer.drain()
        self._writer.close()

    @property
    def is_closing(self) -> bool:
        return self._writer.transport.is_closing()
