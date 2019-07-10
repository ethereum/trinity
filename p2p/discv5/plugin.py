import itertools
from typing import (
    NamedTuple,
)

import trio

from eth_utils import (
    ValidationError,
)

from p2p.discv5.packets import (
    decode_packet,
)


DATAGRAM_BUFFER_SIZE = 2048


class Endpoint(NamedTuple):
    ip_address: bytes
    port: int


class IncomingDatagram(NamedTuple):
    datagram: bytes
    sender: Endpoint


class IncomingPacket(NamedTuple):
    packet: Packet
    sender: Endpoint


class IncomingMessage(NamedTuple):
    message: BaseMessage
    sender: Endpoint
    node_id: Hash32


class OutgoingMessage(NamedTuple):
    message: BaseMessage
    receiver: Endpoint
    node_id: Hash32


class OutgoingPacket(NamedTuple):
    packet: Packet
    receiver: Endpoint


class OutgoingDatagram(NamedTuple):
    datagram: bytes
    receiver: Endpoint


async def receive_datagrams(socket, incoming_datagram_send_channel):
    async with incoming_datagram_send_channel:
        while True:
            datagram, address = await socket.recvfrom(DATAGRAM_BUFFER_SIZE)
            await incoming_datagram_send_channel.send(IncomingDatagram(datagram, sender))


async def decode_packets(incoming_datagram_receive_channel, incoming_packet_send_channel):
    async with incoming_datagram_receive_channel:
        async with incoming_packet_send_channel:
            async for datagram, sender in incoming_datagram_receive_channel:
                try:
                    packet = decode_packet(datagram)
                except ValidationError as error:
                    pass
                else:
                    await incoming_packet_send_channel.send(IncomingPacket(packet, sender))


async def encode_packets(outgoing_packet_receive_channel, outgoing_datagram_send_channel):
    async with outgoing_packet_receive_channel:
        async with outgoing_datagram_send_channel:
            async for packet, receiver in outgoing_packet_receive_channel:
                receiver_datagram = OutgoingPacket(packet.to_wire_bytes(), receiver)
                await outgoing_datagram_send_channel.send(receiver_datagram)


async def send_datagrams(socket, outgoing_datagram_receive_channel):
    async with outgoing_datagram_receive_channel:
        for datagram, receiver in outgoing_datagram_receive_channel:
            socket.sendto(datagram, receiver)


class DiscV5Plugin:

    def __init__(self):
        self._socket = trio.socket.socket(
            family=trio.socket.AF_INET,
            type=trio.socket.SOCK_DGRAM,
        )

        (
            self._incoming_datagram_send_channel,
            self._incoming_datagram_receive_channel,
        ) = trio.open_memory_channel()
        (
            self._incoming_packet_send_channel,
            self._incoming_packet_receive_channel,
        ) = trio.open_memory_channel()
        (
            self._incoming_message_send_channel,
            self._incoming_message_receive_channel,
        ) = trio.open_memory_channel()
        (
            self._outgoing_message_send_channel,
            self._outgoing_message_receive_channel,
        ) = trio.open_memory_channel()
        (
            self._outgoing_packet_send_channel,
            self._outgoing_packet_receive_channel,
        ) = trio.open_memory_channel()
        (
            self._outgoing_datagram_send_channel,
            self._outgoing_datagram_receive_channel,
        ) = trio.open_memory_channel()

        self._packer = Packer(
            incoming_packet_receive_channel=self._incoming_packet_receive_channel,
            outgoing_packet_send_channel=self._outgoing_packet_send_channel,
            incoming_message_send_channel=self._incoming_message_send_channel,
            outgoing_message_receive_channel=self.outgoing_message_receive_channel,
        )
        self._message_dispatcher = MessageDispatcher(
            incoming_message_receive_channel=self._incoming_message_receive_channel,
            outgoing_message_send_channel=self._outgoing_message_send_channel,
        )

        # additional classes that contain the actual logic and only interact with the message
        # dispatcher

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._message_dispatcher.run)
            nursery.start_soon(self._packer.run)
            nursery.start_soon(
                encode_packets,
                self._outgoing_packet_receive_channel,
                self._outgoing_datagram_send_channel,
            )
            nursery.start_soon(
                decode_packets,
                self._incoming_datagram_receive_channel,
                self._incoming_packet_send_channel,
            )
            nursery.start_soon(
                send_datagrams,
                self._socket,
                self._outgoing_datagram_receive_channel,
            )
            nursery.start_soon(
                receive_datagrams,
                self._socket,
                self._incoming_datagram_send_channel,
            )


class Packer:
    """Dispatches packets to a responsible PeerPacker, launching new ones for new connections."""

    def __init__(self,
                 incoming_packet_receive_channel,
                 outgoing_packet_send_channel,
                 incoming_message_send_channel,
                 outgoing_message_receive_channel):
        self._incoming_packet_receive_channel = incoming_packet_receive_channel
        self._outgoing_packet_send_channel = outgoing_packet_send_channel
        self._incoming_message_send_channel = incoming_message_send_channel
        self._outgoing_message_receive_channel = outgoing_message_receive_channel

        self._peer_packers = {}
        self.peer_incoming_packet_send_channels = {}
        self.peer_outgoing_message_send_channels = {}

    async def _run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._dispatch_outgoing_messages, nursery)
            nursery.start_soon(self._dispatch_incoming_packets, nursery)

    async def _dispatch_outgoing_messages(self, nursery):
        async with self._outgoing_message_receive_channel:
            async with self._outgoing_packet_send_channel:
                async for outgoing_message in self._outgoing_message_receive_channel:
                    node_id = outgoing_message.node_id
                    if node_id not in self._peer_packers:
                        self._init_peer_packer(node_id)
                        nursery.start_soon(self._run_peer_packer)
                    await self.peer_outgoing_message_send_channels[node_id].send(outgoing_message)

    async def _dispatch_incoming_packets(self, nursery):
        async with self._incoming_packet_receive_channel:
            async with self._incoming_message_send_channel:
                async for incoming_packet in self._incoming_packet_receive_channel:
                    node_id = incoming_packet.node_id
                    if node_id not in self._peer_packers:
                        self._init_peer_packer(node_id)
                        nursery.start_soon(self._run_peer_packer)
                    await self.peer_incoming_packet_send_channels[node_id].send(incoming_packet)

    def _init_peer_packer(self, node_id):
        if node_id in self._peer_packers:
            raise ValueError(f"Peer packer with node id {node_id} is already initialized")

        incoming_packet_send_channel, incoming_packet_receive_channel = trio.open_memory_channel()
        outgoing_message_send_channel, outgoing_message_receive_channel = trio.open_memory_channel()

        peer_packer = PeerPacker(
            our_node_id=self._our_node_id,
            incoming_packet_receive_channel=incoming_packet_receive_channel,
            outgoing_packet_send_channel=self._outgoing_packet_send_channel,
            incoming_message_send_channel=self._incoming_message_send_channel,
            outgoing_message_receive_channel=outgoing_message_receive_channel,
        )

        self._peer_packers[node_id] = peer_packer
        self._peer_incoming_packet_send_channels[node_id] = incoming_packet_send_channel
        self._peer_outgoing_message_send_channels[node_id] = outgoing_message_send_channel

    async def _run_peer_packer(self, node_id):
        if node_id in self._peer_packers:
            raise ValueError(f"Peer packer with node id {node_id} hasn't been initialized yet")

        peer_packer = self._peer_packers[node_id]
        incoming_packet_send_channel = self._peer_incoming_packet_send_channels[node_id]
        outgoing_message_send_channel = self._peer_outgoing_message_send_channels[node_id]

        try:
            async with incoming_packet_send_channel:
                async with outgoing_message_send_channel:
                    await peer_packer.run()
        finally:
            self._peer_packers.pop(node_id)
            self._incoming_packet_send_channels.pop(node_id)
            self._outgoing_message_send_channel.pop(node_id)


class PeerPacker:
    """Packs and unpacks messages to/from a specific peer.

    Responsible for performing the handshake, and repeating it if necessary.
    """

    def __init__(self,
                 our_node_id,
                 incoming_packet_receive_channel,
                 outgoing_packet_send_channel,
                 incoming_message_send_channel,
                 outgoing_message_receive_channel,
                 message_type_registry,
                 ):
        self._our_node_id = our_node_id

        self._incoming_packet_receive_channel = incoming_packet_receive_channel
        self._outgoing_packet_send_channel = outgoing_packet_send_channel
        self._incoming_message_send_channel = incoming_message_send_channel
        self._outgoing_message_receive_channel = outgoing_message_receive_channel

        self._message_type_registry = message_type_registry

        self._pending_requests = {}  # messages we might have to resend again

        # and handshake data

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._pack_outgoing_messages)
            nursery.start_soon(self._unpack_incoming_packets)


class MessageDispatcher:
    """Entry point at which one can register message handlers and through which send requests."""

    def __init__(self, incoming_message_receive_channel, outgoing_message_send_channel):
        self._incoming_message_receive_channel = incoming_message_receive_channel
        self._outgoing_message_send_channel = outgoing_message_send_channel

        self._message_handlers = {}
        self._message_handler_counter = itertools.count()

    async def run(self):
        async with self._incoming_message_receive_channel:
            async for incoming_message in self._incoming_message_receive_channel:
                registered_channels = tuple(
                    channel for predicate, channel in self._message_handlers.values()
                    if predicate(incoming_message)
                )
                for channel in registered_channels:
                    await channel.send(incoming_message)

    def register_message_handler(self, predicate, incoming_message_send_channel):
        handler_id = next(self._message_handler_counter)
        self._message_handlers[handler_id] = (predicate, incoming_message_send_channel)

    def deregister_message_handler(self, handler_id):
        self._message_handlers.pop(handler_id)

    async def request(self, outgoing_message):
        def response_predicate(incoming_message):
            return all(
                incoming_message.node_id == outgoing_message.node_id,
                incoming_message.message.request_id == outgoing_message.message.request_id
            )

        incoming_message_send_channel, incoming_message_receive_channel = trio.open_memory_channel()
        response_handler_id = self.register_message_handler(
            response_predicate,
            incoming_message_send_channel,
        )

        with incoming_message_receive_channel:
            response = await anext(incoming_message_receive_channel)

        self.deregister_message_handler(response_handler_id)

        return response
