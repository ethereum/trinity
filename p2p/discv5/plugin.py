import logging
from typing import (
    Any,
    Dict,
    Tuple,
    TypeVar,
)

from eth_utils import (
    decode_hex,
)

import trio
from trio.abc import (
    ReceiveChannel,
    SendChannel,
)

from async_service import (
    Service,
    TrioManager,
)

from p2p.discv5.channel_services import (
    DatagramReceiver,
    DatagramSender,
    IncomingDatagram,
    IncomingMessage,
    IncomingPacket,
    OutgoingDatagram,
    OutgoingMessage,
    OutgoingPacket,
    PacketDecoder,
    PacketEncoder,
)
from p2p.discv5.endpoint_tracker import (
    EndpointTracker,
    EndpointVote,
)
from p2p.discv5.enr import (
    ENR,
)
from p2p.discv5.enr_db import (
    MemoryEnrDb,
)
from p2p.discv5.identity_schemes import (
    default_identity_scheme_registry,
)
from p2p.discv5.message_dispatcher import (
    MessageDispatcher,
)
from p2p.discv5.messages import (
    default_message_type_registry,
)
from p2p.discv5.packer import (
    Packer,
)
from p2p.discv5.routing_table import (
    FlatRoutingTable,
)
from p2p.discv5.routing_table_manager import (
    RoutingTableManager,
)
from p2p.discv5.typing import (
    NodeID,
)

ChannelContentType = TypeVar("ChannelContentType")
ChannelPair = Tuple[SendChannel[ChannelContentType], ReceiveChannel[ChannelContentType]]


class DiscV5Plugin(Service):
    logger = logging.getLogger("p2p.discv5.plugin.DiscV5Plugin")

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

        self.local_private_key = decode_hex(self.config["local_private_key"])
        self.local_node_id = NodeID(decode_hex(self.config["local_node_id"]))

        self.message_type_registry = default_message_type_registry
        self.identity_scheme_registry = default_identity_scheme_registry
        self.routing_table = FlatRoutingTable()
        self.enr_db = MemoryEnrDb(self.identity_scheme_registry)

        outgoing_datagram_channels: ChannelPair[OutgoingDatagram] = trio.open_memory_channel(0)
        incoming_datagram_channels: ChannelPair[IncomingDatagram] = trio.open_memory_channel(0)
        outgoing_packet_channels: ChannelPair[OutgoingPacket] = trio.open_memory_channel(0)
        incoming_packet_channels: ChannelPair[IncomingPacket] = trio.open_memory_channel(0)
        outgoing_message_channels: ChannelPair[OutgoingMessage] = trio.open_memory_channel(0)
        incoming_message_channels: ChannelPair[IncomingMessage] = trio.open_memory_channel(0)
        endpoint_vote_channels: ChannelPair[EndpointVote] = trio.open_memory_channel(0)

        self.socket = trio.socket.socket(
            family=trio.socket.AF_INET,
            type=trio.socket.SOCK_DGRAM,
        )

        # XXX: Ignore typing here because the as_service decorator confuses mypy.
        self.datagram_sender = DatagramSender(  # type: ignore
            outgoing_datagram_channels[1],
            self.socket,
        )
        # XXX: Ignore typing here because the as_service decorator confuses mypy.
        self.datagram_receiver = DatagramReceiver(  # type: ignore
            self.socket,
            incoming_datagram_channels[0],
        )

        # XXX: Ignore typing here because the as_service decorator confuses mypy.
        self.packet_encoder = PacketEncoder(  # type: ignore
            outgoing_packet_channels[1],
            outgoing_datagram_channels[0],
        )
        # XXX: Ignore typing here because the as_service decorator confuses mypy.
        self.packet_decoder = PacketDecoder(  # type: ignore
            incoming_datagram_channels[1],
            incoming_packet_channels[0],
        )

        self.packer = Packer(
            local_private_key=self.local_private_key,
            local_node_id=self.local_node_id,
            enr_db=self.enr_db,
            message_type_registry=self.message_type_registry,
            incoming_packet_receive_channel=incoming_packet_channels[1],
            incoming_message_send_channel=incoming_message_channels[0],
            outgoing_message_receive_channel=outgoing_message_channels[1],
            outgoing_packet_send_channel=outgoing_packet_channels[0],
        )

        self.message_dispatcher = MessageDispatcher(
            enr_db=self.enr_db,
            incoming_message_receive_channel=incoming_message_channels[1],
            outgoing_message_send_channel=outgoing_message_channels[0],
        )

        self.endpoint_tracker = EndpointTracker(
            local_private_key=self.local_private_key,
            local_node_id=self.local_node_id,
            enr_db=self.enr_db,
            identity_scheme_registry=self.identity_scheme_registry,
            vote_receive_channel=endpoint_vote_channels[1],
        )

        self.routing_table_manager = RoutingTableManager(
            local_node_id=self.local_node_id,
            routing_table=self.routing_table,
            message_dispatcher=self.message_dispatcher,
            enr_db=self.enr_db,
            outgoing_message_send_channel=outgoing_message_channels[0],
            endpoint_vote_send_channel=endpoint_vote_channels[0],
        )

        self.services = (
            self.datagram_sender,
            self.datagram_receiver,
            self.packet_encoder,
            self.packet_decoder,
            self.packer,
            self.message_dispatcher,
            self.endpoint_tracker,
            self.routing_table_manager
        )

    async def initialize_enr_db(self, config: Dict[str, Any]) -> None:
        for enr_repr in config["enrs"]:
            enr = ENR.from_repr(enr_repr)
            await self.enr_db.insert(enr)

    def initialize_routing_table(self, config: Dict[str, Any]) -> None:
        for node_id_hex in config["routing_table"]:
            node_id = NodeID(decode_hex(node_id_hex))
            self.routing_table.add(node_id)

    async def run(self) -> None:
        self.logger.info("Running on %s:%d", self.config["host"], self.config["port"])

        await self.initialize_enr_db(self.config)
        self.initialize_routing_table(self.config)

        await self.socket.bind((self.config["host"], self.config["port"]))
        async with trio.open_nursery() as nursery:
            for service in self.services:
                nursery.start_soon(TrioManager.run_service, service)
        await self.manager.wait_finished()
        self.logger.debug("Cancelled")


async def run_discv5(config: Dict[str, Any]) -> None:
    plugin = DiscV5Plugin(config)
    await TrioManager.run_service(plugin)
