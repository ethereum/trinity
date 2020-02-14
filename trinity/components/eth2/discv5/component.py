from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import logging
import pathlib
import secrets

import async_service

from eth_keys.datatypes import (
    PrivateKey,
)
from eth_utils import (
    decode_hex,
    encode_hex,
)
from eth_utils.toolz import (
    merge,
)

from lahja import EndpointAPI

from eth.db.backends.level import LevelDB

from p2p.discv5.constants import (
    NUM_ROUTING_TABLE_BUCKETS,
)
from p2p.discv5.abc import (
    NodeDBAPI,
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
from p2p.discv5.enr import ENR
from p2p.discv5.enr import UnsignedENR
from p2p.discv5.enr_db import NodeDB
from p2p.discv5.identity_schemes import default_identity_scheme_registry
from p2p.discv5.message_dispatcher import (
    MessageDispatcher,
)
from p2p.discv5.messages import default_message_type_registry
from p2p.discv5.packer import (
    Packer,
)
from p2p.discv5.routing_table import (
    KademliaRoutingTable,
)
from p2p.discv5.routing_table_manager import (
    RoutingTableManager,
)

from trinity.boot_info import BootInfo
from trinity.extensibility import TrioIsolatedComponent

import trio


DEFAULT_NODEDB_DIR_NAME = "nodes"


logger = logging.getLogger(__name__)


def get_nodedb_dir(boot_info: BootInfo) -> pathlib.Path:
    if boot_info.args.nodedb_dir is None:
        return boot_info.trinity_config.data_dir / DEFAULT_NODEDB_DIR_NAME
    else:
        return pathlib.Path(boot_info.args.nodedb_dir)


def get_local_private_key(boot_info: BootInfo) -> PrivateKey:
    if boot_info.args.discovery_private_key:
        local_private_key_bytes = decode_hex(boot_info.args.discovery_private_key)
    else:
        logger.debug("No private key given, using random one")
        local_private_key_bytes = secrets.token_bytes(32)
    return PrivateKey(local_private_key_bytes)


async def get_local_enr(boot_info: BootInfo,
                        node_db: NodeDBAPI,
                        local_private_key: PrivateKey,
                        ) -> ENR:
    minimal_enr = UnsignedENR(
        sequence_number=1,
        kv_pairs={
            b"id": b"v4",
            b"secp256k1": local_private_key.public_key.to_compressed_bytes(),
            b"udp": boot_info.args.discovery_port,
        },
        identity_scheme_registry=default_identity_scheme_registry,
    ).to_signed_enr(local_private_key.to_bytes())
    node_id = minimal_enr.node_id

    try:
        base_enr = node_db.get_enr(node_id)
    except KeyError:
        logger.info(f"No Node for {encode_hex(node_id)} found, creating new one")
        return minimal_enr
    else:
        if any(base_enr[key] != value for key, value in minimal_enr.items()):
            logger.debug(f"Updating local ENR")
            return UnsignedENR(
                sequence_number=base_enr.sequence_number + 1,
                kv_pairs=merge(dict(base_enr), dict(minimal_enr)),
                identity_scheme_registry=default_identity_scheme_registry,
            ).to_signed_enr(local_private_key.to_bytes())
        else:
            return base_enr


class DiscV5Component(TrioIsolatedComponent):
    name = "DiscV5"

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        discovery_parser = arg_parser.add_argument_group("discovery")

        discovery_parser.add_argument(
            "--enr-dir",
            help="Path to the directory in which ENRs are stored",
        )
        discovery_parser.add_argument(
            "--discovery-boot-enrs",
            nargs="+",
            help="An arbitrary number of ENRs to populate the initial routing table with",
        )
        discovery_parser.add_argument(
            "--discovery-port",
            help="UDP port on which to listen for discovery messages",
            type=int,
            default=9000,
        )
        discovery_parser.add_argument(
            "--discovery-private-key",
            help="hex encoded 32 byte private key representing the discovery network identity",
        )

    @property
    def is_enabled(self) -> bool:
        return True

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        identity_scheme_registry = default_identity_scheme_registry
        message_type_registry = default_message_type_registry

        nodedb_dir = get_nodedb_dir(boot_info)
        nodedb_dir.mkdir(exist_ok=True)
        node_db = NodeDB(default_identity_scheme_registry, LevelDB(nodedb_dir))

        local_private_key = get_local_private_key(boot_info)
        local_enr = await get_local_enr(boot_info, node_db, local_private_key)
        local_node_id = local_enr.node_id

        routing_table = KademliaRoutingTable(local_node_id, NUM_ROUTING_TABLE_BUCKETS)

        node_db.set_enr(local_enr)
        for enr_repr in boot_info.args.discovery_boot_enrs or ():
            enr = ENR.from_repr(enr_repr)
            node_db.set_enr(enr)
            routing_table.update(enr.node_id)

        port = boot_info.args.discovery_port

        socket = trio.socket.socket(
            family=trio.socket.AF_INET,
            type=trio.socket.SOCK_DGRAM,
        )
        outgoing_datagram_channels = trio.open_memory_channel[OutgoingDatagram](0)
        incoming_datagram_channels = trio.open_memory_channel[IncomingDatagram](0)
        outgoing_packet_channels = trio.open_memory_channel[OutgoingPacket](0)
        incoming_packet_channels = trio.open_memory_channel[IncomingPacket](0)
        outgoing_message_channels = trio.open_memory_channel[OutgoingMessage](0)
        incoming_message_channels = trio.open_memory_channel[IncomingMessage](0)
        endpoint_vote_channels = trio.open_memory_channel[EndpointVote](0)

        # types ignored due to https://github.com/ethereum/async-service/issues/5
        datagram_sender = DatagramSender(  # type: ignore
            outgoing_datagram_channels[1],
            socket,
        )
        datagram_receiver = DatagramReceiver(  # type: ignore
            socket,
            incoming_datagram_channels[0],
        )

        packet_encoder = PacketEncoder(  # type: ignore
            outgoing_packet_channels[1],
            outgoing_datagram_channels[0],
        )
        packet_decoder = PacketDecoder(  # type: ignore
            incoming_datagram_channels[1],
            incoming_packet_channels[0],
        )

        packer = Packer(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=local_node_id,
            node_db=node_db,
            message_type_registry=message_type_registry,
            incoming_packet_receive_channel=incoming_packet_channels[1],
            incoming_message_send_channel=incoming_message_channels[0],
            outgoing_message_receive_channel=outgoing_message_channels[1],
            outgoing_packet_send_channel=outgoing_packet_channels[0],
        )

        message_dispatcher = MessageDispatcher(
            node_db=node_db,
            incoming_message_receive_channel=incoming_message_channels[1],
            outgoing_message_send_channel=outgoing_message_channels[0],
        )

        endpoint_tracker = EndpointTracker(
            local_private_key=local_private_key.to_bytes(),
            local_node_id=local_node_id,
            node_db=node_db,
            identity_scheme_registry=identity_scheme_registry,
            vote_receive_channel=endpoint_vote_channels[1],
        )

        routing_table_manager = RoutingTableManager(
            local_node_id=local_node_id,
            routing_table=routing_table,
            message_dispatcher=message_dispatcher,
            node_db=node_db,
            outgoing_message_send_channel=outgoing_message_channels[0],
            endpoint_vote_send_channel=endpoint_vote_channels[0],
        )

        logger.info(f"Starting discovery, listening on port {port}")
        logger.info(f"Local Node ID: {encode_hex(local_enr.node_id)}")
        logger.info(f"Local ENR: {local_enr}")

        await socket.bind(("0.0.0.0", port))
        services = (
            datagram_sender,
            datagram_receiver,
            packet_encoder,
            packet_decoder,
            packer,
            message_dispatcher,
            endpoint_tracker,
            routing_table_manager,
        )
        async with trio.open_nursery() as nursery:
            for service in services:
                nursery.start_soon(async_service.TrioManager.run_service, service)
