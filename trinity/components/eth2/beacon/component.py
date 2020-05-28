from argparse import ArgumentParser, _SubParsersAction
import asyncio
import contextlib
import logging
from typing import Set, Tuple

from lahja import EndpointAPI
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.secp256k1 import create_new_key_pair

from eth2.beacon.typing import SubnetId
from p2p.service import BaseService, run_service
from trinity.boot_info import BootInfo
from trinity.config import BeaconAppConfig, TrinityConfig
from trinity.db.beacon.chain import AsyncBeaconChainDB
from trinity.db.manager import DBClient
from trinity.extensibility import AsyncioIsolatedComponent
from trinity.http.apps.validator_api import ValidatorAPIHandler
from trinity.http.handlers.api_handler import APIHandler
from trinity.http.handlers.metrics_handler import MetricsHandler
from trinity.http.main import HTTPServer
from trinity.http.server import HTTPServer as HTTPAppServer
from trinity.protocol.bcc_libp2p.configs import ATTESTATION_SUBNET_COUNT
from trinity.protocol.bcc_libp2p.node import Node
from trinity.protocol.bcc_libp2p.servers import BCCReceiveServer
from trinity.sync.beacon.chain import BeaconChainSyncer
from trinity.sync.common.chain import SyncBlockImporter

from .chain_maintainer import ChainMaintainer
from .slot_ticker import SlotTicker
from .validator_handler import ValidatorHandler


def _load_secp256k1_key_pair_from(trinity_config: TrinityConfig) -> KeyPair:
    return create_new_key_pair(trinity_config.nodekey.to_bytes())


class BeaconNodeComponent(AsyncioIsolatedComponent):
    name = "Beacon Node"

    logger = logging.getLogger("trinity.components.beacon.BeaconNode")

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        arg_parser.add_argument(
            "--enable-metrics", action="store_true", help="Enables the Metrics Server"
        )
        arg_parser.add_argument(
            "--metrics-port", type=int, help="Metrics server port", default=8008
        )
        arg_parser.add_argument(
            "--debug-libp2p", action="store_true", help="Enable debug logging of libp2p"
        )
        arg_parser.add_argument(
            "--enable-api", action="store_true", help="Enables the API Server"
        )
        arg_parser.add_argument(
            "--api-port", type=int, help="API server port", default=5005
        )
        arg_parser.add_argument(
            "--bn-only", action="store_true", help="Run with BeaconNode only mode"
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
        key_pair = _load_secp256k1_key_pair_from(trinity_config)
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        base_db = DBClient.connect(trinity_config.database_ipc_path)

        if boot_info.args.debug_libp2p:
            logging.getLogger("libp2p").setLevel(logging.DEBUG)
        else:
            logging.getLogger("libp2p").setLevel(logging.INFO)

        with base_db:
            chain_config = beacon_app_config.get_chain_config()
            chain = chain_config.beacon_chain_class(
                base_db, chain_config.genesis_config
            )
            # TODO: To simplify, subsribe all subnets
            subnets: Set[SubnetId] = set(
                SubnetId(subnet_id) for subnet_id in range(ATTESTATION_SUBNET_COUNT)
            )

            # TODO: Handle `bootstrap_nodes`.
            libp2p_node = Node(
                key_pair=key_pair,
                listen_ip="0.0.0.0",
                listen_port=boot_info.args.port,
                preferred_nodes=trinity_config.preferred_nodes,
                chain=chain,
                subnets=subnets,
                event_bus=event_bus,
            )

            receive_server = BCCReceiveServer(
                chain=chain,
                p2p_node=libp2p_node,
                topic_msg_queues=libp2p_node.pubsub.my_topics,
                subnets=subnets,
                cancel_token=libp2p_node.cancel_token,
            )

            chain_maintainer = ChainMaintainer(
                chain=chain, event_bus=event_bus, token=libp2p_node.cancel_token
            )

            validator_handler = ValidatorHandler(
                chain=chain,
                p2p_node=libp2p_node,
                event_bus=event_bus,
                get_ready_attestations_fn=receive_server.get_ready_attestations,
                get_aggregatable_attestations_fn=receive_server.get_aggregatable_attestations,
                import_attestation_fn=receive_server.import_attestation,
                token=libp2p_node.cancel_token,
            )

            slot_ticker = SlotTicker(
                genesis_slot=chain_config.genesis_config.GENESIS_SLOT,
                genesis_time=chain_config.genesis_time,
                seconds_per_slot=chain_config.genesis_config.SECONDS_PER_SLOT,
                event_bus=event_bus,
                token=libp2p_node.cancel_token,
            )

            syncer = BeaconChainSyncer(
                chain_db=AsyncBeaconChainDB(base_db, chain_config.genesis_config),
                peer_pool=libp2p_node.handshaked_peers,
                block_importer=SyncBlockImporter(chain),
                genesis_config=chain_config.genesis_config,
                event_bus=event_bus,
                token=libp2p_node.cancel_token,
            )
            metrics_server = HTTPServer(
                handler=MetricsHandler.handle(chain)(event_bus),
                port=boot_info.args.metrics_port,
            )
            # NOTE: this API server provides an interface into the beacon node
            api_server = HTTPServer(
                handler=APIHandler.handle(chain)(event_bus),
                port=boot_info.args.api_port,
            )

            # NOTE: this API server provides an interface between the beacon node and
            # any connected validator clients.
            validator_api_handler = ValidatorAPIHandler(
                chain, event_bus, chain_config.genesis_time
            )
            validator_api_server = HTTPAppServer(
                routes=validator_api_handler.make_routes(), port=30303
            )

            services: Tuple[BaseService, ...] = (
                libp2p_node,
                receive_server,
                slot_ticker,
                syncer,
                validator_api_server,
            )

            if boot_info.args.enable_metrics:
                services += (metrics_server,)

            if boot_info.args.enable_api:
                services += (api_server,)

            if boot_info.args.bn_only:
                services += (chain_maintainer, validator_handler)

            async with contextlib.AsyncExitStack() as stack:
                for service in services:
                    await stack.enter_async_context(run_service(service))

                await asyncio.gather(*(service.cancellation() for service in services))
