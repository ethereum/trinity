import asyncio
import logging
import platform
from typing import ContextManager

import websockets

from async_service import ManagerAPI, Service
from lahja import EndpointAPI

from eth.abc import ChainAPI

from trinity.constants import (
    TO_NETWORKING_BROADCAST_CONFIG,
)
from trinity._utils.connect import get_eth1_chain_with_remote_db
from trinity._utils.version import (
    construct_trinity_client_identifier,
)

from trinity.boot_info import BootInfo
from trinity.components.builtin.ethstats.ethstats_client import (
    EthstatsClient,
    EthstatsMessage,
    EthstatsData,
    timestamp_ms,
)
from trinity.protocol.common.events import (
    PeerCountRequest,
)


class EthstatsService(Service):
    logger = logging.getLogger('trinity.components.ethstats.Service')

    def __init__(
        self,
        boot_info: BootInfo,
        event_bus: EndpointAPI,
        server_url: str,
        server_secret: str,
        node_id: str,
        node_contact: str,
        stats_interval: int,
    ) -> None:
        self.boot_info = boot_info
        self.event_bus = event_bus

        self.server_url = server_url
        self.server_secret = server_secret
        self.node_id = node_id
        self.node_contact = node_contact
        self.stats_interval = stats_interval

    async def run(self) -> None:
        with self.get_chain() as chain:
            self.chain = chain

            while self.manager.is_running:
                self.logger.info('Connecting to %s...', self.server_url)

                async with websockets.connect(self.server_url) as websocket:
                    client: EthstatsClient = EthstatsClient(
                        websocket,
                        self.node_id,
                    )

                    client_manager = self.manager.run_child_service(client)

                    self.manager.run_task(self.server_handler, client, client_manager)
                    self.manager.run_task(self.statistics_handler, client, client_manager)

                    await client_manager.wait_finished()

                self.logger.info('Connection to %s closed', self.server_url)
                self.logger.info('Reconnecting in 5s...')
                await asyncio.sleep(5)

    # Wait for messages from server, respond when they arrive
    async def server_handler(self, client: EthstatsClient, manager: ManagerAPI) -> None:
        while manager.is_running:
            message: EthstatsMessage = await client.recv()

            if message.command == 'node-pong':
                await client.send_latency((timestamp_ms() - message.data['clientTime']) // 2)
            else:
                self.logger.debug('Unhandled message received: %s: %r', message.command, message)

    # Periodically send statistics and ping server to calculate latency
    async def statistics_handler(self, client: EthstatsClient, manager: ManagerAPI) -> None:
        await client.send_hello(self.server_secret, self.get_node_info())

        while manager.is_running:
            await client.send_node_ping()
            await client.send_stats(await self.get_node_stats())
            await client.send_block(self.get_node_block())

            await asyncio.sleep(self.stats_interval)

    def get_node_info(self) -> EthstatsData:
        """Getter for data that should be sent once, on start-up."""
        return {
            'name': self.node_id,
            'contact': self.node_contact,
            'node': construct_trinity_client_identifier(),
            'net': self.boot_info.trinity_config.network_id,
            'port': self.boot_info.trinity_config.port,
            'os': platform.system(),
            'os_v': platform.release(),
            'client': '0.1.1',
            'canUpdateHistory': False,
        }

    def get_node_block(self) -> EthstatsData:
        """Getter for data that should be sent on every new chain tip change."""
        head = self.chain.get_canonical_head()

        return {
            'number': head.block_number,
            'hash': head.hex_hash,
            'difficulty': head.difficulty,
            'totalDifficulty': self.chain.get_score(head.hash),
            'transactions': [],
            'uncles': [],
        }

    async def get_node_stats(self) -> EthstatsData:
        """Getter for data that should be sent periodically."""
        try:
            peer_count = (await asyncio.wait_for(
                self.event_bus.request(
                    PeerCountRequest(),
                    TO_NETWORKING_BROADCAST_CONFIG,
                ),
                timeout=1
            )).peer_count
        except asyncio.TimeoutError:
            self.logger.warning("Timeout: PeerPool did not answer PeerCountRequest")
            peer_count = 0

        return {
            'active': True,
            'uptime': 100,
            'peers': peer_count,
        }

    def get_chain(self) -> ContextManager[ChainAPI]:
        return get_eth1_chain_with_remote_db(self.boot_info, self.event_bus)
