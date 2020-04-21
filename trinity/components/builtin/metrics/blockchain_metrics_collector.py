from typing import (
    NamedTuple,
)

from async_service import (
    as_service,
    ManagerAPI,
)
from p2p import trio_utils

from lahja import EndpointAPI
from trinity.boot_info import BootInfo
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity._utils.connect import get_eth1_chain_with_remote_db


class BlockchainStats(NamedTuple):
    latest_block: int


def read_blockchain_stats(boot_info: BootInfo, event_bus: EndpointAPI) -> BlockchainStats:
    with get_eth1_chain_with_remote_db(boot_info, event_bus) as chain:
        latest_block_number = chain.get_canonical_head().block_number

    return BlockchainStats(
        latest_block=latest_block_number,
    )


@as_service
async def collect_blockchain_metrics(manager: ManagerAPI,
                                     boot_info: BootInfo,
                                     event_bus: EndpointAPI,
                                     registry: HostMetricsRegistry,
                                     frequency_seconds: int) -> None:
    blockchain_block_gauge = registry.gauge('trinity.blockchain/head/block.gauge')

    async for _ in trio_utils.every(frequency_seconds):
        current = read_blockchain_stats(boot_info, event_bus)
        blockchain_block_gauge.set_value(current.latest_block)
