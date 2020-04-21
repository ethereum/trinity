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
    latest_header: int
    latest_block: int
    latest_receipt: int


def read_blockchain_stats(boot_info: BootInfo, event_bus: EndpointAPI) -> BlockchainStats:
    with get_eth1_chain_with_remote_db(boot_info, event_bus) as chain:
        latest_block_number = chain.get_canonical_head().block_number

    return BlockchainStats(
        latest_header=latest_block_number,
        latest_block=latest_block_number,
        latest_receipt=latest_block_number,
    )


@as_service
async def collect_blockchain_metrics(manager: ManagerAPI,
                                     boot_info: BootInfo,
                                     event_bus: EndpointAPI,
                                     registry: HostMetricsRegistry,
                                     frequency_seconds: int) -> None:
    blockchain_header_gauge = registry.gauge('trinity.blockchain/head/header.gauge')
    blockchain_block_gauge = registry.gauge('trinity.blockchain/head/block.gauge')
    blockchain_receipt_gauge = registry.gauge('trinity.blockchain/head/receipt.gauge')

    async for _ in trio_utils.every(frequency_seconds):
        current = read_blockchain_stats(boot_info, event_bus)
        blockchain_header_gauge.set_value(current.latest_header)
        blockchain_block_gauge.set_value(current.latest_block)
        blockchain_receipt_gauge.set_value(current.latest_receipt)
