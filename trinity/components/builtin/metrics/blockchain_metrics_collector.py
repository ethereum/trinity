from typing import (
    NamedTuple,
)

from async_service import (
    as_service,
    ManagerAPI,
)
from eth.typing import ChainGaps
from eth_typing import BlockNumber
from p2p import trio_utils

from lahja import EndpointAPI
from trinity.boot_info import BootInfo
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity._utils.connect import (
    get_eth1_chain_with_remote_db,
    get_eth1_chain_db_with_remote_db,
)


class BlockchainStats(NamedTuple):
    latest_block: int
    header_db_completeness: float
    block_db_completeness: float


def read_blockchain_stats(boot_info: BootInfo, event_bus: EndpointAPI) -> BlockchainStats:
    latest_block_number = read_latest_block_number(boot_info, event_bus)
    header_db_gaps = read_header_db_gaps(boot_info, event_bus)
    block_db_gaps = read_block_db_gaps(boot_info, event_bus)
    return BlockchainStats(
        latest_block=latest_block_number,
        header_db_completeness=calculate_completeness(header_db_gaps, latest_block_number),
        block_db_completeness=calculate_completeness(block_db_gaps, latest_block_number),
    )


def read_latest_block_number(boot_info: BootInfo, event_bus: EndpointAPI) -> BlockNumber:
    with get_eth1_chain_with_remote_db(boot_info, event_bus) as chain:
        return chain.get_canonical_head().block_number


def read_header_db_gaps(boot_info: BootInfo, event_bus: EndpointAPI) -> ChainGaps:
    with get_eth1_chain_db_with_remote_db(boot_info, event_bus) as chaindb:
        return chaindb.get_header_chain_gaps()


def read_block_db_gaps(boot_info: BootInfo, event_bus: EndpointAPI) -> ChainGaps:
    with get_eth1_chain_db_with_remote_db(boot_info, event_bus) as chaindb:
        return chaindb.get_chain_gaps()


def calculate_completeness(gaps: ChainGaps, tip: BlockNumber) -> float:
    # completeness calculated as percentage of imported blocks or headers to tip
    if tip == 0:  # divide by 0 error with fresh db
        return 0

    all_gaps, db_tip = gaps
    sum_of_gaps = sum([(top - bottom) for bottom, top in all_gaps])
    # add missing gap if db tip is behind head
    if db_tip < tip:
        # +1 to gap b/c db_tip is "the first *missing* block number"
        sum_of_gaps += (tip - db_tip + 1)
    return ((tip - sum_of_gaps) / tip) * 100


@as_service
async def collect_blockchain_metrics(manager: ManagerAPI,
                                     boot_info: BootInfo,
                                     event_bus: EndpointAPI,
                                     registry: HostMetricsRegistry,
                                     frequency_seconds: int) -> None:
    blockchain_head_gauge = registry.gauge('trinity.blockchain/head/block.gauge')
    header_db_completeness = registry.gauge('trinity.blockchain/completeness/header_db.gauge')
    block_db_completeness = registry.gauge('trinity.blockchain/completeness/block_db.gauge')

    async for _ in trio_utils.every(frequency_seconds):
        current = read_blockchain_stats(boot_info, event_bus)
        blockchain_head_gauge.set_value(current.latest_block)
        header_db_completeness.set_value(current.header_db_completeness)
        block_db_completeness.set_value(current.block_db_completeness)
