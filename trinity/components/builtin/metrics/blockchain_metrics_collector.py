import pathlib
from typing import (
    NamedTuple,
)

from async_service import (
    as_service,
    ManagerAPI,
)
from eth_utils import to_int
from p2p import trio_utils
import web3

from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity.config import TrinityConfig


class BlockchainStats(NamedTuple):
    latest_header: int
    latest_block: int
    latest_receipt: int


def read_blockchain_stats(ipc_path: pathlib.Path) -> BlockchainStats:
    if not ipc_path.exists():
        return BlockchainStats(
            latest_header=0,
            latest_block=0,
            latest_receipt=0,
        )

    # Is there a better way to get this data and avoid importing web3
    ipc_provider = web3.IPCProvider(ipc_path)
    response = ipc_provider.make_request('eth_blockNumber', None)

    # PersistentSocket must be closed here to avoid unclosed socket error
    socket = ipc_provider._socket.__enter__()
    socket.close()

    block = to_int(hexstr=response['result'])
    return BlockchainStats(
        latest_header=block,  # ?
        latest_block=block,
        latest_receipt=block,  # ?
    )


@as_service
async def collect_blockchain_metrics(manager: ManagerAPI,
                                     trinity_config: TrinityConfig,
                                     registry: HostMetricsRegistry,
                                     frequency_seconds: int) -> None:
    ipc_path = trinity_config.jsonrpc_ipc_path

    previous: BlockchainStats = None

    blockchain_header_gauge = registry.gauge('trinity.blockchain/head/header.gauge')
    blockchain_block_gauge = registry.gauge('trinity.blockchain/head/block.gauge')
    blockchain_receipt_gauge = registry.gauge('trinity.blockchain/head/receipt.gauge')

    async for _ in trio_utils.every(frequency_seconds):
        if previous is not None:
            current = read_blockchain_stats(ipc_path)
            blockchain_header_gauge.set_value(current.latest_header)
            blockchain_block_gauge.set_value(current.latest_block)
            blockchain_receipt_gauge.set_value(current.latest_receipt)
        previous = current
