from typing import (
    NamedTuple,
    Tuple,
    Type,
)

from eth.abc import VirtualMachineAPI

from eth_typing.evm import BlockNumber

from eth.chains.mainnet import MAINNET_GENESIS_HEADER, MAINNET_VM_CONFIGURATION
from eth.chains.ropsten import ROPSTEN_GENESIS_HEADER, ROPSTEN_VM_CONFIGURATION
from eth.rlp.headers import BlockHeader

from p2p.constants import (
    MAINNET_BOOTNODES,
    ROPSTEN_BOOTNODES,
)
from trinity.constants import (
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID
)


class Eth1NetworkConfiguration(NamedTuple):

    network_id: int
    chain_name: str
    data_dir_name: str
    eip1085_filename: str
    bootnodes: Tuple[str, ...]
    genesis_header: BlockHeader
    vm_configuration: Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...]


PRECONFIGURED_NETWORKS = {
    MAINNET_NETWORK_ID: Eth1NetworkConfiguration(
        MAINNET_NETWORK_ID,
        'MainnetChain',
        'mainnet',
        'mainnet.json',
        MAINNET_BOOTNODES,
        MAINNET_GENESIS_HEADER,
        MAINNET_VM_CONFIGURATION,
    ),
    ROPSTEN_NETWORK_ID: Eth1NetworkConfiguration(
        ROPSTEN_NETWORK_ID,
        'RopstenChain',
        'ropsten',
        'ropsten.json',
        ROPSTEN_BOOTNODES,
        ROPSTEN_GENESIS_HEADER,
        ROPSTEN_VM_CONFIGURATION,
    ),
}
