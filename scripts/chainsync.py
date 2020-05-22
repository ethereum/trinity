"""Run the chain syncer, storing data in the given db dir.

Run with `python -m scripts.chainsync -db <path>`.
"""
import argparse
import asyncio
import logging
from pathlib import Path
import signal
from typing import (
    cast,
    Type,
    Union,
)

from async_service import Service, background_asyncio_service

from eth_typing import BlockNumber

from eth.chains.ropsten import RopstenChain, ROPSTEN_GENESIS_HEADER, ROPSTEN_VM_CONFIGURATION
from eth.chains.mainnet import MainnetChain, MAINNET_GENESIS_HEADER, MAINNET_VM_CONFIGURATION
from eth.db.atomic import AtomicDB
from eth.db.backends.level import LevelDB
from eth.exceptions import HeaderNotFound

from p2p import ecies
from p2p.constants import DEVP2P_V5
from p2p.kademlia import Node

from trinity.config import Eth1ChainConfig
from trinity.constants import DEFAULT_PREFERRED_NODES
from trinity.db.eth1.chain import AsyncChainDB
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.protocol.common.context import ChainContext
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.protocol.les.peer import LESPeerPool

from trinity.sync.full.chain import RegularChainSyncer
from trinity.sync.light.chain import LightChainSyncer
from trinity._utils.chains import load_nodekey
from trinity._utils.version import construct_trinity_client_identifier


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', type=str, required=True)
    parser.add_argument('-light', action="store_true")
    parser.add_argument('-nodekey', type=str)
    parser.add_argument('-enode', type=str, required=False, help="The enode we should connect to")
    parser.add_argument('-debug', action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG

    loop = asyncio.get_event_loop()

    base_db = LevelDB(args.db)
    headerdb = AsyncHeaderDB(AtomicDB(base_db))
    chaindb = AsyncChainDB(AtomicDB(base_db))
    try:
        genesis = chaindb.get_canonical_block_header_by_number(BlockNumber(0))
    except HeaderNotFound:
        genesis = ROPSTEN_GENESIS_HEADER
        chaindb.persist_header(genesis)

    peer_pool_class: Type[Union[ETHPeerPool, LESPeerPool]] = ETHPeerPool
    if args.light:
        peer_pool_class = LESPeerPool

    if genesis.hash == ROPSTEN_GENESIS_HEADER.hash:
        chain_id = RopstenChain.chain_id
        vm_config = ROPSTEN_VM_CONFIGURATION
    elif genesis.hash == MAINNET_GENESIS_HEADER.hash:
        chain_id = MainnetChain.chain_id
        vm_config = MAINNET_VM_CONFIGURATION  # type: ignore
    else:
        raise RuntimeError("Unknown genesis: %s", genesis)

    if args.nodekey:
        privkey = load_nodekey(Path(args.nodekey))
    else:
        privkey = ecies.generate_privkey()

    chain_config = Eth1ChainConfig.from_preconfigured_network(chain_id)
    chain = chain_config.initialize_chain(base_db)
    context = ChainContext(
        headerdb=headerdb,
        network_id=chain_id,
        vm_configuration=vm_config,
        client_version_string=construct_trinity_client_identifier(),
        listen_port=30309,
        p2p_version=DEVP2P_V5,
    )

    peer_pool = peer_pool_class(privkey=privkey, context=context)

    if args.enode:
        nodes = tuple([Node.from_uri(args.enode)])
    else:
        nodes = DEFAULT_PREFERRED_NODES[chain_id]

    async with background_asyncio_service(peer_pool):
        await peer_pool.connect_to_nodes(nodes)
        assert len(peer_pool) == 1
        syncer: Service = None
        if args.light:
            syncer = LightChainSyncer(chain, headerdb, cast(LESPeerPool, peer_pool))
        else:
            syncer = RegularChainSyncer(chain, chaindb, cast(ETHPeerPool, peer_pool))
        logging.getLogger().setLevel(log_level)

        async with background_asyncio_service(syncer) as syncer_manager:
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, syncer_manager.cancel)

            await syncer_manager.wait_finished()


if __name__ == "__main__":
    asyncio.run(_main())
