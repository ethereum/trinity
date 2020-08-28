"""Connect to the given enode and request some arbitrary data from it.

Run with `python -m scripts.peer -enode <enode>`.

By default connects as a ROPSTEN peer using the ETH protocol.
"""
import argparse
import asyncio
import logging
import signal
from typing import (
    cast,
    Tuple,
    Type,
    Union,
)
from async_service.asyncio import background_asyncio_service

from eth_typing import BlockNumber
from eth_utils import DEBUG2_LEVEL_NUM

from eth.chains.mainnet import MainnetChain, MAINNET_GENESIS_HEADER, MAINNET_VM_CONFIGURATION
from eth.chains.ropsten import RopstenChain, ROPSTEN_GENESIS_HEADER, ROPSTEN_VM_CONFIGURATION
from eth.db.atomic import AtomicDB
from eth.db.backends.memory import MemoryDB

from p2p import ecies
from p2p.constants import DEVP2P_V5, MAINNET_BOOTNODES, ROPSTEN_BOOTNODES
from p2p.kademlia import Node

from trinity.db.eth1.header import AsyncHeaderDB
from trinity.protocol.common.context import ChainContext
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.les.peer import LESPeer, LESPeerPool
from trinity._utils.version import construct_trinity_client_identifier


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-enode', type=str, help="The enode we should connect to", required=True)
    parser.add_argument('-mainnet', action='store_true')
    parser.add_argument('-light', action='store_true', help="Connect as a light node")
    parser.add_argument('-debug', action="store_true")
    args = parser.parse_args()

    log_level = logging.INFO
    if args.debug:
        log_level = DEBUG2_LEVEL_NUM
    logging.basicConfig(
        level=log_level, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    peer_class: Union[Type[ETHPeer], Type[LESPeer]]
    pool_class: Union[Type[ETHPeerPool], Type[LESPeerPool]]

    if args.light:
        peer_class = LESPeer
        pool_class = LESPeerPool
    else:
        peer_class = ETHPeer
        pool_class = ETHPeerPool

    bootnodes: Tuple[str, ...]
    if args.mainnet:
        bootnodes = MAINNET_BOOTNODES
        chain_id = MainnetChain.chain_id
        vm_config = MAINNET_VM_CONFIGURATION
        genesis = MAINNET_GENESIS_HEADER
    else:
        bootnodes = ROPSTEN_BOOTNODES
        chain_id = RopstenChain.chain_id
        vm_config = ROPSTEN_VM_CONFIGURATION
        genesis = ROPSTEN_GENESIS_HEADER

    headerdb = AsyncHeaderDB(AtomicDB(MemoryDB()))
    headerdb.persist_header(genesis)
    loop = asyncio.get_event_loop()
    if args.enode == "bootnodes":
        nodes = [Node.from_uri(enode) for enode in bootnodes]
    else:
        nodes = [Node.from_uri(args.enode)]

    context = ChainContext(
        headerdb=headerdb,
        network_id=chain_id,
        vm_configuration=vm_config,
        client_version_string=construct_trinity_client_identifier(),
        listen_port=30309,
        p2p_version=DEVP2P_V5,
    )
    peer_pool = pool_class(privkey=ecies.generate_privkey(), context=context)

    async def request_stuff() -> None:
        nonlocal peer_pool
        # Request some stuff from ropsten's block 2440319
        # (https://ropsten.etherscan.io/block/2440319), just as a basic test.
        peer = peer_pool.highest_td_peer
        if peer_class == ETHPeer:
            peer = cast(ETHPeer, peer)
            headers = await peer.eth_api.get_block_headers(BlockNumber(2440319), max_headers=100)
            hashes = tuple(header.hash for header in headers)
            peer.eth_api.send_get_block_bodies(hashes)
            peer.eth_api.send_get_receipts(hashes)
        else:
            peer = cast(LESPeer, peer)
            headers = await peer.les_api.get_block_headers(BlockNumber(2440319), max_headers=100)
            peer.les_api.send_get_block_bodies(list(hashes))
            peer.les_api.send_get_receipts(hashes[:1])

    async with background_asyncio_service(peer_pool) as manager:
        for sig in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(sig, manager.cancel)

        await peer_pool.connect_to_nodes(nodes)
        await asyncio.sleep(1)
        if len(peer_pool) == 0:
            peer_pool.logger.error(f"Unable to connect to any of {nodes}")
            return

        try:
            await asyncio.wait_for(request_stuff(), timeout=2)
        except asyncio.TimeoutError:
            peer_pool.logger.error("Timeout waiting for replies")
        await manager.wait_finished()


if __name__ == "__main__":
    asyncio.run(_main())
