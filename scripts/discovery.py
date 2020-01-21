import argparse
import functools
import logging
from pathlib import Path
import uuid

import trio

from eth_keys import keys

from eth_utils import DEBUG2_LEVEL_NUM

from async_service import background_trio_service

from lahja import ConnectionConfig, TrioEndpoint

from eth.db.atomic import AtomicDB
from eth.db.backends.memory import MemoryDB

from p2p import kademlia
from p2p.discovery import DiscoveryService, generate_eth_cap_enr_field
from p2p.discv5.enr_db import FileEnrDb
from p2p.discv5.identity_schemes import default_identity_scheme_registry

from trinity.constants import (
    MAINNET_NETWORK_ID,
    NETWORKING_EVENTBUS_ENDPOINT,
    ROPSTEN_NETWORK_ID
)
from trinity.network_configurations import PRECONFIGURED_NETWORKS
from trinity.db.eth1.header import TrioHeaderDB


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-bootnode', type=str, help="The enode to use as bootnode")
    parser.add_argument(
        '-networkid',
        type=int,
        choices=[ROPSTEN_NETWORK_ID, MAINNET_NETWORK_ID],
        default=ROPSTEN_NETWORK_ID,
        help="1 for mainnet, 3 for testnet"
    )
    parser.add_argument('-l', type=str, help="Log level", default="info")
    parser.add_argument('-enrdb', type=str, help="Path to ENR database")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    if args.l == "debug2":  # noqa: E741
        log_level = DEBUG2_LEVEL_NUM
    else:
        log_level = getattr(logging, args.l.upper())
    logging.getLogger('p2p').setLevel(log_level)

    enr_db_path = Path(args.enrdb)
    enr_db_path.mkdir(exist_ok=True)

    network_cfg = PRECONFIGURED_NETWORKS[args.networkid]
    # Listen on a port other than 30303 so that we can test against a local geth instance
    # running on that port.
    listen_port = 30304
    # Use a hard-coded privkey so that our enode is always the same.
    privkey = keys.PrivateKey(
        b'~\x054{4\r\xd64\x0f\x98\x1e\x85;\xcc\x08\x1eQ\x10t\x16\xc0\xb0\x7f)=\xc4\x1b\xb7/\x8b&\x83')  # noqa: E501
    addr = kademlia.Address('127.0.0.1', listen_port, listen_port)
    if args.bootnode:
        bootstrap_nodes = tuple([kademlia.Node.from_uri(args.bootnode)])
    else:
        bootstrap_nodes = tuple(kademlia.Node.from_uri(enode) for enode in network_cfg.bootnodes)

    ipc_path = Path(f"networking-{uuid.uuid4()}.ipc")
    networking_connection_config = ConnectionConfig(
        name=NETWORKING_EVENTBUS_ENDPOINT,
        path=ipc_path
    )

    headerdb = TrioHeaderDB(AtomicDB(MemoryDB()))
    headerdb.persist_header(network_cfg.genesis_header)
    vm_config = network_cfg.vm_configuration
    enr_field_providers = (functools.partial(generate_eth_cap_enr_field, vm_config, headerdb),)
    enr_db = FileEnrDb(default_identity_scheme_registry, enr_db_path)
    socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
    await socket.bind(('0.0.0.0', listen_port))
    async with TrioEndpoint.serve(networking_connection_config) as endpoint:
        service = DiscoveryService(
            privkey, addr, bootstrap_nodes, endpoint, socket, enr_db, enr_field_providers)
        async with background_trio_service(service):
            await service.manager.wait_finished()


if __name__ == "__main__":
    trio.run(main)
