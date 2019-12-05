import argparse
import logging
from pathlib import Path
import uuid

import trio

from eth_utils import DEBUG2_LEVEL_NUM

from async_service import TrioManager

from lahja import ConnectionConfig, TrioEndpoint

from p2p import constants
from p2p import ecies
from p2p import kademlia
from p2p.discovery import DiscoveryService

from trinity.constants import NETWORKING_EVENTBUS_ENDPOINT


def _test() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-bootnode', type=str, help="The enode to use as bootnode")
    parser.add_argument('-debug', action="store_true")
    args = parser.parse_args()

    log_level = logging.DEBUG
    if args.debug:
        log_level = DEBUG2_LEVEL_NUM
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')

    # Listen on a port other than 30303 so that we can test against a local geth instance
    # running on that port.
    listen_port = 30304
    privkey = ecies.generate_privkey()
    addr = kademlia.Address('127.0.0.1', listen_port, listen_port)
    if args.bootnode:
        bootstrap_nodes = tuple([kademlia.Node.from_uri(args.bootnode)])
    else:
        bootstrap_nodes = tuple(
            kademlia.Node.from_uri(enode) for enode in constants.ROPSTEN_BOOTNODES)

    ipc_path = Path(f"networking-{uuid.uuid4()}.ipc")
    networking_connection_config = ConnectionConfig(
        name=NETWORKING_EVENTBUS_ENDPOINT,
        path=ipc_path
    )

    async def run() -> None:
        socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
        await socket.bind(('0.0.0.0', listen_port))
        async with TrioEndpoint.serve(networking_connection_config) as endpoint:
            service = DiscoveryService(privkey, addr, bootstrap_nodes, endpoint, socket)
            await TrioManager.run_service(service)

    trio.run(run)


if __name__ == "__main__":
    _test()
