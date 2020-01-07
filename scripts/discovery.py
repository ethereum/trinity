import argparse
import logging
from pathlib import Path
from typing import cast
import uuid

import trio

from eth_keys import keys

from eth_utils import DEBUG2_LEVEL_NUM

from async_service import background_trio_service

from lahja import ConnectionConfig, TrioEndpoint

from p2p import constants
from p2p import kademlia
from p2p.discovery import DiscoveryService
from p2p.discv5.typing import NodeID

from trinity.constants import NETWORKING_EVENTBUS_ENDPOINT


class TestDiscoveryService(DiscoveryService):

    async def periodically_refresh(self) -> None:
        await self.manager.wait_finished()

    async def bootstrap(self) -> None:
        return None


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-bootnode', type=str, help="The enode to use as bootnode")
    parser.add_argument('-debug', action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    log_level = logging.INFO
    if args.debug:
        log_level = DEBUG2_LEVEL_NUM
    logging.getLogger('p2p').setLevel(log_level)

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
        bootstrap_nodes = tuple(
            kademlia.Node.from_uri(enode) for enode in constants.ROPSTEN_BOOTNODES)

    ipc_path = Path(f"networking-{uuid.uuid4()}.ipc")
    networking_connection_config = ConnectionConfig(
        name=NETWORKING_EVENTBUS_ENDPOINT,
        path=ipc_path
    )

    socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
    await socket.bind(('0.0.0.0', listen_port))
    async with TrioEndpoint.serve(networking_connection_config) as endpoint:
        service = TestDiscoveryService(privkey, addr, bootstrap_nodes, endpoint, socket)
        service.logger.info("Enode: %s", service.this_node.uri())
        async with background_trio_service(service):
            enr = await service.get_enr(bootstrap_nodes[0])
            assert enr == await service._enr_db.get(cast(NodeID, bootstrap_nodes[0].id_bytes))


if __name__ == "__main__":
    trio.run(main)
