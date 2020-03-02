import argparse
import functools
import logging
from typing import Set
import uuid

import trio

from lahja import ConnectionConfig, TrioEndpoint

from p2p.events import PeerCandidatesRequest
from p2p.constants import DISCOVERY_EVENTBUS_ENDPOINT
from p2p.discv5.typing import NodeID

from trinity.exceptions import ENRMissingForkID
from trinity.network_configurations import PRECONFIGURED_NETWORKS
from trinity.protocol.eth.forkid import (
    extract_fork_blocks,
    extract_forkid,
)
from trinity.protocol.common.peer import skip_candidate_if_on_list_or_fork_mismatch


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    logger = logging.getLogger()

    parser = argparse.ArgumentParser()
    parser.add_argument('--ipc', type=str, help="The path to DiscoveryService's IPC file")
    args = parser.parse_args()

    # XXX: This is an ugly hack, but it's the easiest way to ensure we use the same network as the
    # DiscoveryService instance we connect to.
    for n_id, network_cfg in PRECONFIGURED_NETWORKS.items():
        if network_cfg.data_dir_name in args.ipc:
            network_id = n_id
            break
    else:
        raise AssertionError("Failed to detect network_id")

    logger.info(f"Asking DiscoveryService for peers on {network_cfg.chain_name}")
    connection_config = ConnectionConfig(DISCOVERY_EVENTBUS_ENDPOINT, args.ipc)
    network_cfg = PRECONFIGURED_NETWORKS[network_id]
    vm_config = network_cfg.vm_configuration
    fork_blocks = extract_fork_blocks(vm_config)
    MAX_PEERS = 60
    skip_list: Set[NodeID] = set()
    async with TrioEndpoint(f"discv4-driver-{uuid.uuid4()}").run() as client:
        with trio.fail_after(2):
            await client.connect_to_endpoints(connection_config)
            await client.wait_until_any_endpoint_subscribed_to(PeerCandidatesRequest)

        while True:
            logger.info("Skip list has %d peers", len(skip_list))
            should_skip = functools.partial(
                skip_candidate_if_on_list_or_fork_mismatch,
                network_cfg.genesis_header.hash,
                network_cfg.genesis_header.block_number,
                fork_blocks,
                skip_list,
            )
            with trio.fail_after(1):
                response = await client.request(PeerCandidatesRequest(MAX_PEERS, should_skip))
            candidates = response.candidates
            missing_forkid = []
            for candidate in candidates:
                try:
                    extract_forkid(candidate.enr)
                except ENRMissingForkID:
                    missing_forkid.append(candidate.id)
            logger.info(
                "Got %d connection candidates, %d of those with a matching ForkID",
                len(candidates),
                len(candidates) - len(missing_forkid),
            )

            # Add candidates with no forkid to the skip list, just so that we keep triggering
            # random discovery lookups and hopefully come across more candidates with
            # compatible forkids
            logger.info("Adding %d candidates with no ForkID to skip list", len(missing_forkid))
            skip_list.update(missing_forkid)
            await trio.sleep(10)


if __name__ == "__main__":
    trio.run(main)
