import asyncio
import argparse
import logging

from cancel_token import CancelToken

from p2p.kademlia import Node

from trinity.protocol import firehose


logger = logging.getLogger('firehose')


async def run_sync(node, state_root):
    peer = await firehose.connect_to(node.pubkey, node.address.ip, node.address.udp_port)

    asyncio.create_task(peer.run())
    await asyncio.wait_for(peer.events.started.wait(), timeout=1)

    result = await peer.requests.get_node_chunk(
        state_root, prefix=(0,), timeout=1
    )
    logger.info(f'received result with {len(result["nodes"])} nodes')

    peer.cancel_token.trigger()
    await asyncio.wait_for(peer.events.finished.wait(), timeout=1)


def main(args):
    logger.info(f'attempting to connect to {args.enode}')

    node = Node.from_uri(args.enode)
    logger.info(f'attempting to connect to {node} ({node.address.udp_port})')

    root = bytes.fromhex(args.root)
    logger.info(f'attempting to sync from root: {root.hex()}')

    # TODO: cleanly handle KeyboardInterrupt?
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sync(node, root))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-enode', type=str, required=True
    )
    parser.add_argument(
        '-root', type=str, required=True
    )
    args = parser.parse_args()

    main(args)
