import asyncio
import argparse
import logging

from eth.db.atomic import AtomicDB
from eth.db.backends.level import LevelDB

from cancel_token import CancelToken

from p2p.kademlia import Node

from trinity.protocol import firehose


logger = logging.getLogger('firehose')


async def run_sync(node, state_root, base_db):
    peer = await firehose.connect_to(node.pubkey, node.address.ip, node.address.udp_port)

    asyncio.create_task(peer.run())
    await asyncio.wait_for(peer.events.started.wait(), timeout=1)

    atomic = AtomicDB(base_db)

    #await firehose.simple_get_leaves_sync(atomic, peer, state_root)

    syncer = firehose.ParallelGetLeavesSync(
        atomic, peer, state_root, concurrency=5, target=args.target
    )
    await syncer.run()

    logger.info(f'finished syncing')

    peer.cancel_token.trigger()
    await asyncio.wait_for(peer.events.finished.wait(), timeout=1)


def main(args):
    logger.info(f'attempting to connect to {args.enode}')

    node = Node.from_uri(args.enode)
    logger.info(f'attempting to connect to {node} ({node.address.udp_port})')

    root = bytes.fromhex(args.root)
    logger.info(f'attempting to sync from root: {root.hex()}')

    logger.info(f'data will be saved to {args.db}')
    base_db = LevelDB(db_path=args.db)

    # TODO: cleanly handle KeyboardInterrupt?
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sync(node, root, base_db))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-enode', type=str, required=True
    )
    parser.add_argument(
        '-root', type=str, required=True
    )
    parser.add_argument(
        '-db', type=str, required=True, help="Where to save the received data"
    )
    parser.add_argument(
        '-target', type=float, default=1.0, help="When to stop syncing"
    )
    args = parser.parse_args()

    main(args)
