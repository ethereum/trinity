"""
A firehose peer which serves requests using a geth database
"""
import asyncio
import argparse
import logging

from cancel_token import CancelToken

from eth.db.backends.level import LevelDB
from eth.db.chain import ChainDB
from eth.vm.forks import HomesteadVM

from p2p import ecies

from trinity.protocol import firehose


logger = logging.getLogger('firehose')


def main(args):
    # TODO: throw an error if the db doesn't exist, don't just make a new one!
    base_db = LevelDB(db_path=args.db)
    chaindb = firehose.GethChainDB(base_db)

    state_root = chaindb.get_canonical_head().state_root
    logger.info(f'State root: {state_root.hex()}')

    cancel_token = CancelToken('server')
    privkey = ecies.generate_privkey()

    if args.cachedb:
        cache = firehose.LeavesCache(args.cachedb)
    else:
        cache = None

    peer_pool = firehose.MiniPeerPool(privkey, cancel_token)
    listener = firehose.FirehoseListener(privkey, args.port, peer_pool, cancel_token)
    server = firehose.FirehoseRequestServer(chaindb, peer_pool, cache, cancel_token)

    async def wait_shutdown():
        await asyncio.wait_for(server.events.finished.wait(), timeout=1)
        await asyncio.wait_for(listener.events.finished.wait(), timeout=1)
        await asyncio.wait_for(peer_pool.events.finished.wait(), timeout=1)

    loop = asyncio.get_event_loop()
    try:
        loop.create_task(server.run())
        loop.create_task(peer_pool.run())
        loop.create_task(listener.run())
        loop.run_forever()
    except KeyboardInterrupt:
        cancel_token.trigger()
        loop.run_until_complete(wait_shutdown())


def test_gethchaindb(args):
    """
    TODO: add your test db (105MB, the first 47376 blocks) to the repo so these tests
    can be automated. Ideally you'd also commit a trinity database with the same data and
    hypothesis test all these calls to ensure they give the same results.
    """
    base_db = LevelDB(db_path=args.db)
    chaindb = firehose.GethChainDB(base_db)

    print(chaindb.get_canonical_head())

    hash_900 = chaindb.get_canonical_block_hash(900)
    print(hash_900.hex())
    assert hash_900.hex() == '30c8ce9d6553c70775ff2e82d148d29bffbd8e825ad269333febb42d514a95cb'

    score = chaindb.get_score(hash_900)
    assert score == 19305821396022
    print(score)

    header = chaindb.get_canonical_block_header_by_number(900)
    print(header)

    by_hash = chaindb.get_block_header_by_hash(hash_900)
    print(by_hash)
    assert by_hash == header

    assert chaindb.header_exists(hash_900)

    assert not chaindb.header_exists(b'\x00' * 32)

    hash_947 = chaindb.get_canonical_block_hash(947)
    uncles = chaindb.get_block_uncles(hash_947)
    assert len(uncles) == 1
    print(uncles)

    header_46147 = chaindb.get_canonical_block_header_by_number(46147)
    txn_class = HomesteadVM.get_transaction_class()
    txns = chaindb.get_block_transactions(header_46147, txn_class)
    print(txns)
    txn_hash = '5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060'
    assert txns[0].hash.hex() == txn_hash

    index = chaindb.get_transaction_index(txns[0].hash)
    assert index == (46147, 0)
    print(index)

    txn = chaindb.get_transaction_by_index(46147, 0, txn_class)
    assert txn == txns[0]
    assert txn.hash.hex() == txn_hash

    hashes = chaindb.get_block_transaction_hashes(header_46147)
    assert len(hashes) == 1
    assert hashes[0].hex() == txn_hash


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-db', type=str, required=True, help="The geth database to serve from"
    )
    parser.add_argument(
        '-port', type=int, required=True, help="The port to serve from"
    )
    parser.add_argument(
        '-cachedb', type=str, required=False, help="Where to save generated node chunks"
    )

    args = parser.parse_args()
    main(args)
