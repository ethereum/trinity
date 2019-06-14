import argparse
import logging
import pickle

import plyvel

import rlp
from eth.rlp.accounts import Account

from trie.utils.nibbles import (
    nibbles_to_bytes,
)

from trinity.protocol import firehose

def main(args):
    """
    Scans through the provided cachedb and generates a turbodb, which provides a better
    simulation of what the improved database layout will look like.
    """

    # TODO: There was a bug in the cache generation so the first few leaves are missing

    cache = plyvel.DB(
        args.cachedb,
        create_if_missing=False,
        error_if_exists=False,
    )

    turbo = plyvel.DB(
        args.turbodb,
        create_if_missing=True,
        error_if_exists=False,
    )

    for key, value in cache.iterator():
        leaves, proof = pickle.loads(value)

        for path, value in leaves:
            key = nibbles_to_bytes(path)
            acct = rlp.decode(value, sedes=Account)  # test that we can decode the acct
            print(key.hex(), acct)
            turbo.put(key, value)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-cachedb', type=str, required=True, help="The source database"
    )
    parser.add_argument(
        '-turbodb', type=str, required=True, help="The destination database"
    )
    
    args = parser.parse_args()
    main(args)
