import asyncio
import contextlib
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from async_service import background_asyncio_service
from cancel_token import OperationCancelled
from eth_keys import keys
from eth_utils import decode_hex

from eth.constants import ZERO_ADDRESS
from eth.db.backends.level import LevelDB
from eth.tools.builder.chain import (
    build,
    enable_pow_mining,
    genesis,
    latest_mainnet_at,
)


from trinity.protocol.common.peer_pool_event_bus import (
    DefaultPeerPoolEventServer,
)
from trinity.tools.chain import AsyncMiningChain


ZIPPED_FIXTURES_PATH = Path(__file__).parent.parent / 'integration' / 'fixtures'


async def connect_to_peers_loop(peer_pool, nodes):
    """Loop forever trying to connect to one of the given nodes if the pool is not yet full."""
    while peer_pool.manager.is_running:
        try:
            if not peer_pool.is_full:
                await peer_pool.connect_to_nodes(nodes)
            await asyncio.sleep(2)
        except OperationCancelled:
            break


FUNDED_ACCT = keys.PrivateKey(
    decode_hex("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee"))


def load_mining_chain(db):
    GENESIS_PARAMS = {
        'coinbase': ZERO_ADDRESS,
        'difficulty': 5,
        'gas_limit': 3141592,
        'timestamp': 1514764800,
    }

    GENESIS_STATE = {
        FUNDED_ACCT.public_key.to_canonical_address(): {
            "balance": 100000000000000000,
        }
    }

    return build(
        AsyncMiningChain,
        latest_mainnet_at(0),
        enable_pow_mining(),
        genesis(db=db, params=GENESIS_PARAMS, state=GENESIS_STATE),
    )


class DBFixture(Enum):
    TWENTY_POW_HEADERS = '20pow_headers.ldb'
    THOUSAND_POW_HEADERS = '1000pow_headers.ldb'

    # this chain updates and churns storage, as well as creating a bunch of
    # contracts that are later deleted. It was built with:
    # build_pow_churning_fixture(db, 128)
    STATE_CHURNER = 'churn_state.ldb'


def load_fixture_db(db_fixture, db_class=LevelDB):
    """
    Extract the database from the zip file to a temp directory, which has two benefits and one cost:
    - B1. works with xdist, multiple access to the ldb files at the same time
    - B2. prevents dirty-ing the git index
    - C1. slows down test, because of time to extract files
    """
    assert isinstance(db_fixture, DBFixture)
    zipped_path = ZIPPED_FIXTURES_PATH / f"{db_fixture.value}.zip"

    with ZipFile(zipped_path, 'r') as zipped, TemporaryDirectory() as tmpdir:
        zipped.extractall(tmpdir)
        yield db_class(Path(tmpdir) / db_fixture.value)


@contextlib.asynccontextmanager
async def run_peer_pool_event_server(event_bus, peer_pool, handler_type=None):

    handler_type = DefaultPeerPoolEventServer if handler_type is None else handler_type

    event_server = handler_type(
        event_bus,
        peer_pool,
    )
    async with background_asyncio_service(event_server):
        yield event_server
