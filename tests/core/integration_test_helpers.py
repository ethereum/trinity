import contextlib
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from async_service import background_asyncio_service
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


FUNDED_ACCT = keys.PrivateKey(
    decode_hex("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee"))

# How to build your own database:
#
#   - Create your own tests.integration.integration_fixture_builders.build_* method
#   - Use it to create a database like:
#       from eth.db.atomic import AtomicDB
#       import importlib
#       from tests.integration import integration_fixture_builders as ifb
#       db = AtomicDB(); ifb.build_YOUR_NEW_METHOD(db, num_blocks=SOME_SMALL_TEST_NUMBER)
#       # inevitable debugging
#       importlib.reload(ifb)
#       db = AtomicDB(); ifb.build_YOUR_NEW_METHOD(db, num_blocks=SOME_SMALL_TEST_NUMBER)
#       # inevitable debugging, etc...
#       # It works!
#       from eth.db.backends.level import LevelDB
#       ldb = LevelDB('/tmp/NEW_DB_NAME.ldb')
#       ifb.build_YOUR_NEW_METHOD(ldb, num_blocks=FULL_TEST_NUMBER)
#   - Then zip it up:
#       cd /tmp; zip -r NEW_DB_NAME.ldb.zip NEW_DB_NAME.ldb
#       cd -
#       mv /tmp/NEW_DB_NAME.ldb.zip tests/integration/fixtures/.
#   - Finally, add an entry to DBFixture


def load_mining_chain(db, *chain_builder_fns):
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

    chain_builder_fns = (latest_mainnet_at(0),) if not chain_builder_fns else chain_builder_fns

    return build(
        AsyncMiningChain,
        *chain_builder_fns,
        enable_pow_mining(),
        genesis(db=db, params=GENESIS_PARAMS, state=GENESIS_STATE),
    )


class DBFixture(Enum):
    TWENTY_POW_HEADERS = '20pow_headers.ldb'
    THOUSAND_POW_HEADERS = '1000pow_headers.ldb'
    # this chain is an uncle chain to `THOUSAND_POW_HEADERS`. It shares a common history until
    # block 474, forks at 475 and contains uncle headers until block 1000. It was built with:
    # build_uncle_chain_to_existing_db(new_db, existing_db, 475, 526)
    UNCLE_CHAIN = '1000pow_uncle_chain.ldb'
    # this chain updates and churns storage, as well as creating a bunch of
    # contracts that are later deleted. It was built with:
    # build_pow_churning_fixture(db, 128)
    STATE_CHURNER = 'churn_state.ldb'

    # This chain adds a bunch of state in the first few blocks, then doesn't
    #   touch it again. The motivation is to test state backfill.
    # Build only 32 blocks so the DB isn't too big, but it's big enough to check
    #   what happens at the 32-block epoch boundary. (Note that it ends up with
    #   more than 32 blocks, because of some setup blocks at the beginning)
    # build_pow_cold_state_fixture(db, num_blocks=32)
    COLD_STATE = 'cold_state.ldb'


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
