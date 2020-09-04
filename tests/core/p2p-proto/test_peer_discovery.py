import pytest

from eth_enr.tools.factories import ENRFactory

from eth_utils import to_bytes

from eth.chains.ropsten import ROPSTEN_GENESIS_HEADER, ROPSTEN_VM_CONFIGURATION
from eth.db.atomic import AtomicDB
from eth.db.chain import ChainDB

from trinity.components.builtin.peer_discovery.component import generate_eth_cap_enr_field
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.protocol.eth.forkid import extract_forkid, ForkID


@pytest.mark.asyncio
async def test_generate_eth_cap_enr_field():
    base_db = AtomicDB()
    ChainDB(base_db).persist_header(ROPSTEN_GENESIS_HEADER)

    enr_field = await generate_eth_cap_enr_field(ROPSTEN_VM_CONFIGURATION, AsyncHeaderDB(base_db))

    enr = ENRFactory(custom_kv_pairs={enr_field[0]: enr_field[1]})
    assert extract_forkid(enr) == ForkID(hash=to_bytes(hexstr='0x30c7ddbc'), next=10)
