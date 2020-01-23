import asyncio
import pytest

from aiohttp.test_utils import (
    RawTestServer,
    TestClient,
)
from eth_utils import encode_hex

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.tools.factories import (
    BeaconChainFactory,
)
from libp2p.crypto.secp256k1 import create_new_key_pair

from trinity.protocol.bcc_libp2p.node import Node
from trinity.http.handlers.api_handler import APIHandler


GET_METHOD = 'GET'
POST_METHOD = 'POST'


@pytest.fixture()
def chain(num_validators, base_db):
    chain = BeaconChainFactory(num_validators=num_validators, base_db=base_db)
    state_machine = chain.get_state_machine()
    state = chain.get_head_state()
    slot = 4
    post_state = state_machine.state_transition.apply_state_transition(
        state,
        future_slot=slot,
    )
    chain.chaindb.persist_state(post_state)
    chain.chaindb.update_head_state(post_state.slot, post_state.hash_tree_root)
    return chain


@pytest.fixture()
async def libp2p_node(chain, event_bus):
    key_pair = create_new_key_pair()
    libp2p_node = Node(
        key_pair=key_pair,
        listen_ip="0.0.0.0",
        listen_port=40000,
        preferred_nodes=(),
        chain=chain,
        subnets=(),
        event_bus=event_bus,
    )
    asyncio.ensure_future(libp2p_node.run())
    await asyncio.sleep(0.01)
    return libp2p_node


@pytest.fixture
async def http_server(chain, event_bus):
    server = RawTestServer(APIHandler.handle(chain)(event_bus))
    return server


@pytest.fixture
async def http_client(http_server):
    client = TestClient(http_server)
    asyncio.ensure_future(client.start_server())
    await asyncio.sleep(0.01)
    return client


sample_block = BeaconBlock.create()
sample_attestation = Attestation.create()


@pytest.mark.parametrize(
    'num_validators',
    (2,),
)
@pytest.mark.parametrize(
    'method, resource, object, json_data, status_code',
    (
        (GET_METHOD, 'beacon', 'head', '', 200),
        (GET_METHOD, 'beacon', 'block?slot=0', '', 200),
        (GET_METHOD, 'beacon', 'state?slot=4', '', 200),
        (GET_METHOD, 'beacon', 'state?root=AVAILABLE_STATE_ROOT', '', 200),
    )
)
@pytest.mark.asyncio
async def test_restful_http_server(
    http_client,
    event_loop,
    event_bus,
    base_db,
    method,
    resource,
    object,
    json_data,
    status_code,
    num_validators,
    chain,
    libp2p_node,
):
    if "AVAILABLE_STATE_ROOT" in object:
        state_root = encode_hex(chain.get_head_state().hash_tree_root)
        object = object.replace("AVAILABLE_STATE_ROOT", state_root)

    request_path = resource + '/' + object
    response = await http_client.request(method, request_path, json=json_data)

    try:
        assert response.status == status_code
    except Exception:
        print('[ERROR]:', response.reason)
        raise

    # The server may return 200 or 202 or others. 200 and 202 are both success.
    if str(status_code).startswith('2'):
        response_data = await response.json()
        print(f'[SUCCESS]: {request_path}: \t {response_data}\n')

    await http_client.close()
