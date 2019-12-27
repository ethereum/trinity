import pathlib
import tempfile
import asyncio
import pytest

from eth2.beacon.tools.factories import (
    BeaconChainFactory,
)
from libp2p.crypto.secp256k1 import create_new_key_pair

from trinity.protocol.bcc_libp2p.node import Node
from trinity.http.handlers.api_handler import APIHandler

GET_METHOD = 'GET'
POST_METHOD = 'POST'


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as dir:
        yield pathlib.Path(dir) / "db_manager.ipc"

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
    return chain


@pytest.fixture()
def libp2p_node(chain, event_bus):
    key_pair = create_new_key_pair()
    libp2p_node = Node(
        key_pair=key_pair,
        listen_ip="0.0.0.0",
        listen_port=30303,
        preferred_nodes=(),
        chain=chain,
        subnets=(),
        event_bus=event_bus,
    )
    asyncio.ensure_future(libp2p_node.run())
    return libp2p_node


@pytest.mark.parametrize(
    'num_validators',
    (2,),
)
@pytest.mark.parametrize(
    'method, resource, object, data, status_code',
    (
        (GET_METHOD, 'beacon', 'head', '', 200),
        (GET_METHOD, 'beacon', 'block?slot=0', '', 200),
        (GET_METHOD, 'beacon', 'state?slot=4', '', 200),
        (GET_METHOD, 'network', 'peers', '', 200),
        (GET_METHOD, 'network', 'peer_id', '', 200),
        (GET_METHOD, 'validator', 'duties', '', 200),
    )
)
@pytest.mark.asyncio
async def test_restful_http_server(
    aiohttp_raw_server,
    aiohttp_client,
    event_loop,
    base_db,
    event_bus,
    method,
    resource,
    object,
    data,
    status_code,
    num_validators,
    chain,
    libp2p_node,
):
    server = await aiohttp_raw_server(APIHandler.handle(chain)(event_bus))
    client = await aiohttp_client(server)

    try:
        request_path = resource + '/' + object
        if method == GET_METHOD:
            response = await client.get(request_path)

        assert response.status == status_code

        if str(status_code).startswith('2'):
            response_data = await response.json()
            print(f'{request_path}: \t {response_data}\n')

    except KeyboardInterrupt:
        pass
    finally:
        # await libp2p_node.cancel()
        await server.close()
