import json

import pytest
from eth import MainnetChain

from trinity.chains.full import FullChain
from trinity.config import TrinityConfig
from trinity.rpc import RPCServer
from trinity.rpc.modules import initialize_eth1_modules, Admin, Net


def build_request(method, params):
    return {"jsonrpc": "2.0", "method": method, "params": params, "id": 3}


def result_from_response(response_str):
    response = json.loads(response_str)
    return (response.get('result', None), response.get('error', None))


class MainnetFullChain(FullChain):
    vm_configuration = MainnetChain.vm_configuration
    chain_id = MainnetChain.chain_id


@pytest.mark.parametrize(
    "api, param, disabled_modules, expected_result, expected_error",
    (
        (
            ("net_listening", (), (), True, None),
            ("net_listening", (), (Admin,), True, None),
            ("net_listening", (), (Net,), None, "Access of net module prohibited"),
            ("net_listening", (), (Admin, Net,), None, "Access of net module prohibited"),
        )
    )
)
async def test_access_control(event_bus,
                              api,
                              param,
                              disabled_modules,
                              expected_result,
                              expected_error):

    chain = MainnetFullChain(None)
    trinity_config = TrinityConfig(app_identifier="eth1", network_id=1)
    rpc = RPCServer(initialize_eth1_modules(chain, event_bus, trinity_config), chain, event_bus)

    request = build_request(api, param)
    response = await rpc.execute_with_access_control(disabled_modules, request)
    result, error = result_from_response(response)
    assert result == expected_result
    assert error == expected_error
