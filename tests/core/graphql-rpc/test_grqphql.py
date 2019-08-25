import asyncio
import json
import os
import time

import pytest
from eth_utils import (
    to_checksum_address,
    to_bytes,
    decode_hex,
)
from eth_utils.toolz import (
    assoc,
)

from trinity.graph_ql.server import (
    GraphQlServer,
)


# TODO: move this method to utils, copied from test_ipc.py
def wait_for(path):
    for _ in range(100):
        if os.path.exists(path):
            return True
        time.sleep(0.01)
    return False


# TODO: move this method to utils, copied from test_ipc.py
def build_request(query):
    data = {
        'query': query
    }
    text = json.dumps(data)
    return to_bytes(text=text)


# TODO: move this method to utils, copied from test_ipc.py
def can_decode_json(potential):
    try:
        json.loads(potential.decode())
        return True
    except json.decoder.JSONDecodeError:
        return False


# TODO: move this method to utils, copied from test_ipc.py
async def get_ipc_response(
        jsonrpc_ipc_pipe_path,
        request_msg,
        event_loop,
        event_bus):

    # Give event subsriptions a moment to propagate.
    await asyncio.sleep(0.01)

    assert wait_for(jsonrpc_ipc_pipe_path), "IPC server did not successfully start with IPC file"

    reader, writer = await asyncio.open_unix_connection(str(jsonrpc_ipc_pipe_path), loop=event_loop)

    writer.write(request_msg)
    await writer.drain()
    result_bytes = b''
    while not can_decode_json(result_bytes):
        result_bytes += await asyncio.tasks.wait_for(reader.readuntil(b'}'), 0.25, loop=event_loop)

    writer.close()
    return json.loads(result_bytes.decode())


@pytest.fixture()
def chain(chain_with_block_validation):
    return chain_with_block_validation


@pytest.fixture()
def rpc(chain, event_loop):
    # overrides fixture from conftest
    return GraphQlServer(chain, event_loop)


@pytest.fixture
def simple_contract_address():
    return b'\x88' * 20


@pytest.fixture
def genesis_state(base_genesis_state, simple_contract_address):
    """
    Includes runtime bytecode of compiled Solidity:

        pragma solidity ^0.4.24;

        contract GetValues {
            function getMeaningOfLife() public pure returns (uint256) {
                return 42;
            }
            function getGasPrice() public view returns (uint256) {
                return tx.gasprice;
            }
            function getBalance() public view returns (uint256) {
                return msg.sender.balance;
            }
            function doRevert() public pure {
                revert("always reverts");
            }
            function useLotsOfGas() public view {
                uint size;
                for (uint i = 0; i < 2**255; i++){
                    assembly {
                        size := extcodesize(0)
                    }
                }
            }
        }
    """
    return assoc(
        base_genesis_state,
        simple_contract_address,
        {
            'balance': 0,
            'nonce': 0,
            'code': decode_hex('60806040526004361061006c5763ffffffff7c010000000000000000000000000000000000000000000000000000000060003504166312065fe08114610071578063455259cb14610098578063858af522146100ad57806395dd7a55146100c2578063afc874d2146100d9575b600080fd5b34801561007d57600080fd5b506100866100ee565b60408051918252519081900360200190f35b3480156100a457600080fd5b506100866100f3565b3480156100b957600080fd5b506100866100f7565b3480156100ce57600080fd5b506100d76100fc565b005b3480156100e557600080fd5b506100d7610139565b333190565b3a90565b602a90565b6000805b7f80000000000000000000000000000000000000000000000000000000000000008110156101355760003b9150600101610100565b5050565b604080517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152600e60248201527f616c776179732072657665727473000000000000000000000000000000000000604482015290519081900360640190fd00a165627a7a72305820645df686b4a16d5a69fc6d841fc9ad700528c14b35ca5629e11b154a9d3dff890029'),  # noqa: E501
            'storage': {},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'request_msg, expected',
    (
        (
            build_request("{ block {number} }"),
            {'result': {'block': {'number': '0x0'}}, 'errors': None},
        ),

        (
            build_request("""
            {
                estimateGas(
                    data: { to: "0x0000000000000000000000000000000000000000"}, 
                    blockNumber: "latest"
                )
            }
            """),
            {'errors': None, 'result': {'estimateGas': 21000}},
        ),
        (
            build_request("{gasPrice}"),
            {'errors': None, 'result': {'gasPrice': 1000000000}},
        ),
        (
            build_request("""
                {
                    account(
                        address: "0x0000000000000000000000000000000000000000",
                        blockNumber: "latest"
                    ){
                        balance
                    }
                }
            """),
            {'errors': None, 'result': {'account': {'balance': 0}}}
        ),
        (
            build_request("""{
                call(data: {
                        to: "0x0000000000000000000000000000000000000004", 
                        data: "0x123456"
                    }
                ){
                    data, gasUsed, status
                }
            }"""),
            {'errors': None, 'result': {'call': {'data': '0x123456', 'gasUsed': 18, 'status': 1}}}
        ),
    ),
)
async def test_rpc_methods(
        jsonrpc_ipc_pipe_path,
        request_msg,
        expected,
        event_loop,
        event_bus,
        ipc_server
):
    result = await get_ipc_response(jsonrpc_ipc_pipe_path, request_msg, event_loop, event_bus)
    assert result == expected

