from pathlib import Path
import tempfile
import uuid

from lahja import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint
import pytest
import trio

# Fixtures below are copied from https://github.com/ethereum/lahja/blob/f0b7ead13298de82c02ed92cfb2d32a8bc00b42a/tests/core/trio/conftest.py  # noqa: E501


@pytest.fixture
def ipc_base_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def generate_unique_name() -> str:
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


@pytest.fixture
def endpoint_server_config(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    return config


@pytest.fixture
async def endpoint_server(endpoint_server_config):
    async with TrioEndpoint.serve(endpoint_server_config) as endpoint:
        yield endpoint


@pytest.fixture
async def endpoint_client(endpoint_server_config, endpoint_server):
    async with TrioEndpoint("client-for-testing").run() as client:
        await client.connect_to_endpoints(endpoint_server_config)
        while not endpoint_server.is_connected_to("client-for-testing"):
            await trio.sleep(0)
        yield client
