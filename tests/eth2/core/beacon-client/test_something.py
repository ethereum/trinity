import pytest


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.mark.trio
async def test_server_genesis_time_endpoint(client, genesis_time):
    response = await client.get('/beacon/genesis')
    body = await response.get_json()
    assert body['genesis_time'] == str(genesis_time)


@pytest.mark.trio
async def test_beacon_client_retrieve_genesis_time(http_app, genesis_time, http_client):
    response = await http_client.get_genesis_time()
    assert response == genesis_time
