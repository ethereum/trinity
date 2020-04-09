import asyncio
import os
import time

import pytest

from eth_utils import to_tuple

from eth.db.trie import make_trie_root_and_nodes
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt

from trinity.tools.factories import LatestETHPeerPairFactory, ETHV65PeerPairFactory


@pytest.fixture(
    params=(
        ETHV65PeerPairFactory,
        LatestETHPeerPairFactory,
    )
)
async def eth_peer_and_remote(request):
    async with request.param() as (peer, remote):
        yield peer, remote


def get_request_id_or_none(request):
    try:
        return request.payload.request_id
    except AttributeError:
        return None


@to_tuple
def mk_receipts(num_receipts):
    for _ in range(num_receipts):
        yield Receipt(
            state_root=os.urandom(32),
            gas_used=21000,
            bloom=0,
            logs=[],
        )


def mk_header_and_receipts(block_number, num_receipts):
    receipts = mk_receipts(num_receipts)
    root_hash, trie_root_and_data = make_trie_root_and_nodes(receipts)
    header = BlockHeader(
        difficulty=1000000,
        block_number=block_number,
        gas_limit=3141592,
        timestamp=int(time.time()),
        receipt_root=root_hash,
    )
    return header, receipts, (root_hash, trie_root_and_data)


@to_tuple
def mk_headers(*counts):
    for idx, num_receipts in enumerate(counts, 1):
        yield mk_header_and_receipts(idx, num_receipts)


@pytest.mark.asyncio
async def test_eth_peer_get_receipts_round_trip_with_full_response(eth_peer_and_remote):
    peer, remote = eth_peer_and_remote

    headers_bundle = mk_headers(1, 3, 2, 5, 4)
    headers, receipts, trie_roots_and_data = zip(*headers_bundle)
    receipts_bundle = tuple(zip(receipts, trie_roots_and_data))

    async def send_receipts(_, cmd):
        remote.eth_api.send_receipts(receipts, get_request_id_or_none(cmd))

    remote.connection.add_msg_handler(send_receipts)
    get_receipts_task = asyncio.ensure_future(peer.eth_api.get_receipts(headers))

    response = await get_receipts_task

    assert len(response) == len(headers)
    assert response == receipts_bundle


@pytest.mark.asyncio
async def test_eth_peer_get_receipts_round_trip_with_partial_response(eth_peer_and_remote):
    peer, remote = eth_peer_and_remote

    headers_bundle = mk_headers(1, 3, 2, 5, 4)
    headers, receipts, trie_roots_and_data = zip(*headers_bundle)
    receipts_bundle = tuple(zip(receipts, trie_roots_and_data))

    async def send_receipts(_, cmd):
        remote.eth_api.send_receipts(
            (receipts[2], receipts[1], receipts[4]), get_request_id_or_none(cmd)
        )

    remote.connection.add_msg_handler(send_receipts)
    get_receipts_task = asyncio.ensure_future(peer.eth_api.get_receipts(headers))

    response = await get_receipts_task

    assert len(response) == 3
    assert response == (receipts_bundle[2], receipts_bundle[1], receipts_bundle[4])


@pytest.mark.asyncio
async def test_eth_peer_get_receipts_round_trip_with_noise(eth_peer_and_remote):
    peer, remote = eth_peer_and_remote

    headers_bundle = mk_headers(1, 3, 2, 5, 4)
    headers, receipts, trie_roots_and_data = zip(*headers_bundle)
    receipts_bundle = tuple(zip(receipts, trie_roots_and_data))

    async def send_receipts(_, cmd):
        remote.eth_api.send_transactions([])
        await asyncio.sleep(0)
        remote.eth_api.send_receipts(receipts, get_request_id_or_none(cmd))
        await asyncio.sleep(0)
        remote.eth_api.send_transactions([])
        await asyncio.sleep(0)

    remote.connection.add_msg_handler(send_receipts)
    get_receipts_task = asyncio.ensure_future(peer.eth_api.get_receipts(headers))

    response = await get_receipts_task

    assert len(response) == len(headers)
    assert response == receipts_bundle


@pytest.mark.asyncio
async def test_eth_peer_get_receipts_round_trip_no_match_invalid_response(eth_peer_and_remote):
    peer, remote = eth_peer_and_remote

    headers_bundle = mk_headers(1, 3, 2, 5, 4)
    headers, receipts, trie_roots_and_data = zip(*headers_bundle)
    receipts_bundle = tuple(zip(receipts, trie_roots_and_data))

    wrong_headers = mk_headers(4, 3, 8)
    _, wrong_receipts, _ = zip(*wrong_headers)

    async def send_receipts(_, cmd):
        remote.eth_api.send_receipts(wrong_receipts, get_request_id_or_none(cmd))
        await asyncio.sleep(0)
        remote.eth_api.send_receipts(receipts, get_request_id_or_none(cmd))
        await asyncio.sleep(0)

    remote.connection.add_msg_handler(send_receipts)
    get_receipts_task = asyncio.ensure_future(peer.eth_api.get_receipts(headers))

    response = await get_receipts_task

    assert len(response) == len(headers)
    assert response == receipts_bundle
