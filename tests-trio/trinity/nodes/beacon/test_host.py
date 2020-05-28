from async_generator import asynccontextmanager
from async_service import background_trio_service
import pytest
import trio

from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from trinity.nodes.beacon.request_responder import GoodbyeReason


@asynccontextmanager
async def _run_host(host, listen_maddr):
    async with host.run(listen_addrs=(listen_maddr,)):
        async with background_trio_service(host._gossiper.pubsub):
            async with background_trio_service(host._gossiper.gossipsub):
                yield


@pytest.mark.trio
async def test_hosts_can_gossip_blocks(host_factory):
    host_a_blocks = set()
    host_a, host_a_listen_maddr = host_factory("a", host_a_blocks)

    host_b_blocks = set()
    host_b, host_b_listen_maddr = host_factory("b", host_b_blocks)

    with trio.move_on_after(2 * 60):
        async with _run_host(host_a, host_a_listen_maddr):
            async with _run_host(host_b, host_b_listen_maddr):
                await host_b.add_peer_from_maddr(host_a_listen_maddr)
                await host_a.subscribe_gossip_channels()
                await host_b.subscribe_gossip_channels()

                # NOTE: subscription fails to register if we do not sleep here...
                # Need to debug inside `libp2p`...
                await trio.sleep(1)

                block = SignedBeaconBlock.create(signature=b"\xcc" * 96)
                await host_a.broadcast_block(block)
                block_source = host_b.stream_block_gossip()
                gossiped_block = await block_source.__anext__()

                assert gossiped_block == block

                # NOTE: the following is racy...
                # Need to debug inside `libp2p`...
                await host_a.unsubscribe_gossip_channels()
                await trio.sleep(1)
                await host_b.unsubscribe_gossip_channels()


@pytest.mark.trio
async def test_hosts_can_do_req_resp(host_factory):
    host_a_blocks = set()
    host_a, host_a_listen_maddr = host_factory("c", host_a_blocks)

    host_b_blocks = set([SignedBeaconBlock.create(message=BeaconBlock.create(slot=0))])
    host_b, host_b_listen_maddr = host_factory("d", host_b_blocks)

    with trio.move_on_after(2 * 60):
        async with _run_host(host_a, host_a_listen_maddr):
            async with _run_host(host_b, host_b_listen_maddr):
                await host_b.add_peer_from_maddr(host_a_listen_maddr)
                assert len(host_a.get_network().connections) == 1
                assert len(host_b.get_network().connections) == 1

                peer_b = host_b.get_id()

                status_b = await host_a.exchange_status(
                    peer_b, host_a._status_provider()
                )
                assert status_b

                some_blocks = set()
                async for block in host_a.get_blocks_by_range(peer_b, 0, 20):
                    some_blocks.add(block)
                assert len(some_blocks) == 1

                no_blocks = set()
                async for block in host_a.get_blocks_by_range(peer_b, 30, 3):
                    no_blocks.add(block)
                assert len(no_blocks) == 0

                a_block = some_blocks.pop()
                the_same_block = await host_a.get_blocks_by_root(
                    peer_b, a_block.message.hash_tree_root
                ).__anext__()
                assert a_block == the_same_block

                b_seq_number = await host_a.send_ping(peer_b)
                assert b_seq_number == ord("d")

                b_metadata = await host_a.get_metadata(peer_b)
                assert b_metadata.seq_number == b_seq_number

                await host_a.send_goodbye_to(peer_b, GoodbyeReason.shutting_down)
                assert len(host_a.get_network().connections) == 0
                assert len(host_b.get_network().connections) == 0
