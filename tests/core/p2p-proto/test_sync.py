import asyncio
import collections
from contextlib import asynccontextmanager
import logging
import uuid

from async_service import Service, background_asyncio_service
from eth.consensus import ConsensusContext
from eth.constants import EMPTY_SHA3
from eth.db.atomic import AtomicDB
from eth.db.schema import SchemaV1
from eth.exceptions import (
    BlockNotFound,
    HeaderNotFound,
)
from eth.rlp.accounts import Account
from eth.vm.forks import (
    MuirGlacierVM,
    PetersburgVM,
)
from eth_utils import decode_hex
from lahja import ConnectionConfig, AsyncioEndpoint
import pytest
import rlp
from trie import (
    HexaryTrie,
)
from trie.constants import (
    BLANK_NODE_HASH,
)
from trie.exceptions import (
    MissingTraversalNode,
)
from trie.iter import (
    NodeIterator,
)

from trinity.components.builtin.metrics.registry import NoopMetricsRegistry
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.chain import AsyncChainDB
from trinity.protocol.eth.payloads import NewBlockHash
from trinity.protocol.eth.peer import ETHPeerPoolEventServer
from trinity.protocol.eth.sync import ETHHeaderChainSyncer
from trinity.protocol.les.servers import LightRequestServer
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockImportServer,
)
from trinity.sync.common.checkpoint import Checkpoint
from trinity.sync.common.chain import (
    SimpleBlockImporter,
)
from trinity.sync.full.chain import FastChainSyncer, RegularChainSyncer, RegularChainBodySyncer

from trinity.protocol.eth.servers import ETHRequestServer
from trinity.protocol.les.peer import (
    LESPeerPoolEventServer,
)

from trinity.sync.beam.chain import (
    BeamSyncer,
    BodyChainGapSyncer,
    FromCheckpointBodyChainGapSyncer,
)
from trinity.sync.beam.queen import QueeningQueue
from trinity.sync.header.chain import (
    HeaderChainSyncer,
    HeaderChainGapSyncer,
    SequentialHeaderChainGapSyncer,
)
from trinity.sync.light.chain import LightChainSyncer

from trinity.tools.factories import (
    ChainContextFactory,
    LatestETHPeerPairFactory,
    LESV2PeerPairFactory,
)
from trinity.tools.chain import (
    ByzantiumTestChain,
    LatestTestChain,
)

from tests.core.integration_test_helpers import (
    DBFixture,
    load_fixture_db,
    load_mining_chain,
    run_peer_pool_event_server,
)
from tests.core.peer_helpers import (
    MockPeerPoolWithConnectedPeers,
)


# This causes the chain syncers to request/send small batches of things, which will cause us to
# exercise parts of the code that wouldn't otherwise be exercised if the whole sync was completed
# by requesting a single batch.
@pytest.fixture(autouse=True)
def small_header_batches(monkeypatch):
    from trinity.protocol.eth import constants
    monkeypatch.setattr(constants, 'MAX_HEADERS_FETCH', 10)
    monkeypatch.setattr(constants, 'MAX_BODIES_FETCH', 5)


@pytest.fixture
def chaindb_with_gaps(chaindb_fresh, chaindb_1000):
    # Make a chain with gaps. This fixture can not be used in a test alongside `chaindb_fresh`
    # because it alters the `chaindb_fresh` fixture.
    for block_number in (250, 500):
        header_at = chaindb_1000.get_canonical_block_header_by_number(block_number)
        score_at = chaindb_1000.get_score(header_at.hash)
        chaindb_fresh.persist_checkpoint_header(header_at, score_at)

    assert chaindb_fresh.get_header_chain_gaps() == (((1, 249), (251, 499)), 501)
    yield chaindb_fresh


@pytest.fixture
def chaindb_with_block_gaps(chaindb_fresh, chaindb_1000):
    # Make a chain with gaps. This fixture can not be used in a test alongside `chaindb_fresh`
    # because it alters the `chaindb_fresh` fixture.

    all_headers = [
        chaindb_1000.get_canonical_block_header_by_number(block_number)
        for block_number in range(1, 1001)
    ]
    chaindb_fresh.persist_header_chain(all_headers)

    fat_chain = LatestTestChain(chaindb_1000.db)
    for block_number in (250, 500):
        block = fat_chain.get_canonical_block_by_number(block_number)
        receipts = block.get_receipts(chaindb_1000)
        chaindb_fresh.persist_unexecuted_block(block, receipts)

    assert chaindb_fresh.get_chain_gaps() == (((1, 249), (251, 499)), 501)
    yield chaindb_fresh


@pytest.fixture
def chaindb_with_headers_from_checkpoint(chaindb_fresh, chaindb_1000):
    # Make a chain with gaps. This fixture can not be used in a test alongside `chaindb_fresh`
    # because it alters the `chaindb_fresh` fixture.
    for block_number in range(970, 1001):
        header_at = chaindb_1000.get_canonical_block_header_by_number(block_number)
        score_at = chaindb_1000.get_score(header_at.hash)
        chaindb_fresh.persist_checkpoint_header(header_at, score_at)

    assert chaindb_fresh.get_header_chain_gaps() == (((1, 969),), 1001)

    # The below is a bit of a hack to make the following assert True.
    # i.e: without it, get_chain_gaps() does not retrieve any gaps
    fat_chain = LatestTestChain(chaindb_1000.db)
    block_number = 1000
    block = fat_chain.get_canonical_block_by_number(block_number)
    receipts = block.get_receipts(chaindb_1000)
    chaindb_fresh.persist_unexecuted_block(block, receipts)

    assert chaindb_fresh.get_chain_gaps() == (((1, 999),), 1001)
    yield chaindb_fresh


@pytest.mark.asyncio
async def test_fast_syncer(request, event_loop, event_bus, chaindb_fresh, chaindb_1000):

    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        client = FastChainSyncer(LatestTestChain(chaindb_fresh.db), chaindb_fresh, client_peer_pool)
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db)
        )):

            client_peer.logger.info("%s is serving 1000 blocks", client_peer)
            server_peer.logger.info("%s is syncing up 1000 blocks", server_peer)

            async with background_asyncio_service(client) as manager:
                await asyncio.wait_for(manager.wait_finished(), timeout=20)

            head = chaindb_fresh.get_canonical_head()
            assert head == chaindb_1000.get_canonical_head()
            # TODO assert that the block transactions and uncles are present too.


@pytest.mark.asyncio
@pytest.mark.parametrize('enable_state_backfill', [False, True])
async def test_beam_syncer_with_checkpoint_too_close_to_tip(
        caplog,
        request,
        event_loop,
        event_bus,
        enable_state_backfill,
        chaindb_fresh,
        chaindb_churner):

    checkpoint = Checkpoint(
        block_hash=decode_hex('0x814aca8a5855f216fee0f627945f70b3c019ae2c8b3aeb528ea7049ed83cfc82'),
        score=645,
    )

    caplog.set_level(logging.INFO)
    try:
        await test_beam_syncer_loads_recent_state_root(
            request,
            event_loop,
            event_bus,
            chaindb_fresh,
            chaindb_churner,
            beam_to_block=66,
            enable_state_backfill=enable_state_backfill,
            checkpoint=checkpoint,
        )
    except asyncio.TimeoutError:
        # Beam syncer timing out and printing an info to the user is the expected behavior.
        # Our checkpoint is right before the tip and the chain doesn't advance forward.
        assert "Checkpoint is too near" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('enable_state_backfill', [False, True])
async def test_beam_syncer_with_checkpoint(
        request,
        event_loop,
        event_bus,
        enable_state_backfill,
        chaindb_fresh,
        chaindb_churner):

    checkpoint = Checkpoint(
        block_hash=decode_hex('0x5b8d32e4aebda3da7bdf2f0588cb42256e2ed0c268efec71b38278df8488a263'),
        score=55,
    )

    await test_beam_syncer_loads_recent_state_root(
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_churner,
        beam_to_block=66,
        enable_state_backfill=enable_state_backfill,
        checkpoint=checkpoint,
    )


@asynccontextmanager
async def _beam_syncing(
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_churner,
        beam_to_block,
        checkpoint=None,
        VM_at_0=PetersburgVM,
        enable_state_backfill=False,
):

    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_churner.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    backfiller = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer), backfiller as (client2_peer, backfill_peer):

        # Need a name that will be unique per xdist-process, otherwise
        #   lahja IPC endpoints in each process will clobber each other
        unique_process_name = uuid.uuid4()

        # manually add endpoint for beam vm to make requests
        pausing_config = ConnectionConfig.from_name(f"PausingEndpoint-{unique_process_name}")

        # manually add endpoint for trie data gatherer to serve requests
        gatherer_config = ConnectionConfig.from_name(f"GathererEndpoint-{unique_process_name}")

        client_peer_pool = MockPeerPoolWithConnectedPeers(
            [client_peer, backfill_peer],
            event_bus=event_bus,
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)
        backfill_peer_pool = MockPeerPoolWithConnectedPeers([client2_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), run_peer_pool_event_server(
            event_bus, backfill_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_churner.db)
        )), AsyncioEndpoint.serve(
            pausing_config
        ) as pausing_endpoint, AsyncioEndpoint.serve(gatherer_config) as gatherer_endpoint:

            client_chain = make_pausing_beam_chain(
                ((0, VM_at_0), ),
                chain_id=999,
                consensus_context_class=ConsensusContext,
                db=chaindb_fresh.db,
                event_bus=pausing_endpoint,
                metrics_registry=NoopMetricsRegistry(),
                loop=event_loop,
            )

            client = BeamSyncer(
                client_chain,
                chaindb_fresh.db,
                AsyncChainDB(chaindb_fresh.db),
                client_peer_pool,
                gatherer_endpoint,
                NoopMetricsRegistry(),
                force_beam_block_number=beam_to_block,
                checkpoint=checkpoint,
                enable_state_backfill=enable_state_backfill,
                enable_backfill=False,
            )

            client_peer.logger.info("%s is serving churner blocks", client_peer)
            backfill_peer.logger.info("%s is serving backfill state", backfill_peer)
            server_peer.logger.info("%s is syncing up churner blocks", server_peer)

            import_server = BlockImportServer(
                pausing_endpoint,
                client_chain,
            )
            async with background_asyncio_service(import_server):
                await pausing_endpoint.connect_to_endpoints(gatherer_config)
                async with background_asyncio_service(client):
                    yield client


# Cases of interest:
# - 66: Missing an account trie node required for account deletion trie fixups,
#       when "resuming" execution after completing all transactions
# - 68: If some storage saves succeed and some fail, you might get:
#       After persisting storage trie, a root node was not found.
#       State root for account 0x49361e4f811f49542f19d691cf5f79d39983e8e0 is missing for
#       hash 0x4d76d61d563099c7fa0088068bc7594d27334f5df2df43110bf86ff91dce5be6
# -  2: Normally we need to look back ~6 headers to look for duplicate uncles. This
#       tests the lowest block number code path where an alternate code path is triggered,
#       to look up fewer than usual.
# -  7: Normally we need to look back ~6 headers to look for duplicate uncles. This
#       tests the highest block number code path where an alternate code path is triggered,
#       to look up fewer than usual.
# This test was reduced to a few cases for speed. To run the full suite, use
# range(1, 130) for beam_to_block. (and optionally follow the instructions at target_head)
@pytest.mark.asyncio
@pytest.mark.parametrize('beam_to_block', [1, 2, 7, 66, 68, 129])
@pytest.mark.parametrize('enable_state_backfill', [False, True])
async def test_beam_syncer_loads_recent_state_root(
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_churner,
        beam_to_block,
        enable_state_backfill,
        checkpoint=None):

    sync_test_service = _beam_syncing(
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_churner,
        beam_to_block,
        checkpoint,
    )

    async with sync_test_service:
        # We can sync at least 10 blocks in 1s at current speeds, (or
        # reach the current one) Trying to keep the tests short-ish. A
        # fuller test could always set the target header to the
        # chaindb_churner canonical head, and increase the timeout
        # significantly
        target_block_number = min(beam_to_block + 10, 129)
        target_head = chaindb_churner.get_canonical_block_header_by_number(
            target_block_number,
        )
        await wait_for_head(chaindb_fresh, target_head, sync_timeout=10)
        assert target_head.state_root in chaindb_fresh.db


@pytest.mark.asyncio
# Cases of interest:
# -  0: This doesn't test beam sync or backfill, since all state is available immediately.
#       The test checks things that don't happen if we start at 0, so it's excluded from the range.
# -  2: Normally we need to look back ~6 headers to look for duplicate uncles. This
#       tests the lowest block number code path where an alternate code path is triggered,
#       to look up fewer than usual.
# -  7: Normally we need to look back ~6 headers to look for duplicate uncles. This
#       tests the highest block number code path where an alternate code path is triggered,
#       to look up fewer than usual.
# -  8: First block that looks up all possible headers for duplicate uncles
# - 25: Random block in the middle.
# - 38: During a full range(0, 43) scan, found this block failing. It (sometimes?) requests
#       duplicate hashes in the same request.
# - 42: Last block
# In order to run a deeper test, we can run:
# @pytest.mark.parametrize('beam_to_block', range(1, 43))
@pytest.mark.parametrize('beam_to_block', [2, 7, 8, 25, 38, 42])
async def test_beam_syncer_backfills_all_state(
        caplog,
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_cold_state,
        beam_to_block,
        checkpoint=None):

    sync_test_service = _beam_syncing(
        request,
        event_loop,
        event_bus,
        chaindb_fresh,
        chaindb_cold_state,
        beam_to_block,
        checkpoint,
        VM_at_0=MuirGlacierVM,
        enable_state_backfill=True,
    )

    caplog.set_level(logging.INFO)
    async with sync_test_service as beam_syncer:
        # Manually verify that all state is in the state database now
        await wait_for_full_state_db(chaindb_fresh, beam_to_block, timeout=20)
        beam_syncer.logger.info("State DB complete by manual inspection")

        # Check that backfiller exits on state completion
        try:
            await asyncio.wait_for(beam_syncer._backfiller.manager.wait_finished(), timeout=10)
        except asyncio.TimeoutError:
            beam_syncer.logger.warning("Backfiller never exited, even though all state is present")
            if beam_syncer._backfiller._check_complete():
                beam_syncer.logger.warning("Backfiller thinks it's complete, but never exited")
            else:
                beam_syncer.logger.warning(
                    "Backfiller thinks it's missing %s",
                    beam_syncer._backfiller._account_tracker,
                )
            # Whatever the reason, this TimeoutError means the backfiller service didn't exit
            raise

    # Double-check that backfiller exited for the right reason.
    assert "Downloaded all accounts, storage and bytecode state" in caplog.text


@pytest.mark.asyncio
async def test_regular_syncer(request, event_loop, event_bus, chaindb_fresh, chaindb_20):
    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_20.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )

    async with peer_pair as (client_peer, server_peer):

        client = RegularChainSyncer(
            ByzantiumTestChain(chaindb_fresh.db),
            chaindb_fresh,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_20.db)
        )):

            server_peer.logger.info("%s is serving 20 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 20", client_peer)

            async with background_asyncio_service(client):
                await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())
                head = chaindb_fresh.get_canonical_head()
                assert head.state_root in chaindb_fresh.db


class FallbackTesting_RegularChainSyncer(Service):
    class HeaderSyncer_OnlyOne:
        def __init__(self, real_syncer):
            self._real_syncer = real_syncer

        async def new_sync_headers(self, max_batch_size):
            async for headers in self._real_syncer.new_sync_headers(1):
                yield headers
                await asyncio.sleep(2)
                raise Exception("This should always get cancelled quickly, say within 2s")

    class HeaderSyncer_PauseThenRest:
        def __init__(self, real_syncer):
            self._real_syncer = real_syncer
            self._ready = asyncio.Event()
            self._headers_requested = asyncio.Event()

        async def new_sync_headers(self, max_batch_size):
            self._headers_requested.set()
            await self._ready.wait()
            async for headers in self._real_syncer.new_sync_headers(max_batch_size):
                yield headers

        def unpause(self):
            self._ready.set()

        async def until_headers_requested(self):
            await self._headers_requested.wait()

    def __init__(self, chain, db, peer_pool) -> None:
        self._chain = chain
        self._header_syncer = ETHHeaderChainSyncer(chain, db, peer_pool)
        self._single_header_syncer = self.HeaderSyncer_OnlyOne(self._header_syncer)
        self._paused_header_syncer = self.HeaderSyncer_PauseThenRest(self._header_syncer)
        self._draining_syncer = RegularChainBodySyncer(
            chain,
            db,
            peer_pool,
            self._single_header_syncer,
            SimpleBlockImporter(chain),
        )
        self._body_syncer = RegularChainBodySyncer(
            chain,
            db,
            peer_pool,
            self._paused_header_syncer,
            SimpleBlockImporter(chain),
        )

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._header_syncer)
        starting_header = await self._chain.coro_get_canonical_head()

        # want body_syncer to start early so that it thinks the genesis is the canonical head
        self.manager.run_child_service(self._body_syncer)
        await self._paused_header_syncer.until_headers_requested()

        # now drain out the first header and save it to db
        async with background_asyncio_service(self._draining_syncer):
            # Run until first header is saved to db, then exit
            latest_header = starting_header
            while starting_header == latest_header:
                await asyncio.sleep(0.03)
                latest_header = await self._chain.coro_get_canonical_head()

        # now permit the next syncer to begin
        self._paused_header_syncer.unpause()

        # run regular sync until cancelled
        await self.manager.wait_finished()


@pytest.mark.asyncio
async def test_regular_syncer_fallback(request, event_loop, event_bus, chaindb_fresh, chaindb_20):
    """
    Test the scenario where a header comes in that's not in memory (but is in the DB)
    """
    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_20.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )

    async with peer_pair as (client_peer, server_peer):

        client = FallbackTesting_RegularChainSyncer(
            ByzantiumTestChain(chaindb_fresh.db),
            chaindb_fresh,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_20.db)
        )):

            server_peer.logger.info("%s is serving 20 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 20", client_peer)

            async with background_asyncio_service(client):
                await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())
                head = chaindb_fresh.get_canonical_head()
                assert head.state_root in chaindb_fresh.db


@pytest.mark.asyncio
async def test_header_syncer(request,
                             event_loop,
                             event_bus,
                             chaindb_fresh,
                             chaindb_1000):
    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_to_server, server_to_client):

        client = HeaderChainSyncer(
            LatestTestChain(chaindb_fresh.db),
            chaindb_fresh,
            MockPeerPoolWithConnectedPeers([client_to_server], event_bus=event_bus)
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_to_client], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_to_client.logger.info("%s is serving 1000 blocks", server_to_client)
            client_to_server.logger.info("%s is syncing up 1000", client_to_server)

            # Artificially split header sync into two parts, to verify that
            #   cycling to the next sync works properly. Split by erasing the canonical
            #   lookups in a middle chunk. We have to erase a range of them because of
            #   how the skeleton syncer asks for every ~192 headers. The skeleton request
            #   would skip right over a single missing header.
            erase_block_numbers = range(500, 700)
            erased_canonicals = []
            for blocknum in erase_block_numbers:
                dbkey = SchemaV1.make_block_number_to_hash_lookup_key(blocknum)
                canonical_hash = chaindb_1000.db[dbkey]
                erased_canonicals.append((dbkey, canonical_hash))
                del chaindb_1000.db[dbkey]

            async with background_asyncio_service(client):
                target_head = chaindb_1000.get_canonical_block_header_by_number(
                    erase_block_numbers[0] - 1
                )
                await wait_for_head(chaindb_fresh, target_head)

                # gut check that we didn't skip past the erased range of blocks
                head = chaindb_fresh.get_canonical_head()
                assert head.block_number < erase_block_numbers[0]

                # TODO validate that the skeleton syncer has cycled??

                # Replace the missing headers so that syncing can resume
                for dbkey, canonical_hash in erased_canonicals:
                    chaindb_1000.db[dbkey] = canonical_hash

                complete_chain_tip = chaindb_1000.get_canonical_head()

                # Not entirely certain that sending new block hashes is necessary, but...
                #   it shouldn't hurt anything. Trying to fix this flaky test:
                # https://app.circleci.com/pipelines/github/ethereum/trinity/6855/workflows/131f9b03-8c99-4419-8e88-d2ef216e3dbb/jobs/259263/steps  # noqa: E501

                server_to_client.eth_api.send_new_block_hashes(
                    NewBlockHash(complete_chain_tip.hash, complete_chain_tip.block_number),
                )

                await wait_for_head(chaindb_fresh, complete_chain_tip)


@pytest.mark.asyncio
async def test_block_gapfill_syncer(request,
                                    event_loop,
                                    event_bus,
                                    chaindb_with_block_gaps,
                                    chaindb_1000):
    client_context = ChainContextFactory(headerdb__db=chaindb_with_block_gaps.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        syncer = BodyChainGapSyncer(
            LatestTestChain(chaindb_with_block_gaps.db),
            chaindb_with_block_gaps,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus),
        )
        # In production, this would be the block time but we want our test to pause/resume swiftly
        syncer._idle_time = 0.01
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            async with background_asyncio_service(syncer):
                chain_with_gaps = LatestTestChain(chaindb_with_block_gaps.db)
                fat_chain = LatestTestChain(chaindb_1000.db)

                # Ensure we can pause/resume immediately and not just after syncing has started
                syncer.pause()
                syncer.resume()

                # Sync the first 100 blocks, then check that pausing/resume works
                await wait_for_block(
                    chain_with_gaps, fat_chain.get_canonical_block_by_number(100))

                # Pause the syncer and take note how far we have synced at this point
                syncer.pause()
                # We need to give async code a moment to settle before we save the progress to
                # ensure it has stabilized before we save it.
                await asyncio.sleep(0.25)
                paused_chain_gaps = chain_with_gaps.chaindb.get_chain_gaps()

                # Consider it victory if after 0.5s no new blocks were written to the database
                await asyncio.sleep(0.5)
                assert paused_chain_gaps == chain_with_gaps.chaindb.get_chain_gaps()

                # Resume syncing
                syncer.resume()

                await wait_for_block(
                    chain_with_gaps, fat_chain.get_canonical_block_by_number(1000), sync_timeout=20)

                for block_num in range(1, 1001):
                    assert chain_with_gaps.get_canonical_block_by_number(
                        block_num) == fat_chain.get_canonical_block_by_number(block_num)

                # We need to give the async calls a moment to settle before we can read the updated
                # chain gaps.
                await asyncio.sleep(0.25)
                assert chain_with_gaps.chaindb.get_chain_gaps() == ((), 1001)


@pytest.mark.asyncio
async def test_block_gapfill_from_checkpoint_syncer_1(event_bus,
                                                      chaindb_with_headers_from_checkpoint,
                                                      chaindb_1000):
    client_context = ChainContextFactory(headerdb__db=chaindb_with_headers_from_checkpoint.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):
        chain_with_gaps = LatestTestChain(chaindb_with_headers_from_checkpoint.db)

        syncer = BodyChainGapSyncer(
            chain_with_gaps,
            chaindb_with_headers_from_checkpoint,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus),
        )

        # In production, this would be the block time but we want our test to pause/resume swiftly
        syncer._idle_time = 0.01
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        syncer._max_backfill_block_bodies_at_once = 100

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            async with background_asyncio_service(syncer):
                fat_chain = LatestTestChain(chaindb_1000.db)

                # Wait for blocks backfilling
                await wait_for_block(
                    chain_with_gaps, fat_chain.get_canonical_block_by_number(999), sync_timeout=20)

                await asyncio.sleep(0.25)
                assert chain_with_gaps.chaindb.get_chain_gaps() == (((1, 970),), 1001)


@pytest.mark.asyncio
async def test_block_gapfill_from_checkpoint_syncer_2(event_bus,
                                                      chaindb_with_headers_from_checkpoint,
                                                      chaindb_1000):
    client_context = ChainContextFactory(headerdb__db=chaindb_with_headers_from_checkpoint.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):
        chain_with_gaps = LatestTestChain(chaindb_with_headers_from_checkpoint.db)

        syncer = FromCheckpointBodyChainGapSyncer(
            chain_with_gaps,
            chaindb_with_headers_from_checkpoint,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus),
            970,
        )

        # In production, this would be the block time but we want our test to pause/resume swiftly
        syncer._idle_time = 0.01
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        syncer._max_backfill_block_bodies_at_once = 100

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            async with background_asyncio_service(syncer):
                fat_chain = LatestTestChain(chaindb_1000.db)

                # Wait for blocks backfilling
                await wait_for_block(
                    chain_with_gaps, fat_chain.get_canonical_block_by_number(999), sync_timeout=20)

                await asyncio.sleep(0.25)
                assert chain_with_gaps.chaindb.get_chain_gaps() == (((1, 970),), 1001)


@pytest.mark.asyncio
async def test_header_gapfill_syncer(request,
                                     event_loop,
                                     event_bus,
                                     chaindb_with_gaps,
                                     chaindb_1000):

    client_context = ChainContextFactory(headerdb__db=chaindb_with_gaps.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        client = HeaderChainGapSyncer(
            LatestTestChain(chaindb_with_gaps.db),
            chaindb_with_gaps,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            async with background_asyncio_service(client):
                await wait_for_head(
                    # We check for 249 because 250 exists from the very beginning (the checkpoint)
                    chaindb_with_gaps, chaindb_1000.get_canonical_block_header_by_number(249))


@pytest.mark.asyncio
async def test_sequential_header_gapfill_syncer(request,
                                                event_loop,
                                                event_bus,
                                                chaindb_with_gaps,
                                                chaindb_1000):
    client_context = ChainContextFactory(headerdb__db=chaindb_with_gaps.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_1000.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        chain_with_gaps = LatestTestChain(chaindb_with_gaps.db)
        client = SequentialHeaderChainGapSyncer(
            chain_with_gaps,
            chaindb_with_gaps,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        )
        # Ensure we use small chunks to be able to test pause/resume properly
        client._max_backfill_header_at_once = 100
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_1000.db),
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            async with background_asyncio_service(client):
                # We intentionally only sync up to a block *below* the first gap to have a more
                # difficult scenario for pause/resume. We want to make sure we not only can pause
                # at the times where we synced up to an actual gap. Instead we want to be sure
                # we can pause after we synced up to the `_max_backfill_header_at_once` limit which
                # may be shorter than the actual gap in the chain.
                await wait_for_head(
                    chaindb_with_gaps, chaindb_1000.get_canonical_block_header_by_number(100)
                )

                # Pause the syncer for a moment and check if it continued syncing (it should not!)
                client.pause()
                # Verify that we stopped the chain fast enough, before the gap was fully filled
                # This is a potential source of test flakiness
                with pytest.raises(HeaderNotFound):
                    chaindb_with_gaps.get_canonical_block_header_by_number(249)
                await asyncio.sleep(1)
                # Make sure that the gap filling doesn't complete for a while. We could
                # theoretically get false positives if it's not paused but very slow to fill headers
                with pytest.raises(HeaderNotFound):
                    chaindb_with_gaps.get_canonical_block_header_by_number(249)
                # Now resume syncing
                client.resume()

                await wait_for_head(
                    # We check for 499 because 500 is there from the very beginning (the checkpoint)
                    chaindb_with_gaps, chaindb_1000.get_canonical_block_header_by_number(499)
                )
                # This test is supposed to only fill in headers, so the following should fail.
                # If this ever succeeds it probably means the fixture was re-created with trivial
                # blocks and the test will fail and remind us what kind of fixture we want here.
                with pytest.raises(BlockNotFound):
                    chain_with_gaps.get_canonical_block_by_number(499)


@pytest.mark.asyncio
async def test_header_gap_fill_detects_invalid_attempt(caplog,
                                                       event_loop,
                                                       event_bus,
                                                       chaindb_with_gaps,
                                                       chaindb_1000,
                                                       chaindb_uncle):

    client_context = ChainContextFactory(headerdb__db=chaindb_with_gaps.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_uncle.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        client = SequentialHeaderChainGapSyncer(
            LatestTestChain(chaindb_with_gaps.db),
            chaindb_with_gaps,
            client_peer_pool
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)
        uncle_chaindb = AsyncChainDB(chaindb_uncle.db)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, uncle_chaindb,
        )):

            server_peer.logger.info("%s is serving 1000 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 1000", client_peer)

            # We check for 499 because 500 exists from the very beginning (the checkpoint)
            expected_block_number = 499
            final_header = chaindb_1000.get_canonical_block_header_by_number(expected_block_number)
            async with background_asyncio_service(client):
                try:
                    await wait_for_head(
                        chaindb_with_gaps,
                        final_header,
                        sync_timeout=5,
                    )
                except asyncio.TimeoutError:
                    assert "Attempted to fill gap with invalid header" in caplog.text
                    # Monkey patch the uncle chaindb to effectively make the attacker peer
                    # switch to the correct chain.
                    uncle_chaindb.db = chaindb_1000.db
                    # The hack goes on: Now that our attacker peer turned friendly we may be stuck
                    # waiting for a new skeleton peer forever. This isn't a real life scenario
                    # because: a.) an attacker probably won't turn friendly and b.) new blocks and
                    # peers will constantly yield new skeleton peers.
                    # This ugly hack will tick the chain tip monitor as we simulate a joining peer.
                    for subscriber in client_peer_pool._subscribers:
                        subscriber.register_peer(client_peer)

                    await wait_for_head(
                        chaindb_with_gaps,
                        final_header,
                        sync_timeout=20,
                    )
                else:
                    raise AssertionError("Succeeded when it was expected to fail")


@pytest.mark.asyncio
async def test_light_syncer(request,
                            event_loop,
                            event_bus,
                            chaindb_fresh,
                            chaindb_20):
    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_20.db)
    peer_pair = LESV2PeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )
    async with peer_pair as (client_peer, server_peer):

        client = LightChainSyncer(
            LatestTestChain(chaindb_fresh.db),
            chaindb_fresh,
            MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
        )
        server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=LESPeerPoolEventServer
        ), background_asyncio_service(LightRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_20.db),
        )):

            server_peer.logger.info("%s is serving 20 blocks", server_peer)
            client_peer.logger.info("%s is syncing up 20", client_peer)

            async with background_asyncio_service(client):
                await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())


@pytest.mark.asyncio
@pytest.mark.parametrize("has_parallel_peasant_call", (True, False))
async def test_queening_queue_recovers_from_penalty_with_one_peer(
        event_bus,
        chaindb_fresh,
        chaindb_20,
        has_parallel_peasant_call):

    local_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    remote_context = ChainContextFactory(headerdb__db=chaindb_20.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=local_context,
        bob_peer_context=remote_context,
        event_bus=event_bus,
    )
    async with peer_pair as (connection_to_local, connection_to_remote):

        local_peer_pool = MockPeerPoolWithConnectedPeers(
            [connection_to_remote],
            event_bus=event_bus,
        )

        async with run_peer_pool_event_server(
            event_bus, local_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_20.db),
        )):
            queue = QueeningQueue(local_peer_pool)

            async with background_asyncio_service(queue):
                queen = await asyncio.wait_for(queue.get_queen_peer(), timeout=0.01)
                assert queen == connection_to_remote

                queue.penalize_queen(connection_to_remote, delay=0.1)
                assert queue.queen is None
                with pytest.raises(asyncio.TimeoutError):
                    # The queen should be penalized for this entire period, and
                    #   there are no alternative peers, so this call should hang:
                    await asyncio.wait_for(queue.get_queen_peer(), timeout=0.05)

                if has_parallel_peasant_call:
                    waiting_on_peasant = asyncio.ensure_future(queue.pop_fastest_peasant())

                # But after waiting long enough, even with just one peer, the blocking
                #   call should return. Whether or not there is also a waiting call looking for
                #   a peasant.
                final_queen = await asyncio.wait_for(queue.get_queen_peer(), timeout=0.075)
                assert final_queen == connection_to_remote

                if has_parallel_peasant_call:
                    waiting_on_peasant.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiting_on_peasant


@pytest.fixture
def leveldb_churner():
    yield from load_fixture_db(DBFixture.STATE_CHURNER)


@pytest.fixture
def chaindb_churner(leveldb_churner):
    chain = load_mining_chain(AtomicDB(leveldb_churner))
    assert chain.chaindb.get_canonical_head().block_number == 129
    return chain.chaindb


@pytest.fixture
def leveldb_cold_state():
    yield from load_fixture_db(DBFixture.COLD_STATE)


@pytest.fixture
def chaindb_cold_state(leveldb_cold_state):
    chain = load_mining_chain(AtomicDB(leveldb_cold_state))
    chaindb = chain.chaindb
    head = chaindb.get_canonical_head()
    assert head.block_number == 42
    assert head.state_root == (
        b"\xecVG\x1dT\xd7l'M/\xfd\xfe\xf961:\xc2\x10\xc5\xbd)+&\xd6\x82\xe43\x1c$$\xb3\xb5"
    )
    return chaindb


async def wait_for_head(headerdb, header, sync_timeout=10):
    # A full header sync may involve several round trips, so we must be willing to wait a little
    # bit for them.

    async def wait_loop():
        header_at_block = None
        while header_at_block != header:
            try:
                header_at_block = headerdb.get_canonical_block_header_by_number(header.block_number)
            except HeaderNotFound:
                await asyncio.sleep(0.1)
            else:
                break
        assert header_at_block == header
    try:
        await asyncio.wait_for(wait_loop(), sync_timeout)
    except asyncio.TimeoutError:
        canonical_head = headerdb.get_canonical_head()
        logging.error(
            "Could not finish syncing to %s within %ds, only arrived at %s",
            header,
            sync_timeout,
            canonical_head,
        )
        raise


async def wait_for_block(chain, block, sync_timeout=10):
    """
    Await until the block the given ``block`` is found in the ``chain``
    """

    async def wait_loop():
        synced_block = None
        while synced_block != block:
            try:
                synced_block = chain.get_canonical_block_by_number(block.number)
            except BlockNotFound:
                await asyncio.sleep(0.1)
            else:
                break
        assert synced_block == block
    try:
        await asyncio.wait_for(wait_loop(), sync_timeout)
    except asyncio.TimeoutError:
        gaps, future_tip = chain.chaindb.get_chain_gaps()
        logging.error(
            "Could not finish syncing to %s within %ds, gaps %s, future tip %s",
            block,
            sync_timeout,
            gaps,
            future_tip,
        )
        raise


async def wait_for_full_state_db(headerdb, min_block_number, timeout):
    collected_data = collections.defaultdict(int)

    def _has_full_state_db(headerdb):
        head = headerdb.get_canonical_head()
        if head.block_number < min_block_number:
            return False

        # reset statistics
        collected_data.clear()

        raw_db = headerdb.db
        state_root = head.state_root
        trie = HexaryTrie(raw_db, state_root)
        iterator = NodeIterator(trie)
        try:
            for key, value in iterator.items():
                collected_data["num_accounts"] += 1

                account = rlp.decode(value, sedes=Account)

                if account.code_hash == EMPTY_SHA3:
                    pass
                elif account.code_hash not in raw_db:
                    # missing bytecodes
                    logging.warning("Missing bytecode for account at 0x%s", key.hex())
                    return False
                else:
                    collected_data["num_bytecodes"] += 1

                if account.storage_root != BLANK_NODE_HASH:
                    storage_trie = HexaryTrie(raw_db, account.storage_root)
                    try:
                        for _key in NodeIterator(storage_trie).keys():
                            # We don't care what the keys are, just that we can iterate through them
                            collected_data["num_storage_slots"] += 1
                    except MissingTraversalNode as exc:
                        logging.warning("Missing storage for account at 0x%s: %s", key.hex(), exc)
                        return False

        except MissingTraversalNode:
            return False
        else:
            return True

    async def _wait_loop():
        while not _has_full_state_db(headerdb):
            await asyncio.sleep(0.3)

    try:
        await asyncio.wait_for(_wait_loop(), timeout)
    except asyncio.TimeoutError:
        canonical_head = headerdb.get_canonical_head()
        logging.error(
            "Could not finish syncing all state. Synced from block #%s to %s in %ds, with %s",
            min_block_number,
            canonical_head,
            timeout,
            collected_data,
        )
        raise
