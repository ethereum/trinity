import pathlib
import signal
import sys
import tempfile
import time
import os
import zipfile

import pexpect
import pytest

from eth.constants import (
    GENESIS_BLOCK_NUMBER,
)

from eth_utils.hexadecimal import remove_0x_prefix

from tests.integration.helpers import (
    run_command_and_detect_errors,
    scan_for_errors,
)

from trinity.config import (
    TrinityConfig,
)
from trinity.constants import (
    ASSETS_DIR,
)
from trinity.tools.async_process_runner import AsyncProcessRunner
from trinity._utils.async_iter import (
    contains_all
)
from trinity._utils.chains import (
    get_nodekey_path,
    load_nodekey,
)


ROPSTEN_GENESIS_HASH = '0x41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d'
MAINNET_GENESIS_HASH = '0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3'


# Great for debugging the AsyncProcessRunner
# @pytest.mark.asyncio
# async def test_ping():
#     async with AsyncProcessRunner.run(['ping', 'www.google.de'], 10) as runner:
#         assert await contains_all(runner.stdout, {'bytes from'})


@pytest.fixture(autouse=True)
def single_process_group(monkeypatch):
    monkeypatch.setenv('TRINITY_SINGLE_PROCESS_GROUP', "1")


def amend_command_for_unused_port(command, unused_tcp_port):
    # use a random port each time, in case a previous run went awry and left behind a
    # trinity instance
    command += (f'--port={unused_tcp_port}',)
    return command


@pytest.fixture
def prepopulated_datadir():
    # A mainnet datadir with the first 580 headers, but no block bodies.
    zipped_datadir = pathlib.Path(__file__).parent / 'fixtures' / 'trinity_headerchain_datadir.zip'
    with zipfile.ZipFile(zipped_datadir, 'r') as zipped, tempfile.TemporaryDirectory() as temp_dir:
        zipped.extractall(temp_dir)
        yield pathlib.Path(temp_dir) / 'trinity'


@pytest.mark.asyncio
async def test_trinity_sync_from_trinity(unused_tcp_port_factory, prepopulated_datadir):
    port1 = unused_tcp_port_factory()
    listen_ip = '0.0.0.0'
    nodekey = load_nodekey(get_nodekey_path(prepopulated_datadir / 'mainnet'))
    serving_enode = f'enode://{remove_0x_prefix(nodekey.public_key.to_hex())}@{listen_ip}:{port1}'
    command1 = amend_command_for_unused_port(
        ('trinity', '--trinity-root-dir', str(prepopulated_datadir), '--disable-discovery'),
        port1)
    async with AsyncProcessRunner.run(command1, timeout_sec=120) as runner1:
        assert await contains_all(runner1.stderr, {serving_enode})
        port2 = unused_tcp_port_factory()
        command2 = amend_command_for_unused_port(
            ('trinity', '--disable-discovery', '--preferred-node', serving_enode),
            port2)
        async with AsyncProcessRunner.run(command2, timeout_sec=120) as runner2:
            assert await contains_all(runner2.stderr, {
                "Adding ETHPeer",
                "Imported 192 headers",
                "Caught up to skeleton peer",
            })

            # A weak assertion to try and ensure our nodes are actually talking to each other.
            assert await contains_all(runner1.stderr, {
                "Adding ETHPeer",
            })


@pytest.mark.parametrize(
    'command',
    (
        ('trinity',),
        ('trinity', '--ropsten',),
    )
)
@pytest.mark.asyncio
async def test_expected_logs_for_full_mode(command, unused_tcp_port):
    command = amend_command_for_unused_port(command, unused_tcp_port)
    # Since this short-circuits on success, we can set the timeout high.
    # We only hit the timeout if the test fails.
    async with AsyncProcessRunner.run(command, timeout_sec=120) as runner:
        assert await contains_all(runner.stderr, {
            "Started DB server process",
            "Starting components",
            "Components started",
            "Running Tx Pool",
            "Running server",
            "IPC started at",
        })


@pytest.mark.parametrize(
    'command',
    (
        ('trinity', '--disable-tx-pool',),
        ('trinity', '--disable-tx-pool', '--ropsten',),
    )
)
@pytest.mark.asyncio
async def test_expected_logs_for_full_mode_with_txpool_disabled(command, unused_tcp_port):
    command = amend_command_for_unused_port(command, unused_tcp_port)
    # Since this short-circuits on sucess, we can set the timeout high.
    # We only hit the timeout if the test fails.
    async with AsyncProcessRunner.run(command, timeout_sec=120) as runner:
        assert await contains_all(runner.stderr, {
            "Started DB server process",
            "Starting components",
            "Components started",
            "Running server",
            "IPC started at",
        })


@pytest.mark.parametrize(
    'command',
    (
        ('trinity', '--sync-mode=light'),
        ('trinity', '--sync-mode=light', '--ropsten'),
    )
)
@pytest.mark.asyncio
async def test_expected_logs_with_disabled_txpool(command, unused_tcp_port):
    command = amend_command_for_unused_port(command, unused_tcp_port)
    # Since this short-circuits on success, we can set the timeout high.
    # We only hit the timeout if the test fails.
    async with AsyncProcessRunner.run(command, timeout_sec=120) as runner:
        assert await contains_all(runner.stderr, {
            "Started DB server process",
            "Starting components",
            "Components started",
        })


@pytest.mark.parametrize(
    'command',
    (
        ('trinity', '--sync-mode=light',),
        ('trinity', '--sync-mode=light', '--ropsten',),
    )
)
@pytest.mark.asyncio
async def test_expected_logs_for_light_mode(command):
    async with AsyncProcessRunner.run(command, timeout_sec=40) as runner:
        assert await contains_all(runner.stderr, {
            "Started DB server process",
            "Starting components",
            "Components started",
            "IPC started at",
        })


@pytest.mark.parametrize(
    'command, expected_network_id, expected_genesis_hash, expected_chain_id',
    (
        (('trinity', '--log-level=DEBUG'), 1, MAINNET_GENESIS_HASH, 1),
        (('trinity', '--ropsten', '--log-level=DEBUG'), 3, ROPSTEN_GENESIS_HASH, 3),
        (
            (
                'trinity',
                f'--genesis={ASSETS_DIR}/eip1085/devnet.json',
                # We don't have a way to refer to the tmp xdg_trinity_root here so we
                # make up this replacement marker
                '--data-dir={trinity_root_path}/devnet',
                '--network-id=4711',
                '--disable-tx-pool',
                '--log-level=DEBUG',
            ),
            4711, '0x065fd78e53dcef113bf9d7732dac7c5132dcf85c9588a454d832722ceb097422', 3735928559
        ),
    )
)
@pytest.mark.asyncio
async def test_web3_commands_via_attached_console(command,
                                                  expected_network_id,
                                                  expected_genesis_hash,
                                                  expected_chain_id,
                                                  xdg_trinity_root,
                                                  unused_tcp_port):

    command = tuple(
        fragment.replace('{trinity_root_path}', str(xdg_trinity_root))
        for fragment
        in command
    )
    # The test mostly fails because the JSON-RPC requests time out. We slim down
    # services to make the application less busy and improve the overall answer rate.
    command += ('--sync-mode=none', '--disable-discovery', '--disable-upnp')
    command = amend_command_for_unused_port(command, unused_tcp_port)
    attach_cmd = list(command[1:] + ('attach',))

    async with AsyncProcessRunner.run(command, timeout_sec=120) as runner:
        assert await contains_all(runner.stderr, {
            "Started DB server process",
            "Starting components",
            "Components started",
            "IPC started at",
            # Ensure we do not start making requests before Trinity is ready.
            # Waiting for the JSON-RPC API to be announced seems to be
            # late enough in the process for this to be the case.
            "New EventBus Endpoint connected bjson-rpc-api",
        })

        attached_trinity = pexpect.spawn(
            'trinity', attach_cmd, logfile=sys.stdout, encoding="utf-8")
        try:
            attached_trinity.expect_exact("An instance of Web3 connected to the running chain")
            attached_trinity.sendline("w3.net.version")
            attached_trinity.expect_exact(f"'{expected_network_id}'")
            attached_trinity.sendline("w3")
            attached_trinity.expect_exact("web3.main.Web3")
            attached_trinity.sendline("w3.eth.getBlock(0).number")
            attached_trinity.expect_exact(str(GENESIS_BLOCK_NUMBER))
            attached_trinity.sendline("w3.eth.getBlock(0).hash")
            attached_trinity.expect_exact(expected_genesis_hash)
            # The following verifies the admin_nodeInfo API but doesn't check the exact return
            # value of every property. Some values are non deterministic such as the current head
            # which might vary depending on how fast the node starts syncing.
            attached_trinity.sendline("w3.geth.admin.node_info()")
            attached_trinity.expect_exact("'enode': 'enode://")
            attached_trinity.expect_exact("'ip': '::'")
            attached_trinity.expect_exact("'listenAddr': '[::]")
            attached_trinity.expect_exact("'name': 'Trinity/")
            attached_trinity.expect_exact("'ports': AttributeDict({")
            attached_trinity.expect_exact("'protocols': AttributeDict({'eth': AttributeDict({'version': 'eth/65'")  # noqa: E501
            attached_trinity.expect_exact("'difficulty': ")
            attached_trinity.expect_exact(f"'genesis': '{expected_genesis_hash}'")
            attached_trinity.expect_exact("'head': '0x")
            attached_trinity.expect_exact(f"'network': {expected_network_id}")
            attached_trinity.expect_exact("'config': AttributeDict({")
            attached_trinity.expect_exact(f"'chainId': {expected_chain_id}")

        except pexpect.TIMEOUT:
            raise Exception("Trinity attach timeout")
        finally:
            attached_trinity.close()


@pytest.mark.parametrize(
    'command',
    (
        # mainnet
        ('trinity',),
        ('trinity', '--sync-mode=light',),
        # ropsten
        ('trinity', '--ropsten',),
        ('trinity', '--sync-mode=light', '--ropsten',),
    )
)
@pytest.mark.asyncio
async def test_does_not_throw_errors_on_short_run(command, unused_tcp_port):
    command = amend_command_for_unused_port(command, unused_tcp_port)
    # This is our last line of defence. This test basically observes the first
    # 20 seconds of the Trinity boot process and fails if Trinity logs any exceptions
    await run_command_and_detect_errors(command, 20)


@pytest.mark.parametrize(
    'command,expected_stderr_logs,unexpected_stderr_logs,expected_file_logs,unexpected_file_logs',
    (
        pytest.param(
            # Default run without any flag
            ('trinity',),
            # Expected stderr logs
            {'Started main process'},
            # Unexpected stderr logs
            {'>>> ping'},
            # Expected file logs
            {'Started main process', 'Logging initialized'},
            # Unexpected file logs
            {'>>> ping'},
            id="noflag:starts-both,loginit-file,ping-none",
        ),
        pytest.param(
            # Enable DEBUG2 logs across the board
            ('trinity', '-l=DEBUG2'),
            {'Started main process', '>>> ping'},
            {},
            {'Started main process', '>>> ping'},
            {},
            id="all>=debug2:starts-both,ping-both",
        ),
        pytest.param(
            # Enable DEBUG2 logs for everything except discovery which is reduced to ERROR logs
            ('trinity', '-l=DEBUG2', '-l', 'p2p.discovery=ERROR'),
            {'Started main process', '<Manager[ConnectionTrackerServer] flags=SRcfe>: running root task run[daemon=False]'},  # noqa: E501
            {'>>> ping'},
            {'Started main process', '<Manager[ConnectionTrackerServer] flags=SRcfe>: running root task run[daemon=False]'},  # noqa: E501
            {'>>> ping'},
            id="all>=debug2,p2p>=error:starts-both,manager_debug-both,ping-none",
        ),
        # Commented out because sometimes it passes and sometimes it fails.
        # pytest.param(
        #     # Reduce stderr logging to ERROR logs but report DEBUG2 or higher for file logs
        #     ('trinity', '--stderr-log-level=ERROR', '--file-log-level=DEBUG2',),
        #     {},
        #     {'Started main process', '>>> ping'},
        #     {'Started main process', '>>> ping'},
        #     {},
        #     # TODO: investigate in #1347
        #     marks=(pytest.mark.xfail),
        # ),
        pytest.param(
            # Reduce everything to ERROR logs, except discovery that should report DEBUG2 or higher
            ('trinity', '-l=ERROR', '-l', 'p2p.discovery=DEBUG2'),
            {'>>> ping'},
            {'Started main process'},
            {},
            {},
            # Increasing per-module log level to a higher value than the general log level does
            # not yet work for file logging. Once https://github.com/ethereum/trinity/issues/689
            # is resolved, the following should work.
            # {'>>> ping'},
            # {'Started main process'},
            # TODO: investigate in #1347
            marks=(pytest.mark.xfail),
            id="all>=error,p2p>=debug2:starts-nostderr,ping-stderr",
        ),
    )
)
@pytest.mark.asyncio
async def test_logger_configuration(command,
                                    expected_stderr_logs,
                                    unexpected_stderr_logs,
                                    expected_file_logs,
                                    unexpected_file_logs,
                                    unused_tcp_port):

    command = amend_command_for_unused_port(command, unused_tcp_port)

    def contains_substring(iterable, substring):
        return any(substring in x for x in iterable)

    # Saw occasional (<25%, >5%) failures in CI at 30s because of slow machines or bad luck
    async with AsyncProcessRunner.run(command, timeout_sec=45) as runner:

        stderr_logs = []

        # Collect logs up to the point when the sync begins so that we have enough logs
        # for assertions
        marker_seen_at = 0
        async for line in runner.stderr:
            if marker_seen_at != 0 and time.time() - marker_seen_at > 3:
                break
            if "DiscoveryService" in line:
                marker_seen_at = time.time()
            stderr_logs.append(line)

        for log in expected_stderr_logs:
            if not contains_substring(stderr_logs, log):
                raise AssertionError(f"Log should contain `{log}` but does not")

        for log in unexpected_stderr_logs:
            if contains_substring(stderr_logs, log):
                raise AssertionError(f"Log should not contain `{log}` but does")

        log_dir = TrinityConfig(app_identifier="eth1", network_id=1).log_dir
        log_file_path = max(log_dir.glob('*'), key=os.path.getctime)
        with open(log_file_path) as log_file:
            file_content = log_file.read()

            for log in expected_file_logs:
                if log not in file_content:
                    raise AssertionError(f"Logfile should contain `{log}` but does not")

            for log in unexpected_file_logs:
                if log in file_content:
                    raise AssertionError(f"Logfile should not contain `{log}` but does")


@pytest.mark.parametrize(
    'command',
    (
        # This should also cover beam sync but currently shutting down from beam sync causes errors.
        ('trinity', '--sync-mode=full', ),
    )
)
@pytest.mark.asyncio
# The test ensures Trinity shuts down without reporting any errors.
async def test_shutdown_does_not_throw_errors(command, unused_tcp_port):

    command = amend_command_for_unused_port(command, unused_tcp_port)

    async def run_then_shutdown_and_yield_output():
        # This test spins up Trinity, waits until it has started syncing, sends a SIGINT and then
        # tries to scan the entire shutdown process for errors. It needs a little bit more time.
        async with AsyncProcessRunner.run(command, timeout_sec=50) as runner:

            # Somewhat arbitrary but we wait until the syncer starts before we trigger the shutdown.
            # At this point, most of the internals should be set up, leaving us with more room for
            # failure which is what we are looking for in this test.
            trigger = "BeamSyncService"
            triggered = False
            async for line in runner.stderr:
                if trigger in line:
                    triggered = True
                    runner.kill(signal.SIGINT)

                # We are only interested in the output that is created
                # after we initiate the shutdown
                if triggered:
                    yield line

    await scan_for_errors(run_then_shutdown_and_yield_output())


@pytest.mark.parametrize(
    'block_number, info, success',
    (
        (0, "Successfully exported Block #0", True),
        (1, "Block number 1 does not exist in the database", False),
    )
)
@pytest.mark.asyncio
async def test_block_export(block_number, info, success):

    with tempfile.TemporaryDirectory() as export_path:

        export_file = pathlib.Path(export_path) / 'export.rlp'

        assert not export_file.exists()

        command = ('trinity', 'export', str(export_file), str(block_number))
        async with AsyncProcessRunner.run(command, timeout_sec=40) as runner:
            assert await contains_all(runner.stderr, {info})

        file_exists_after_export = export_file.exists()
        assert file_exists_after_export if success else not file_exists_after_export


@pytest.mark.parametrize(
    'file, info, success',
    (
        # We can not import block 2 into a fresh db (need to import block 1 first)
        ('block_2.rlp', {'Import failed'}, False),
        ('block_1.rlp', {'Successfully imported Block #1'}, True),
        (
            # Two blocks in this file
            'block_1_2.rlp',
            {
                'Successfully imported Block #1',
                'Successfully imported Block #2',
            },
            True
        ),
    )
)
@pytest.mark.asyncio
async def test_block_import(file, info, success):

    fixtures_path = pathlib.Path(__file__).parent / 'fixtures' / 'mainnet_blocks'

    import_file = fixtures_path / file

    assert import_file.exists()

    command = ('trinity', 'import', str(import_file),)
    async with AsyncProcessRunner.run(command, timeout_sec=40) as runner:
        assert await contains_all(runner.stderr, info)
