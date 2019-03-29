import asyncio

import pytest
from trinity._utils.async_iter import (
    contains_all
)
from trinity.tools.async_process_runner import (
    AsyncProcessRunner,
)


dir_alice = "/tmp/ttt/alice"
dir_bob = "/tmp/ttt/bob"
cmd_alice = f"trinity-beacon --mock-blocks=true --trinity-root-dir={dir_alice} --beacon-nodekey=6b94ffa2d9b8ee85afb9d7153c463ea22789d3bbc5d961cc4f63a41676883c19 -l debug"  # noqa: E501
cmd_list_alice = cmd_alice.split(' ')
cmd_bob = f"trinity-beacon --trinity-root-dir={dir_bob} --port=5566 --beacon-nodekey=f5ad1c57b5a489fc8f21ad0e5a19c1f1a60b8ab357a2100ff7e75f3fa8a4fd2e --bootstrap_nodes=enode://c289557985d885a3f13830a475d649df434099066fbdc840aafac23144f6ecb70d7cc16c186467f273ad7b29707aa15e6a50ec3fde35ae2e69b07b3ddc7a36c7@0.0.0.0:30303  -l debug"  # noqa: E501
cmd_list_bob = cmd_bob.split(' ')


async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return proc
    # stdout, stderr = await proc.communicate()



@pytest.mark.asyncio
async def test_beacon_node_syncing(async_process_runner):
    # await async_process_runner.run(command, timeout_sec=30)

    timeout = 60
    # await run(f"mkdir -p {dir_alice}")
    # await create_and_run(f"mkdir -p {dir_alice}")
    # await AsyncProcessRunner.create_and_run(f"mkdir -p {dir_alice}")
    # await run(f"mkdir -p {dir_bob}")
    await async_process_runner.run(["rm", "-rf", dir_alice])
    await async_process_runner.run(["mkdir", "-p", dir_alice])
    await async_process_runner.run(["rm", "-rf", dir_bob])
    await async_process_runner.run(["mkdir", "-p", dir_bob])

    # await async_process_runner.run(cmd_list_alice, timeout_sec=timeout)
    # assert await contains_all(async_process_runner.stderr, {
    #     "Running server",
    # })
    p_alice = await run(cmd_alice)
    p_bob = await run(cmd_bob)
    await asyncio.sleep(timeout)
    print("!@# awaiting read")
    log_alice = await p_alice.stderr.read(100000)
    log_bob = await p_bob.stderr.read(100000)
    # print(f"!@# log_alice={log_alice}")
    assert b"Running server" in log_alice
    assert b"Running server" in log_bob
    print(f"!@# log_bob={log_bob!r}")
    await asyncio.sleep(timeout)
    log_bob_new = await p_bob.stderr.read(100000)
    msg_their_head_slot = b"their head slot"
    assert msg_their_head_slot in log_bob or msg_their_head_slot in log_bob_new



    # stdout_alice, stderr_alice = await p_alice.communicate()
    # print(f"!@# stderr_alice={stderr_alice}")

    # runner_alice, runner_bob = await asyncio.gather(
    #     AsyncProcessRunner.create_and_run(cmd_list_alice, timeout_sec=timeout),
    #     AsyncProcessRunner.create_and_run(cmd_list_bob, timeout_sec=timeout),
    # )
    # runner_alice = await AsyncProcessRunner.create_and_run(cmd_list_alice, timeout_sec=timeout)
    # assert await contains_all(runner_alice.stderr, {
    #     "Running server",
    # })
    # print(f"!@# runner_alice={runner_alice}")
    # runner_bob = await AsyncProcessRunner.create_and_run(cmd_list_bob, timeout_sec=timeout)
    # assert await contains_all(runner_bob.stderr, {
    #     "Running server",
    # })
    # print(f"!@# runner_alice={runner_bob}")


    # await async_process_runner.run(cmd_alice, timeout_sec=timeout)
    # assert await contains_all(async_process_runner.stderr, {
    #     "Running server",
    # })
    # await async_process_runner.run(cmd_bob, timeout_sec=timeout)
    # assert await contains_all(async_process_runner.stderr, {
    #     "Running server",
    # })

    # await asyncio.gather(
    #     runner_alice,
    #     runner_bob,
    # )

    # assert await contains_all(runner_alice.stderr, {
    #     "Running server",
    # })
    # assert await contains_all(runner_bob.stderr, {
    #     "Running server",
    # })
