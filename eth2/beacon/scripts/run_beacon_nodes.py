#!/usr/bin/env python

import asyncio
import os.path
import signal
import sys


dir_root = "/tmp/ttt"
dir_alice = f"{dir_root}/alice"
dir_bob = f"{dir_root}/bob"
exe_gen_genesis = f"{os.path.dirname(sys.argv[0])}/../../../trinity/plugins/eth2/beacon/gen_genesis_json.py"
port_alice = 30304
port_bob = 30305
index_alice = 0
index_bob = 1
cmd_alice = f"trinity-beacon --validator_index={index_alice} --port={port_alice} --trinity-root-dir={dir_alice} --beacon-nodekey=6b94ffa2d9b8ee85afb9d7153c463ea22789d3bbc5d961cc4f63a41676883c19 --mock-blocks=true -l debug"
cmd_bob = f"trinity-beacon --validator_index={index_bob} --port={port_bob} --trinity-root-dir={dir_bob} --beacon-nodekey=f5ad1c57b5a489fc8f21ad0e5a19c1f1a60b8ab357a2100ff7e75f3fa8a4fd2e --bootstrap_nodes=enode://c289557985d885a3f13830a475d649df434099066fbdc840aafac23144f6ecb70d7cc16c186467f273ad7b29707aa15e6a50ec3fde35ae2e69b07b3ddc7a36c7@0.0.0.0:{port_alice} -l debug"
file_genesis_json = "genesis.json"

time_bob_wait_for_alice = 30


async def read_log(name, stream_reader):
    while True:
        line = (await stream_reader.readline()).decode('utf-8').replace('\n', '')
        print(f"{name}: {line}")
        await asyncio.sleep(0.01)


async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return proc


async def main():
    proc = await run(
        f"rm -rf {dir_alice} {dir_bob}"
    )
    await proc.wait()
    proc = await run(
        f"mkdir -p {dir_alice} {dir_bob}"
    )
    await proc.wait()

    proc = await run(
        f"{exe_gen_genesis} > {dir_alice}/{file_genesis_json}"
    )
    await proc.wait()
    proc = await run(
        f"{exe_gen_genesis} > {dir_bob}/{file_genesis_json}"
    )
    await proc.wait()

    procs_to_stop = []

    def sigint_handler(sig, frame):
        for proc in procs_to_stop:
            print(f"Stopping proc={proc}")
            proc.terminate()
        sys.exit(123)

    signal.signal(signal.SIGINT, sigint_handler)

    print("Spinning up Alice")
    proc_alice = await run(cmd_alice)
    procs_to_stop.append(proc_alice)
    asyncio.ensure_future(read_log("Alice.stdout", proc_alice.stdout))
    asyncio.ensure_future(read_log("Alice.stderr", proc_alice.stderr))

    print(f"Sleeping {time_bob_wait_for_alice} seconds to wait until Alice is initialized")
    await asyncio.sleep(time_bob_wait_for_alice)

    print("Spinning up Bob")
    proc_bob = await run(cmd_bob)
    procs_to_stop.append(proc_bob)
    asyncio.ensure_future(read_log("Bob.stdout", proc_bob.stdout))
    asyncio.ensure_future(read_log("Bob.stderr", proc_bob.stderr))

    await asyncio.sleep(1000000)


asyncio.get_event_loop().run_until_complete(main())
