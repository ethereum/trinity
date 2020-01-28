#!/usr/bin/env python

import asyncio
from collections import defaultdict
import json
import logging
from pathlib import Path
import signal
import sys
import time
from typing import (
    ClassVar,
    Dict,
    List,
    MutableSet,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

import eth_utils
from eth_utils import encode_hex, remove_0x_prefix
from libp2p.crypto.secp256k1 import Secp256k1PrivateKey
from libp2p.peer.id import ID
from multiaddr import Multiaddr
from ruamel.yaml import YAML
import ssz
from web3 import Web3
from web3.middleware import geth_poa_middleware

from eth2.beacon.constants import GWEI_PER_ETH
from eth2.beacon.tools.builder.validator import (
    mk_key_pair_from_seed_index,
    sign_proof_of_possession,
)
from eth2.beacon.types.deposit_data import DepositData
from trinity.components.eth2.beacon.validator import ETH1_FOLLOW_DISTANCE
from trinity.components.eth2.eth1_monitor.configs import deposit_contract_json


async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    return proc


class Log(NamedTuple):
    name: str
    pattern: str
    # TODO: probably we can add dependent relationship between logs?
    timeout: int


class EventTimeOutError(Exception):
    pass


SERVER_RUNNING = Log(name="server running", pattern="Running server", timeout=60)
START_SYNCING = Log(name="start syncing", pattern="their head slot", timeout=200)

USE_FAKE_ETH1_DATA = False
genesis_dir = "eth2/beacon/scripts/poa_testnet.json"
pw_dir = "eth2/beacon/scripts/pwd.txt"
data_dir = "eth2/beacon/scripts/testnet"
eth1_addr = "0x1d9960C6535Ca1A032922d3F20fF44B2d18B0c37"


class Eth1Client:
    name: str
    port: int
    rpcport: Optional[int]

    start_time: float
    proc: asyncio.subprocess.Process
    # TODO: use CancelToken instead
    tasks: List[asyncio.Task]
    logs_expected: Dict[str, MutableSet[Log]]
    has_log_happened: Dict[Log, bool]

    dir_root: ClassVar[Path] = Path("/tmp/bbbb")
    running_nodes: ClassVar[List] = []
    logger: ClassVar[logging.Logger] = logging.getLogger(
        "eth2.beacon.scripts.run_beacon_nodes.Eth1Client"
    )

    def __init__(
        self, name: str, port: int, start_time: float, rpcport: Optional[int] = None
    ) -> None:
        self.name = name
        self.port = port
        self.rpcport = rpcport

        self.tasks = []
        self.start_time = start_time
        self.logs_expected = {}
        self.logs_expected["stdout"] = set()
        self.logs_expected["stderr"] = set()
        self.has_log_happened = defaultdict(lambda: False)

    def __repr__(self) -> str:
        return f"<Eth1Client {self.logging_name} {self.proc}>"

    @property
    def logging_name(self) -> str:
        return f"{self.name}"

    @property
    def cmd(self) -> str:
        _cmds = [
            "geth",
            "--networkid 5566",
            f"--datadir {data_dir}",
            f"--port {self.port}",
            "--rpc",
            "--rpcapi eth,web3,personal,net",
            f"--rpcport {self.rpcport}",
            "--nodiscover",
            f"--unlock {eth1_addr}",
            f"--password {pw_dir}",
            "--allow-insecure-unlock",
            "--mine",
        ]
        _cmd = " ".join(_cmds)
        return _cmd

    def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        self.proc.terminate()

    @classmethod
    def stop_all_nodes(cls) -> None:
        for node in cls.running_nodes:
            print(f"Stopping node={node}")
            node.stop()

    def add_log(self, from_stream: str, log: Log) -> None:
        if from_stream not in ("stdout", "stderr"):
            return
        self.logs_expected[from_stream].add(log)

    async def run(self) -> None:
        print(f"Spinning up {self.name}")
        self.proc = await run(self.cmd)
        self.running_nodes.append(self)
        self.tasks.append(
            asyncio.ensure_future(self._print_logs("stdout", self.proc.stdout))
        )
        self.tasks.append(
            asyncio.ensure_future(self._print_logs("stderr", self.proc.stderr))
        )
        try:
            await self._log_monitor()
        except EventTimeOutError as e:
            self.logger.debug(e)
            # FIXME: nasty
            self.stop_all_nodes()
            sys.exit(2)

    async def _log_monitor(self) -> None:
        while True:
            for from_stream, logs in self.logs_expected.items():
                for log in logs:
                    current_time = time.monotonic()
                    ellapsed_time = current_time - self.start_time
                    if not self.has_log_happened[log] and (ellapsed_time > log.timeout):
                        raise EventTimeOutError(
                            f"{self.logging_name}: log {log.name!r} is time out, "
                            f"which should have occurred in {from_stream}."
                        )
            await asyncio.sleep(0.1)

    async def _print_logs(
        self, from_stream: str, stream_reader: asyncio.StreamReader
    ) -> None:
        async for line_bytes in stream_reader:
            line = line_bytes.decode("utf-8").replace("\n", "")
            # TODO: Preprocessing
            self._record_happening_logs(from_stream, line)
            print(f"{self.logging_name}\t{line}")

    def _record_happening_logs(self, from_stream: str, line: str) -> None:
        for log in self.logs_expected[from_stream]:
            if log.pattern in line:
                self.logger.debug('log "log.name" occurred in %s', from_stream)
                self.has_log_happened[log] = True


class Node:
    name: str
    node_privkey: str
    port: int
    preferred_nodes: Tuple["Node", ...]
    rpcport: Optional[int]
    metrics_port: Optional[int]
    api_port: Optional[int]
    eth1_monitor_config: Optional[str]
    eth1_client_rpcport: Optional[int]

    start_time: float
    proc: asyncio.subprocess.Process
    # TODO: use CancelToken instead
    tasks: List[asyncio.Task]
    logs_expected: Dict[str, MutableSet[Log]]
    has_log_happened: Dict[Log, bool]

    dir_root: ClassVar[Path] = Path("/tmp/aaaa")
    running_nodes: ClassVar[List] = []
    logger: ClassVar[logging.Logger] = logging.getLogger(
        "eth2.beacon.scripts.run_beacon_nodes.Node"
    )

    def __init__(
        self,
        name: str,
        node_privkey: str,
        port: int,
        start_time: float,
        validators: Sequence[int],
        rpcport: Optional[int] = None,
        metrics_port: Optional[int] = None,
        api_port: Optional[int] = None,
        eth1_monitor_config: Optional[str] = None,
        eth1_client_rpcport: Optional[int] = None,
        preferred_nodes: Optional[Tuple["Node", ...]] = None,
    ) -> None:
        self.name = name
        self.node_privkey = Secp256k1PrivateKey.new(bytes.fromhex(node_privkey))
        self.port = port
        self.validators = validators
        if preferred_nodes is None:
            preferred_nodes = []
        self.preferred_nodes = preferred_nodes
        self.rpcport = rpcport
        self.metrics_port = metrics_port
        self.api_port = api_port
        self.eth1_monitor_config = eth1_monitor_config
        self.eth1_client_rpcport = eth1_client_rpcport

        self.tasks = []
        self.start_time = start_time
        self.logs_expected = {}
        self.logs_expected["stdout"] = set()
        self.logs_expected["stderr"] = set()
        # TODO: Add other logging messages in our beacon node to indicate
        # that the beacon node is successfully bootstrapped.
        # self.add_log("stderr", SERVER_RUNNING)
        self.has_log_happened = defaultdict(lambda: False)

    def __repr__(self) -> str:
        return f"<Node {self.logging_name} {self.proc}>"

    @property
    def logging_name(self) -> str:
        return f"{self.name}@{str(self.peer_id)[:5]}"

    @property
    def root_dir(self) -> Path:
        return self.dir_root / self.name

    @property
    def peer_id(self) -> ID:
        return ID.from_pubkey(self.node_privkey.get_public_key())

    @property
    def maddr(self) -> Multiaddr:
        return Multiaddr(
            f"/ip4/127.0.0.1/tcp/{self.port}/p2p/{self.peer_id.to_base58()}"
        )

    @property
    def cmd(self) -> str:
        _cmds = [
            "trinity-beacon",
            f"--port={self.port}",
            f"--trinity-root-dir={self.root_dir}",
            f"--beacon-nodekey={remove_0x_prefix(encode_hex(self.node_privkey.to_bytes()))}",
            f"--preferred_nodes={','.join(str(node.maddr) for node in self.preferred_nodes)}",
            f"--rpcport={self.rpcport}",
            "--enable-http",
            "--enable-metrics",
            "--enable-api",
            f"--api-port={self.api_port}",
            f"--metrics-port={self.metrics_port}",
            "--disable-discovery",
            "-l debug",
        ]
        if USE_FAKE_ETH1_DATA:
            _cmds += ["--fake-eth1-data"]
        else:
            _cmds += [
                f"--eth1-monitor-config={self.eth1_monitor_config}",
                f"--web3-http-endpoint=http://127.0.0.1:{str(self.eth1_client_rpcport)}",
            ]
        _cmds += [
            "interop",
            f"--validators={','.join(str(v) for v in self.validators)}",
            f"--start-time={self.start_time}",
            "--wipedb",
        ]
        _cmd = " ".join(_cmds)
        return _cmd

    def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        self.proc.terminate()

    @classmethod
    def stop_all_nodes(cls) -> None:
        for node in cls.running_nodes:
            print(f"Stopping node={node}")
            node.stop()

    def add_log(self, from_stream: str, log: Log) -> None:
        if from_stream not in ("stdout", "stderr"):
            return
        self.logs_expected[from_stream].add(log)

    async def run(self) -> None:
        print(f"Spinning up {self.name}")
        self.proc = await run(self.cmd)
        self.running_nodes.append(self)
        self.tasks.append(
            asyncio.ensure_future(self._print_logs("stdout", self.proc.stdout))
        )
        self.tasks.append(
            asyncio.ensure_future(self._print_logs("stderr", self.proc.stderr))
        )
        try:
            await self._log_monitor()
        except EventTimeOutError as e:
            self.logger.debug(e)
            # FIXME: nasty
            self.stop_all_nodes()
            sys.exit(2)

    async def _log_monitor(self) -> None:
        while True:
            for from_stream, logs in self.logs_expected.items():
                for log in logs:
                    current_time = time.monotonic()
                    ellapsed_time = current_time - self.start_time
                    if not self.has_log_happened[log] and (ellapsed_time > log.timeout):
                        raise EventTimeOutError(
                            f"{self.logging_name}: log {log.name!r} is time out, "
                            f"which should have occurred in {from_stream}."
                        )
            await asyncio.sleep(0.1)

    async def _print_logs(
        self, from_stream: str, stream_reader: asyncio.StreamReader
    ) -> None:
        async for line_bytes in stream_reader:
            line = line_bytes.decode("utf-8").replace("\n", "")
            # TODO: Preprocessing
            self._record_happening_logs(from_stream, line)
            print(f"{self.logging_name}\t{line}")

    def _record_happening_logs(self, from_stream: str, line: str) -> None:
        for log in self.logs_expected[from_stream]:
            if log.pattern in line:
                self.logger.debug('log "log.name" occurred in %s', from_stream)
                self.has_log_happened[log] = True


async def init_eth1_chain():
    import shutil
    chain_data_dir = Path(data_dir, 'geth')
    if chain_data_dir.exists():
        shutil.rmtree(chain_data_dir)
    proc = await run(f"geth init --datadir {data_dir} {genesis_dir}")
    await proc.wait()


def deposit(w3, deposit_contract, deposit_count):
    pubkey, privkey = mk_key_pair_from_seed_index(deposit_count)
    withdrawal_credentials = b"\x12" * 32
    deposit_data_message = DepositData.create(
        pubkey=pubkey,
        withdrawal_credentials=withdrawal_credentials,
        amount=32 * GWEI_PER_ETH,
    )
    signature = sign_proof_of_possession(
        deposit_message=deposit_data_message, privkey=privkey
    )
    deposit_data_message = deposit_data_message.set("signature", signature)
    deposit_input = (
        deposit_data_message.pubkey,
        deposit_data_message.withdrawal_credentials,
        deposit_data_message.signature,
        ssz.get_hash_tree_root(deposit_data_message),
    )
    tx_hash = deposit_contract.functions.deposit(*deposit_input).transact(
        {
            "from": w3.eth.accounts[0],
            "value": deposit_data_message.amount * eth_utils.denoms.gwei,
            "gas": 3000000,
        }
    )
    tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)
    assert tx_receipt["status"]


def deploy_contract(w3):
    contract_json = json.loads(deposit_contract_json)
    contract_bytecode = contract_json["bytecode"]
    contract_abi = contract_json["abi"]
    registration = w3.eth.contract(abi=contract_abi, bytecode=contract_bytecode)
    tx_hash = registration.constructor().transact(
        {"from": w3.eth.accounts[0], "gas": 4000000}
    )
    tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)
    assert tx_receipt["status"]
    registration_deployed = w3.eth.contract(
        address=tx_receipt.contractAddress, abi=contract_abi
    )
    return registration_deployed, tx_receipt["blockNumber"]


async def generate_deposits(w3, deposit_contract, interval):
    deposit_count = 0
    while deposit_count < 1000:
        deposit(w3, deposit_contract, deposit_count)
        deposit_count += 1
        await asyncio.sleep(interval)


async def main():
    eth1_start_time = int(time.time())

    proc = await run(f"rm -rf {Node.dir_root}")
    await proc.wait()
    proc = await run(f"mkdir -p {Node.dir_root}")
    await proc.wait()

    def sigint_handler(sig, frame):
        Node.stop_all_nodes()
        sys.exit(123)

    signal.signal(signal.SIGINT, sigint_handler)

    # global variable `genesis_dir` is the path to genesis file
    # global variable `data_dir` is the path to eth1 chain data
    # global variable `pw_dir` is the path to password of the geth account
    # global variable `eth1_addr` is the account of eth1 chain miner

    # geth version used: 1.9.9-stable
    if not USE_FAKE_ETH1_DATA:
        await init_eth1_chain()

        eth1_client_rpcport = 8444
        eth1_clinet = Eth1Client(
            name="alice", port=30301, rpcport=eth1_client_rpcport, start_time=eth1_start_time
        )

        asyncio.ensure_future(eth1_clinet.run())

        # Wait for geth client to bootstrap
        await asyncio.sleep(1)

        w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:" + str(eth1_client_rpcport)))

        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        assert w3.isConnected()

        deposit_contract, deployed_block_number = deploy_contract(w3)
        print(deposit_contract.address, deployed_block_number)

        # This is the block time set in genesis.json
        BLOCK_TIME = 3
        asyncio.ensure_future(generate_deposits(w3, deposit_contract, 2 * BLOCK_TIME))

        eth1_monitor_config = {
            "deposit_contract_abi": deposit_contract.abi,
            "deposit_contract_address": deposit_contract.address,
            "num_blocks_confirmed": 5,
            "polling_period": 5,
            "start_block_number": deployed_block_number,
        }
        yaml = YAML(typ="unsafe")
        config_path = Node.dir_root / "eth1_monitor_config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(eth1_monitor_config, f)

        # Wait for eth1 to progress further enough so
        # the blocks requested by beacon node will be available.
        await asyncio.sleep(2 * ETH1_FOLLOW_DISTANCE * BLOCK_TIME)

    start_delay = 20
    start_time = int(time.time()) + start_delay

    param_alice = {
        "name": "alice",
        "node_privkey": "6b94ffa2d9b8ee85afb9d7153c463ea22789d3bbc5d961cc4f63a41676883c19",
        "port": 30304,
        "preferred_nodes": [],
        "validators": [0, 1, 2, 3, 4, 5, 6, 7],
        "rpcport": 8555,
        "api_port": 5555,
        "start_time": start_time,
        "metrics_port": 9555,
    }
    if not USE_FAKE_ETH1_DATA:
        param_alice["eth1_client_rpcport"] = eth1_client_rpcport
        param_alice["eth1_monitor_config"] = config_path
    node_alice = Node(**param_alice)
    param_bob = {
        "name": "bob",
        "node_privkey": "f5ad1c57b5a489fc8f21ad0e5a19c1f1a60b8ab357a2100ff7e75f3fa8a4fd2e",
        "port": 30305,
        "preferred_nodes": [node_alice],
        "validators": [8, 9, 10, 11, 12, 13, 14, 15],
        "rpcport": 8666,
        "api_port": 5666,
        "start_time": start_time,
        "metrics_port": 9666,
    }
    if not USE_FAKE_ETH1_DATA:
        param_bob["eth1_client_rpcport"] = eth1_client_rpcport
        param_bob["eth1_monitor_config"] = config_path
    node_bob = Node(**param_bob)

    asyncio.ensure_future(node_alice.run())
    await asyncio.sleep(30)
    asyncio.ensure_future(node_bob.run())

    await asyncio.sleep(1000000)


asyncio.get_event_loop().run_until_complete(main())
