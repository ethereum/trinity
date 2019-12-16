from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import json
import time

from async_service import Service, TrioManager

from eth_typing import BlockNumber

from lahja import EndpointAPI

# from web3 import Web3

from eth2.beacon.typing import Timestamp
from trinity.boot_info import BootInfo
from trinity.components.eth2.eth1_monitor.configs import deposit_contract_json
from trinity.components.eth2.eth1_monitor.eth1_data_provider import FakeEth1DataProvider
from trinity.config import BeaconAppConfig
from trinity.db.manager import DBClient
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    TrioIsolatedComponent,
)


from .eth1_monitor import Eth1Monitor


# Fake eth1 monitor config
# TODO: These configs should be read from a config file, e.g., `eth1_monitor_config.yaml`.
DEPOSIT_CONTRACT_ABI = json.loads(deposit_contract_json)["abi"]
DEPOSIT_CONTRACT_ADDRESS = b"\x12" * 20
NUM_BLOCKS_CONFIRMED = 100
POLLING_PERIOD = 10
START_BLOCK_NUMBER = BlockNumber(1000)

# Configs for fake Eth1DataProvider
NUM_DEPOSITS_PER_BLOCK = 0
START_BLOCK_TIMESTAMP = Timestamp(int(time.time()) - 2100)  # Around 45 mins ago


class Eth1MonitorComponent(TrioIsolatedComponent):

    name = "Eth1 Monitor"
    endpoint_name = "eth1-monitor"

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        # TODO: For now we use fake eth1 monitor.
        pass
        # arg_parser.add_argument(
        #     "--eth1client-rpc",
        #     help="RPC HTTP endpoint of Eth1 client ",
        # )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config

        # TODO: For now we use fake eth1 monitor.
        # if self.boot_info.args.eth1client_rpc:
        #     w3: Web3 = Web3.HTTPProvider(self.boot_info.args.eth1client_rpc)
        # else:
        #     w3: Web3 = None
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        base_db = DBClient.connect(trinity_config.database_ipc_path)
        with base_db:
            chain_config = beacon_app_config.get_chain_config()
            chain = chain_config.beacon_chain_class(
                base_db,
                chain_config.genesis_config,
            )
            state = chain.get_state_by_slot(chain_config.genesis_config.GENESIS_SLOT)
            fake_eth1_data_provider = FakeEth1DataProvider(
                start_block_number=START_BLOCK_NUMBER,
                start_block_timestamp=START_BLOCK_TIMESTAMP,
                num_deposits_per_block=NUM_DEPOSITS_PER_BLOCK,
                num_initial_deposits=state.eth1_data.deposit_count,
            )

            eth1_monitor_service: Service = Eth1Monitor(
                eth1_data_provider=fake_eth1_data_provider,
                num_blocks_confirmed=NUM_BLOCKS_CONFIRMED,
                polling_period=POLLING_PERIOD,
                start_block_number=START_BLOCK_NUMBER,
                event_bus=event_bus,
                base_db=base_db,
            )

            try:
                await TrioManager.run_service(eth1_monitor_service)
            except Exception:
                await event_bus.broadcast(ShutdownRequest("Eth1 Monitor ended unexpectedly"))
                raise
