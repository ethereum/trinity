from argparse import ArgumentParser, _SubParsersAction
import json
from pathlib import Path

from async_service import Service, TrioManager

from eth_typing import BlockNumber

from lahja import EndpointAPI

# from web3 import Web3

from eth2.beacon.tools.builder.initializer import (
    create_keypair_and_mock_withdraw_credentials,
)
from eth2.beacon.tools.builder.validator import create_mock_deposit_data
from eth2.beacon.tools.fixtures.loading import load_yaml_at
from eth2.beacon.typing import Timestamp
from trinity.boot_info import BootInfo
from trinity.components.eth2.eth1_monitor.configs import deposit_contract_json
from trinity.components.eth2.eth1_monitor.eth1_data_provider import FakeEth1DataProvider
from trinity.components.eth2.beacon.base_validator import ETH1_FOLLOW_DISTANCE
from trinity.config import BeaconAppConfig
from trinity.db.manager import DBClient
from trinity.events import ShutdownRequest
from trinity.extensibility import TrioIsolatedComponent

from .eth1_monitor import Eth1Monitor
from .eth1_data_provider import AVERAGE_BLOCK_TIME

# Fake eth1 monitor config
# TODO: These configs should be read from a config file, e.g., `eth1_monitor_config.yaml`.
DEPOSIT_CONTRACT_ABI = json.loads(deposit_contract_json)["abi"]
DEPOSIT_CONTRACT_ADDRESS = b"\x12" * 20
NUM_BLOCKS_CONFIRMED = 2
POLLING_PERIOD = AVERAGE_BLOCK_TIME // 2
START_BLOCK_NUMBER = BlockNumber(1000)

# Configs for fake Eth1DataProvider
NUM_DEPOSITS_PER_BLOCK = 1


class Eth1MonitorComponent(TrioIsolatedComponent):

    name = "Eth1 Monitor"
    endpoint_name = "eth1-monitor"

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        # TODO: For now we use fake eth1 monitor.
        pass
        # arg_parser.add_argument(
        #     "--eth1client-rpc",
        #     help="RPC HTTP endpoint of Eth1 client ",
        # )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        chain_config = beacon_app_config.get_chain_config()
        base_db = DBClient.connect(trinity_config.database_ipc_path)

        # TODO: For now we use fake eth1 monitor.
        # if boot_info.args.eth1client_rpc:
        #     w3: Web3 = Web3.HTTPProvider(boot_info.args.eth1client_rpc)
        # else:
        #     w3: Web3 = None

        # TODO: For now we use fake eth1 monitor. So we load validators data from
        # interop setting and hardcode the deposit data into fake eth1 data provider.
        chain = chain_config.beacon_chain_class(base_db, chain_config.genesis_config)
        config = chain.get_state_machine().config
        key_set = load_yaml_at(
            Path("eth2/beacon/scripts/quickstart_state/keygen_16_validators.yaml")
        )
        pubkeys, privkeys, withdrawal_credentials = create_keypair_and_mock_withdraw_credentials(
            config, key_set  # type: ignore
        )
        initial_deposits = (
            create_mock_deposit_data(
                config=config,
                pubkey=pubkey,
                privkey=privkey,
                withdrawal_credentials=withdrawal_credential,
            )
            for pubkey, privkey, withdrawal_credential in zip(
                pubkeys, privkeys, withdrawal_credentials
            )
        )

        # Set the timestamp of start block earlier enough so that eth1 monitor
        # can query up to 2 * `ETH1_FOLLOW_DISTANCE` of blocks in the beginning.
        start_block_timestamp = (
            chain_config.genesis_data.genesis_time - 3 * ETH1_FOLLOW_DISTANCE * AVERAGE_BLOCK_TIME
        )
        with base_db:
            fake_eth1_data_provider = FakeEth1DataProvider(
                start_block_number=START_BLOCK_NUMBER,
                start_block_timestamp=Timestamp(start_block_timestamp),
                num_deposits_per_block=NUM_DEPOSITS_PER_BLOCK,
                initial_deposits=tuple(initial_deposits),
            )

            eth1_monitor_service: Service = Eth1Monitor(
                eth1_data_provider=fake_eth1_data_provider,
                num_blocks_confirmed=NUM_BLOCKS_CONFIRMED,
                polling_period=POLLING_PERIOD,
                start_block_number=BlockNumber(START_BLOCK_NUMBER - 1),
                event_bus=event_bus,
                base_db=base_db,
            )

            try:
                await TrioManager.run_service(eth1_monitor_service)
            except Exception:
                await event_bus.broadcast(
                    ShutdownRequest("Eth1 Monitor ended unexpectedly")
                )
                raise
