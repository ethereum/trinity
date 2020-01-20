from argparse import ArgumentParser, _SubParsersAction
from pathlib import Path

from async_service import Service, TrioManager

from eth_typing import BlockNumber

from lahja import EndpointAPI

from web3 import Web3

from eth2.beacon.tools.builder.initializer import (
    create_keypair_and_mock_withdraw_credentials,
)
from eth2.beacon.tools.builder.validator import create_mock_deposit_data
from eth2.beacon.tools.fixtures.loading import load_yaml_at
from eth2.beacon.typing import Timestamp
from trinity.boot_info import BootInfo
from trinity.components.eth2.constants import ETH1_MONITOR_CONFIG
from trinity.components.eth2.eth1_monitor.eth1_data_provider import (
    BaseEth1DataProvider,
    FakeEth1DataProvider,
    Web3Eth1DataProvider,
)
from trinity.components.eth2.beacon.validator import ETH1_FOLLOW_DISTANCE
from trinity.config import BeaconAppConfig
from trinity.db.manager import DBClient
from trinity.events import ShutdownRequest
from trinity.extensibility import TrioIsolatedComponent

from .eth1_monitor import Eth1Monitor
from .eth1_data_provider import AVERAGE_BLOCK_TIME

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
        arg_parser.add_argument(
            "--eth1-monitor-config",
            type=str,
            help="path to eth1 monitor config file",
        )
        arg_parser.add_argument(
            "--fake-eth1-data",
            help="Use fake Eth1 data provider",
            action="store_true",
        )
        arg_parser.add_argument(
            "--web3-ipc-endpoint",
            type=str,
            help="IPC endpoint of web3 provider, e.g., /my/node/ipc/path",
        )
        arg_parser.add_argument(
            "--web3-http-endpoint",
            type=str,
            help="HTTP endpoint of web3 provider, e.g., http://127.0.0.1:8545",
        )
        arg_parser.add_argument(
            "--web3-ws-endpoint",
            type=str,
            help="WebSocket endpoint of web3 provider, e.g., ws://127.0.0.1:8546",
        )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        chain_config = beacon_app_config.get_chain_config()
        base_db = DBClient.connect(trinity_config.database_ipc_path)

        # Load the config from eth1_monitor_config
        if boot_info.args.eth1_monitor_config:
            eth1_monitor_config = load_yaml_at(Path(boot_info.args.eth1_monitor_config))
        else:
            eth1_monitor_config = load_yaml_at(
                trinity_config.trinity_root_dir / ETH1_MONITOR_CONFIG
            )
        (deposit_contract_abi,) = eth1_monitor_config["deposit_contract_abi"],
        deposit_contract_address = eth1_monitor_config["deposit_contract_address"]
        (num_blocks_confirmed,) = eth1_monitor_config["num_blocks_confirmed"],
        (polling_period,) = eth1_monitor_config["polling_period"],
        (start_block_number,) = eth1_monitor_config["start_block_number"],

        if boot_info.args.fake_eth1_data:
            # Load validators data from interop setting and
            # hardcode the deposit data into fake eth1 data provider.
            chain = chain_config.beacon_chain_class(base_db, chain_config.genesis_config)
            config = chain.get_state_machine().config
            key_set = load_yaml_at(
                Path("eth2/beacon/scripts/quickstart_state/keygen_16_validators.yaml")
            )
            pubkeys, privkeys, withdrawal_creds = create_keypair_and_mock_withdraw_credentials(
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
                    pubkeys, privkeys, withdrawal_creds
                )
            )

            # Set the timestamp of start block earlier enough so that eth1 monitor
            # can query up to 2 * `ETH1_FOLLOW_DISTANCE` of blocks in the beginning.
            start_block_timestamp = (
                chain_config.genesis_data.genesis_time -
                3 * ETH1_FOLLOW_DISTANCE * AVERAGE_BLOCK_TIME
            )
            eth1_data_provider: BaseEth1DataProvider = FakeEth1DataProvider(
                start_block_number=start_block_number,
                start_block_timestamp=Timestamp(start_block_timestamp),
                num_deposits_per_block=NUM_DEPOSITS_PER_BLOCK,
                initial_deposits=tuple(initial_deposits),
            )
        else:
            if boot_info.args.web3_ipc_endpoint is not None:
                provider = Web3.IPCProvider(boot_info.args.web3_ipc_endpoint)
            elif boot_info.args.web3_http_endpoint is not None:
                provider = Web3.HTTPProvider(boot_info.args.web3_http_endpoint)
            elif boot_info.args.web3_ws_endpoint is not None:
                provider = Web3.WebsocketProvider(boot_info.args.web3_ws_endpoint)
            else:
                provider = None

            w3: Web3 = Web3(provider=provider)
            if not w3.isConnected():
                raise Exception("No web3 provider")

            eth1_data_provider = Web3Eth1DataProvider(
                w3=w3,
                deposit_contract_address=deposit_contract_address,
                deposit_contract_abi=deposit_contract_abi,
            )

        with base_db:
            eth1_monitor_service: Service = Eth1Monitor(
                eth1_data_provider=eth1_data_provider,
                num_blocks_confirmed=num_blocks_confirmed,
                polling_period=polling_period,
                start_block_number=BlockNumber(start_block_number - 1),
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
