from typing import (
    Callable,
    NamedTuple,
)

import web3

from p2p.service import (
    BaseService,
)

from trinity._utils.shellart import (
    bold_green,
)


class DepositContractInfo(NamedTuple):
    contract_address: bytes
    event_abi_deposit: str
    event_abi_eth2genesis: str


class GenesisLogData(NamedTuple):
    pass


class DepositLogData(NamedTuple):
    pass


# DEPOSIT_CONTRACT_ADDRESS = 0x1234
# # https://github.com/ethereum/deposit_contract/blob/dev/deposit_contract/contracts/validator_registration.v.py#L12-L19  # noqa: E501
# DEPOSIT_EVENT_ABI = ""


CBGenesisData = Callable[[GenesisLogData], None]
CBDepositData = Callable[[DepositLogData], None]


class Eth1Monitor(BaseService):
    """
    - Register event filter through Web3
    - Callback when
    """
    _web3: web3.Web3
    _deposit_contract_data: DepositContractInfo
    _event_filter: web3.eth.filter
    # callback which is called when a new genesis log is emmited
    _cb_genesis_data: CBGenesisData
    # callback which is called when a new deposit log is emmited
    _cb_deposit_data: CBDepositData

    def __init__(self, _web3: web3.Web3, _deposit_contract_data: DepositContractInfo) -> None:
        self._web3 = _web3
        self._deposit_contract_data = _deposit_contract_data

    def register_event_filter(self) -> None:
        pass

    def subscribe_genesis_data(self, cb_func: CBGenesisData) -> None:
        self._cb_genesis_data = cb_func

    def subscribe_deposit_data(self, cb_func: CBDepositData) -> None:
        self._cb_deposit_data = cb_func

    async def _run(self) -> None:
        await self.event_bus.wait_until_serving()
        # self.run_daemon_task(self.handle_slot_tick())
        await self.cancellation()
