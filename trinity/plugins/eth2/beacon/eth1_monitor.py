from typing import (
    NamedTuple,
)

import web3

from p2p.service import (
    BaseService,
)

from trinity._utils.shellart import (
    bold_green,
)


class DepositContractData(NamedTuple):
    contract_address: bytes
    event_abi_deposit: str
    event_abi_eth2genesis: str



# DEPOSIT_CONTRACT_ADDRESS = 0x1234
# # https://github.com/ethereum/deposit_contract/blob/dev/deposit_contract/contracts/validator_registration.v.py#L12-L19  # noqa: E501
# DEPOSIT_EVENT_ABI = ""


class Eth1Monitor(BaseService):
    """
    - Register event filter through Web3
    - Callback when
    """
    _web3: web3.Web3
    _deposit_contract_data: DepositContractData
    _event_filter: web3.eth.filter

    def __init__(self, _web3: web3.Web3, _deposit_contract_data: DepositContractData) -> None:
        self._web3 = _web3
        self._deposit_contract_data = _deposit_contract_data

    def register_event_filter(self):
        pass

    async def _run(self) -> None:
        await self.event_bus.wait_until_serving()
        # self.run_daemon_task(self.handle_slot_tick())
        await self.cancellation()
