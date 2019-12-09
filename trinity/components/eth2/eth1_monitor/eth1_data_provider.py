from abc import ABC, abstractmethod
from typing import Any, Dict, NamedTuple, Tuple, Union

from eth_typing import Address, BLSPubkey, BLSSignature, BlockNumber, Hash32

from eth_utils import encode_hex, event_abi_to_log_topic
from web3 import Web3
from web3.utils.events import get_event_data

from eth2.beacon.typing import Gwei, Timestamp


class Eth1Block(NamedTuple):
    block_hash: Hash32
    number: BlockNumber
    timestamp: Timestamp


class DepositLog(NamedTuple):
    block_hash: Hash32
    pubkey: BLSPubkey
    # NOTE: The following noqa is to avoid a bug in pycodestyle. We can remove it after upgrading
    #   `flake8`. Ref: https://github.com/PyCQA/pycodestyle/issues/635#issuecomment-411916058
    withdrawal_credentials: Hash32  # noqa: E701
    amount: Gwei
    signature: BLSSignature

    @classmethod
    def from_contract_log_dict(cls, log: Dict[Any, Any]) -> "DepositLog":
        log_args = log["args"]
        return cls(
            block_hash=log["blockHash"],
            pubkey=log_args["pubkey"],
            withdrawal_credentials=log_args["withdrawal_credentials"],
            amount=Gwei(int.from_bytes(log_args["amount"], "little")),
            signature=log_args["signature"],
        )


class BaseEth1DataProvider(ABC):

    @abstractmethod
    def get_block(self, arg: Union[BlockNumber, str]) -> Eth1Block:
        ...

    @abstractmethod
    def get_logs(self, block_number: BlockNumber) -> Tuple[DepositLog, ...]:
        ...

    @abstractmethod
    def get_deposit_count(self, block_number: BlockNumber) -> bytes:
        ...

    @abstractmethod
    def get_deposit_root(self, block_number: BlockNumber) -> Hash32:
        ...


class Web3Eth1DataProvider(BaseEth1DataProvider):

    w3: Web3

    _deposit_contract: "Web3.eth.contract"
    _deposit_event_abi: Dict[str, Any]
    _deposit_event_topic: str

    def __init__(
        self,
        w3: Web3,
        deposit_contract_address: Address,
        deposit_contract_abi: Dict[str, Any],
    ) -> None:
        self.w3 = w3
        self._deposit_contract = self.w3.eth.contract(
            address=deposit_contract_address, abi=deposit_contract_abi
        )
        self._deposit_event_abi = (
            self._deposit_contract.events.DepositEvent._get_event_abi()
        )
        self._deposit_event_topic = encode_hex(
            event_abi_to_log_topic(self._deposit_event_abi)
        )

    def get_block(self, arg: Union[int, str]) -> Eth1Block:
        block_dict = self.w3.eth.getBlock(arg)
        return Eth1Block(
            block_hash=Hash32(block_dict["hash"]),
            number=BlockNumber(block_dict["number"]),
            timestamp=Timestamp(block_dict["timestamp"]),
        )

    def get_logs(self, block_number: BlockNumber) -> Tuple[DepositLog, ...]:
        # NOTE: web3 v4 does not support `contract.events.Event.getLogs`.
        # After upgrading to v5, we can change to use the function.
        logs = self.w3.eth.getLogs(
            {
                "fromBlock": block_number,
                "toBlock": block_number,
                "address": self._deposit_contract.address,
                "topics": [self._deposit_event_topic],
            }
        )
        parsed_logs = tuple(
            DepositLog.from_contract_log_dict(
                get_event_data(self._deposit_event_abi, log)
            )
            for log in logs
        )
        return parsed_logs

    def get_deposit_count(self, block_number: BlockNumber) -> bytes:
        return self._deposit_contract.functions.get_deposit_count().call(
            block_identifier=block_number
        )

    def get_deposit_root(self, block_number: BlockNumber) -> Hash32:
        return self._deposit_contract.functions.get_deposit_root().call(
            block_identifier=block_number
        )
