from abc import ABC, abstractmethod
import time
from typing import Any, Dict, NamedTuple, Optional, Tuple, Union

from eth.exceptions import BlockNotFound

from eth_typing import Address, BLSPubkey, BLSSignature, BlockNumber, Hash32
from eth_utils import encode_hex, event_abi_to_log_topic

from web3 import Web3

from eth2.beacon.constants import GWEI_PER_ETH
from eth2.beacon.tools.builder.validator import (
    make_deposit_tree_and_root,
    mk_key_pair_from_seed_index,
    sign_proof_of_possession,
)
from eth2.beacon.types.deposit_data import DepositMessage, DepositData
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


def convert_deposit_log_to_deposit_data(deposit_log: DepositLog) -> DepositData:
    return DepositData.create(
        pubkey=deposit_log.pubkey,
        withdrawal_credentials=deposit_log.withdrawal_credentials,
        amount=deposit_log.amount,
        signature=deposit_log.signature,
    )


class BaseEth1DataProvider(ABC):
    @abstractmethod
    def get_block(self, arg: Union[Hash32, int, str]) -> Optional[Eth1Block]:
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

    def get_block(self, arg: Union[Hash32, int, str]) -> Optional[Eth1Block]:
        block_dict = self.w3.eth.getBlock(arg)
        if block_dict is None:
            raise BlockNotFound
        return Eth1Block(
            block_hash=Hash32(block_dict["hash"]),
            number=BlockNumber(block_dict["number"]),
            timestamp=Timestamp(block_dict["timestamp"]),
        )

    def get_logs(self, block_number: BlockNumber) -> Tuple[DepositLog, ...]:
        # NOTE: this installs/uninstalls an event filter; we could forego
        # this if we had a transaction receipt or transaction hash instead;
        # however, that only makes sense when monitoring unmined txs: it's
        # otherwise easier to process entire blocks of deposits as batches
        logs = self.w3.eth.getLogs(
            {
                "fromBlock": block_number,
                "toBlock": block_number,
                "address": self._deposit_contract.address,
                "topics": [self._deposit_event_topic],
            }
        )
        processed_logs = tuple(
            self._deposit_contract.events.DepositEvent().processLog(log)
            for log in logs
        )
        parsed_logs = tuple(
            DepositLog.from_contract_log_dict(log)
            for log in processed_logs
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


# NOTE: This constant is for `FakeEth1DataProvider`
AVERAGE_BLOCK_TIME = 5


class FakeEth1DataProvider(BaseEth1DataProvider):

    start_block_number: BlockNumber
    start_block_timestamp: Timestamp

    num_deposits_per_block: int

    deposits: Tuple[DepositData, ...]
    num_initial_deposits: int
    latest_processed_block_number: BlockNumber

    def __init__(
        self,
        start_block_number: BlockNumber,
        start_block_timestamp: Timestamp,
        num_deposits_per_block: int,
        initial_deposits: Tuple[DepositData, ...],
    ) -> None:
        self.start_block_number = start_block_number
        self.start_block_timestamp = start_block_timestamp
        self.num_deposits_per_block = num_deposits_per_block
        self.deposits = initial_deposits
        self.num_initial_deposits = len(initial_deposits)
        self.latest_processed_block_number = start_block_number

    def _get_latest_block_number(self) -> BlockNumber:
        current_time = int(time.time())
        distance = (current_time - self.start_block_timestamp) // AVERAGE_BLOCK_TIME
        return BlockNumber(self.start_block_number + distance)

    def _get_block_time(self, block_number: BlockNumber) -> Timestamp:
        return Timestamp(
            self.start_block_timestamp +
            (block_number - self.start_block_number) * AVERAGE_BLOCK_TIME
        )

    def get_block(self, arg: Union[Hash32, int, str]) -> Optional[Eth1Block]:
        # If `arg` is block number
        if isinstance(arg, int):
            block_time = self._get_block_time(BlockNumber(arg))
            return Eth1Block(
                block_hash=Hash32(int(arg).to_bytes(32, byteorder="big")),
                number=BlockNumber(arg),
                timestamp=Timestamp(block_time),
            )
        # If `arg` is block hash
        elif isinstance(arg, bytes):
            block_number = int.from_bytes(arg, byteorder="big")
            latest_block_number = self._get_latest_block_number()
            # Block that's way in the future is presumed to be fake eth1 block in genesis state.
            # Return the block at `start_block_number` in this case.
            # The magic number `100` here stands for distance that's way in the future.
            if block_number > latest_block_number + 100:
                return Eth1Block(
                    block_hash=Hash32(
                        self.start_block_number.to_bytes(32, byteorder="big")
                    ),
                    number=BlockNumber(self.start_block_number),
                    timestamp=Timestamp(self.start_block_timestamp),
                )
            block_time = self._get_block_time(BlockNumber(block_number))
            return Eth1Block(
                block_hash=arg,
                number=BlockNumber(block_number),
                timestamp=Timestamp(block_time),
            )
        else:
            # Assume `arg` == 'latest'
            latest_block_number = self._get_latest_block_number()
            block_time = self._get_block_time(latest_block_number)
            return Eth1Block(
                block_hash=Hash32(latest_block_number.to_bytes(32, byteorder="big")),
                number=BlockNumber(latest_block_number),
                timestamp=block_time,
            )

    def get_logs(self, block_number: BlockNumber) -> Tuple[DepositLog, ...]:
        block_hash = block_number.to_bytes(32, byteorder="big")
        logs: Tuple[DepositLog, ...] = tuple()
        if block_number < self.start_block_number:
            return logs
        elif block_number == self.start_block_number:
            logs = tuple(
                DepositLog(
                    block_hash=Hash32(block_hash),
                    pubkey=deposit.pubkey,
                    withdrawal_credentials=deposit.withdrawal_credentials,
                    signature=deposit.signature,
                    amount=deposit.amount,
                )
                for deposit in self.deposits
            )
            return logs
        else:
            amount: Gwei = Gwei(32 * GWEI_PER_ETH)
            for seed in range(self.num_deposits_per_block):
                # Multiply `block_number` by 10 to shift it one digit to the left so
                # the input to the function is generated deterministically but does not
                # conflict with blocks in the future.
                pubkey, privkey = mk_key_pair_from_seed_index(block_number * 10 + seed)
                withdrawal_credentials = Hash32(b'\x12' * 32)
                deposit_data_message = DepositMessage.create(
                    pubkey=pubkey,
                    withdrawal_credentials=withdrawal_credentials,
                    amount=amount,
                )
                signature = sign_proof_of_possession(
                    deposit_message=deposit_data_message,
                    privkey=privkey,
                )
                logs = logs + (
                    DepositLog(
                        block_hash=Hash32(block_hash),
                        pubkey=pubkey,
                        withdrawal_credentials=withdrawal_credentials,
                        signature=signature,
                        amount=amount,
                    ),
                )
            return logs

    def get_deposit_count(self, block_number: BlockNumber) -> bytes:
        if block_number <= self.start_block_number:
            return self.num_initial_deposits.to_bytes(32, byteorder="little")
        deposit_count = (
            self.num_initial_deposits +
            (block_number - self.start_block_number) * self.num_deposits_per_block
        )
        return deposit_count.to_bytes(32, byteorder="little")

    def get_deposit_root(self, block_number: BlockNumber) -> Hash32:
        # Check and update deposit data when deposit root is requested
        if self.latest_processed_block_number < block_number:
            for blk_number in range(self.latest_processed_block_number + 1, block_number + 1):
                deposit_logs = self.get_logs(BlockNumber(blk_number))
                self.deposits += tuple(
                    convert_deposit_log_to_deposit_data(deposit_log)
                    for deposit_log in deposit_logs
                )
            self.latest_processed_block_number = block_number
        deposit_count_bytes = self.get_deposit_count(block_number)
        deposit_count = int.from_bytes(deposit_count_bytes, byteorder="little")
        deposits = self.deposits[:deposit_count]
        _, deposit_root = make_deposit_tree_and_root(deposits)
        return deposit_root
