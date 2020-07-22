from dataclasses import dataclass
from typing import Any, Generic, Type, TypeVar  # noqa: F401

from eth_typing import BlockNumber, Hash32
from lahja import BaseEvent, BaseRequestResponseEvent
import ssz

from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.typing import Timestamp

TData = TypeVar("TData", Deposit, Eth1Data)
T = TypeVar("T", bound="SSZSerializableEvent[Any]")


@dataclass
class SSZSerializableEvent(BaseEvent, Generic[TData]):

    sedes: Type[TData]
    data_bytes: bytes
    error: Exception = None

    @classmethod
    def from_data(cls: Type[T], data: TData) -> T:
        # FIXME: Find out why mypy complains with the following error here:
        #   Access to generic instance variables via class is ambiguous
        return cls(
            sedes=cls.sedes,  # type: ignore
            data_bytes=ssz.encode(data),
        )

    def to_data(self) -> TData:
        if self.error is not None:
            raise self.error
        return ssz.decode(self.data_bytes, self.sedes)


class GetDepositResponse(SSZSerializableEvent[Deposit]):
    sedes = Deposit


@dataclass
class GetDepositRequest(BaseRequestResponseEvent[GetDepositResponse]):
    deposit_count: int
    deposit_index: int

    @staticmethod
    def expected_response_type() -> Type[GetDepositResponse]:
        return GetDepositResponse


class GetEth1DataResponse(SSZSerializableEvent[Eth1Data]):
    sedes = Eth1Data


@dataclass
class GetEth1DataRequest(BaseRequestResponseEvent[GetEth1DataResponse]):
    distance: BlockNumber
    eth1_voting_period_start_timestamp: Timestamp

    @staticmethod
    def expected_response_type() -> Type[GetEth1DataResponse]:
        return GetEth1DataResponse


@dataclass
class GetDistanceResponse(BaseEvent):
    distance: int
    error: Exception = None


@dataclass
class GetDistanceRequest(BaseRequestResponseEvent[GetDistanceResponse]):
    block_hash: Hash32
    eth1_voting_period_start_timestamp: Timestamp

    @staticmethod
    def expected_response_type() -> Type[GetDistanceResponse]:
        return GetDistanceResponse
