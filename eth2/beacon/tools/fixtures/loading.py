
from typing import (
    Sequence,
)
from dataclasses import (
    dataclass,
)

from eth2.beacon.types.states import BeaconState
from eth2.configs import (
    Eth2Config,
)


@dataclass
class BaseStateTestCase:
    line_number: int
    bls_setting: bool
    description: str
    pre: BeaconState
    post: BeaconState


@dataclass
class TestFile:
    file_name: str
    config: Eth2Config
    test_cases: Sequence[BaseStateTestCase]
