from pathlib import Path
from typing import (
    Sequence,
    Optional,
)

from dataclasses import (
    dataclass,
)
import pytest
from ruamel.yaml import (
    YAML,
)

from eth_utils import (
    to_tuple,
)
from py_ecc import bls  # noqa: F401
from ssz.tools import (
    from_formatted_dict,
)

from eth2.configs import (
    Eth2Config,
)
from eth2.beacon.tools.misc.ssz_vector import (
    override_vector_lengths,
)
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    Slot,
)
# from eth2.beacon.state_machines.forks.serenity.configs import (
#     SERENITY_CONFIG,
# )
from eth2.beacon.tools.fixtures.loading import (
    BaseStateTestCase,
    TestFile,
)

# Test files
ROOT_PROJECT_DIR = Path(__file__).cwd()

BASE_FIXTURE_PATH = ROOT_PROJECT_DIR / 'eth2-fixtures' / 'tests'

SANITY_FIXTURE_PATH = BASE_FIXTURE_PATH / 'sanity'

SLOTS_FIXTURE_PATH = SANITY_FIXTURE_PATH / 'slots'

FIXTURE_FILE_NAMES = [
    "sanity_slots_minimal.yaml",
    # "sanity_slots_mainnet.yaml",
]


# test_format
@dataclass
class SanityTestCase(BaseStateTestCase):
    slots: Optional[Sequence[Slot]] = None
    blocks: Optional[Sequence[BeaconBlock]] = None


# Mock bls verification for these tests
#
def mock_bls_verify(message_hash, pubkey, signature, domain):
    return True


def mock_bls_verify_multiple(pubkeys,
                             message_hashes,
                             signature,
                             domain):
    return True


@pytest.fixture(autouse=True)
def mock_bls(mocker, request):
    if 'noautofixture' in request.keywords:
        return

    mocker.patch('py_ecc.bls.verify', side_effect=mock_bls_verify)
    mocker.patch('py_ecc.bls.verify_multiple', side_effect=mock_bls_verify_multiple)


#
# Helpers for generating test suite
#
def get_all_test_files(file_names):
    test_files = ()
    yaml = YAML()
    for file_name in file_names:
        file_to_open = SLOTS_FIXTURE_PATH / file_name
        with open(file_to_open, 'U') as f:
            new_text = f.read()
            try:
                data = yaml.load(new_text)
                # config = SERENITY_CONFIG
                config_name = data['config']
                config = get_config(config_name)
                parsed_test_cases = tuple(
                    parse_test_case(test_case, config)
                    for test_case in data['test_cases']
                )
                test_file = TestFile(
                    file_name=file_name,
                    config=config,
                    test_cases=parsed_test_cases,
                )
            except yaml.YAMLError as exc:
                print(exc)
            test_files += test_file
    return test_files


def get_config(config_name):
    # TODO: change the path after the constants presets are copied to submodule
    path = ROOT_PROJECT_DIR / 'tests/eth2/fixtures-tests'
    yaml = YAML()
    file_name = config_name + '.yaml'
    file_to_open = path / file_name
    with open(file_to_open, 'U') as f:
        new_text = f.read()
        try:
            data = yaml.load(new_text)
        except yaml.YAMLError as exc:
            print(exc)
    return Eth2Config(**data)


def parse_test_case(test_case, config):
    if 'bls_setting' not in test_case or test_case['bls_setting'] == 2:
        bls_setting = False
    else:
        bls_setting = True

    description = test_case['description']
    override_vector_lengths(config)
    pre = from_formatted_dict(test_case['pre'], BeaconState)
    post = from_formatted_dict(test_case['post'], BeaconState)

    if 'blocks' in test_case:
        blocks = tuple(from_formatted_dict(block, BeaconBlock) for block in test_case['blocks'])
    else:
        blocks = None

    slots = test_case['slots'] if 'slots' in test_case else None
    return SanityTestCase(
        line_number=test_case.lc.line,
        bls_setting=bls_setting,
        description=description,
        pre=pre,
        post=post,
        slots=slots,
        blocks=blocks,
    )


def state_fixture_mark_fn(fixture_name):
    if fixture_name == 'test_transfer':
        return pytest.mark.skip(reason="has not implemented")
    else:
        return None


@to_tuple
def get_test_cases(fixture_file_names):
    # TODO: batch reading files
    test_files = get_all_test_files(fixture_file_names)
    for test_file in test_files:
        for test_case in test_file.test_cases:
            test_id = f"{test_file.file_name}::{test_case.description}:{test_case.line_number}"
            mark = state_fixture_mark_fn(test_case.description)
            if mark is not None:
                yield pytest.param(test_case, id=test_id, marks=(mark,))
            else:
                yield pytest.param(test_case, id=test_id)


all_test_cases = get_test_cases(FIXTURE_FILE_NAMES)


@pytest.mark.parametrize(
    "test_case",
    all_test_cases
)
def test_state(base_db, test_case):
    execute_state_transtion(test_case, base_db)


def generate_config_by_dict(dict_config):
    for key in list(dict_config):
        if 'DOMAIN_' in key:
            # DOMAIN is defined in SignatureDomain
            dict_config.pop(key, None)
    return Eth2Config(**dict_config)


def execute_state_transtion(test_case, base_db):
    pass
