from eth2.beacon.tools.fixtures.test_generation import (
    generate_pytests_from_eth2_fixture,
    pytest_from_eth2_fixture,
)
from eth2.beacon.tools.fixtures.test_types.bls import BLSTestType


def pytest_generate_tests(metafunc):
    generate_pytests_from_eth2_fixture(metafunc)


@pytest_from_eth2_fixture(
    {"test_types": {BLSTestType: lambda handler: handler.name == "aggregate"}}
)
def test_aggregate(test_case):
    test_case.execute()


@pytest_from_eth2_fixture(
    {"test_types": {BLSTestType: lambda handler: handler.name == "sign"}}
)
def test_sign(test_case):
    test_case.execute()
