from typing import Any, Dict, Optional, Tuple, Type, Union

from eth_utils import decode_hex

from eth2._utils.bls import BLSPubkey, BLSSignature, bls
from eth2._utils.bls.backends import MilagroBackend
from eth2.beacon.tools.fixtures.test_handler import TestHandler
from eth2.beacon.tools.fixtures.test_part import TestPart
from eth2.configs import Eth2Config

from . import TestType

SignatureData = Dict[str, Any]
SequenceOfBLSPubkey = Tuple[BLSPubkey, ...]
SequenceOfBLSSignature = Tuple[BLSSignature, ...]


def get_input_bls_signatures(
    test_case: Dict[str, Any]
) -> Dict[str, Tuple[BLSSignature, ...]]:
    return {
        "signatures": tuple(
            BLSSignature(decode_hex(item)) for item in test_case["input"]
        )
    }


def get_input_aggregate_verify(test_case: Dict[str, Any]) -> SignatureData:
    return {
        "pairs": tuple(
            (decode_hex(item["pubkey"]), decode_hex(item["message"]))
            for item in test_case["input"]["pairs"]
        ),
        "signature": BLSSignature(decode_hex(test_case["input"]["signature"])),
    }


def get_input_verify(test_case: Dict[str, Any]) -> SignatureData:
    return {
        "pubkey": BLSPubkey(decode_hex(test_case["input"]["pubkey"])),
        "message": decode_hex(test_case["input"]["message"]),
        "signature": BLSSignature(decode_hex(test_case["input"]["signature"])),
    }


def get_input_fast_aggregate_verify(test_case: Dict[str, Any]) -> Dict[str, Any]:
    pubkeys = tuple(
        BLSPubkey(decode_hex(item)) for item in test_case["input"]["pubkeys"]
    )

    return {
        "pubkeys": pubkeys,
        "message": decode_hex(test_case["input"]["message"]),
        "signature": BLSSignature(decode_hex(test_case["input"]["signature"])),
    }


def get_input_sign(test_case: Dict[str, Any]) -> Dict[str, Union[int, bytes]]:
    return {
        "privkey": int.from_bytes(decode_hex(test_case["input"]["privkey"]), "big"),
        "message": decode_hex(test_case["input"]["message"]),
    }


def get_output_bls_pubkey(test_case: Dict[str, Any]) -> BLSPubkey:
    return BLSPubkey(decode_hex(test_case["output"]))


def get_output_bls_signature(test_case: Dict[str, Any]) -> BLSSignature:
    return BLSSignature(decode_hex(test_case["output"]))


class AggregateHandler(TestHandler[SequenceOfBLSSignature, BLSSignature]):
    name = "aggregate"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, Any]
    ) -> SequenceOfBLSSignature:
        test_case_data = test_case_parts["data"].load()
        return get_input_bls_signatures(test_case_data)["signatures"]

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> BLSSignature:
        test_case_data = test_case_parts["data"].load()
        return get_output_bls_signature(test_case_data)

    @classmethod
    def run_with(
        _cls, inputs: SequenceOfBLSSignature, _config: Optional[Eth2Config]
    ) -> BLSSignature:
        # BLS override
        bls.use(MilagroBackend)

        return bls.Aggregate(inputs)

    @staticmethod
    def condition(output: BLSSignature, expected_output: BLSSignature) -> None:
        assert output == expected_output


class AggregateVerifyHandler(TestHandler[SignatureData, bool]):
    name = "aggregate_verify"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, Any]
    ) -> SignatureData:
        test_case_data = test_case_parts["data"].load()
        return get_input_aggregate_verify(test_case_data)

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> bool:
        test_case_data = test_case_parts["data"].load()
        return test_case_data["output"]

    @classmethod
    def run_with(_cls, inputs: SignatureData, _config: Optional[Eth2Config]) -> bool:
        # BLS override
        bls.use(MilagroBackend)

        return bls.AggregateVerify(inputs["pairs"], inputs["signature"])

    @staticmethod
    def condition(output: bool, expected_output: bool) -> None:
        assert output == expected_output


class SignHandler(TestHandler[SignatureData, BLSSignature]):
    name = "sign"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, bytes]
    ) -> SignatureData:
        test_case_data = test_case_parts["data"].load()
        return get_input_sign(test_case_data)

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> BLSSignature:
        test_case_data = test_case_parts["data"].load()
        return get_output_bls_signature(test_case_data)

    @classmethod
    def run_with(
        _cls, inputs: SignatureData, _config: Optional[Eth2Config]
    ) -> BLSSignature:
        # BLS override
        bls.use(MilagroBackend)

        return bls.Sign(inputs["privkey"], inputs["message"])

    @staticmethod
    def condition(output: BLSSignature, expected_output: BLSSignature) -> None:
        assert output == expected_output


class VerifyHandler(TestHandler[SignatureData, bool]):
    name = "verify"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, bytes]
    ) -> SignatureData:
        test_case_data = test_case_parts["data"].load()
        return get_input_verify(test_case_data)

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> bool:
        test_case_data = test_case_parts["data"].load()
        return test_case_data["output"]

    @classmethod
    def run_with(_cls, inputs: SignatureData, _config: Optional[Eth2Config]) -> bool:
        # BLS override
        bls.use(MilagroBackend)

        return bls.Verify(inputs["pubkey"], inputs["message"], inputs["signature"])

    @staticmethod
    def condition(output: bool, expected_output: bool) -> None:
        assert output == expected_output


class FastAggregateVerifyHandler(TestHandler[SignatureData, bool]):
    name = "fast_aggregate_verify"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, bytes]
    ) -> SignatureData:
        test_case_data = test_case_parts["data"].load()
        return get_input_fast_aggregate_verify(test_case_data)

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> bool:
        test_case_data = test_case_parts["data"].load()
        return test_case_data["output"]

    @classmethod
    def run_with(_cls, inputs: SignatureData, _config: Optional[Eth2Config]) -> bool:
        # BLS override
        bls.use(MilagroBackend)

        return bls.FastAggregateVerify(
            inputs["pubkeys"], inputs["message"], inputs["signature"]
        )

    @staticmethod
    def condition(output: bool, expected_output: bool) -> None:
        assert output == expected_output


BLSHandlerType = Tuple[
    Type[AggregateHandler],
    Type[SignHandler],
    Type[VerifyHandler],
    Type[FastAggregateVerifyHandler],
    Type[AggregateVerifyHandler],
]


class BLSTestType(TestType[BLSHandlerType]):
    name = "bls"

    handlers = (
        AggregateHandler,
        SignHandler,
        VerifyHandler,
        FastAggregateVerifyHandler,
        AggregateVerifyHandler,
    )
