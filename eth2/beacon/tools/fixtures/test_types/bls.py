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
    input_data = test_case["input"]
    pubkeys = tuple(decode_hex(key) for key in input_data["pubkeys"])
    messages = tuple(decode_hex(msg) for msg in input_data["messages"])
    return {
        "pubkeys": pubkeys,
        "messages": messages,
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
    output = test_case["output"]
    if output:
        return BLSSignature(decode_hex(test_case["output"]))
    else:
        return BLSSignature(b"\x00")


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

    @staticmethod
    def valid(test_case_parts: Dict[str, TestPart]) -> bool:
        part = test_case_parts["data"].load()
        return bool(part["output"])

    @classmethod
    def run_with(
        _cls, inputs: SequenceOfBLSSignature, _config: Optional[Eth2Config]
    ) -> BLSSignature:
        # BLS override
        bls.use(MilagroBackend)

        return bls.aggregate(*inputs)

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

        return bls.aggregate_verify(
            inputs["signature"], inputs["pubkeys"], inputs["messages"]
        )

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

        return bls.sign(inputs["privkey"], inputs["message"])

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

        return bls.verify(inputs["message"], inputs["signature"], inputs["pubkey"])

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

        return bls.fast_aggregate_verify(
            inputs["message"], inputs["signature"], *inputs["pubkeys"]
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
