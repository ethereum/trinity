from typing import Any, Dict, Optional, Tuple, Type, Union, cast

from eth_typing import Hash32
from eth_utils import decode_hex

from eth2._utils.bls import BLSPubkey, BLSSignature, bls
from eth2._utils.bls.backends import MilagroBackend
from eth2.beacon.tools.fixtures.test_handler import TestHandler
from eth2.beacon.tools.fixtures.test_part import TestPart
from eth2.configs import Eth2Config

from . import TestType

SequenceOfBLSPubkey = Tuple[BLSPubkey, ...]
SequenceOfBLSSignature = Tuple[BLSSignature, ...]
SignatureDescriptor = Dict[str, Union[int, bytes]]


def get_input_bls_pubkeys(
    test_case: Dict[str, Any]
) -> Dict[str, Tuple[BLSPubkey, ...]]:
    return {
        "pubkeys": tuple(BLSPubkey(decode_hex(item)) for item in test_case["input"])
    }


def get_input_bls_signatures(
    test_case: Dict[str, Any]
) -> Dict[str, Tuple[BLSSignature, ...]]:
    return {
        "signatures": tuple(
            BLSSignature(decode_hex(item)) for item in test_case["input"]
        )
    }


def get_input_bls_privkey(test_case: Dict[str, Any]) -> Dict[str, int]:
    return {"privkey": int.from_bytes(decode_hex(test_case["input"]), "big")}


def get_input_sign_message(test_case: Dict[str, Any]) -> Dict[str, Union[int, bytes]]:
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


class SignHandler(TestHandler[SignatureDescriptor, BLSSignature]):
    name = "sign"

    @classmethod
    def parse_inputs(
        _cls, test_case_parts: Dict[str, TestPart], metadata: Dict[str, bytes]
    ) -> SignatureDescriptor:
        test_case_data = test_case_parts["data"].load()
        return get_input_sign_message(test_case_data)

    @staticmethod
    def parse_outputs(test_case_parts: Dict[str, TestPart]) -> BLSSignature:
        test_case_data = test_case_parts["data"].load()
        return get_output_bls_signature(test_case_data)

    @classmethod
    def run_with(
        _cls, inputs: SignatureDescriptor, _config: Optional[Eth2Config]
    ) -> BLSSignature:
        # BLS override
        bls.use(MilagroBackend)

        return bls.Sign(int(inputs["privkey"]), cast(Hash32, inputs["message"]))

    @staticmethod
    def condition(output: BLSSignature, expected_output: BLSSignature) -> None:
        assert output == expected_output


BLSHandlerType = Tuple[Type[AggregateHandler], Type[SignHandler]]


class BLSTestType(TestType[BLSHandlerType]):
    name = "bls"

    handlers = (AggregateHandler, SignHandler)
