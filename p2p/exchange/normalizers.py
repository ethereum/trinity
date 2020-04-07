from typing import Type

from .abc import NormalizerAPI
from .typing import TResponseCommand, TResult


class BaseNormalizer(NormalizerAPI[TResponseCommand, TResult]):
    is_normalization_slow = False


class PayloadNormalizer(NormalizerAPI[TResponseCommand, TResult]):
    """
    A normalizer that directly delegates to the payload of the response command.
    """

    def __init__(self,
                 command_type: Type[TResponseCommand],
                 payload_type: Type[TResult],
                 is_normalization_slow: bool = False):
        self.is_normalization_slow = is_normalization_slow

        # The constructor allows mypy to defer typehints automatically from something like:
        # normalizer = DefaultNormalizer(BlockHeadersV66, Tuple[BlockHeaderAPI, ...])
        # instead of the slightly longer form:
        # normalizer: DefaultNormalizer[BlockHeadersV66, Tuple[BlockHeaderAPI, ...]] = DefaultNormalizer() # noqa: E501

    @staticmethod
    def normalize_result(cmd: TResponseCommand) -> TResult:
        return cmd.payload
