from typing import Type, Callable

from .abc import NormalizerAPI
from .typing import TResponseCommand, TResult


class BaseNormalizer(NormalizerAPI[TResponseCommand, TResult]):
    is_normalization_slow = False


def _pick_payload(cmd: TResponseCommand) -> TResult:
    return cmd.payload


class DefaultNormalizer(NormalizerAPI[TResponseCommand, TResult]):
    """
    A normalizer that directly delegates to the payload of the response command.
    """

    def __init__(self,
                 command_type: Type[TResponseCommand],
                 payload_type: Type[TResult],
                 is_normalization_slow: bool = False,
                 normalize_fn: Callable[[TResponseCommand], TResult] = _pick_payload):
        self.is_normalization_slow = is_normalization_slow
        self.normalize_fn = normalize_fn

        # The constructor allows mypy to defer typehints automatically from something like:
        # normalizer = DefaultNormalizer(BlockHeadersV66, Tuple[BlockHeaderAPI, ...])
        # instead of the slightly longer form:
        # normalizer: DefaultNormalizer[BlockHeadersV66, Tuple[BlockHeaderAPI, ...]] = DefaultNormalizer() # noqa: E501

    def normalize_result(self, cmd: TResponseCommand) -> TResult:
        return self.normalize_fn(cmd)
