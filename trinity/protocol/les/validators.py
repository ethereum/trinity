from typing import (
    Any,
    Dict,
)

from eth_utils import (
    ValidationError,
)

from trinity.protocol.common.validators import (
    BaseBlockHeadersValidator,
)
from . import constants


class GetBlockHeadersValidator(BaseBlockHeadersValidator):
    protocol_max_request_size = constants.MAX_HEADERS_FETCH


def match_payload_request_id(request: Dict[str, Any], response: Dict[str, Any]) -> None:
    # The request here would be a dict which would be processed to
    # make a NamedTuple message at the time of sending on wire.
    # But the response would be a NamedTuple message
    if request['request_id'] != response.request_id:
        raise ValidationError("Request `id` does not match")
