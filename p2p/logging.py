from typing import (
    Any,
)

from p2p._utils import trim_middle
from p2p.constants import LONGEST_ALLOWED_LOG_STRING


def loggable(log_object: Any) -> str:
    return trim_middle(str(log_object), LONGEST_ALLOWED_LOG_STRING)
