from argparse import Namespace
from typing import Dict, NamedTuple, Optional, TYPE_CHECKING

from trinity.config import TrinityConfig

if TYPE_CHECKING:
    from multiprocessing import Queue  # noqa: F401


class TrinityBootInfo(NamedTuple):
    args: Namespace
    trinity_config: TrinityConfig
    log_queue: 'Queue[str]'
    log_level: Optional[int]
    logger_levels: Dict[str, int]
