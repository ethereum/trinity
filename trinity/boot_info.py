from argparse import Namespace
from typing import Dict, NamedTuple, Optional

from trinity.config import TrinityConfig


class TrinityBootInfo(NamedTuple):
    args: Namespace
    trinity_config: TrinityConfig
    log_level: Optional[int]
    logger_levels: Dict[str, int]
