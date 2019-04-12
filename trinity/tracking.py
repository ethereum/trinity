from argparse import (
    Namespace,
)
import logging

from trinity.config import (
    TrinityConfig,
)


logger = logging.getLogger('trinity.tracking')


def clear_node_db(args: Namespace, trinity_config: TrinityConfig) -> None:
    db_path = trinity_config.nodedb_path
    if db_path.exists():
        logger.info("Removing node database at: %s", db_path.resolve())
        db_path.unlink()
    else:
        logger.info("No node database found at: %s", db_path.resolve())
