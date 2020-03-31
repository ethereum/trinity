import shutil
from typing import Collection

from eth.db.backends.level import LevelDB
import trio

from eth2.beacon.db.chain import BeaconChainDB
from trinity._utils.trio_utils import wait_for_interrupts
from trinity.boot_info import BootInfo
from trinity.bootstrap import (
    configure_parsers,
    ensure_data_dir_is_initialized,
    install_logging,
    load_trinity_config_from_parser_args,
    parse_cli,
    resolve_common_log_level_or_error,
    validate_component_cli,
)
from trinity.cli_parser import parser, subparser
from trinity.components.registry import get_components_for_trio_beacon_client
from trinity.config import BeaconAppConfig
from trinity.constants import APP_IDENTIFIER_BEACON
from trinity.extensibility import TrioComponent
from trinity.initialization import (
    ensure_beacon_dirs,
    initialize_beacon_database,
    is_beacon_database_initialized,
)


async def _run_components_until_interrupt(
    components: Collection[TrioComponent], boot_info
):
    async with trio.open_nursery() as nursery:
        for component_cls in components:
            component = component_cls(boot_info)
            if not component.is_enabled:
                continue
            nursery.start_soon(component.run)
        await wait_for_interrupts()
        nursery.cancel_scope.cancel()


def main_beacon() -> None:
    app_identifier = APP_IDENTIFIER_BEACON
    component_types = get_components_for_trio_beacon_client()
    sub_configs = (BeaconAppConfig,)

    configure_parsers(parser, subparser, component_types)
    args = parse_cli()

    common_log_level = resolve_common_log_level_or_error(args)

    trinity_config = load_trinity_config_from_parser_args(
        parser, args, app_identifier, sub_configs
    )

    ensure_data_dir_is_initialized(trinity_config)
    handlers, child_process_log_level, logger_levels = install_logging(
        args, trinity_config, common_log_level
    )
    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        child_process_log_level=child_process_log_level,
        logger_levels=logger_levels,
        profile=bool(args.profile),
    )

    validate_component_cli(component_types, boot_info)

    if hasattr(args, "func"):
        args.func(args, trinity_config)
        return

    # TODO what if my components doesn't care about this??
    app_config = trinity_config.get_app_config(BeaconAppConfig)
    ensure_beacon_dirs(app_config)

    runtime_component_types = tuple(
        component_cls
        for component_cls in component_types
        if issubclass(component_cls, TrioComponent)
    )
    trio.run(
        _run_components_until_interrupt, runtime_component_types, boot_info
    )
    if trinity_config.trinity_tmp_root_dir:
        shutil.rmtree(trinity_config.trinity_root_dir)
