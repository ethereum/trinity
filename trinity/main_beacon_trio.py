from eth.db.backends.level import LevelDB
import trio

from eth2.beacon.db.chain import BeaconChainDB
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
from trinity.components.registry import get_components_for_beacon_client
from trinity.config import BeaconAppConfig
from trinity.constants import APP_IDENTIFIER_BEACON
from trinity.extensibility import ComponentAPI
from trinity.initialization import (
    ensure_beacon_dirs,
    initialize_beacon_database,
    is_beacon_database_initialized,
)

TrioComponentAPI = ComponentAPI


async def _run_trio_components_until_interrupt(components, boot_info):
    async with trio_util.get_interrupt_channel() as channel:
        async with trio.open_nursery() as nursery:
            for component_cls in components:
                component = component_cls(boot_info)
                # TODO: do we want global nursery?
                nursery.start_soon(component.run, nursery)
            await channel.receive()


def main_beacon() -> None:
    app_identifier = APP_IDENTIFIER_BEACON
    component_types = get_components_for_beacon_client()
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

    # TODO resolve as just an `Application`?
    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, "func"):
        args.func(args, trinity_config)
        return

    # TODO what if my components doesn't care about this??
    app_config = trinity_config.get_app_config(BeaconAppConfig)
    ensure_beacon_dirs(app_config)

    base_db = LevelDB(db_path=app_config.database_dir)
    chain_config = app_config.get_chain_config()
    chaindb = BeaconChainDB(base_db, chain_config.genesis_config)

    if not is_beacon_database_initialized(chaindb):
        initialize_beacon_database(chain_config, chaindb, base_db)

    runtime_component_types = tuple(
        component_cls
        for component_cls in component_types
        if issubclass(component_cls, TrioComponentAPI)
    )
    trio.run(_run_trio_components_until_interrupt, runtime_component_types, boot_info)
