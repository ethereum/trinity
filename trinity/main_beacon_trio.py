import logging
import os
import shutil
from typing import Collection, Sequence, Tuple, Type

import trio

from trinity._utils.trio_utils import wait_for_interrupts
from trinity._utils.version import construct_trinity_client_identifier
from trinity.boot_info import BootInfo
from trinity.bootstrap import construct_boot_info
from trinity.components.registry import get_components_for_trio_beacon_client
from trinity.config import BaseAppConfig, BeaconTrioAppConfig
from trinity.constants import APP_IDENTIFIER_BEACON
from trinity.extensibility import BaseComponentAPI
from trinity.extensibility.trio import TrioComponent
from trinity.initialization import ensure_beacon_trio_dirs


async def _run_trio_components_until_interrupt(
    components: Collection[Type[TrioComponent]], boot_info: BootInfo
) -> None:
    async with trio.open_nursery() as nursery:
        for component_cls in components:
            component = component_cls(boot_info)
            if not component.is_enabled:
                continue
            nursery.start_soon(component.run)
        await wait_for_interrupts()
        nursery.cancel_scope.cancel()


def _initialize_beacon_filesystem(boot_info: BootInfo) -> None:
    app_config = boot_info.trinity_config.get_app_config(BeaconTrioAppConfig)
    ensure_beacon_trio_dirs(app_config)


def main_entry_trio(
    app_identifier: str,
    component_types: Tuple[Type[BaseComponentAPI], ...],
    sub_configs: Sequence[Type[BaseAppConfig]],
) -> None:
    boot_info, _ = construct_boot_info(app_identifier, component_types, sub_configs)
    args = boot_info.args
    trinity_config = boot_info.trinity_config

    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, "func"):
        args.func(args, trinity_config)
        return

    _initialize_beacon_filesystem(boot_info)

    logger = logging.getLogger("trinity")
    pid = os.getpid()
    identifier = construct_trinity_client_identifier()
    logger.info(
        "Booted client with identifier: %s and process id %d", identifier, pid
    )

    runtime_component_types = tuple(
        component_cls
        for component_cls in component_types
        if issubclass(component_cls, TrioComponent)
    )
    trio.run(_run_trio_components_until_interrupt, runtime_component_types, boot_info)
    # NOTE: mypy bug that does not type this... it works in `./bootstrap.py`
    if trinity_config.trinity_tmp_root_dir:  # type: ignore
        shutil.rmtree(trinity_config.trinity_root_dir)


def main_beacon() -> None:
    app_identifier = APP_IDENTIFIER_BEACON
    component_types = get_components_for_trio_beacon_client()
    sub_configs = (BeaconTrioAppConfig,)
    main_entry_trio(app_identifier, component_types, sub_configs)
