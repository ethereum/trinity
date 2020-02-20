from typing import (
    NamedTuple,
    Tuple,
)

from async_service import (
    as_service,
    ManagerAPI,
)

import psutil
from p2p import trio_utils
from trinity.components.builtin.metrics.registry import HostMetricsRegistry


class CpuStats(NamedTuple):

    # Time spent on all processes
    global_time: int
    # Time spent waiting on IO
    global_wait_io: int


class DiskStats(NamedTuple):

    # Number of read operations executed
    read_count: int
    # Number of bytes read
    read_bytes: int
    # Number of write operations executed
    write_count: int
    # Number of bytes written
    write_bytes: int


def read_cpu_stats() -> CpuStats:
    stats = psutil.cpu_times()
    return CpuStats(
        global_time=int(stats.user + stats.nice + stats.system),
        global_wait_io=int(stats.iowait),
    )


def read_disk_stats() -> DiskStats:
    stats = psutil.disk_io_counters()
    return DiskStats(
        read_count=stats.read_count,
        read_bytes=stats.read_bytes,
        write_count=stats.write_count,
        write_bytes=stats.write_bytes,
    )


@as_service
async def collect_process_metrics(manager: ManagerAPI,
                                  registry: HostMetricsRegistry,
                                  frequency_seconds: int) -> None:

    previous: Tuple[CpuStats, DiskStats] = None

    cpu_sysload_gauge = registry.gauge('trinity.system/cpu/sysload.gauge')
    cpu_syswait_gauge = registry.gauge('trinity.system/cpu/syswait.gauge')

    disk_readdata_meter = registry.meter('trinity.system/disk/readdata.meter')
    disk_writedata_meter = registry.meter('trinity.system/disk/writedata.meter')

    async for _ in trio_utils.every(frequency_seconds):
        current = (read_cpu_stats(), read_disk_stats())

        if previous is not None:
            current_cpu_stats, current_disk_stats = current
            previous_cpu_stats, previous_disk_stats = previous
            global_time = current_cpu_stats.global_time - previous_cpu_stats.global_time
            cpu_sysload_gauge.set_value(global_time / frequency_seconds)
            global_wait = current_cpu_stats.global_wait_io - previous_cpu_stats.global_wait_io
            cpu_syswait_gauge.set_value(global_wait / frequency_seconds)

            read_bytes = current_disk_stats.read_bytes - previous_disk_stats.read_bytes
            disk_readdata_meter.mark(read_bytes)

            write_bytes = current_disk_stats.write_bytes - previous_disk_stats.write_bytes
            disk_writedata_meter.mark(write_bytes)

        previous = current
