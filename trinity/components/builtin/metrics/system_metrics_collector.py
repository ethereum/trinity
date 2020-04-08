from typing import (
    NamedTuple,
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


class NetworkStats(NamedTuple):

    # Number of network packets sent
    out_packets: int
    # Number of network packets received
    in_packets: int


class SystemStats(NamedTuple):
    cpu_stats: CpuStats
    disk_stats: DiskStats
    network_stats: NetworkStats


def read_cpu_stats() -> CpuStats:
    stats = psutil.cpu_times()
    try:
        # iowait is only available with linux
        wait = stats.iowait
    except AttributeError:
        wait = 0
    return CpuStats(
        global_time=int(stats.user + stats.nice + stats.system),
        global_wait_io=int(wait),
    )


def read_disk_stats() -> DiskStats:
    stats = psutil.disk_io_counters()
    return DiskStats(
        read_count=stats.read_count,
        read_bytes=stats.read_bytes,
        write_count=stats.write_count,
        write_bytes=stats.write_bytes,
    )


def read_network_stats() -> NetworkStats:
    stats = psutil.net_io_counters()
    return NetworkStats(
        in_packets=stats.packets_recv,
        out_packets=stats.packets_sent
    )


@as_service
async def collect_process_metrics(manager: ManagerAPI,
                                  registry: HostMetricsRegistry,
                                  frequency_seconds: int) -> None:

    previous: SystemStats = None

    cpu_sysload_gauge = registry.gauge('trinity.system/cpu/sysload.gauge')
    cpu_syswait_gauge = registry.gauge('trinity.system/cpu/syswait.gauge')

    disk_readdata_meter = registry.meter('trinity.system/disk/readdata.meter')
    disk_writedata_meter = registry.meter('trinity.system/disk/writedata.meter')

    network_in_packets_meter = registry.meter('trinity.network/in/packets/total.meter')
    network_out_packets_meter = registry.meter('trinity.network/out/packets/total.meter')

    async for _ in trio_utils.every(frequency_seconds):
        current = SystemStats(
            cpu_stats=read_cpu_stats(),
            disk_stats=read_disk_stats(),
            network_stats=read_network_stats(),
        )

        if previous is not None:

            global_time = current.cpu_stats.global_time - previous.cpu_stats.global_time
            cpu_sysload_gauge.set_value(global_time / frequency_seconds)
            global_wait = current.cpu_stats.global_wait_io - previous.cpu_stats.global_wait_io
            cpu_syswait_gauge.set_value(global_wait / frequency_seconds)

            read_bytes = current.disk_stats.read_bytes - previous.disk_stats.read_bytes
            disk_readdata_meter.mark(read_bytes)

            write_bytes = current.disk_stats.write_bytes - previous.disk_stats.write_bytes
            disk_writedata_meter.mark(write_bytes)

            in_packets = current.network_stats.in_packets - previous.network_stats.in_packets
            network_in_packets_meter.mark(in_packets)
            out_packets = current.network_stats.out_packets - previous.network_stats.out_packets
            network_out_packets_meter.mark(out_packets)

        previous = current
