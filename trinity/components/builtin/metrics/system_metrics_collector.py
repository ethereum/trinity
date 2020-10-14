from typing import (
    Iterator,
    List,
    NamedTuple,
)

from async_service import (
    as_service,
    ManagerAPI,
)
from eth_utils import to_tuple
import psutil

from p2p import trio_utils

from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity.exceptions import MetricsReportingError


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


class ProcessStats(NamedTuple):
    process_count: int
    thread_count: int


class SystemStats(NamedTuple):
    cpu_stats: CpuStats
    disk_stats: DiskStats
    network_stats: NetworkStats
    process_stats: ProcessStats


def read_cpu_stats() -> CpuStats:
    stats = psutil.cpu_times()
    try:
        # iowait is only available with linux
        iowait = stats.iowait
    except AttributeError:
        iowait = 0
    return CpuStats(
        global_time=int(stats.user + stats.nice + stats.system),
        global_wait_io=int(iowait),
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
        out_packets=stats.packets_sent,
    )


@to_tuple
def get_all_python_processes() -> Iterator[psutil.Process]:
    for process in psutil.process_iter():
        try:
            commands = process.cmdline()
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            continue
        except psutil.ZombieProcess:
            continue
        if any('python' in cmd for cmd in commands):
            yield process


def get_main_trinity_process() -> psutil.Process:
    python_processes = get_all_python_processes()
    for process in python_processes:
        if any('trinity' in cmd for cmd in process.cmdline()):
            return process
    raise MetricsReportingError("No 'trinity' process found.")


def read_process_stats() -> ProcessStats:
    main_trinity_process = get_main_trinity_process()
    child_processes = main_trinity_process.children(recursive=True)
    num_processes = len(child_processes) + 1
    num_child_threads = sum(collect_thread_counts_for_processes(child_processes))
    num_threads = num_child_threads + main_trinity_process.num_threads()
    return ProcessStats(
        process_count=num_processes,
        thread_count=num_threads,
    )


@to_tuple
def collect_thread_counts_for_processes(all_processes: List[psutil.Process]) -> Iterator[int]:
    for process in all_processes:
        try:
            yield process.num_threads()
        except psutil.NoSuchProcess:
            continue


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

    process_count_gauge = registry.gauge('trinity.system/processes/count.gauge')
    thread_count_gauge = registry.gauge('trinity.system/threads/count.gauge')

    async for _ in trio_utils.every(frequency_seconds):
        current = SystemStats(
            cpu_stats=read_cpu_stats(),
            disk_stats=read_disk_stats(),
            network_stats=read_network_stats(),
            process_stats=read_process_stats(),
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

            process_count_gauge.set_value(current.process_stats.process_count)
            thread_count_gauge.set_value(current.process_stats.thread_count)

        previous = current
