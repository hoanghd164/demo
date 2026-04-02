#!/usr/bin/env python3
import os
import sys
import json
import re
import subprocess

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def parse_memory_string(mem_str):
    match = re.match(r'(\d+(\.\d+)?)([a-zA-Z]+)', mem_str)
    if match:
        return match.group(1), match.group(3)
    raise ValueError("Invalid memory string format")

def convert_to_bytes(mem_str):
    if mem_str == '--':
        return 0
    units = {
        "B": 1, "KB": 1000, "KiB": 1024, "MB": 1000**2, "MiB": 1024**2,
        "GB": 1000**3, "GiB": 1024**3, "TB": 1000**4, "TiB": 1024**4,
        "PB": 1000**5, "PiB": 1024**5, "EB": 1000**6, "EiB": 1024**6,
        "ZB": 1000**7, "ZiB": 1024**7, "YB": 1000**8, "YiB": 1024**8,
        "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6, "kB": 1000
    }
    numeric_part, unit_part = parse_memory_string(mem_str)
    return int(float(numeric_part) * units.get(unit_part, 1))

def build_metric(name, labels, value):
    return {
        "name": name,
        **labels,
        "value": value
    }

def get_podman_stats():
    try:
        result = subprocess.run(['podman', 'stats', '--no-stream', '--format', 'json'], stdout=subprocess.PIPE)
        stats_list = json.loads(result.stdout)
        metrics = []

        for stat in stats_list:
            labels = {
                # "container_id": stat['id'],
                "container_name": stat['name']
            }

            mem_used, mem_limit = stat['mem_usage'].split('/')
            metrics += [
                build_metric("podman_memory_limit", labels, convert_to_bytes(mem_limit.strip())),
                build_metric("podman_memory_usage", labels, convert_to_bytes(mem_used.strip())),
                build_metric("podman_memory_percentage", labels, float(stat['mem_percent'].strip('%'))),
                build_metric("podman_cpu_usage", labels, float(stat['cpu_percent'].strip('%')))
            ]

            if stat['net_io'] != '-- / --':
                net_in, net_out = stat['net_io'].split('/')
                metrics.append(build_metric("podman_network_input", labels, convert_to_bytes(net_in.strip())))
                metrics.append(build_metric("podman_network_output", labels, convert_to_bytes(net_out.strip())))
            else:
                metrics.append(build_metric("podman_network_input", labels, 0))
                metrics.append(build_metric("podman_network_output", labels, 0))

            block_in, block_out = stat['block_io'].split('/')
            metrics.append(build_metric("podman_block_input", labels, convert_to_bytes(block_in.strip())))
            metrics.append(build_metric("podman_block_output", labels, convert_to_bytes(block_out.strip())))

        return metrics
    except Exception as e:
        return [build_metric("podman_error", {"message": str(e).replace('"', "'")}, 1)]

def get_containers():
    try:
        result = subprocess.run(['podman', 'ps', '-a', '--format', 'json'], stdout=subprocess.PIPE)
        containers = json.loads(result.stdout)
        metrics = []

        for container in containers:
            inspect = subprocess.run(['podman', 'inspect', container['Id']], stdout=subprocess.PIPE)
            data = json.loads(inspect.stdout)[0]
            state = data.get("State", {})
            value = int(state.get("Running", False))

            labels = {
                "container_name": data['Name'],
            }
            metrics.append(build_metric("podman_container_status", labels, value))
        return metrics
    except Exception as e:
        return [build_metric("podman_error", {"message": str(e).replace('"', "'")}, 1)]

if __name__ == "__main__":
    sensor_name = "podman"
    final_metrics = []

    try:
        project = os.getenv("PROJECT", "staging")
        config = load_config(project)
        enabled = config.get("sensor", {}).get(sensor_name, {}).get("enable", False)
        if not enabled:
            sys.exit(0)

        final_metrics += get_podman_stats()
        final_metrics += get_containers()

    except Exception as e:
        final_metrics = [build_metric("podman_error", {"message": str(e).replace('"', "'")}, 1)]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_metrics, sensor_name)