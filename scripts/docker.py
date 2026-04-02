#!/usr/bin/env python3
import os
import sys
import subprocess
import json
import re
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def parse_memory_string(mem_str):
    mem_str = mem_str.strip()
    if mem_str == "0": return "0", "B"
    match = re.match(r'(\d+(\.\d+)?)([a-zA-Z]+)', mem_str)
    return (match.group(1), match.group(3)) if match else ("0", "B")

def convert_to_bytes(mem_str):
    units = {
        "B": 1, "KB": 1000, "KiB": 1024, "MB": 1000**2, "MiB": 1024**2,
        "GB": 1000**3, "GiB": 1024**3, "TB": 1000**4, "TiB": 1024**4,
        "PB": 1000**5, "PiB": 1024**5, "kB": 1000, "M": 1024**2, "G": 1024**3
    }
    num, unit = parse_memory_string(mem_str)
    return int(float(num) * units.get(unit, 1))

def safe_split(value, sep='/', count=2):
    parts = value.split(sep)
    return [p.strip() for p in parts] + ['0'] * (count - len(parts))

def write_metrics(lines, prom_dir, filename):
    os.makedirs(prom_dir, exist_ok=True)
    with open(os.path.join(prom_dir, filename), "w") as f:
        f.write("# HELP docker_metrics Exported docker metrics\n")
        f.write("# TYPE docker_metric gauge\n")
        f.write("\n".join(lines) + "\n")

def docker_stats():
    result = subprocess.run(['docker', 'stats', '--no-stream'], stdout=subprocess.PIPE, text=True)
    lines = result.stdout.strip().split('\n')
    if len(lines) < 2:
        return []

    headers = re.split(r'\s{2,}', lines[0])
    data_lines = lines[1:]

    metrics = []
    for line in data_lines:
        fields = re.split(r'\s{2,}', line)
        if len(fields) != len(headers): continue
        stat = dict(zip(headers, fields))
        cid = stat.get("CONTAINER ID", "")
        name = stat.get("NAME", "")
        cpu = stat.get("CPU %", "0").strip('%')
        mem_used, mem_limit = safe_split(stat.get("MEM USAGE / LIMIT", "0/0"))
        net_in, net_out = safe_split(stat.get("NET I/O", "0B/0B"))
        blk_in, blk_out = safe_split(stat.get("BLOCK I/O", "0B/0B"))

        metrics += [
            f'docker_cpu_usage{{container="{name}"}} {float(cpu)}',
            f'docker_memory_usage{{container="{name}"}} {convert_to_bytes(mem_used)}',
            f'docker_memory_limit{{container="{name}"}} {convert_to_bytes(mem_limit)}',
            f'docker_network_input{{container="{name}"}} {convert_to_bytes(net_in)}',
            f'docker_network_output{{container="{name}"}} {convert_to_bytes(net_out)}',
            f'docker_block_input{{container="{name}"}} {convert_to_bytes(blk_in)}',
            f'docker_block_output{{container="{name}"}} {convert_to_bytes(blk_out)}',
        ]
    return metrics

def docker_containers():
    result = subprocess.run(['docker', 'ps', '-a', '--format', '{{.ID}}'], stdout=subprocess.PIPE, text=True)
    ids = result.stdout.strip().split('\n')
    metrics = []
    for cid in ids:
        if not cid: continue
        inspect = subprocess.run(['docker', 'inspect', cid], stdout=subprocess.PIPE, text=True)
        info = json.loads(inspect.stdout)[0]
        name = info['Name'].lstrip('/')
        state = info['State']
        value = (
            1 if state.get('Running') else
            2 if state.get('Paused') else
            3 if state.get('Restarting') else
            4 if state.get('OOMKilled') else
            5 if state.get('Dead') else 0
        )
        metrics.append(f'docker_container_status{{container="{name}"}} {value}')
    return metrics

def main():
    sensor_name = "docker_container"
    final_results = []

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        try:
            final_metrics += docker_stats()
            final_metrics += docker_containers()

        except Exception as e:
            final_metrics.append({
                "name": "docker_error",
                "role": sensor_name,
                "message": str(e).replace('"', "'"),
                "value": 1
            })
            traceback.print_exc()

    except Exception as e:
        final_results = [{
            "name": "docker_error",
            "role": sensor_name,
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)

if __name__ == "__main__":
    main()