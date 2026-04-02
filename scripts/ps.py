#!/usr/bin/env python3
import os
import sys
import subprocess
import re
import traceback
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics, 
)

env = dict(os.environ)
env["LC_ALL"] = "C"

PS_CMD = ["ps", "-ww", "-eo", "pid,pcpu,pmem,vsize,rss,etimes,args", "--no-headers"]
_INT = re.compile(r"^\d+$")
_NUM = re.compile(r"^\d+(\.\d+)?$")
KTHREAD_RE = re.compile(r'^\[[^\]]+\]$')

def collect_process_metrics():
    metrics = []
    try:
        result = subprocess.run(PS_CMD, capture_output=True, text=True, check=True, env=env)
        now = int(time.time())

        for line in result.stdout.splitlines():
            parts = line.strip().split(maxsplit=6)
            if len(parts) < 7:
                continue

            pid, cpu, mem, vsize, rss, etimes, args = parts

            if not (_INT.match(pid) and _NUM.match(cpu) and _NUM.match(mem)
                    and _INT.match(vsize) and _INT.match(rss) and _INT.match(etimes)):
                continue

            try:
                vsize_bytes = int(vsize) * 1024
                rss_bytes   = int(rss)   * 1024
                uptime_sec  = int(etimes)
                start_epoch = now - uptime_sec

                process_name = args.split()[0] if args else "unknown"

                if KTHREAD_RE.match(process_name):
                    continue

            except Exception:
                continue

            labels = {"process": process_name, "pid": pid}

            metrics.append({"name": "ps_custom_cpu_usage",                  "value": float(cpu),  **labels})
            metrics.append({"name": "ps_custom_memory_usage",               "value": float(mem),  **labels})
            metrics.append({"name": "ps_custom_memory_virtual_bytes",       "value": vsize_bytes, **labels})
            metrics.append({"name": "ps_custom_memory_resident_bytes",      "value": rss_bytes,   **labels})
            metrics.append({"name": "ps_custom_process_uptime_seconds",     "value": uptime_sec,  **labels})
            metrics.append({"name": "ps_custom_process_start_time_seconds", "value": start_epoch, **labels})

    except Exception as e:
        metrics.append({
            "name": "ps_error",
            "role": "ps",
            "message": str(e).replace('"', "'"),
            "value": 1
        })
        traceback.print_exc()

    return metrics

if __name__ == '__main__':
    sensor_name = "ps"
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        load_config(project)
        final_results = collect_process_metrics()
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)
    except Exception as e:
        error_metrics = [{
            "name": "ps_error",
            "role": "ps",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()
        write_prometheus_metrics(prom_dirs, error_metrics, sensor_name)
        sys.exit(1)