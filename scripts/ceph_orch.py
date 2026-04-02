#!/usr/bin/env python3
import os
import sys
import json
import socket
import subprocess

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    get_sensor_config,
    write_prometheus_metrics
)

def ceph_orch_ps(cluster: str):
    results = []
    try:
        result = subprocess.run(
            [f'{command}', '--cluster', cluster, 'orch', 'ps', '--refresh', '--format', 'json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False
        )
        if result.returncode != 0:
            error_msg = result.stderr.strip().replace('"', "'")
            results.append({
                "name": "ceph_orch_error",
                "message": f"ceph_orch_ps_error: {error_msg}",
                "value": 1
            })
            return results

        data = json.loads(result.stdout)

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        results.append({
            "name": "ceph_orch_error",
            "message": f"unexpected_error: {error_msg}",
            "value": 1
        })
        return results

    for daemon in data:
        labels = {
            "service_name": daemon.get("service_name", ""),
            "daemon_name": daemon.get("daemon_name", ""),
            "daemon_id": daemon.get("daemon_id", ""),
            "daemon_type": daemon.get("daemon_type", ""),
            "version": daemon.get("version", ""),
        }

        # ---- FIX: Ceph orch ps JSON uses status (0/1) and status_desc ("running"/"stopped")
        status_desc = str(daemon.get("status_desc", "")).lower().strip()
        if status_desc:
            status_val = 1 if status_desc == "running" else 0
        else:
            # fallback: some versions may only provide numeric "status"
            raw_status = daemon.get("status", 0)
            try:
                status_val = 1 if int(raw_status) == 1 else 0
            except Exception:
                status_val = 0

        results.append({
            "name": "ceph_orch_ps_status",
            "value": status_val,
            **labels
        })

        # memory_usage is numeric bytes in your output (e.g. 62306385)
        mem_val = daemon.get("memory_usage", 0)
        try:
            mem_val = float(mem_val)
        except (TypeError, ValueError):
            mem_val = 0.0

        results.append({
            "name": "ceph_orch_ps_memory_usage",
            "value": mem_val,
            **labels
        })

        # cpu_percentage is usually a string like "6.16%"
        cpu_val = daemon.get("cpu_percentage", 0)
        if isinstance(cpu_val, str):
            cpu_val = cpu_val.strip().rstrip('%')
        try:
            cpu_val = float(cpu_val)
        except (TypeError, ValueError):
            cpu_val = 0.0

        results.append({
            "name": "ceph_orch_ps_cpu_percentage",
            "value": cpu_val,
            **labels
        })

    return results


def is_active_mgr(command, cluster):
    """Return True if current host is active MGR, False if not, None if unknown."""
    try:
        cmd = [command, "--cluster", cluster, "mgr", "stat", "-f", "json"]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
        if result.returncode != 0:
            return None
        mgr_stat = json.loads(result.stdout.strip())
        active_name = mgr_stat.get("active_name", "")
        if not active_name:
            return None
        hostname = socket.gethostname().split(".")[0]
        print(hostname == active_name.split(".")[0])
        return hostname == active_name.split(".")[0]
    except Exception:
        return None


if __name__ == "__main__":
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    final_results = []
    sensor_name = 'ceph_orch'

    try:
        if not os.environ.get("FULL_PATH_CONFIG_FILENAME"):
            project = os.environ.get("PROJECT", "staging")
            load_config(project)

        sensor_cfg = get_sensor_config().get(sensor_name, {})
        cluster = str(sensor_cfg.get("cluster", "LAB"))
        command = sensor_cfg.get("command", "ceph")

        active_mgr = is_active_mgr(command, cluster)
        
        if active_mgr:
            final_results.extend(ceph_orch_ps(cluster))
            final_results.append({
                "name": "ceph_mgr_is_active",
                "sensor_name": sensor_name,
                "value": 1
            })
        else:
            final_results.append({
                "name": "ceph_mgr_is_active",
                "sensor_name": sensor_name,
                "value": 0
            })

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        final_results = [{
            "name": "ceph_orch_error",
            "message": f"config_or_runtime_error: {error_msg}",
            "value": 1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)