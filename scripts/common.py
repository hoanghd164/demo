#!/usr/bin/env python3

import sys
import os
import subprocess
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def read_conntrack_values():
    try:
        result = subprocess.run(['lsmod'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if 'nf_conntrack' not in result.stdout.decode('utf-8'):
            subprocess.run(['modprobe', 'nf_conntrack'], check=True)

        with open('/proc/sys/net/netfilter/nf_conntrack_max', 'r') as f:
            max_value = int(f.read().strip())
        with open('/proc/sys/net/netfilter/nf_conntrack_count', 'r') as f:
            count_value = int(f.read().strip())

        usage_percent = (count_value / max_value) * 100 if max_value > 0 else 0

        return [
            {"name": "nf_conntrack_max", "role": "nf_conntrack", "value": max_value},
            {"name": "nf_conntrack_count", "role": "nf_conntrack", "value": count_value},
            {"name": "nf_conntrack_percent", "role": "nf_conntrack", "value": usage_percent}
        ]
    except Exception as e:
        return [{"name": "fortigate_error", "role": "nf_conntrack", "message": str(e), "value": 1}]

def read_keepalived_status():
    metrics = []
    try:
        result = subprocess.run(['systemctl', 'is-active', 'keepalived'], stdout=subprocess.PIPE, text=True)
        status = result.stdout.strip()
        metrics.append({
            "name": "keepalived_status",
            "role": "keepalived",
            "value": 1 if status == 'active' else 0
        })

        script = """
        command -v keepalived >/dev/null 2>&1 || exit 0
        [ -f /var/run/keepalived.pid ] || exit 0
        kill -s $(keepalived --signum=DATA) $(cat /var/run/keepalived.pid) 2>/dev/null
        grep -E 'VRRP Instance|State' /tmp/keepalived.data | awk '/State/ {print $NF}' | grep -E 'MASTER|BACKUP'
        """

        result = subprocess.run(['bash', '-c', script], stdout=subprocess.PIPE)
        state = result.stdout.decode('utf-8').strip()
        metrics.append({
            "name": "keepalived_role",
            "role": "keepalived",
            "value": 1 if state == 'MASTER' else 0
        })

    except Exception as e:
        metrics.append({"name": "fortigate_error", "role": "keepalived", "message": str(e), "value": 1})
    return metrics

def read_service_status(services):
    if not isinstance(services, list):
        raise TypeError(f"Expected a list of services, got {type(services).__name__}")

    list_services_status = []
    for service in services:
        try:
            result = subprocess.run(['systemctl', 'is-active', service], stdout=subprocess.PIPE, text=True)
            status = result.stdout.strip()
            value = 1 if status == 'active' else 0
            list_services_status.append({
                "name": "service_status",
                "role": "service",
                "service_name": service,
                "value": value
            })
        except Exception as e:
            list_services_status.append({
                "name": "fortigate_error",
                "role": "service",
                "service_name": service,
                "message": str(e),
                "value": 1
            })
    return list_services_status

def main():
    sensor_name = "common"
    final_results = []
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        common_cfg = config.get("sensor", {}).get(sensor_name, {})

        if common_cfg.get("nf_conntrack", {}).get("enable", False):
            final_results += read_conntrack_values()

        if common_cfg.get("keepalived", {}).get("enable", False):
            final_results += read_keepalived_status()

        if common_cfg.get("service_status", {}).get("enable", False):
            services = common_cfg.get("service_status", {}).get("targets", [])
            final_results += read_service_status(services)

    except Exception as e:
        final_results = [{
            "name": f"{sensor_name}_error",
            "role": sensor_name,
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()
        prom_dirs = ["/tmp"]

    write_prometheus_metrics(prom_dirs, final_results, sensor_name)

if __name__ == "__main__":
    main()
