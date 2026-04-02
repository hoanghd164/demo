#!/usr/bin/env python3
import subprocess
import re
import socket
import os
import sys
from concurrent.futures import ThreadPoolExecutor
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def icmp(target_ipaddr, site, description):
    try:
        response = subprocess.check_output(
            f"ping -c 2 -W 1 {target_ipaddr}",
            shell=True,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        rtt_match = re.search(r"time=([\d.]+) ms", response)
        rtt = float(rtt_match.group(1)) if rtt_match else 0
    except subprocess.CalledProcessError:
        rtt = 0

    return [
        {
            "name": "icmp_response_time",
            "description": description,
            "site": site,
            "target_ipaddr": target_ipaddr,
            "value": rtt
        },
        {
            "name": "icmp_status",
            "description": description,
            "site": site,
            "target_ipaddr": target_ipaddr,
            "value": 1 if rtt > 0 else 0
        }
    ]

def tcp_status(target_ipaddr, target_port, description, device_name, site):
    success = False
    for _ in range(3):
        try:
            with socket.create_connection((target_ipaddr, int(target_port)), timeout=1):
                success = True
                break
        except:
            continue

    return {
        "name": "tcp_status",
        "description": description,
        "device_name": device_name,
        "site": site,
        "target_ipaddr": target_ipaddr,
        "target_port": str(target_port),
        "value": 1 if success else 0
    }

def generate_combined_output(targets):
    output = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for target in targets:
            ip = target['ip']
            site = target.get('site', '')
            device = target.get('device', '')
            for proto in target.get('protocol', []):
                port = proto.get('port')
                desc = proto.get('description', '')
                if port == 'icmp':
                    futures.append(executor.submit(icmp, ip, site, desc))
                else:
                    futures.append(executor.submit(tcp_status, ip, port, desc, device, site))
        for future in futures:
            result = future.result()
            if isinstance(result, list):
                output.extend(result)
            else:
                output.append(result)
    return output

if __name__ == "__main__":

    sensor_name = "check_host"
    final_results = []

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", [])

        def is_valid_ip(ip):
            try:
                socket.inet_aton(ip)
                return True
            except socket.error:
                return False

        for target in targets:
            ip = target.get("ip")
            if not is_valid_ip(ip):
                final_results.append({
                    "name": "check_host_error",
                    "target_ipaddr": ip,
                    "message": "invalid_ip",
                    "value": 1
                })
                continue

            try:
                results = generate_combined_output([target])
                final_results.extend(results)
            except Exception as e:
                final_results.append({
                    "name": "check_host_error",
                    "target_ipaddr": ip,
                    "message": str(e).replace('"', "'"),
                    "value": 1
                })

    except Exception as e:
        final_results.append({
            "name": "check_host_error",
            "message": str(e).replace('"', "'"),
            "value": 1
        })
        traceback.print_exc()

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)