#!/usr/bin/env python3
import os
import sys
import json
import subprocess

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

error_metrics = []

def convert_memory_to_bytes(memory_str):
    if memory_str.endswith('Ki'):
        return int(memory_str.strip('Ki')) * 1024
    elif memory_str.endswith('Mi'):
        return int(memory_str.strip('Mi')) * 1024**2
    elif memory_str.endswith('Gi'):
        return int(memory_str.strip('Gi')) * 1024**3
    elif memory_str.endswith('Ti'):
        return int(memory_str.strip('Ti')) * 1024**4
    elif memory_str.endswith('K'):
        return int(memory_str.strip('K')) * 1000
    elif memory_str.endswith('M'):
        return int(memory_str.strip('M')) * 1000**2
    elif memory_str.endswith('G'):
        return int(memory_str.strip('G')) * 1000**3
    elif memory_str.endswith('T'):
        return int(memory_str.strip('T')) * 1000**4
    else:
        return int(memory_str)

def convert_cpu_to_millicores(cpu_str):
    if cpu_str.endswith('m'):
        return int(cpu_str.strip('m'))
    else:
        return int(cpu_str) * 1000

def get_node_info():
    metrics = []
    try:
        result = subprocess.run(['kubectl', 'get', 'nodes', '-o', 'json'], capture_output=True, text=True, check=True)
        nodes = json.loads(result.stdout)

        for item in nodes['items']:
            name = item['metadata']['name']
            labels = item['metadata'].get('labels', {})
            role = 'master' if 'node-role.kubernetes.io/master' in labels else 'worker' if 'node-role.kubernetes.io/worker' in labels else 'none'
            node_ip = next(addr['address'] for addr in item['status']['addresses'] if addr['type'] == 'InternalIP')
            version = item['status']['nodeInfo']['kubeletVersion']
            os_image = item['status']['nodeInfo']['osImage']
            tunnel_ip = item['metadata']['annotations'].get('projectcalico.org/IPv4IPIPTunnelAddr', '')
            status = 1 if next(cond['status'] for cond in item['status']['conditions'] if cond['type'] == 'Ready') == 'True' else 0
            capacity = item['status']['capacity']
            cpu = float(capacity.get('cpu', 0))
            memory = convert_memory_to_bytes(capacity.get('memory', '0'))
            pods = int(capacity.get('pods', 0))

            labels_str = {
                "node": name,
                "node_ip": node_ip,
                "tunnel_ip": tunnel_ip,
                "role": role,
                "version": version
            }

            metrics.extend([
                {"name": "kube_node_status", "value": status, **labels_str},
                {"name": "kube_node_cpu_limit", "value": cpu, **labels_str},
                {"name": "kube_node_memory_limit", "value": memory, **labels_str},
                {"name": "kube_node_max_pods", "value": pods, **labels_str}
            ])
    except Exception as e:
        error_msg = str(e).replace('"', "'")
        error_metrics.append({
            "name": "kubernetes_error",
            "message": f"node_info_fail: {error_msg}",
            "value": 1
        })
    return metrics

def get_pod_info():
    metrics = []
    try:
        result = subprocess.run(['kubectl', 'get', 'po', '-A', '-o', 'json'], capture_output=True, text=True, check=True)
        pods = json.loads(result.stdout)

        for pod in pods['items']:
            namespace = pod['metadata']['namespace']
            pod_name = pod['metadata']['name']
            node = pod['spec'].get('nodeName', 'none')
            pod_ip = pod['status'].get('podIP', 'none')
            status = 1 if pod['status'].get('phase') == 'Running' else 0
            restarts = sum(container.get('restartCount', 0) for container in pod['status'].get('containerStatuses', []))

            base_labels = {
                "namespace": namespace,
                "pod": pod_name,
                "node": node,
                "pod_ip": pod_ip
            }

            metrics.append({"name": "kube_pod_status", "value": status, **base_labels})
            metrics.append({"name": "kube_pod_restart", "value": restarts, **base_labels})

            for container in pod['spec'].get('containers', []):
                resources = container.get('resources', {})
                limits = resources.get('limits', {})
                requests = resources.get('requests', {})

                if 'memory' in limits:
                    metrics.append({
                        "name": "kube_pod_memory_limit", "value": convert_memory_to_bytes(limits['memory']),
                        "unit": "bytes", **base_labels
                    })
                if 'cpu' in limits:
                    metrics.append({
                        "name": "kube_pod_cpu_limit", "value": convert_cpu_to_millicores(limits['cpu']),
                        "unit": "millicores", **base_labels
                    })
                if 'memory' in requests:
                    metrics.append({
                        "name": "kube_pod_memory_request", "value": convert_memory_to_bytes(requests['memory']),
                        "unit": "bytes", **base_labels
                    })
                if 'cpu' in requests:
                    metrics.append({
                        "name": "kube_pod_cpu_request", "value": convert_cpu_to_millicores(requests['cpu']),
                        "unit": "millicores", **base_labels
                    })
    except Exception as e:
        error_msg = str(e).replace('"', "'")
        error_metrics.append({
            "name": "kubernetes_error",
            "message": f"pod_info_fail: {error_msg}",
            "value": 1
        })
    return metrics

if __name__ == "__main__":
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    sensor_name = "kubernetes"
    final_metrics = []
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)
        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        final_metrics.extend(get_node_info())
        final_metrics.extend(get_pod_info())

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        final_metrics = [{
            "name": "kubernetes_error",
            "message": f"config_or_main_fail: {error_msg}",
            "value": 1
        }]

    final_metrics.extend(error_metrics)
    write_prometheus_metrics(prom_dirs, final_metrics, sensor_name)