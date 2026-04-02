import os
import sys
import requests
from requests.auth import HTTPBasicAuth

# Add parent dir to sys.path to import config_loader
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def rabbitmq_info(instance_name, url_info):
    output = []
    try:
        url = f"http://{url_info['ip']}:{url_info['port']}/api/queues"
        auth = HTTPBasicAuth(url_info['user'], url_info['password'])
        response = requests.get(url, auth=auth, timeout=5)
        response.raise_for_status()
        queues = response.json()

        output.append({
            "name": "rabbitmq_total_queue",
            "role": "rabbitmq",
            "target_name": instance_name,
            "rabbitmq_host": url_info['ip'],
            "rabbitmq_port": url_info['port'],
            "value": len(queues)
        })

        total_messages = sum(queue.get('messages', 0) for queue in queues)
        output.append({
            "name": "rabbitmq_total_messages",
            "role": "rabbitmq",
            "target_name": instance_name,
            "rabbitmq_host": url_info['ip'],
            "rabbitmq_port": url_info['port'],
            "value": total_messages
        })

        for queue in queues:
            output.append({
                "name": "rabbitmq_details_queue",
                "role": "rabbitmq",
                "target_name": instance_name,
                "queue_name": queue.get('name'),
                "rabbitmq_host": url_info['ip'],
                "rabbitmq_port": url_info['port'],
                "value": queue.get('messages', 0)
            })

    except Exception as e:
        output.append({
            "name": "rabbitmq_error",
            "role": "rabbitmq",
            "target_name": instance_name,
            "error": str(e).replace('"', "'"),
            "value": 1
        })

    return output


if __name__ == '__main__':
    final_metrics = []
    sensor_name = 'rabbitmq'

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)
        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})

        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", {})
        for name, info in targets.items():
            final_metrics.extend(rabbitmq_info(name, info))

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        metrics = [{
            "name": "rabbitmq_error",
            "role": "rabbitmq",
            "message": f"config_or_main_fail: {error_msg}",
            "value": 1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_metrics, sensor_name)