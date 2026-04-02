import subprocess
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def sanitize_label(label):
    return label.replace("-", "_").replace(".", "_").replace("/", "_")

def get_process_details(process_name):
    try:
        output = subprocess.check_output(
            f"ps -eo user,pid,command | grep {process_name}",
            shell=True,
            text=True
        )
        lines = output.strip().split('\n')
        data = []
        for line in lines:
            if line and f"grep {process_name}" not in line:
                fields = line.split(maxsplit=2)
                if len(fields) >= 3:
                    data.append({
                        "user": fields[0],
                        "pid": fields[1],
                        "command": fields[2],
                        "value": 1
                    })
        return data
    except subprocess.CalledProcessError:
        return []

def generate_metrics(process_names):
    metrics = []
    for proc in process_names:
        instances = get_process_details(proc)

        metrics.append({
            "name": "total_process",
            "process_name": proc,
            "value": len(instances)
        })

        for proc_info in instances:
            metrics.append({
                "name": "process_details",
                "process_name": proc,
                "user": proc_info["user"],
                "pid": proc_info["pid"],
                "command": proc_info["command"],
                "value": proc_info["value"]
            })
    return metrics

if __name__ == '__main__':
    sensor_name = "process_status"
    final_metrics = []

    try:
        project = os.getenv("PROJECT", "staging")
        config = load_config(project)
        sensor_cfg = config["sensor"].get(sensor_name, {})
        enabled = sensor_cfg.get("enable", False)
        process_list = sensor_cfg.get("targets", [])

        if enabled and process_list:
            final_metrics = generate_metrics(process_list)
        else:
            final_metrics = [{
                "name": f"{sensor_name}_status",
                "value": 0
            }]
    except Exception as e:
        print(f"❌ {sensor_name} failed: {e}", file=sys.stderr)
        final_metrics = [{
            "name": f"{sensor_name}_error",
            "message": str(e),
            "value": 1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_metrics, sensor_name)