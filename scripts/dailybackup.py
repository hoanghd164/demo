#!/usr/bin/env python3
import os
import sys
import time
import re
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    write_prometheus_metrics,
    get_str
)

def walk_dir_json(path, depth=0, max_depth=2):
    result = {"name": os.path.basename(path), "path": path, "type": "directory", "children": []}
    if depth >= max_depth:
        return result

    try:
        with os.scandir(path) as entries:
            for entry in sorted(entries, key=lambda e: e.name):
                try:
                    stat = entry.stat()
                    mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat.st_mtime))
                    item = {
                        "name": entry.name,
                        "path": entry.path,
                        "size_bytes": stat.st_size,
                        "modified_time": mtime,
                        "type": "directory" if entry.is_dir() else "file"
                    }
                    if entry.is_dir():
                        item["children"] = walk_dir_json(entry.path, depth + 1, max_depth)["children"]
                    result["children"].append(item)
                except Exception as e:
                    result["children"].append({
                        "name": entry.name,
                        "path": entry.path,
                        "error": str(e)
                    })
    except Exception as e:
        result["error"] = str(e)
    return result

def reformat_by_volume_id(volume_entry):
    result = {}
    vol_name = volume_entry['name']
    match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$', vol_name)
    if not match:
        return result
    volume_id = match.group(1)
    result[volume_id] = {"full": None, "diffs": []}
    for child in volume_entry.get("children", []):
        if "size_bytes" not in child:
            continue
        base = {
            "name": child["name"],
            "size_bytes": child["size_bytes"],
            "modified_time": child["modified_time"]
        }
        if "-full" in child["name"]:
            result[volume_id]["full"] = base
        elif "-diff" in child["name"]:
            result[volume_id]["diffs"].append(base)
    result[volume_id]["diffs"].sort(key=lambda x: x["modified_time"])
    return result

def to_epoch(timestamp_str):
    return int(time.mktime(time.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")))

def main(pool_name, pool_paths):
    final_output = {}
    prom_metrics = []

    for pool_path in pool_paths:
        result = walk_dir_json(pool_path)
        for volume_entry in result.get("children", []):
            partial = reformat_by_volume_id(volume_entry)
            for vol_id, data in partial.items():
                data["__pool_path"] = pool_path
                final_output[vol_id] = data

    for vol_id, data in final_output.items():
        pool_path_value = ""
        if data.get("full") and "__pool_path" in data["full"]:
            pool_path_value = data["full"]["__pool_path"]
        elif data.get("diffs") and len(data["diffs"]) > 0 and "__pool_path" in data["diffs"][0]:
            pool_path_value = data["diffs"][0]["__pool_path"]

        base_labels = {
            "pool_name": pool_name,
            "pool_path": pool_path_value,
            "id": vol_id
        }

        if data["full"]:
            mtime = data["full"]["modified_time"]
            ts = to_epoch(mtime)
            labels = {
                **base_labels,
                "volume_name": data["full"]["name"],
                "type": "full",
                "modified_time": mtime,
                "modified_timestamp": str(ts)
            }
            prom_metrics.append({"name": "dailybackup_volumes_summary", **labels, "value": data["full"]["size_bytes"]})

        for diff in data["diffs"]:
            mtime = diff["modified_time"]
            ts = to_epoch(mtime)
            labels = {
                **base_labels,
                "volume_name": diff["name"],
                "type": "diff",
                "modified_time": mtime,
                "modified_timestamp": str(ts)
            }
            prom_metrics.append({"name": "dailybackup_volumes_summary", **labels, "value": diff["size_bytes"]})

    prom_metrics = [m for m in prom_metrics if isinstance(m, dict)]
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, prom_metrics, sensor_name=f"dailybackup_{pool_name}")

    return True

if __name__ == '__main__':
    sensor_name = 'dailybackup'

    while True:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🔁 Starting dailybackup scan", flush=True)

        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config['sensor'][sensor_name]
        active_sensor = sensor_cfg.get('enable', False)
        pools = sensor_cfg.get('pools', {})
        interval = sensor_cfg.get('interval', 7200)

        if active_sensor:
            threads = []

            for pool_name, paths in pools.items():
                valid_paths = [p.rstrip('/') for p in paths if os.path.exists(p.rstrip('/'))]
                if not valid_paths:
                    continue

                print(f"📦 Scanning: {pool_name} ({', '.join(valid_paths)})")

                t = threading.Thread(target=main, args=(pool_name, valid_paths))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Done. Sleeping {interval} seconds...\n", flush=True)
        time.sleep(interval)
