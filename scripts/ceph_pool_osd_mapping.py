#!/usr/bin/env python3
"""
ceph_pool_osd_mapping.py
------------------------
Generate Prometheus textfile metrics that map:
  pool_name → crush_root_bucket → hostname (node) → osd_id

Metrics produced:
  ceph_pool_node_mapping{cluster, pool_name, crush_root, hostname} = 1
  ceph_pool_osd_mapping{cluster, pool_name, crush_root, hostname, osd_id} = 1
  ceph_pool_osd_mapping_last_updated{cluster} = <unix timestamp>
"""

import json
import subprocess
import sys
import time
import os
import logging
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd: list) -> object:
    """Run a ceph command and return parsed JSON."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            log.error("Command failed: %s\nSTDERR: %s", " ".join(cmd), result.stderr.strip())
            return None
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        log.error("Command timed out: %s", " ".join(cmd))
        return None
    except json.JSONDecodeError as e:
        log.error("JSON parse error for command %s: %s", " ".join(cmd), e)
        return None


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_metric(name: str, labels: dict, value) -> str:
    label_str = ",".join(
        f'{k}="{escape_label(str(v))}"' for k, v in labels.items()
    )
    return f"{name}{{{label_str}}} {value}"


# ---------------------------------------------------------------------------
# CRUSH helpers
# ---------------------------------------------------------------------------

def get_crush_root_for_rule(crush_dump: dict, rule_id: int):
    for rule in crush_dump.get("rules", []):
        if rule["rule_id"] == rule_id:
            for step in rule.get("steps", []):
                if step.get("op") == "take":
                    item_name = step.get("item_name", "")
                    return item_name.split("~")[0]
    return None


def get_hosts_in_bucket(crush_dump: dict, bucket_name: str) -> list:
    name_map = {b["name"]: b for b in crush_dump.get("buckets", [])}

    def recurse(name: str) -> list:
        bucket = name_map.get(name)
        if not bucket:
            return []
        hosts = []
        for item in bucket.get("items", []):
            child_name = item.get("id")
            child_bucket = next(
                (b for b in crush_dump["buckets"] if b["id"] == child_name), None
            )
            if child_bucket:
                if child_bucket.get("type_name") == "host":
                    hosts.append(child_bucket["name"])
                else:
                    hosts.extend(recurse(child_bucket["name"]))
        return hosts

    return recurse(bucket_name)


def get_osds_for_host(crush_dump: dict, hostname: str) -> list:
    host_bucket = next(
        (b for b in crush_dump.get("buckets", []) if b["name"] == hostname), None
    )
    if not host_bucket:
        return []
    device_map = {d["id"]: d["name"] for d in crush_dump.get("devices", [])}
    osds = []
    for item in host_bucket.get("items", []):
        osd_id = item.get("id")
        if osd_id is not None and osd_id >= 0:
            osd_name = device_map.get(osd_id, f"osd.{osd_id}")
            osds.append(osd_name)
    return osds


# ---------------------------------------------------------------------------
# Collect
# ---------------------------------------------------------------------------

def collect(cluster: str, command: str) -> list:
    lines = []
    timestamp = int(time.time())

    log.info("Fetching osd dump...")
    osd_dump = run([command, "--cluster", cluster, "osd", "dump", "--format", "json"])
    if osd_dump is None:
        return [{"name": "ceph_pool_osd_mapping_error", "cluster": cluster, "message": "osd dump failed", "value": 1}]

    log.info("Fetching crush dump...")
    crush_dump = run([command, "--cluster", cluster, "osd", "crush", "dump", "--format", "json"])
    if crush_dump is None:
        return [{"name": "ceph_pool_osd_mapping_error", "cluster": cluster, "message": "crush dump failed", "value": 1}]

    pools = {}
    for pool in osd_dump.get("pools", []):
        pools[pool["pool"]] = {
            "name": pool["pool_name"],
            "crush_rule": pool["crush_rule"],
        }

    log.info("Found %d pools", len(pools))

    lines.append("# HELP ceph_pool_node_mapping Maps a Ceph pool to the nodes (hosts) serving it via CRUSH")
    lines.append("# TYPE ceph_pool_node_mapping gauge")
    lines.append("# HELP ceph_pool_osd_mapping Maps a Ceph pool to individual OSDs and their host via CRUSH")
    lines.append("# TYPE ceph_pool_osd_mapping gauge")
    lines.append("# HELP ceph_pool_osd_mapping_last_updated Unix timestamp of last successful metric collection")
    lines.append("# TYPE ceph_pool_osd_mapping_last_updated gauge")

    node_metrics = []
    osd_metrics = []

    for pool_id, pool_info in pools.items():
        pool_name = pool_info["name"]
        crush_rule = pool_info["crush_rule"]

        crush_root = get_crush_root_for_rule(crush_dump, crush_rule)
        if not crush_root:
            log.warning("Pool '%s': cannot resolve crush root for rule %d", pool_name, crush_rule)
            continue

        log.info("Pool '%s' → crush_root '%s'", pool_name, crush_root)

        hosts = get_hosts_in_bucket(crush_dump, crush_root)
        if not hosts:
            log.warning("Pool '%s': no hosts found under bucket '%s'", pool_name, crush_root)
            continue

        for hostname in hosts:
            node_metrics.append(build_metric(
                "ceph_pool_node_mapping",
                {"cluster": cluster, "pool_name": pool_name, "crush_root": crush_root, "hostname": hostname},
                1
            ))
            osds = get_osds_for_host(crush_dump, hostname)
            for osd_name in osds:
                osd_metrics.append(build_metric(
                    "ceph_pool_osd_mapping",
                    {"cluster": cluster, "pool_name": pool_name, "crush_root": crush_root, "hostname": hostname, "osd_id": osd_name},
                    1
                ))

    lines.extend(node_metrics)
    lines.extend(osd_metrics)
    lines.append(build_metric("ceph_pool_osd_mapping_last_updated", {"cluster": cluster}, timestamp))

    log.info("Generated %d node metrics, %d OSD metrics", len(node_metrics), len(osd_metrics))
    return lines


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sensor_name = "ceph_pool_osd_mapping"

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        cluster = sensor_cfg.get("cluster", "ceph")
        command = sensor_cfg.get("command", "/usr/bin/ceph")

        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

        results = collect(cluster, command)
        write_prometheus_metrics(prom_dirs, results, sensor_name)

    except Exception as e:
        traceback.print_exc()
        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
        write_prometheus_metrics(prom_dirs, [
            {"name": "ceph_pool_osd_mapping_error", "message": str(e).replace('"', "'"), "value": 1}
        ], sensor_name)
        sys.exit(1)
