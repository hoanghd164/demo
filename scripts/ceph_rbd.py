#!/usr/bin/env python3
"""
ceph_rbd.py
-----------
Collects RBD volume and snapshot metrics and writes to node_exporter textfile collector.

Metrics produced:
  rbd_volume_size_bytes, rbd_volume_locked, rbd_volume_snap_total,
  rbd_volume_snap_daily_count, rbd_volume_snap_export_count,
  rbd_volume_snap_migrate_count, rbd_volume_snap_other_count,
  rbd_volume_snap_latest_daily, rbd_volume_snap_oldest_daily,
  rbd_volume_has_today_snap, rbd_snap_info, rbd_snap_size_bytes,
  rbd_snap_protected, rbd_pool_broken_count
"""

import json
import re
import subprocess
import sys
import os
import traceback
from datetime import date
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics,
)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def run_rbd_command(pool: str, ceph_conf: str, keyring: str, rbd_user: str) -> tuple:
    """
    Run rbd ls -l command and return (data, broken_count).
    Parses stdout even on partial failure — broken images are counted via stderr.
    """
    cmd = [
        "rbd",
        "-c", ceph_conf,
        "--keyring", keyring,
        "--id", rbd_user,
        "ls", pool,
        "-l",
        "--format", "json"
    ]
    broken_count = 0
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.stderr:
            broken_count = len(re.findall(
                r'error opening [^:]+:\s*\(\d+\) No such file or directory',
                result.stderr
            ))
            if broken_count:
                print(
                    f"WARN: pool {pool} has {broken_count} broken image(s) "
                    f"(header missing) — skipping them, collecting rest",
                    file=sys.stderr
                )

        if result.stdout and result.stdout.strip():
            try:
                return json.loads(result.stdout), broken_count
            except json.JSONDecodeError as e:
                print(f"ERROR: failed to parse JSON for pool {pool}: {e}", file=sys.stderr)
                return [], broken_count

        if result.returncode != 0:
            print(f"ERROR: rbd command failed for pool {pool}: {result.stderr[:300]}", file=sys.stderr)
        return [], broken_count

    except subprocess.TimeoutExpired:
        print(f"ERROR: rbd command timed out for pool {pool}", file=sys.stderr)
        return [], 0


def classify_snap(snap_name: str) -> str:
    if snap_name.isdigit() and len(snap_name) == 8:
        return "daily"
    elif snap_name.startswith("export-"):
        return "export"
    elif snap_name.startswith("migrate-"):
        return "migrate"
    else:
        return "other"


def parse_rbd_data(pool: str, data: list) -> dict:
    volumes = {}
    snaps = defaultdict(list)

    for item in data:
        image_id = item.get("id", "")
        image_name = item.get("image", "")

        if "snapshot" not in item:
            volumes[image_id] = {
                "name": image_name,
                "size": item.get("size", 0),
                "locked": 1 if item.get("lock_type") == "exclusive" else 0,
                "format": item.get("format", 2),
            }
        else:
            snaps[image_id].append({
                "name": item.get("snapshot", ""),
                "snapshot_id": item.get("snapshot_id", 0),
                "size": item.get("size", 0),
                "protected": 1 if item.get("protected") == "true" else 0,
                "type": classify_snap(item.get("snapshot", "")),
            })

    return {"volumes": volumes, "snaps": snaps}


def generate_metrics(pool: str, parsed: dict, broken_count: int, today: str) -> list:
    metrics = []
    volumes = parsed["volumes"]
    snaps = parsed["snaps"]

    metrics.append(f'rbd_pool_broken_count{{pool="{pool}"}} {broken_count}')

    for image_id, vol in volumes.items():
        image_name = vol["name"]
        vol_snaps = snaps.get(image_id, [])

        daily_snaps   = [s for s in vol_snaps if s["type"] == "daily"]
        export_snaps  = [s for s in vol_snaps if s["type"] == "export"]
        migrate_snaps = [s for s in vol_snaps if s["type"] == "migrate"]
        other_snaps   = [s for s in vol_snaps if s["type"] == "other"]

        daily_dates  = sorted([s["name"] for s in daily_snaps])
        latest_daily = daily_dates[-1] if daily_dates else "0"
        oldest_daily = daily_dates[0]  if daily_dates else "0"
        has_today    = 1 if today in daily_dates else 0

        base = f'pool="{pool}",image="{image_name}",image_id="{image_id}"'

        metrics.append(f'rbd_volume_size_bytes{{{base}}} {vol["size"]}')
        metrics.append(f'rbd_volume_locked{{{base}}} {vol["locked"]}')
        metrics.append(f'rbd_volume_snap_total{{{base}}} {len(vol_snaps)}')
        metrics.append(f'rbd_volume_snap_daily_count{{{base}}} {len(daily_snaps)}')
        metrics.append(f'rbd_volume_snap_export_count{{{base}}} {len(export_snaps)}')
        metrics.append(f'rbd_volume_snap_migrate_count{{{base}}} {len(migrate_snaps)}')
        metrics.append(f'rbd_volume_snap_other_count{{{base}}} {len(other_snaps)}')
        metrics.append(f'rbd_volume_snap_latest_daily{{{base}}} {latest_daily}')
        metrics.append(f'rbd_volume_snap_oldest_daily{{{base}}} {oldest_daily}')
        metrics.append(f'rbd_volume_has_today_snap{{{base}}} {has_today}')

        for snap in vol_snaps:
            snap_labels = (
                f'pool="{pool}",'
                f'image="{image_name}",'
                f'image_id="{image_id}",'
                f'snap="{snap["name"]}",'
                f'snap_type="{snap["type"]}",'
                f'snap_id="{snap["snapshot_id"]}"'
            )
            metrics.append(f'rbd_snap_info{{{snap_labels}}} 1')
            metrics.append(f'rbd_snap_size_bytes{{{snap_labels}}} {snap["size"]}')
            metrics.append(f'rbd_snap_protected{{{snap_labels}}} {snap["protected"]}')

    return metrics


def build_help_headers() -> list:
    return [
        '# HELP rbd_volume_size_bytes Provisioned size of RBD volume in bytes',
        '# TYPE rbd_volume_size_bytes gauge',
        '# HELP rbd_volume_locked Whether volume has exclusive lock (1=attached, 0=detached)',
        '# TYPE rbd_volume_locked gauge',
        '# HELP rbd_volume_snap_total Total number of snapshots for this volume',
        '# TYPE rbd_volume_snap_total gauge',
        '# HELP rbd_volume_snap_daily_count Number of daily backup snapshots (YYYYMMDD format)',
        '# TYPE rbd_volume_snap_daily_count gauge',
        '# HELP rbd_volume_snap_export_count Number of export- prefixed snapshots',
        '# TYPE rbd_volume_snap_export_count gauge',
        '# HELP rbd_volume_snap_migrate_count Number of migrate- prefixed snapshots',
        '# TYPE rbd_volume_snap_migrate_count gauge',
        '# HELP rbd_volume_snap_other_count Number of snapshots not matching known patterns',
        '# TYPE rbd_volume_snap_other_count gauge',
        '# HELP rbd_volume_snap_latest_daily Latest daily snap date as YYYYMMDD integer',
        '# TYPE rbd_volume_snap_latest_daily gauge',
        '# HELP rbd_volume_snap_oldest_daily Oldest daily snap date as YYYYMMDD integer',
        '# TYPE rbd_volume_snap_oldest_daily gauge',
        '# HELP rbd_volume_has_today_snap Whether volume has a daily snap created today (1=yes)',
        '# TYPE rbd_volume_has_today_snap gauge',
        '# HELP rbd_snap_info Snapshot existence marker, always 1 if snap exists',
        '# TYPE rbd_snap_info gauge',
        '# HELP rbd_snap_size_bytes Provisioned size of volume at time of snapshot',
        '# TYPE rbd_snap_size_bytes gauge',
        '# HELP rbd_snap_protected Whether snapshot is protected from deletion (1=yes)',
        '# TYPE rbd_snap_protected gauge',
        '# HELP rbd_pool_broken_count Number of broken images in pool (header missing/corrupt, errno 2)',
        '# TYPE rbd_pool_broken_count gauge',
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sensor_name = "ceph_rbd"

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        pools     = sensor_cfg.get("pools", [])
        ceph_conf = sensor_cfg.get("ceph_conf", "/etc/ceph/ceph.conf")
        keyring   = sensor_cfg.get("keyring", "/etc/ceph/ceph.client.admin.keyring")
        rbd_user  = sensor_cfg.get("rbd_user", "admin")

        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

        today = date.today().strftime('%Y%m%d')
        all_metrics = build_help_headers()

        for pool in pools:
            print(f"INFO: collecting metrics for pool {pool}...", file=sys.stderr)
            raw_data, broken_count = run_rbd_command(pool, ceph_conf, keyring, rbd_user)

            if not raw_data and broken_count == 0:
                print(f"WARN: no data for pool {pool}, skipping", file=sys.stderr)
                continue

            parsed = parse_rbd_data(pool, raw_data)
            metrics = generate_metrics(pool, parsed, broken_count, today)
            all_metrics.extend(metrics)

            vol_count  = len(parsed["volumes"])
            snap_count = sum(len(v) for v in parsed["snaps"].values())
            print(
                f"INFO: pool {pool} -> {vol_count} volumes, {snap_count} snapshots, "
                f"{broken_count} broken",
                file=sys.stderr
            )

        write_prometheus_metrics(prom_dirs, all_metrics, sensor_name)

    except Exception as e:
        traceback.print_exc()
        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
        write_prometheus_metrics(prom_dirs, [
            {"name": "ceph_rbd_error", "message": str(e).replace('"', "'"), "value": 1}
        ], sensor_name)
        sys.exit(1)
