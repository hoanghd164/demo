#!/usr/bin/env python3
import os
import sys
import json
import socket
import subprocess
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    get_str,
    get_sensor_config,
    write_prometheus_metrics
)

def run_ceph_command(args):
    try:
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        stdout = result.stdout.strip()
        # 'ceph pg dump' prints "dumped all" to stdout before JSON in some versions
        if stdout and not stdout.startswith(("{", "[")):
            lines = stdout.splitlines()
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith(("{", "[")):
                    stdout = "\n".join(lines[i:])
                    break
        return json.loads(stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr.strip())
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON parse error: {e}")
    except Exception as e:
        raise RuntimeError(str(e))


def get_fsid(cluster_name=None, cache_path="/tmp/osd_host_map_cache.json"):
    try:
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                cached = json.load(f)
                if "fsid" in cached:
                    return cached["fsid"]
    except Exception:
        pass

    try:
        cmd = [f"{command}", "fsid"]
        if cluster_name:
            cmd = [f"{command}", "--cluster", cluster_name, "fsid"]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True
        )
        fsid = result.stdout.strip()

        cached = {}
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                try:
                    cached = json.load(f)
                except Exception:
                    cached = {}

        cached["fsid"] = fsid
        with open(cache_path, "w") as f:
            json.dump(cached, f)

        return fsid
    except Exception as e:
        print(f"[ERROR] Failed to get fsid: {e}")
        return None


def get_osd_host_map(fsid=None, cluster_name=None, refresh_hours=6, cache_path="/tmp/osd_host_map_cache.json"):
    now = datetime.utcnow()
    try:
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                cached = json.load(f)
                cached_time = datetime.strptime(cached.get("timestamp", ""), "%Y-%m-%dT%H:%M:%S")
                if (now - cached_time) < timedelta(hours=refresh_hours) and "osd_map" in cached:
                    return cached["osd_map"]
    except Exception as e:
        print(f"[WARNING] Failed to read OSD host cache: {e}")

    try:
        if cluster_name:
            cmd = [f"{command}", "--cluster", cluster_name, "osd", "metadata", "-f", "json"]
        elif fsid:
            cmd = [f"{command}", "--fsid", fsid, "osd", "metadata", "-f", "json"]
        else:
            cmd = [f"{command}", "osd", "metadata", "-f", "json"]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True
        )
        osd_metadata = json.loads(result.stdout)
        osd_map = {
            str(entry["id"]): entry["hostname"]
            for entry in osd_metadata if "id" in entry and "hostname" in entry
        }

        cached = {"timestamp": now.strftime("%Y-%m-%dT%H:%M:%S"), "osd_map": osd_map}
        if fsid:
            cached["fsid"] = fsid

        with open(cache_path, "w") as f:
            json.dump(cached, f)

        return osd_map
    except Exception as e:
        print(f"[ERROR] Failed to get OSD host map: {e}")
        return {}


# ---------------------------------------------------------------------------
# Metric parsers (pure functions, no subprocess calls)
# ---------------------------------------------------------------------------

def collect_recovery_metrics(pgmap):
    summary = pgmap.get("recovery_summary", {})
    return {
        "ceph_custom_recovery_bytes_per_sec": pgmap.get("recovering_bytes_per_sec") or summary.get("bytes_per_sec", 0),
        "ceph_custom_recovery_objects_per_sec": pgmap.get("recovering_objects_per_sec") or summary.get("objects_per_sec", 0)
    }


def collect_client_io_metrics(pgmap):
    return {
        "ceph_client_read_bytes_per_sec": pgmap.get("read_bytes_sec", 0),
        "ceph_client_write_bytes_per_sec": pgmap.get("write_bytes_sec", 0),
        "ceph_client_read_ops_per_sec": pgmap.get("read_op_per_sec", 0),
        "ceph_client_write_ops_per_sec": pgmap.get("write_op_per_sec", 0)
    }


def collect_progress_metrics(events):
    progress, percent, days = 0.0, 0.0, 0
    for ev in events.values():
        if "Global Recovery Event" in ev.get("message", ""):
            progress = ev.get("progress", 0.0)
            percent = round(progress * 100, 2)
            match = re.search(r"\((\d+)d\)", ev["message"])
            days = int(match.group(1)) if match else 0
            break
    return {
        "ceph_global_recovery_progress": progress,
        "ceph_global_recovery_progress_percent": percent,
        "ceph_global_recovery_days": days
    }


def format_metrics_as_dicts(data_dict, extra_labels=None):
    metrics = []
    for name, value in data_dict.items():
        entry = {"name": name, "value": value}
        if extra_labels:
            entry.update(extra_labels)
        metrics.append(entry)
    return metrics


def parse_df_metrics(df_data):
    results = []
    for pool in df_data.get("pools", []):
        labels = {
            "pool_id": str(pool.get("id")),
            "pool_name": pool.get("name"),
            "source_cmd": "ceph df detail"
        }
        for k, v in pool.get("stats", {}).items():
            results.append({"name": f"ceph_df_pool_{k}", "value": v, **labels})
    return results


def parse_pool_ls_metrics(pool_ls):
    mode_map = {"off": 0, "on": 1, "warn": 2}
    results = []
    for pool in pool_ls:
        labels = {
            "pool_id": str(pool.get("pool_id")),
            "pool_name": pool.get("pool_name"),
            "source_cmd": "ceph osd pool ls detail"
        }
        results.append({
            "name": "ceph_pool_pg_autoscale_mode",
            "value": mode_map.get(pool.get("pg_autoscale_mode", ""), -1),
            **labels
        })
        keys = [
            "pg_num", "pg_placement_num", "pg_placement_num_target", "pg_num_target",
            "pg_num_pending", "type", "size", "min_size", "crush_rule"
        ]
        for k in keys:
            if k in pool:
                results.append({
                    "name": f"ceph_pool_{k}",
                    "value": pool[k],
                    **labels
                })
    return results


def parse_autoscale_metrics(pools):
    mode_map = {"off": 0, "on": 1, "warn": 2}
    results = []
    for p in pools:
        labels = {
            "pool_id": str(p.get("pool_id")),
            "pool_name": p.get("pool_name"),
            "source_cmd": "ceph osd pool autoscale-status"
        }
        results.extend([
            {"name": "ceph_pool_bulk_flag", "value": 1 if p.get("bulk") else 0, **labels},
            {"name": "ceph_pool_pg_autoscale_mode", "value": mode_map.get(p.get("pg_autoscale_mode", ""), -1), **labels},
            {"name": "ceph_pool_target_ratio_percent", "value": p.get("target_ratio", 0) * 100, **labels}
        ])
        skip_keys = {"pool_id", "pool_name", "bulk", "pg_autoscale_mode", "target_ratio"}
        for k, v in p.items():
            if k not in skip_keys:
                numeric_value = 1 if v is True else 0 if v is False else v
                results.append({"name": f"ceph_pool_{k}", "value": numeric_value, **labels})
    return results


def parse_osd_perf_metrics(osd_perf_json):
    results = []
    for osd in osd_perf_json.get("osdstats", {}).get("osd_perf_infos", []):
        osd_id = osd.get("id")
        stats = osd.get("perf_stats", {})
        labels = {"osd_id": str(osd_id), "source_cmd": "ceph osd perf"}
        results.extend([
            {"name": "ceph_osd_commit_latency_ms", "value": stats.get("commit_latency_ms", 0), **labels},
            {"name": "ceph_osd_apply_latency_ms", "value": stats.get("apply_latency_ms", 0), **labels}
        ])
    return results


def parse_slow_requests_from_health(health_detail_json):
    """
    Parse 'ceph health detail -f json' to extract per-OSD slow request counts.

    Format A — per-OSD counts in detail[] (REQUEST_SLOW):
      "detail": [{"message": "28 ops are blocked > 32 sec on osd.39"}, ...]

    Format B — OSD list in summary only, detail[] empty (SLOW_OPS):
      "summary": {"message": "49 slow ops, ... daemons [osd.0,osd.1] have slow ops."}

    Returns: dict {osd_id_str: count}
    """
    osd_counts = {}
    checks = health_detail_json.get("checks", {})
    slow_check = checks.get("REQUEST_SLOW") or checks.get("SLOW_OPS") or {}
    details = slow_check.get("detail", [])

    # Format A: per-OSD info in detail[]
    osd_detail_pattern = re.compile(r"(\d+)\s+(?:ops|slow requests)\s+.*?osd\.(\d+)")
    for entry in details:
        msg = entry.get("message", "")
        match = osd_detail_pattern.search(msg)
        if match:
            count = int(match.group(1))
            osd_id = match.group(2)
            osd_counts[str(osd_id)] = max(osd_counts.get(str(osd_id), 0), count)

    # Format B: detail[] empty, parse summary.message
    if not osd_counts:
        summary_msg = slow_check.get("summary", {}).get("message", "")
        if summary_msg:
            total_match = re.search(r"(\d+)\s+slow ops", summary_msg)
            total_slow = int(total_match.group(1)) if total_match else 0

            daemons_match = re.search(r"daemons\s+\[([^\]]+)\]", summary_msg)
            if daemons_match and total_slow > 0:
                daemon_list = daemons_match.group(1)
                osd_ids = re.findall(r"osd\.(\d+)", daemon_list)
                if osd_ids:
                    per_osd = max(1, total_slow // len(osd_ids))
                    remainder = total_slow % len(osd_ids)
                    for i, osd_id in enumerate(osd_ids):
                        count = per_osd + (1 if i < remainder else 0)
                        osd_counts[str(osd_id)] = count

    return osd_counts


# ---------------------------------------------------------------------------
# Parallel fetching helper
# ---------------------------------------------------------------------------

def _fetch_labeled(label, func, *args, **kwargs):
    """Run func(*args, **kwargs) and return (label, result). For thread pool."""
    return label, func(*args, **kwargs)

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
    sensor_name = "ceph"
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    
    try:
        sensor_cfg = get_sensor_config().get(sensor_name, {})
        cluster = sensor_cfg.get("cluster", "ceph")
        command = sensor_cfg.get("command", "ceph")

        # ==================================================================
        # PHASE 1: Fire off all independent ceph commands in parallel
        # ==================================================================

        active_mgr = is_active_mgr(command, cluster)
        
        if active_mgr:
            ceph_cmds = {
                "status":        [f"{command}", "-s", "--cluster", cluster, "-f", "json"],
                "df":            [f"{command}", "df", "detail", "--cluster", cluster, "-f", "json"],
                "pool_ls":       [f"{command}", "osd", "pool", "ls", "detail", "--cluster", cluster, "-f", "json"],
                "osd_perf":      [f"{command}", "osd", "perf", "--cluster", cluster, "-f", "json"],
                "autoscale":     [f"{command}", "osd", "pool", "autoscale-status", "--cluster", cluster, "-f", "json"],
                "pg_dump":       [f"{command}", "pg", "dump", "--cluster", cluster, "--format", "json"],
                "osd_dump":      [f"{command}", "osd", "dump", "--cluster", cluster, "-f", "json"],
                "health_detail": [f"{command}", "health", "detail", "--cluster", cluster, "-f", "json"],
            }

            fetched = {}
            with ThreadPoolExecutor(max_workers=len(ceph_cmds)) as executor:
                futures = {
                    executor.submit(_fetch_labeled, label, run_ceph_command, cmd): label
                    for label, cmd in ceph_cmds.items()
                }
                for future in as_completed(futures):
                    label, result = future.result()
                    fetched[label] = result

            # ==================================================================
            # PHASE 2: Parse results (pure CPU, very fast)
            # ==================================================================

            ceph_status = fetched["status"]
            pgmap = ceph_status.get("pgmap", {})
            progress_events = ceph_status.get("progress_events", {})

            final_results += format_metrics_as_dicts(collect_recovery_metrics(pgmap), {"source_cmd": f"{command} -s"})
            final_results += format_metrics_as_dicts(collect_client_io_metrics(pgmap), {"source_cmd": f"{command} -s"})
            final_results += format_metrics_as_dicts(collect_progress_metrics(progress_events), {"source_cmd": f"{command} -s"})
            final_results.append({"name": "ceph_cluster_total_pgs", "value": pgmap.get("num_pgs", 0), "source_cmd": f"{command} -s"})

            final_results += parse_df_metrics(fetched["df"])
            final_results += parse_pool_ls_metrics(fetched["pool_ls"])
            final_results += parse_osd_perf_metrics(fetched["osd_perf"])
            final_results += parse_autoscale_metrics(fetched["autoscale"])

            # --- Build OSD host map (uses cache, very fast on cache hit) ---
            fsid = get_fsid(cluster_name=cluster)
            osd_host_map = get_osd_host_map(fsid=fsid, cluster_name=cluster)

            # --- OSD status map from already-fetched osd_dump ---
            osd_status_map = {}
            for osd in fetched["osd_dump"].get("osds", []):
                osd_status_map[str(osd["osd"])] = osd.get("up", 0)

            # ==================================================================
            # Pool OSD mapping from pg_dump + osd_dump
            # ==================================================================
            try:
                pool_id_to_name = {str(p.get("pool_id")): p.get("pool_name") for p in fetched["pool_ls"]}

                pool_crush_rule = {}
                for p in fetched["pool_ls"]:
                    pname = p.get("pool_name")
                    if pname and pname != ".mgr":
                        pool_crush_rule[pname] = p.get("crush_rule", -1)

                all_osd_ids = set()
                for osd in fetched["osd_dump"].get("osds", []):
                    all_osd_ids.add(str(osd["osd"]))

                pg_dump_data = fetched["pg_dump"]
                pg_stats_list = (
                    pg_dump_data.get("pg_stats")
                    or pg_dump_data.get("pg_map", {}).get("pg_stats")
                    or []
                )

                pool_osd_map = {}
                for pg in pg_stats_list:
                    pgid = pg.get("pgid", "")
                    pool_id_str = pgid.split(".")[0] if "." in pgid else ""
                    pool_name = pool_id_to_name.get(pool_id_str)
                    if not pool_name or pool_name == ".mgr":
                        continue
                    if pool_name not in pool_osd_map:
                        pool_osd_map[pool_name] = set()
                    for field in ("acting", "up"):
                        for osd_id in pg.get(field, []):
                            if osd_id >= 0 and osd_id != 2147483647:
                                pool_osd_map[pool_name].add(str(osd_id))

                for pool_name in pool_crush_rule:
                    if pool_name not in pool_osd_map:
                        pool_osd_map[pool_name] = set()
                    pool_osd_map[pool_name].update(all_osd_ids)

                for pool_name, osd_ids in pool_osd_map.items():
                    if pool_name == ".mgr":
                        continue
                    for osd_id in sorted(osd_ids, key=lambda x: int(x)):
                        host = osd_host_map.get(osd_id, "unknown")
                        status = osd_status_map.get(osd_id, 0)
                        final_results.append({
                            "name": "ceph_pool_osd_mapping",
                            "value": status,
                            "pool_name": pool_name,
                            "osd_id": osd_id,
                            "on_host": host,
                            "source_cmd": "ceph pg dump"
                        })

                if not pool_osd_map:
                    final_results.append({
                        "name": "ceph_pool_osd_mapping_failed",
                        "value": 0,
                        "reason": f"pg_stats empty (keys: {list(pg_dump_data.keys())[:5]})"
                    })

            except Exception as e:
                final_results.append({
                    "name": "ceph_pool_osd_mapping_failed",
                    "value": 0,
                    "reason": str(e).splitlines()[0][:100]
                })

            # ==================================================================
            # Slow request metrics from 'ceph health detail'
            # ==================================================================
            try:
                slow_osd_counts = parse_slow_requests_from_health(fetched["health_detail"])
                for osd_id, hostname in osd_host_map.items():
                    value = slow_osd_counts.get(osd_id, 0)
                    final_results.append({
                        "name": "ceph_osd_slow_requests_total",
                        "value": value,
                        "osd_id": osd_id,
                        "host": hostname,
                        "source_cmd": "ceph health detail"
                    })
            except Exception as e:
                final_results.append({
                    "name": "ceph_slow_request_parse_failed",
                    "value": 1,
                    "reason": str(e).splitlines()[0][:100]
                })

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
        cluster = sensor_cfg.get("cluster", "unknown")
        final_results = [{
            "name": "ceph_error",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]

    write_prometheus_metrics(prom_dirs, final_results, sensor_name)