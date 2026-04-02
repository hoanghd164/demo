import os
import sys
import json
import socket
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def sanitize(value):
    return str(value).replace('"', '\"')

def process_bucket(bucket, cluster, reshard_threshold=200000):
    try:
        stats = json.loads(subprocess.check_output(
            ["radosgw-admin", "--cluster", cluster, "bucket", "stats", "--bucket", bucket],
            timeout=20
        ).decode())

        usage = stats.get("usage", {}).get("rgw.main", {})
        quota = stats.get("bucket_quota", {})
        owner = stats.get("owner", "unknown")
        creation_time_str = stats.get("creation_time", "")
        zonegroup = stats.get("zonegroup", "")
        placement_rule = stats.get("placement_rule", "")
        bucket_id = stats.get("id", "")
        marker = stats.get("marker", "")
        num_shards = stats.get("num_shards", 0)
        index_generation = stats.get("index_generation", 0)
        num_objects = usage.get("num_objects", 0)

        # Check encryption via metadata
        try:
            meta = json.loads(subprocess.check_output(
                ["radosgw-admin", "--cluster", cluster, "metadata", "get",
                 f"bucket.instance:{bucket}:{bucket_id}"],
                timeout=20
            ).decode())
            attrs = meta.get("data", {}).get("attrs", [])
            encrypted = 1 if any("sse" in attr.get("key", "") for attr in attrs) else 0
        except Exception:
            encrypted = -1  # unknown

        if num_shards and num_shards > 0:
            avg_objects_per_shard = num_objects / num_shards
        else:
            avg_objects_per_shard = 0

        if num_shards and num_shards > 0 and reshard_threshold > 0:
            ratio_to_threshold = (num_objects / num_shards) / reshard_threshold
        else:
            ratio_to_threshold = 0

        try:
            creation_ts = int(datetime.strptime(creation_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
        except Exception:
            creation_ts = 0

        labels = {
            "bucket": bucket,
            "owner": owner,
            "zonegroup": zonegroup,
            "placement_rule": placement_rule,
            "id": bucket_id,
            "marker": marker
        }

        return [
            {"name": "ceph_rgw_bucket_num_objects", "value": num_objects, **labels},
            {"name": "ceph_rgw_bucket_size_bytes", "value": usage.get("size", 0), **labels},
            {"name": "ceph_rgw_bucket_size_actual_bytes", "value": usage.get("size_actual", 0), **labels},
            {"name": "ceph_rgw_bucket_size_utilized_bytes", "value": usage.get("size_utilized", 0), **labels},
            {"name": "ceph_rgw_bucket_size_kb", "value": usage.get("size_kb", 0), **labels},
            {"name": "ceph_rgw_bucket_size_kb_actual", "value": usage.get("size_kb_actual", 0), **labels},
            {"name": "ceph_rgw_bucket_size_kb_utilized", "value": usage.get("size_kb_utilized", 0), **labels},
            {"name": "ceph_rgw_bucket_creation_timestamp", "value": creation_ts, **labels},
            {"name": "ceph_rgw_bucket_quota_enabled", "value": int(quota.get("enabled", False)), **labels},
            {"name": "ceph_rgw_bucket_quota_check_on_raw", "value": int(quota.get("check_on_raw", False)), **labels},
            {"name": "ceph_rgw_bucket_quota_max_size_bytes", "value": quota.get("max_size", -1), **labels},
            {"name": "ceph_rgw_bucket_quota_max_size_kb", "value": quota.get("max_size_kb", 0), **labels},
            {"name": "ceph_rgw_bucket_quota_max_objects", "value": quota.get("max_objects", -1), **labels},
            {"name": "ceph_rgw_bucket_num_shards", "value": num_shards, **labels},
            {"name": "ceph_rgw_bucket_index_generation", "value": index_generation, **labels},
            {"name": "ceph_rgw_bucket_avg_objects_per_shard", "value": avg_objects_per_shard, **labels},
            {"name": "ceph_rgw_bucket_objects_per_shard_ratio", "value": ratio_to_threshold, **labels},
            {"name": "ceph_rgw_bucket_encrypted", "value": encrypted, **labels},
        ]
    except Exception as e:
        reason = str(e).splitlines()[0]
        reason_code = "not_found" if "ret=-2" in reason else "unknown"
        return [{
            "name": "ceph_rgw_bucket_stats_failed",
            "value": 1,
            "bucket": bucket,
            "reason": reason_code
        }]

def collect_rgw_bucket_metrics(cluster, limit_buckets=10000, max_workers=8, reshard_threshold=200000):
    try:
        buckets = json.loads(subprocess.check_output(["radosgw-admin", "--cluster", cluster, "bucket", "list"]).decode())
        if limit_buckets > 0:
            buckets = buckets[:limit_buckets]
    except Exception as e:
        error_msg = str(e).replace('"', "'")
        return [{"name": "ceph_rgw_error", "message": f"bucket_list_fail: {error_msg}", "value": 1}]

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_bucket, b, cluster, reshard_threshold) for b in buckets]
        for future in as_completed(futures):
            results.extend(future.result())

    return results

def collect_user_metrics(user_id, cluster):
    try:
        user_data = json.loads(subprocess.check_output([
            "radosgw-admin", "--cluster", cluster, "metadata", "get", f"user:{user_id}"
        ]).decode())

        data = user_data.get("data", {})
        quota = data.get("user_quota", {})
        create_time = data.get("create_date", "")
        display_name = data.get("display_name", "unknown")
        email = data.get("email", "unknown")
        suspended = int(data.get("suspended", 0))
        max_buckets = data.get("max_buckets", -1)

        try:
            creation_ts = int(datetime.strptime(create_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
        except Exception:
            creation_ts = 0

        labels = {
            "user_id": user_id,
            "display_name": display_name,
            "email": email
        }

        return [
            {"name": "ceph_rgw_user_quota_enabled", "value": int(quota.get("enabled", False)), **labels},
            {"name": "ceph_rgw_user_quota_check_on_raw", "value": int(quota.get("check_on_raw", False)), **labels},
            {"name": "ceph_rgw_user_quota_max_size_bytes", "value": quota.get("max_size", -1), **labels},
            {"name": "ceph_rgw_user_quota_max_size_kb", "value": quota.get("max_size_kb", 0), **labels},
            {"name": "ceph_rgw_user_quota_max_objects", "value": quota.get("max_objects", -1), **labels},
            {"name": "ceph_rgw_user_creation_timestamp", "value": creation_ts, **labels},
            {"name": "ceph_rgw_user_quota_max_buckets", "value": max_buckets, **labels},
            {"name": "ceph_rgw_user_suspended", "value": suspended, **labels}
        ]
    except Exception as e:
        reason = str(e).splitlines()[0]
        return [{
            "name": "ceph_rgw_user_stats_failed",
            "value": 1,
            "user_id": user_id,
            "reason": reason[:100]
        }]

def collect_placement_pool_metrics(cluster):
    try:
        output = subprocess.check_output(
            ["radosgw-admin", "--cluster", cluster, "zone", "get"],
            timeout=10
        ).decode()
        zone_info = json.loads(output)

        placement_pools = zone_info.get("placement_pools", [])
        metrics = []

        for placement in placement_pools:
            key = placement.get("key", "")
            val = placement.get("val", {})
            index_pool = val.get("index_pool", "unknown")
            data_extra_pool = val.get("data_extra_pool", "unknown")

            for storage_class, class_info in val.get("storage_classes", {}).items():
                data_pool = class_info.get("data_pool", "unknown")
                compression = class_info.get("compression_type", "none")

                metric = {
                    "name": "ceph_rgw_zone_placement_pool_info",
                    "value": 1,
                    "placement": key,
                    "storage_class": storage_class,
                    "data_pool": data_pool,
                    "index_pool": index_pool,
                    "data_extra_pool": data_extra_pool,
                    "compression": compression
                }
                metrics.append(metric)

        return metrics
    except Exception as e:
        return [{
            "name": "ceph_rgw_zone_placement_pool_info_failed",
            "value": 1,
            "reason": str(e).replace('"', "'")[:100]
        }]

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

    sensor_name = "ceph_rgw"
    final_results = []
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)
        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})

        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        cluster           = sensor_cfg.get("cluster", "ceph")
        command           = sensor_cfg.get("command", "ceph")
        max_workers       = int(sensor_cfg.get("max_workers", 8))
        limit_buckets     = int(sensor_cfg.get("limit_bucket", 10000))
        reshard_threshold = int(sensor_cfg.get("reshard_threshold", 200000))

        active_mgr = is_active_mgr(command, cluster)

        if active_mgr:
            bucket_metrics = collect_rgw_bucket_metrics(
                cluster, limit_buckets, max_workers, reshard_threshold
            )
            final_results.extend(bucket_metrics)

            user_ids = {
                m["owner"] for m in bucket_metrics
                if isinstance(m, dict) and m.get("name", "").startswith("ceph_rgw_bucket_") and "owner" in m
            }

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(collect_user_metrics, uid, cluster) for uid in sorted(user_ids)]
                for future in as_completed(futures):
                    final_results.extend(future.result())

            placement_metrics = collect_placement_pool_metrics(cluster)
            final_results.extend(placement_metrics)

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
        final_results = [{"name": "ceph_rgw_error", "message": f"config_or_main_fail: {error_msg}", "value": 1}]

    write_prometheus_metrics(prom_dirs, final_results, sensor_name)