#!/usr/bin/env python3
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    get_sensor_config,
    write_prometheus_metrics
)


def run_aws(cmd, timeout=30):
    """Run an aws CLI command; return True if exit code == 0."""
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        return result.returncode == 0
    except Exception:
        return False


def check_s3_endpoint(provider, access_type, profile, endpoint,
                      bucket, local_src, local_dst, encryption,
                      request_count, methods):
    """
    Run checks based on configured methods against one endpoint+bucket pair.

    When encryption is configured (e.g. AES256):
      - Run checks WITH --sse AES256
      - Also run checks WITHOUT --sse (non-encrypted)
    When encryption is false/not set:
      - Run checks WITHOUT --sse only
    """
    key = os.path.basename(local_src)
    use_sse = bool(encryption) and str(encryption).upper() not in ("FALSE", "NONE", "")

    # Always check non-encrypted
    modes = [{"use_sse": False, "encryption_label": "false"}]
    if use_sse:
        modes.append({"use_sse": True, "encryption_label": str(encryption)})

    results = []
    for mode in modes:
        results.extend(_check_s3_single_mode(
            provider, access_type, profile, endpoint,
            bucket, key, local_src, local_dst,
            mode["use_sse"], mode["encryption_label"],
            request_count, methods
        ))

    return results


def _check_s3_single_mode(provider, access_type, profile, endpoint,
                           bucket, key, local_src, local_dst,
                           use_sse, encryption_label, request_count, methods):
    """Run configured methods for a single encryption mode."""
    results = []

    base_labels = {
        "provider":    provider,
        "access_type": access_type,
        "bucket":      bucket,
        "endpoint":    endpoint,
        "local_src":   local_src,
        "local_dst":   local_dst,
        "encryption":  encryption_label,
    }

    do_upload   = "upload" in methods
    do_download = "download" in methods
    do_head     = "head" in methods
    do_delete   = "delete" in methods

    # --- Build commands ---
    upload_cmd = [
        "aws", "--profile", profile,
        "--endpoint-url", endpoint,
        "s3", "cp", local_src, f"s3://{bucket}/{key}",
        "--only-show-errors"
    ]
    if use_sse:
        upload_cmd += ["--sse", "AES256"]

    download_cmd = [
        "aws", "--profile", profile,
        "--endpoint-url", endpoint,
        "s3", "cp", f"s3://{bucket}/{key}", local_dst,
        "--only-show-errors"
    ]

    head_cmd = [
        "aws", "--profile", profile,
        "--endpoint-url", endpoint,
        "s3api", "head-object",
        "--bucket", bucket,
        "--key", key
    ]

    delete_cmd = [
        "aws", "--profile", profile,
        "--endpoint-url", endpoint,
        "s3", "rm", f"s3://{bucket}/{key}",
        "--only-show-errors"
    ]

    # --- Phase 1: UPLOAD (needed before download/head/delete can work) ---
    upload_ok = False
    if do_upload or do_download or do_delete:
        upload_ok = run_aws(upload_cmd)
        if do_upload:
            results.append({"name": "s3_upload_status", "value": 1 if upload_ok else 0, **base_labels})

    # --- Phase 2: DOWNLOAD + HEAD in parallel ---
    phase2_tasks = {}
    if do_download:
        phase2_tasks["download"] = download_cmd
    if do_head:
        phase2_tasks["head"] = head_cmd

    if phase2_tasks:
        with ThreadPoolExecutor(max_workers=len(phase2_tasks)) as pool:
            futures = {}

            if "download" in phase2_tasks:
                if upload_ok:
                    futures["download"] = pool.submit(run_aws, download_cmd)
                else:
                    futures["download"] = None

            if "head" in phase2_tasks:
                def do_head_check():
                    for _ in range(max(1, int(request_count))):
                        if run_aws(head_cmd):
                            return True
                    return False
                futures["head"] = pool.submit(do_head_check)

            if "download" in futures:
                download_ok = futures["download"].result() if futures["download"] else False
                results.append({"name": "s3_download_status", "value": 1 if download_ok else 0, **base_labels})

            if "head" in futures:
                head_ok = futures["head"].result() if futures["head"] else False
                results.append({"name": "s3_head_status", "value": 1 if head_ok else 0, **base_labels})

    # --- Phase 3: DELETE (after download/head, needs object to exist) ---
    if do_delete:
        if upload_ok:
            delete_ok = run_aws(delete_cmd)
        else:
            delete_ok = False
        results.append({"name": "s3_delete_status", "value": 1 if delete_ok else 0, **base_labels})

    return results


def collect_all_tasks(sensor_cfg):
    """Build list of all (args) tuples from config."""
    tasks = []
    providers = sensor_cfg.get("provider", {})

    for provider_name, access_configs in providers.items():
        if not isinstance(access_configs, list):
            continue

        for cfg in access_configs:
            if not isinstance(cfg, dict):
                continue

            access_type = "unknown"
            for k in cfg:
                if k in ("public", "private"):
                    access_type = k
                    break

            if not cfg.get("enable", True):
                continue

            profile       = str(cfg.get("profile", "default"))
            encryption    = cfg.get("encryption", None)
            request_count = max(1, int(cfg.get("request", 1)))
            methods       = [m.lower().strip() for m in cfg.get("method", ["upload", "download", "head", "delete"])]
            buckets       = cfg.get("bucket", [])
            local_srcs    = cfg.get("local_src", [])
            local_dsts    = cfg.get("local_dst", [])
            endpoints     = cfg.get("endpoints", [])

            for bucket in buckets:
                for endpoint in endpoints:
                    for local_src, local_dst in zip(local_srcs, local_dsts):
                        tasks.append((
                            provider_name, access_type, profile,
                            endpoint, bucket,
                            local_src, local_dst,
                            encryption, request_count, methods
                        ))
    return tasks


if __name__ == "__main__":
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    final_results = []
    sensor_name   = "s3"

    try:
        if not os.environ.get("FULL_PATH_CONFIG_FILENAME"):
            project = os.environ.get("PROJECT", "staging")
            load_config(project)

        sensor_cfg = get_sensor_config().get(sensor_name, {})

        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        tasks = collect_all_tasks(sensor_cfg)

        max_workers = min(16, len(tasks)) if tasks else 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(check_s3_endpoint, *args): args
                for args in tasks
            }
            for future in as_completed(futures):
                args = futures[future]
                try:
                    final_results.extend(future.result())
                except Exception as e:
                    error_msg = str(e).replace('"', "'")
                    final_results.append({
                        "name":        "s3_error",
                        "provider":    args[0],
                        "access_type": args[1],
                        "bucket":      args[4],
                        "endpoint":    args[3],
                        "message":     f"check_error: {error_msg}",
                        "value":       1
                    })

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        final_results = [{
            "name":    "s3_error",
            "message": f"config_or_runtime_error: {error_msg}",
            "value":   1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)