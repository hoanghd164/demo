#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
import threading
import datetime
import shutil
import re

managed_processes = []
process_lock = threading.Lock()

from config_loader import (
    load_config, get_str, get_int
)

def run_script(script_path: str, output_file: str, sensor: str):
    ext = os.path.splitext(script_path)[1].lower()

    if ext == ".sh":
        process = subprocess.Popen(["/bin/bash", script_path, output_file])
    elif ext == ".py":
        print(f"🚀 Running ({sensor}): {os.path.basename(script_path)}")
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{WORKING_DIR}:{env.get('PYTHONPATH', '')}"
        process = subprocess.Popen(
            ["/etc/admin.collector/venv/bin/python", script_path, output_file],
            env=env
        )
    else:
        print(f"⚠️ Unsupported script type: {script_path}")
        return

    with process_lock:
        managed_processes.append((process, script_path))
    print(f"🧵 PID {process.pid} running {script_path}")
    process.wait()
    result = process

    status = "✅ Done" if result.returncode == 0 else "❌ Failed"
    print(f"{status}: {os.path.basename(script_path)}")

def run_script_loop(script_path: str, sensor: str, interval: int):
    def loop():
        output_file = os.path.join(PROM_DIR, f"{os.path.splitext(os.path.basename(script_path))[0]}.prom")
        while True:
            try:
                run_script(script_path, output_file, sensor)
            except Exception as e:
                print(f"[Loop] Error running {script_path}: {e}")
            time.sleep(interval)
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

def collect_enabled_scripts(scripts, config):
    enabled = []
    sensor_config = config.get("sensor", {})
    for sensor_name, sensor_data in sensor_config.items():
        if not sensor_data.get("enable"):
            continue
        for script in sensor_data.get("scripts", []):
            if script in scripts:
                enabled.append((script, sensor_name))
    return enabled


SAFE_PROM_PREFIXES = (
    "/var/lib/node_exporter",
    "/textfile_collector",
    "/tmp",
    "/run/node_exporter",
)

def ensure_empty_dir(path: str):
    if not path or not path.strip():
        return
    path = os.path.abspath(path.strip())

    if path == "/" or path.count(os.sep) < 3:
        raise ValueError(f"Từ chối dọn dẹp path quá ngắn hoặc root: {path}")

    if not any(path.startswith(p) for p in SAFE_PROM_PREFIXES):
        raise ValueError(
            f"Từ chối dọn dẹp path ngoài vùng an toàn: {path}\n"
            f"Cho phép: {SAFE_PROM_PREFIXES}"
        )

    if os.path.exists(path):
        if os.path.isdir(path):
            for name in os.listdir(path):
                full = os.path.join(path, name)
                try:
                    if os.path.isfile(full) or os.path.islink(full):
                        os.remove(full)
                    elif os.path.isdir(full):
                        shutil.rmtree(full)
                except Exception as e:
                    print(f"Lỗi khi xóa {full}: {e}")
        else:
            os.remove(path)
            os.makedirs(path, exist_ok=True)
    else:
        os.makedirs(path, exist_ok=True)

def get_keep_scripts_from_config(config: dict) -> set[str]:
    keep = set()
    sensor_cfg = config.get("sensor", {}) or {}
    for sensor_name, sensor_data in sensor_cfg.items():
        scripts = (sensor_data or {}).get("scripts", []) or []
        for s in scripts:
            keep.add(os.path.basename(str(s).strip()))
    return keep

def safe_cleanup_scripts_dir(scripts_dir: str, keep: set[str], exts=(".py", ".sh"), dry_run=True):
    scripts_dir = os.path.abspath(scripts_dir)

    if scripts_dir in ("/", "/etc", "/var", "/usr", "/bin", "/sbin", "/opt"):
        raise ValueError(f"Từ chối cleanup thư mục nguy hiểm: {scripts_dir}")

    removed = []
    kept = []

    for fname in os.listdir(scripts_dir):
        full = os.path.join(scripts_dir, fname)
        if not os.path.isfile(full):
            continue

        if exts and not fname.endswith(exts):
            kept.append(fname)
            continue

        if fname in keep:
            kept.append(fname)
            continue

        removed.append(fname)
        if not dry_run:
            os.remove(full)

    return kept, removed

def cleanup_config_keep_systemd_project(
    service_path="/etc/systemd/system/admin_collector.service",
    config_dir="/etc/admin.collector/source/config"):
    
    with open(service_path, "r", encoding="utf-8", errors="ignore") as f:
        m = re.search(r"^ExecStart=.*\s--project\s+([A-Za-z0-9._-]+)\s*$", f.read(), re.M)
    if not m:
        raise RuntimeError("Không tìm thấy '--project <name>' trong dòng ExecStart của service")
    project = m.group(1)

    subprocess.run(
        ["bash", "-lc",
         f'project="{project}"; find "{config_dir}" -maxdepth 1 -type f ! -name "${{project}}.yml" -exec rm -f -- {{}} +'
        ],
        check=True
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run monitoring scripts with per-sensor intervals.")
    parser.add_argument("--project", required=True, help="Project name (e.g., staging)")
    args = parser.parse_args()

    try:
        config = load_config(args.project)
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        sys.exit(1)

    WORKING_DIR = get_str("WORKING_DIR", "").strip() or "/etc/admin.collector/source"
    SCRIPTS_DIR = os.path.join(WORKING_DIR, "scripts")
    if not os.path.isdir(SCRIPTS_DIR):
        print(f"❌ SCRIPTS_DIR không tồn tại: {SCRIPTS_DIR}")
        sys.exit(1)
    PROM_DIR = get_str("NODE_EXPORTER_PROM_DIR", "/tmp")

    PROM_DIRS = [p.strip() for p in get_str("NODE_EXPORTER_PROM_DIR", "/tmp").split(":") if p.strip()]

    for p in PROM_DIRS:
        ensure_empty_dir(p)

    INTERVAL = get_int("INTERVAL", 30)

    KEEP_SCRIPTS = get_keep_scripts_from_config(config)
    # KEEP_SCRIPTS.update({"__init__.py"})

    if not KEEP_SCRIPTS:
        print("⚠️ KEEP_SCRIPTS rỗng (không có sensor nào có 'scripts:') — bỏ qua cleanup scripts để tránh xóa toàn bộ.")
        kept, removed = [], []
    else:
        kept, removed = safe_cleanup_scripts_dir(
            SCRIPTS_DIR,
            KEEP_SCRIPTS,
            exts=(".py", ".sh"),
            dry_run=False,
        )

    cleanup_config_keep_systemd_project()
    all_scripts = [f for f in os.listdir(SCRIPTS_DIR) if f.endswith(('.py', '.sh'))]
    enabled_scripts = collect_enabled_scripts(all_scripts, config)

    from config_loader import write_prometheus_metrics

    if not enabled_scripts:
        print("⚠️ No enabled scripts found in config.")

        warning_metric = [{
            "name": "collector_warning",
            "message": "no_enabled_scripts",
            "value": 1
        }]
        write_prometheus_metrics(
            get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":"),
            warning_metric,
            "collector",
            {"collector_warning": ("Collector bootstrap warnings", "gauge")}
        )

        while True:
            time.sleep(300)

    print(f"🔍 Found {len(enabled_scripts)} script(s) to run...")

    # Initialize each script in a separate thread with its own interval.
    for script, sensor in enabled_scripts:
        script_path = os.path.join(SCRIPTS_DIR, script)
        sensor_interval = config.get("sensor", {}).get(sensor, {}).get("interval", INTERVAL)
        run_script_loop(script_path, sensor, sensor_interval)

    # Keep the main process alive and handle Ctrl+C
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🛑 [{now}] KeyboardInterrupt received. Cleaning up processes...")

        with process_lock:
            for proc, path in managed_processes:
                if proc.poll() is None:
                    print(f"🔪 Killing PID {proc.pid} ({os.path.basename(path)})")
                    try:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                            print(f"✅ Terminated PID {proc.pid} ({os.path.basename(path)})")
                        except subprocess.TimeoutExpired:
                            print(f"⏱ PID {proc.pid} did not terminate in time. Force killing...")
                            proc.kill()
                    except Exception as e:
                        print(f"⚠️ Failed to terminate {proc.pid}: {e}")
