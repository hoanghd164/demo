#!/usr/bin/env python3
import os
import sys
import re
import json
import time
import fnmatch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    get_sensor_config,
    write_prometheus_metrics
)

# ---------------------------------------------------------------------------
# file_stats
# ---------------------------------------------------------------------------

_FILE_STATE_FILE = "/tmp/audit_file_stats_state.json"
_INOTIFY_LOG     = "/var/log/inotify-audit-monitor.log"

def _load_state(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path, state):
    try:
        with open(path, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


def _get_inode(path):
    try:
        return os.stat(path).st_ino
    except Exception:
        return None


def file_mtime(path):
    try:
        return int(os.stat(path).st_mtime)
    except (FileNotFoundError, PermissionError, OSError):
        return -1


def _read_inotify_log_since(log_path, offset, inode_saved, watched_paths):
    per_path_counts  = {}
    per_path_last_ts = {}
    try:
        current_inode = _get_inode(log_path)
        if current_inode is None:
            return per_path_counts, per_path_last_ts, offset, inode_saved
        current_size = os.path.getsize(log_path)
        if inode_saved is not None and current_inode != inode_saved:
            offset = 0
        elif offset > current_size:
            offset = 0
        normalized = {os.path.normpath(p) for p in watched_paths}
        with open(log_path, "r", errors="replace") as f:
            f.seek(offset)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    continue
                ts_str, _event, fpath = parts
                try:
                    ts = int(ts_str)
                except ValueError:
                    continue
                fpath_norm = os.path.normpath(fpath)
                if fpath_norm in normalized:
                    per_path_counts[fpath_norm]  = per_path_counts.get(fpath_norm, 0) + 1
                    per_path_last_ts[fpath_norm] = max(per_path_last_ts.get(fpath_norm, 0), ts)
            new_offset = f.tell()
        return per_path_counts, per_path_last_ts, new_offset, current_inode
    except Exception:
        return per_path_counts, per_path_last_ts, offset, inode_saved


_INOTIFY_CONF = "/etc/inotify-audit-monitor.conf"


def _sync_inotify_conf(paths, conf_path=_INOTIFY_CONF):
    """
    Đảm bảo tất cả paths trong danh sách đều có trong inotify conf.
    - Nếu conf chưa tồn tại → tạo mới
    - Nếu có path mới chưa có trong conf → append vào
    - Reload daemon nếu có thay đổi
    Không xóa path cũ trong conf (có thể do admin thêm tay).
    """
    # Đọc các path đang có trong conf
    existing = set()
    try:
        with open(conf_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    existing.add(line)
    except FileNotFoundError:
        pass
    except Exception:
        return

    # Tìm paths cần thêm
    to_add = [p for p in paths if p not in existing]
    if not to_add:
        return  # không có gì thay đổi, không cần reload

    # Append paths mới vào conf
    try:
        with open(conf_path, "a") as f:
            f.write("\n# Auto-added by audit sensor\n")
            for p in to_add:
                f.write(p + "\n")
    except Exception:
        return

    # Reload daemon để pick up paths mới
    try:
        import subprocess
        subprocess.run(
            ["systemctl", "restart", "inotify-audit-monitor"],
            stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, timeout=5
        )
    except Exception:
        pass


def collect_file_metrics(files, folders, state_file=_FILE_STATE_FILE, inotify_log=_INOTIFY_LOG):
    """
    Metrics per path:
      file_mtime{path}         — mtime Unix ts; -1 nếu không tồn tại
      file_change_count{path}  — cumulative count mỗi lần thay đổi
      file_last_changed{path}  — Unix ts lần cuối thay đổi (chỉ khi inotify available)
    """
    results = []
    seen    = set()
    state   = _load_state(state_file)

    inotify_available = os.path.exists(inotify_log)
    inotify_st = state.setdefault("__inotify__", {"offset": 0, "inode": None})

    # Expand tất cả paths
    all_paths = list(files)
    for pattern in folders:
        base_dir     = os.path.dirname(pattern)
        file_pattern = os.path.basename(pattern)
        matched = []
        if os.path.isdir(base_dir):
            for root, _, filenames in os.walk(base_dir):
                for filename in sorted(filenames):
                    if fnmatch.fnmatch(filename, file_pattern):
                        matched.append(os.path.join(root, filename))
        all_paths.extend(matched if matched else [pattern])

    # Sync inotify conf — tu them path moi neu chua co, reload daemon
    _sync_inotify_conf(all_paths)


    # Đọc inotify log 1 lần cho tất cả paths
    inotify_counts  = {}
    inotify_last_ts = {}
    if inotify_available:
        inotify_counts, inotify_last_ts, new_offset, new_inode = \
            _read_inotify_log_since(inotify_log, inotify_st["offset"], inotify_st["inode"], all_paths)
        inotify_st["offset"] = new_offset
        inotify_st["inode"]  = new_inode

    def emit(path):
        if path in seen:
            return
        seen.add(path)
        norm_path = os.path.normpath(path)
        mtime     = file_mtime(path)
        results.append({"name": "file_mtime", "path": path, "value": mtime})
        path_st = state.setdefault(path, {"mtime": mtime, "change_count": 0})

        if inotify_available:
            new_events = inotify_counts.get(norm_path, 0)
            path_st["change_count"] = path_st.get("change_count", 0) + new_events
            path_st["mtime"]        = mtime
            results.append({"name": "file_change_count", "path": path, "value": path_st["change_count"]})
            results.append({"name": "file_last_changed",  "path": path,
                            "value": inotify_last_ts.get(norm_path, path_st.get("last_changed", 0))})
            if norm_path in inotify_last_ts:
                path_st["last_changed"] = inotify_last_ts[norm_path]
        else:
            # Fallback: mtime comparison
            prev_mtime = path_st.get("mtime")
            if prev_mtime is not None and mtime != -1 and mtime != prev_mtime:
                path_st["change_count"] = path_st.get("change_count", 0) + 1
            path_st["mtime"] = mtime
            results.append({"name": "file_change_count", "path": path, "value": path_st.get("change_count", 0)})

    for path in all_paths:
        emit(path)

    _save_state(state_file, state)
    return results


# ---------------------------------------------------------------------------
# command_detect
# ---------------------------------------------------------------------------

_CMD_STATE_FILE = "/tmp/audit_command_detect_state.json"


def _extract_command(line):
    """
    Parse command từ syslog bash audit line.
    rsyslog trên Ubuntu encode tab thành #011. Format thực tế:
      Mar  8 07:10:47 host bash[1234]: root::pts/0:#011 systemctl restart sshd
    """
    s = line.strip()
    s = s.replace("#011", "\t")  # decode rsyslog tab escape

    # Format Ubuntu rsyslog: bash[PID]: user::tty:\t command
    m = re.search(r"\sbash\[\d+\]:\s+\S+::\S+:\s+", s)
    if m:
        return s[m.end():].strip()

    # Format: " bash: command"
    i = s.find(" bash: ")
    if i >= 0:
        return s[i + len(" bash: "):].strip()

    # Format: bash[PID]: command
    m = re.search(r"\sbash\[\d+\]:\s+", s)
    if m:
        return s[m.end():].strip()

    # Fallback
    m = re.search(r"\[[0-9]+\]:\s+", s)
    if m:
        return s[m.end():].strip()

    return s


def _is_noise(cmd):
    if not cmd:
        return True
    if re.fullmatch(r"#\d+", cmd.strip()):
        return True
    return False


def collect_command_detect_metrics(cfg, state_file=_CMD_STATE_FILE):
    """
    Stateful log scanner — đọc chỉ các dòng mới kể từ lần poll trước.

    Metrics:
      command_detect_last_seen{log, keyword}  Unix ts lần cuối thấy keyword; 0=chưa thấy
      command_detect_count{log, keyword}      Cumulative count kể từ lần chạy đầu
    """
    results   = []
    logs      = cfg.get("logs", []) or []
    keywords  = [k for k in (cfg.get("keywords_to_monitor", []) or []) if k]
    excl_users = [u for u in (cfg.get("exclude_users", []) or []) if u]

    if not logs or not keywords:
        return results

    state = _load_state(state_file)

    for log_path in logs:
        is_first_run = log_path not in state
        log_st = state.setdefault(log_path, {
            "inode":      None,
            "offset":     0,
            "detections": {},
            "counts":     {}
        })

        if "counts" not in log_st:
            log_st["counts"] = {}

        for kw in keywords:
            log_st["detections"].setdefault(kw, 0)
            log_st["counts"].setdefault(kw, 0)

        current_inode = _get_inode(log_path)
        if current_inode is None:
            for kw in keywords:
                results.append({"name": "command_detect_last_seen", "log": log_path, "keyword": kw, "value": log_st["detections"].get(kw, 0)})
                results.append({"name": "command_detect_count",     "log": log_path, "keyword": kw, "value": log_st["counts"].get(kw, 0)})
            continue

        if is_first_run:
            log_st["inode"]  = current_inode
            log_st["offset"] = os.path.getsize(log_path)
            for kw in keywords:
                results.append({"name": "command_detect_last_seen", "log": log_path, "keyword": kw, "value": 0})
                results.append({"name": "command_detect_count",     "log": log_path, "keyword": kw, "value": 0})
            continue

        # Xử lý log rotation
        if log_st.get("inode") != current_inode:
            log_st["inode"]  = current_inode
            log_st["offset"] = 0
        elif log_st["offset"] > os.path.getsize(log_path):
            log_st["offset"] = 0

        try:
            with open(log_path, "r", errors="replace") as f:
                f.seek(log_st["offset"])
                for line in f:
                    if "bash" not in line.lower():
                        continue

                    # Exclude theo user — tên user nằm trong field user::tty trước #011
                    # Format: bash[PID]: USERNAME::tty:#011 command
                    if excl_users:
                        m = re.search(r"\sbash\[\d+\]:\s+(\S+)::", line)
                        if m and m.group(1) in excl_users:
                            continue

                    cmd = _extract_command(line)
                    if _is_noise(cmd):
                        continue

                    for kw in keywords:
                        if kw in cmd:
                            log_st["detections"][kw] = int(time.time())
                            log_st["counts"][kw] = log_st["counts"].get(kw, 0) + 1

                log_st["offset"] = f.tell()
        except Exception:
            pass

        for kw in keywords:
            results.append({"name": "command_detect_last_seen", "log": log_path, "keyword": kw, "value": log_st["detections"].get(kw, 0)})
            results.append({"name": "command_detect_count",     "log": log_path, "keyword": kw, "value": log_st["counts"].get(kw, 0)})

    _save_state(state_file, state)
    return results


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    final_results = []
    sensor_name   = "audit"

    try:
        if not os.environ.get("FULL_PATH_CONFIG_FILENAME"):
            project = os.environ.get("PROJECT", "staging")
            load_config(project)

        sensor_cfg = get_sensor_config().get(sensor_name, {})

        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        fs_cfg = sensor_cfg.get("file_stats", {})
        if fs_cfg.get("enable", True):
            files       = fs_cfg.get("files", []) or []
            folders     = fs_cfg.get("folders", []) or []
            inotify_log = fs_cfg.get("inotify_log", _INOTIFY_LOG)
            final_results.extend(collect_file_metrics(files, folders, inotify_log=inotify_log))

        cmd_cfg = sensor_cfg.get("command_detect", {})
        if cmd_cfg.get("enable", True):
            final_results.extend(collect_command_detect_metrics(cmd_cfg))

    except Exception as e:
        error_msg = str(e).replace('"', "'")
        final_results = [{
            "name":    "audit_error",
            "message": f"config_or_runtime_error: {error_msg}",
            "value":   1
        }]

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)