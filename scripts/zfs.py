import subprocess
import os
import re
import sys

# Add parent dir to sys.path to import config_loader
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    get_str,
    get_sensor_config,
    write_prometheus_metrics
)

### SUPPORT UBUNTU 16 ###
def parse_config_section(lines):
    root = {"name": "root", "children": []}
    stack = [(root, -1)]  # (node, indent)

    for line in lines:
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip())
        parts = line.split()
        if len(parts) < 5:
            continue

        name, state, read, write, cksum = parts[0:5]
        node = {
            "name": name,
            "state": state,
            "read": convert_to_bytes(read),
            "write": convert_to_bytes(write),
            "cksum": convert_to_bytes(cksum),
            "children": []
        }

        while stack and indent <= stack[-1][1]:
            stack.pop()

        stack[-1][0]["children"].append(node)
        stack.append((node, indent))

    return root["children"]

def parse_zpool_status():
    result = subprocess.run(['zpool', 'status', '-P'], capture_output=True, text=True)
    lines = result.stdout.splitlines()

    pools = []
    current_pool = {}
    config_lines = []
    in_config = False
    config_started = False

    for line in lines:
        line = line.rstrip()

        if line.startswith("  pool:"):
            if current_pool:
                current_pool["config"] = parse_config_section(config_lines)
                pools.append(current_pool)
                current_pool = {}
                config_lines = []
            current_pool["pool"] = line.split(":", 1)[1].strip()

        elif line.startswith(" state:"):
            current_pool["state"] = line.split(":", 1)[1].strip()

        elif line.startswith(" scan:"):
            current_pool["scan"] = line.split(":", 1)[1].strip()

        elif line.strip() == "config:":
            in_config = True
            config_started = False

        elif in_config:
            if not config_started:
                if re.match(r'\s*NAME\s+STATE\s+READ\s+WRITE\s+CKSUM', line):
                    config_started = True
                continue
            elif line.strip() == "" or line.strip().startswith("errors:"):
                in_config = False
            else:
                config_lines.append(line)

        elif line.startswith("errors:"):
            current_pool["errors"] = line.split(":", 1)[1].strip()

    if current_pool:
        current_pool["config"] = parse_config_section(config_lines)
        pools.append(current_pool)

    return pools

def parse_zpool_status():
    result = subprocess.run(['zpool', 'status', '-P'], capture_output=True, text=True)
    lines = result.stdout.splitlines()

    pools = []
    current_pool = {}
    config_lines = []
    in_config = False
    config_started = False

    for line in lines:
        line = line.rstrip()

        if line.startswith("  pool:"):
            if current_pool:
                current_pool["config"] = parse_config_section(config_lines)
                pools.append(current_pool)
                current_pool = {}
                config_lines = []
            current_pool["pool"] = line.split(":", 1)[1].strip()

        elif line.startswith(" state:"):
            current_pool["state"] = line.split(":", 1)[1].strip()

        elif line.startswith(" scan:"):
            current_pool["scan"] = line.split(":", 1)[1].strip()

        elif line.strip() == "config:":
            in_config = True
            config_started = False

        elif in_config:
            if not config_started:
                if re.match(r'\s*NAME\s+STATE\s+READ\s+WRITE\s+CKSUM', line):
                    config_started = True
                continue
            elif line.strip() == "" or line.strip().startswith("errors:"):
                in_config = False
            else:
                config_lines.append(line)

        elif line.startswith("errors:"):
            current_pool["errors"] = line.split(":", 1)[1].strip()

    if current_pool:
        current_pool["config"] = parse_config_section(config_lines)
        pools.append(current_pool)

    return pools


def convert_status_to_metrics(pool_info):
    metrics = []

    # 1. Trạng thái tổng thể của pool
    metrics.append({
        "name": "zpool_member_status",
        "role": "zpool",
        "zpool_name": pool_info["pool"],
        "member": "pool",
        "value": 1 if pool_info.get("state") == "ONLINE" else 0,
        "type": "pool"
    })

    # 2. Trạng thái lỗi chung của pool
    metrics.append({
        "name": "zpool_error_status",
        "role": "zpool",
        "zpool_name": pool_info["pool"],
        "member": "pool",
        "value": 1 if pool_info.get("errors") == "No known data errors" else 0
    })

    # 3. Duyệt cây đệ quy và tạo metrics
    def walk_tree(node, pool_name):
        if node["name"] == pool_name:
            node_type = "pool"
        elif node.get("children"):
            node_type = "vdev"
        else:
            node_type = "disk"

        # Trạng thái ONLINE/OFFLINE/FAULTED,...
        metrics.append({
            "name": "zpool_member_status",
            "role": "zpool",
            "zpool_name": pool_name,
            "member": node["name"],
            "value": 1 if node["state"] == "ONLINE" else 0,
            "type": node_type
        })

        # Ghi nhận các lỗi read/write/cksum
        for field in ["read", "write", "cksum"]:
            metrics.append({
                "name": f"zpool_{field}_errors",
                "role": "zpool",
                "zpool_name": pool_name,
                "member": node["name"],
                "type": node_type,
                "value": int(node.get(field, 0))
            })

        # Đệ quy duyệt các children nếu có
        for child in node.get("children", []):
            walk_tree(child, pool_name)

    # Gọi duyệt toàn bộ cấu trúc cây
    for top in pool_info.get("config", []):
        walk_tree(top, pool_info["pool"])

    return metrics
### END SUPPORT UBUNTU 16 ###

def convert_to_bytes(value):
    """
    Convert a string with units (K, M, G, T) to bytes safely.
    """
    if not value or value == "-":
        return None
    value = value.strip().upper()
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    try:
        if value[-1] in units:
            return int(float(value[:-1]) * units[value[-1]])
        else:
            return int(value)
    except (ValueError, IndexError):
        return None

def convert_iops(value):
    """
    Convert a string with units (K) to an integer.
    """
    if value[-1] == 'K':
        return int(float(value[:-1]) * 1000)
    else:
        return int(value)

def zpool_capacity():
    try:
        # First try with -p (parsable, numeric output)
        result = subprocess.run(['zpool', 'list', '-p'], capture_output=True, text=True, check=True)
        is_parsable = True
    except subprocess.CalledProcessError:
        # Fall back to -P (human readable with units)
        result = subprocess.run(['zpool', 'list', '-P'], capture_output=True, text=True)
        is_parsable = False

    lines = result.stdout.strip().split('\n')
    header = lines[0].split()
    data = []

    for line in lines[1:]:
        if line:
            values = line.split()
            data.append(dict(zip(header, values)))

    zpool_capacity = []
    zpool_avail = []
    zpool_size = []
    zpool_status = []

    for result in data:
        pool_name = result['NAME']
        pool_cap = int(result['CAP'].strip('%'))
        health = result['HEALTH']

        if is_parsable:
            pool_free = int(result['FREE'])
            pool_size = int(result['SIZE'])
        else:
            pool_free = convert_to_bytes(result['FREE'])
            pool_size = convert_to_bytes(result['SIZE'])

        zpool_capacity.append({
            "name": "zpool_capacity",
            "role": "zpool",
            "zpool_name": pool_name,
            "value": pool_cap
        })

        zpool_avail.append({
            "name": "zpool_avail",
            "role": "zpool",
            "zpool_name": pool_name,
            "value": pool_free
        })

        zpool_size.append({
            "name": "zpool_size",
            "role": "zpool",
            "zpool_name": pool_name,
            "value": pool_size
        })

        zpool_status.append({
            "name": "zpool_status",
            "role": "zpool",
            "zpool_name": pool_name,
            "value": 1 if health == 'ONLINE' else 0
        })

    return zpool_capacity + zpool_avail + zpool_size + zpool_status

def zfs_info():
    def get_zfs_names():
        result = subprocess.run(['zfs', 'list', '-p'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        lines = output.split('\n')
        names = []

        for line in lines[1:]:
            if line.strip():  # Skip empty lines
                parts = line.split()
                names.append(parts[0])
        return names

    result = subprocess.run(['zfs', 'list', '-p'], capture_output=True, text=True)
    lines = result.stdout.split('\n')
    header = lines[0].split()
    data = []

    for line in lines[1:]:
        if line:
            values = line.split()
            data.append(dict(zip(header, values)))

    zfs_capacity_list = []
    zfs_avail_list = []
    zfs_used_list = []
    zfs_total_list = []
    zfs_refres_list = []

    for result in data:
        zfs_used = int(result['USED'])
        zfs_avail = int(result['AVAIL'])
        zfs_refres = int(result['REFER'])
        zfs_mountpoint = result['MOUNTPOINT']
        zfs_total = zfs_used + zfs_avail
        zfs_percent = round((zfs_used / zfs_total) * 100)

        zfs_capacity_list.append({
            "name": "zfs_capacity",
            "role": "zfs",
            "zpool_name": result['NAME'],
            "mountpoint": zfs_mountpoint,
            "value": zfs_percent
        })

        zfs_avail_list.append({
            "name": "zfs_avail",
            "role": "zfs",
            "zpool_name": result['NAME'],
            "mountpoint": zfs_mountpoint,
            "value": zfs_avail
        })

        zfs_used_list.append({
            "name": "zfs_used",
            "role": "zfs",
            "zpool_name": result['NAME'],
            "mountpoint": zfs_mountpoint,
            "value": zfs_used
        })

        zfs_total_list.append({
            "name": "zfs_total",
            "role": "zfs",
            "zpool_name": result['NAME'],
            "mountpoint": zfs_mountpoint,
            "value": zfs_total
        })

        zfs_refres_list.append({
            "name": "zfs_refres",
            "role": "zfs",
            "zpool_name": result['NAME'],
            "mountpoint": zfs_mountpoint,
            "value": zfs_refres
        })
    return zfs_capacity_list + zfs_avail_list + zfs_used_list + zfs_total_list + zfs_refres_list

def get_zfs_names():
    result = subprocess.run(['zfs', 'list', '-p'], stdout=subprocess.PIPE)
    output = result.stdout.decode('utf-8')
    lines = output.split('\n')
    names = []

    for line in lines[1:]:
        if line.strip():  # Skip empty lines
            parts = line.split()
            names.append(parts[0])
    return names

### START ZPOOL STATUS ###
def parse_size_to_bytes(s):
    units = {'K': 1 << 10, 'M': 1 << 20, 'G': 1 << 30, 'T': 1 << 40}
    match = re.match(r'([0-9.]+)([KMGT])', s)
    if match:
        val, unit = match.groups()
        return float(val) * units[unit]
    return 0

def state_to_value(state_str):
    return {
        "ONLINE": 1,
        "DEGRADED": 0,
        "FAULTED": -1
    }.get(state_str.upper(), -2)

def extract_multi_pool_zpool_metrics_textfile(output_path="/var/lib/node_exporter/textfile_collector/zpool_status.prom"):
    result = subprocess.run(["zpool", "status", "-P"], capture_output=True, text=True)
    raw_text = result.stdout 
    lines = raw_text.strip().splitlines()
    pools = []
    current_pool = {}
    collecting = False

    for raw_line in lines:
        line = raw_line.strip()

        if line.startswith("pool:"):
            if current_pool:
                pools.append(current_pool)
                current_pool = {}
            current_pool["pool_name"] = line.split(":", 1)[1].strip()
            collecting = True

        elif collecting:
            if line.startswith("state:"):
                current_pool["state"] = line.split(":", 1)[1].strip()
            elif line.startswith("scan:"):
                current_pool["scan_line"] = line
            elif "resilvered" in line or "done" in line or "with" in line:
                current_pool["progress_line"] = line
            elif line.startswith("action:"):
                current_pool["action_lines"] = [line.split(":", 1)[1].strip()]
                current_pool["_parsing_action"] = True
            elif current_pool.get("_parsing_action"):
                if raw_line.strip() == "":
                    current_pool["_parsing_action"] = False
                elif re.match(r"^(pool:|state:|status:|errors:|scan:|config:|see:)", raw_line.strip(), re.IGNORECASE):
                    current_pool["_parsing_action"] = False
                else:
                    current_pool["action_lines"].append(line)

    if current_pool:
        pools.append(current_pool)

    with open(output_path, "w") as f:
        for pool in pools:
            name = pool["pool_name"]
            scan = pool.get("scan_line", "")
            progress = pool.get("progress_line", "")
            state = pool.get("state", "UNKNOWN")
            state_val = state_to_value(state)
            action_lines = " ".join(pool.get("action_lines", [])).lower()
            # print(f"[DEBUG] {name} action_lines = {action_lines}")

            # State status with explanation
            f.write(
                f'zpool_state_status{{pool_name="{name}", '
                f'state_desc="0-DEGRADED, 1-ONLINE, -1-FAULTED"}} {state_val}\n'
            )

            error_count = 0
            match = re.search(r'with\s+(\d+)\s+errors', progress)
            if match:
                error_count = int(match.group(1))
            f.write(f'zpool_error_count{{pool_name="{name}"}} {error_count}\n')

            needs_clear = 1 if "clear" in action_lines else 0
            needs_replace = 1 if "replace" in action_lines else 0

            f.write(
                f'zpool_needs_clear{{pool_name="{name}", '
                f'clear_desc="1 - needs clear, 0 - clean"}} {needs_clear}\n'
            )
            f.write(
                f'zpool_needs_replace{{pool_name="{name}", '
                f'replace_desc="1 - needs replace, 0 - healthy"}} {needs_replace}\n'
            )

            scanned = 0
            speed = 0
            match = re.search(r"([0-9.]+[TGMK]) scanned at ([0-9.]+[TGMK])/s", scan)
            if match:
                scanned = parse_size_to_bytes(match.group(1))
                speed = parse_size_to_bytes(match.group(2))
            f.write(f'zpool_scanned_bytes_total{{pool_name="{name}"}} {scanned}\n')
            f.write(f'zpool_scan_speed_bytes_per_sec{{pool_name="{name}"}} {speed}\n')

            resilvered = 0
            match = re.search(r"([0-9.]+[TGMK]) resilvered", progress)
            if match:
                resilvered = parse_size_to_bytes(match.group(1))
            f.write(f'zpool_resilvered_bytes_total{{pool_name="{name}"}} {resilvered}\n')

            percent = 0
            match = re.search(r"([0-9.]+)% done", progress)
            if match:
                percent = float(match.group(1))
            f.write(f'zpool_resilver_progress_percent{{pool_name="{name}"}} {percent}\n')

            days = 0
            match = re.search(r"(\d+)\s+days", progress)
            if match:
                days = int(match.group(1))
            f.write(f'zpool_resilver_time_remaining_days{{pool_name="{name}"}} {days}\n')

def extract_zpool_textfile_inline():
    result = subprocess.run(["zpool", "status", "-P"], capture_output=True, text=True)
    raw_text = result.stdout

    lines = raw_text.strip().splitlines()
    pools = []
    current_pool = {}
    collecting = False

    metrics = []

    for raw_line in lines:
        line = raw_line.strip()

        if line.startswith("pool:"):
            if current_pool:
                pools.append(current_pool)
                current_pool = {}
            current_pool["pool_name"] = line.split(":", 1)[1].strip()
            collecting = True

        elif collecting:
            if line.startswith("state:"):
                current_pool["state"] = line.split(":", 1)[1].strip()
            elif line.startswith("scan:"):
                current_pool["scan_line"] = line
            elif "resilvered" in line or "done" in line or "with" in line:
                current_pool["progress_line"] = line
            elif line.startswith("action:"):
                current_pool["action_lines"] = [line.split(":", 1)[1].strip()]
                current_pool["_parsing_action"] = True
            elif current_pool.get("_parsing_action"):
                if raw_line.strip() == "":
                    current_pool["_parsing_action"] = False
                elif re.match(r"^(pool:|state:|status:|errors:|scan:|config:|see:)", raw_line.strip(), re.IGNORECASE):
                    current_pool["_parsing_action"] = False
                else:
                    current_pool["action_lines"].append(line)

    if current_pool:
        pools.append(current_pool)

    for pool in pools:
        name = pool["pool_name"]
        scan = pool.get("scan_line", "")
        progress = pool.get("progress_line", "")
        state = pool.get("state", "UNKNOWN")
        state_val = state_to_value(state)
        action_lines = " ".join(pool.get("action_lines", [])).lower()

        metrics.append({
            "name": "zpool_state_status",
            "pool_name": name,
            "state_desc": "0-DEGRADED, 1-ONLINE, -1-FAULTED",
            "value": state_val
        })

        match = re.search(r'with\s+(\d+)\s+errors', progress)
        error_count = int(match.group(1)) if match else 0
        metrics.append({"name": "zpool_error_count", "pool_name": name, "value": error_count})

        needs_clear = 1 if "clear" in action_lines else 0
        needs_replace = 1 if "replace" in action_lines else 0

        metrics.append({"name": "zpool_needs_clear", "pool_name": name, "clear_desc": "1 - needs clear, 0 - clean", "value": needs_clear})
        metrics.append({"name": "zpool_needs_replace", "pool_name": name, "replace_desc": "1 - needs replace, 0 - healthy", "value": needs_replace})

        scanned = speed = resilvered = percent = days = 0

        match = re.search(r"([0-9.]+[TGMK]) scanned at ([0-9.]+[TGMK])/s", scan)
        if match:
            scanned = parse_size_to_bytes(match.group(1))
            speed = parse_size_to_bytes(match.group(2))

        match = re.search(r"([0-9.]+[TGMK]) resilvered", progress)
        if match:
            resilvered = parse_size_to_bytes(match.group(1))

        match = re.search(r"([0-9.]+)% done", progress)
        if match:
            percent = float(match.group(1))

        match = re.search(r"(\d+)\s+days", progress)
        if match:
            days = int(match.group(1))

        metrics.append({"name": "zpool_scanned_bytes_total", "pool_name": name, "value": scanned})
        metrics.append({"name": "zpool_scan_speed_bytes_per_sec", "pool_name": name, "value": speed})
        metrics.append({"name": "zpool_resilvered_bytes_total", "pool_name": name, "value": resilvered})
        metrics.append({"name": "zpool_resilver_progress_percent", "pool_name": name, "value": percent})
        metrics.append({"name": "zpool_resilver_time_remaining_days", "pool_name": name, "value": days})

    return metrics

if __name__ == '__main__':
    sensor_name = "zfs"
    final_metrics = []
    final_errors = []

    try:
        sensor_cfg = get_sensor_config().get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        try:
            final_metrics += zfs_info()
        except Exception as e:
            final_errors.append({"name": "zfs_error", "message": f"zfs_info_fail: {str(e)}", "value": 1})

        try:
            final_metrics += zpool_capacity()
        except Exception as e:
            final_errors.append({"name": "zfs_error", "message": f"zpool_capacity_fail: {str(e)}", "value": 1})

        try:
            for pool in parse_zpool_status():
                final_metrics += convert_status_to_metrics(pool)
        except Exception as e:
            final_errors.append({"name": "zfs_error", "message": f"parse_zpool_status_fail: {str(e)}", "value": 1})

    except Exception as main_e:
        msg = str(main_e).replace('"', "'")
        final_metrics = [{
            "name": "zfs_error",
            "message": f"zfs_unexpected_error: {msg}",
            "value": 1
        }]
    final_metrics += final_errors

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_metrics, sensor_name)