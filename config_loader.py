import json
import os
import yaml
import shutil

def write_prometheus_metrics(prom_dirs, final_results, sensor_name="custom_sensor"):
    """
    final_results = [
        'cpu_usage{host="server1"} 85.5',
        {"name": "memory_used", "value": 2048, "host": "server1", "region": "vn"},
        {"name": "disk_free", "value": 500},  # không có label
    ]
    """
    if not prom_dirs:
        prom_dirs = ["/var/lib/node_exporter/textfile_collector"]

    os.makedirs(prom_dirs[0], exist_ok=True)
    main_output_file = os.path.join(prom_dirs[0], f"{sensor_name}.prom")

    lines = []

    for r in final_results:
        if isinstance(r, str):
            lines.append(r.strip())
            continue

        if isinstance(r, dict):
            name = r.get("name", "unknown_metric")
            value = r.get("value", 0)

            safe_labels = []
            for k, v in r.items():
                if k not in ["name", "value"]:
                    v_str = str(v).replace('"', "'")
                    safe_labels.append(f'{k}="{v_str}"')
                    
            labels = ",".join(safe_labels)
            line = f'{name}{{{labels}}} {value}'
            lines.append(line)

    with open(main_output_file, "w") as f:
        for line in lines:
            f.write(line + "\n")

    for prom_dir in prom_dirs[1:]:
        os.makedirs(prom_dir, exist_ok=True)
        shutil.copy(main_output_file, os.path.join(prom_dir, f"{sensor_name}.prom"))

def flatten_and_export(d: dict, parent_key=''):
    """
    Flatten nested dict and export as environment variables.
    If the value is a list (e.g., list of paths), join with ':' and export.
    If the list represents directories (like prom_dir), ensure directories exist.
    """
    for key, value in d.items():
        key_clean = str(key).upper()
        new_key = f"{parent_key}_{key_clean}".strip('_') if parent_key else key_clean

        if isinstance(value, dict):
            flatten_and_export(value, new_key)
        elif isinstance(value, list):
            if "PROM_DIR" in new_key:
                for path in value:
                    try:
                        os.makedirs(path, exist_ok=True)
                        os.chmod(path, 0o755)
                    except Exception as e:
                        print(f"❌ Failed to create/set permission for {path}: {e}")
            os.environ[new_key] = ":".join(map(str, value))  # Export as colon-separated string
        else:
            os.environ[new_key] = str(value)

def get_bool(key, default=False):
    return os.environ.get(key, str(default)).lower() in ("1", "true", "yes")

def get_int(key, default=0):
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

def get_str(key, default=""):
    return os.environ.get(key, default)

def get_sensor_config():
    try:
        return json.loads(os.environ.get("SENSOR_CONFIG", "{}"))
    except json.JSONDecodeError:
        return {}

def load_config(project_name: str, working_dir: str = '/etc/admin.collector/source') -> dict:
    """
    Load config YAML file and export all keys into environment variables.
    """
    yml_path = os.path.join(working_dir, 'config', f"{project_name}.yml")
    yaml_path = os.path.join(working_dir, 'config', f"{project_name}.yaml")

    if os.path.isfile(yml_path):
        full_path = yml_path
    elif os.path.isfile(yaml_path):
        full_path = yaml_path
    else:
        raise FileNotFoundError(f"❌ Config file not found: {yml_path} or {yaml_path}")

    with open(full_path, 'r') as file:
        config = yaml.safe_load(file)

    os.environ['WORKING_DIR'] = working_dir
    os.environ['FULL_PATH_CONFIG_FILENAME'] = full_path
    os.environ['PROJECT'] = project_name

    default_config = config.get('default', {})
    flatten_and_export(default_config)

    sensor_config = config.get('sensor', {})
    os.environ['SENSOR_CONFIG'] = json.dumps(sensor_config)

    return config