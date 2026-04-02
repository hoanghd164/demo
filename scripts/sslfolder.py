#!/usr/bin/env python3
import os
import subprocess
import concurrent.futures
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_sensor_config,
    get_str,
    get_int,
    get_bool,
    write_prometheus_metrics
)

sensor_name = 'ssl_checker'

def check_ssl(domain, timeout, proxy_enabled=False, proxy_env=None):
    try:
        script = f'''
        expire_date=$(timeout {timeout} curl -vvv https://{domain} 2>&1 | grep -i 'expire date' | awk -F': ' '{{print $2}}')
        expire_date_formatted=$(date -d "$expire_date" +%s)
        current_date=$(date +%s)
        diff_days=$(((expire_date_formatted - current_date) / 86400))
        echo $diff_days
        '''

        result = subprocess.run(['bash', '-c', script], capture_output=True, env=proxy_env if proxy_enabled else None)
        days_left = int(result.stdout.decode().strip())
        return {
            "name": "folder_ssl_checker",
            "url": domain,
            "value": days_left
        }
    except Exception as e:
        return {
            "name": "folder_ssl_checker",
            "url": domain,
            "description": f"Failed to check SSL days left: {e}",
            "value": -1
        }

def read_domains_from_folders(folders):
    domains = []
    for folder in folders:
        if not os.path.isdir(folder):
            continue
        for filename in os.listdir(folder):
            if filename.endswith('.conf'):
                domain = filename.split('.conf')[0]
                domains.append(domain)
    return domains

def run_ssl_checks(domains, timeout, proxy_enabled, proxy_env):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(check_ssl, domain, timeout, proxy_enabled, proxy_env) for domain in domains]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results

def main():
    project = os.environ.get("PROJECT", "staging")
    config = load_config(project)
    sensor_cfg = get_sensor_config().get(sensor_name, {})

    if not sensor_cfg.get("enable"):
        return

    timeout = sensor_cfg.get("timeout", 3)
    targets = sensor_cfg.get("targets", {})
    folders = targets.get("folders", [])
    urls = targets.get("urls", [])

    if isinstance(folders, str):
        folders = [folders]

    all_domains = list(set(urls + read_domains_from_folders(folders)))

    proxy_enabled = get_bool("PROXY_SERVER_ENABLE")
    proxy_host = get_str("PROXY_SERVER_HOST")
    proxy_port = get_int("PROXY_SERVER_PORT")
    proxy_env = {"https_proxy": f"http://{proxy_host}:{proxy_port}"} if proxy_enabled else None
    final_results = run_ssl_checks(all_domains, timeout, proxy_enabled, proxy_env)
    
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)

if __name__ == '__main__':
    main()