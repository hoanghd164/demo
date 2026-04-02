import subprocess, re, yaml, datetime, threading
import pandas as pd
import json
import os
import socket
import sys

# Add parent dir to sys.path to import config_loader
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def get_ip_address():
    result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
    ip_addresses = result.stdout.strip().split()
    ip_address = next((ip for ip in ip_addresses if ip != '127.0.0.1' and not ip.endswith('.255')), None)
    return ip_address

def api_getdata(PROXMOX_HOST, PASSWORD, PROXMOX_PORT=8006, USERNAME='root', REALM='pam'):
    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = requests.Session()
    session.verify = False  # cân nhắc bật lại nếu có cert hợp lệ

    # Authenticate
    auth_url = f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/access/ticket'
    resp = session.post(auth_url, data={'username': USERNAME, 'password': PASSWORD, 'realm': REALM}, timeout=5)
    auth = resp.json()['data']
    session.headers.update({'CSRFPreventionToken': auth['CSRFPreventionToken']})
    session.cookies.set('PVEAuthCookie', auth['ticket'])

    # Get the list of nodes
    resp = session.get(f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/nodes/', timeout=10)
    nodes = resp.json()['data']

    results = []
    nodeinfo = []
    hostname = None
    cidr = []

    for node in nodes:
        node_name = node["node"]
        network = session.get(
            f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/nodes/{node_name}/network', timeout=5
        ).json()['data']
        qemu = session.get(
            f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/nodes/{node_name}/qemu', timeout=5
        ).json()['data']
        lxc = session.get(
            f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/nodes/{node_name}/lxc', timeout=5
        ).json()['data']

        iface_cidrs = [iface['cidr'] for iface in network if 'cidr' in iface]
        if f"{PROXMOX_HOST}/24" in iface_cidrs:
            cidr = iface_cidrs
            hostname = node_name
            results.append({
                "node": node_name,
                "qemu": qemu,
                "lxc": lxc,
            })

            # lấy nodeinfo cho đúng node
            nodes_again = session.get(
                f'https://{PROXMOX_HOST}:{PROXMOX_PORT}/api2/json/nodes/', timeout=5
            ).json()['data']
            for i in nodes_again:
                if i['node'] == hostname:
                    nodeinfo.append(i)
            break  # đã tìm thấy, thoát vòng lặp

    if hostname is None:
        return None, [], [], []

    return hostname, nodeinfo, cidr, results

def convert_to_bytes(value,unit):
    unit = unit.strip().lower()

    unit_map = {
        'tb': (1024**4),
        't': (1024**4),
        'gb': (1024**3),
        'g': (1024**3),
        'mb': (1024**2),
        'm': (1024**2),
        'kb': 1024,
        'k': 1024,
        'b': 1
    }

    if unit in unit_map:
        multiplier = unit_map[unit]
        return int(value * multiplier)

def storage_convert(id, value):
    id_count = {}
    storage_name = value.split(f':{id}')[0]
    storage_size = value.split('size=')[1]

    if "/dev/disk/by-id" in storage_name:
        storage_name = "disk"

    match = re.match(r'(\d+(\.\d+)?)([A-Za-z]+)', storage_size)

    if match:
        number = float(match.group(1))
        unit = match.group(3)
        storage_size = convert_to_bytes(number,unit)
        id_count[id] = id_count.get(id, 0) + 1

        result_dict = {
            "storage_name": storage_name,
            "storage_size": storage_size
        }

    return result_dict

class PVEMetrics:
    def get_qm_data(self, line, qm_list_resource):
        parts = line.split()
        if len(parts) == 6:
            id, name, status, mem_mb, bootdisk_gb, pid = parts[:6]

            qm_ls_data = {
                "id": int(id),
                "status": status,
                "pid": int(pid)
            }
        else:
            name = ' '.join(parts[1:-4])
            qm_ls_data = {
                "id": parts[0],
                "status": parts[-4],
                "pid": parts[-1]
            }

        qm_config_data = subprocess.check_output(["qm", "config", str(id)]).decode("utf-8")
        if "cipassword" in qm_config_data:
            qm_config_data = re.sub(r'cipassword: \*+\nciuser: ubuntu', '', qm_config_data)
            qm_config_data = re.sub(r'\n\s*\n', '\n', qm_config_data)

        qm_parsed_data = yaml.safe_load(qm_config_data)
        qm_combined_data = {**qm_ls_data, **qm_parsed_data}
        qm_list_resource.append(qm_combined_data)

    def get_lxc_data(self, line, lxc_list_resource):
        parts = line.split()
        id = int(parts[0])
        status = parts[1]

        lxc_ls_data = {
            "id": int(id),
            "status": status
        }

        lxc_data = subprocess.check_output(["pct", "config", str(id)]).decode("utf-8")
        lxc_parsed_data = yaml.safe_load(lxc_data)
        lxc_combined_data = {**lxc_ls_data, **lxc_parsed_data}
        lxc_list_resource.append(lxc_combined_data)

    def get_resource_vms(self):
        try:
            get_qm_info = subprocess.check_output(["qm", "list"], stderr=subprocess.DEVNULL).decode("utf-8")
            get_lxc_info = subprocess.check_output(["pct", "list"], stderr=subprocess.DEVNULL).decode("utf-8")

            qm_lines = get_qm_info.strip().split('\n')[1:]
            lxc_lines = get_lxc_info.strip().split('\n')[1:]

            qm_list_resource = []
            lxc_list_resource = []

            threads = []
            for line in qm_lines:
                thread = threading.Thread(target=self.get_qm_data, args=(line, qm_list_resource))
                threads.append(thread)
                thread.start()

            for line in lxc_lines:
                thread = threading.Thread(target=self.get_lxc_data, args=(line, lxc_list_resource))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            return qm_list_resource, lxc_list_resource
        except:
            return False

class CPULoadAverage:
    @classmethod
    def update_metrics(cls):
        try:
            results = subprocess.check_output("w", shell=True, text=True, stderr=subprocess.DEVNULL).strip('\n').split('\n')
            match = re.search(r'(\d+) users', results[0])
            final_results = []

            if match:
                users_count = match.group(1)
            else:
                users_count = 0

            load_averages = re.findall(r'\d+\.\d+', results[0])

            final_results.append({
                "name": "proxmox_custom_node_cpu_load_average",
                "type": "node",
                "data": "w",
                "time": "1minute",
                "unit": "load_average",
                "id": f"node/{hostname}",
                "value": float(load_averages[0])
            })

            final_results.append({
                "name": "proxmox_custom_node_cpu_load_average",
                "type": "node",
                "data": "w",
                "time": "5minute",
                "unit": "load_average",
                "id": f"node/{hostname}",
                "value": float(load_averages[1])
            })

            final_results.append({
                "name": "proxmox_custom_node_cpu_load_average",
                "type": "node",
                "data": "w",
                "time": "15minute",
                "unit": "load_average",
                "id": f"node/{hostname}",
                "value": float(load_averages[2])
            })

            final_results.append({
                "name": "proxmox_custom_node_users_logged",
                "type": "node",
                "data": "w",
                "unit": "number_of_users",
                "id": f"node/{hostname}",
                "value": int(users_count)
            })
            return final_results
        except:
            return False

class CPUSocketSize:
    def proxmox_node_cpu(self):
        cpu_info = {}
        results = {}

        try:
            lines = subprocess.check_output("lscpu", shell=True, text=True, stderr=subprocess.DEVNULL).split('\n')
            for line in lines:
                if line:
                    key, value = [s.strip() for s in line.split(":", 1)]
                    cpu_info[key] = value

            match_keys = ["Architecture", "CPU op-mode(s)", "Byte Order", "CPU(s)", "On-line CPU(s) list", "Socket(s)", "Vendor ID", "Model name", "Virtualization","L1d cache","L1i cache","L2 cache","L3 cache"]
            cpu_info = {key: value for key, value in cpu_info.items() if key in match_keys}

            for key, value in cpu_info.items():
                transformed_key = re.sub(r'\([^)]*\)', '', key).strip().replace(" ", "_").lower().replace('(', '').replace(')', '').replace("-", "_")
                results[transformed_key] = value
        
            output = subprocess.check_output("top -cn1 | grep '%Cpu(s)' | awk '{print $8}'", shell=True, text=True)
            output = output.strip()
            output = re.sub(r'[^0-9.]', '', output)
            cpu_idle_percent = float(output) if output else 0.0

            if isinstance(cpu_idle_percent, float) and cpu_idle_percent >= 0:
                cpu_used = {
                    "name": "proxmox_custom_node_cpu_used",
                    "unit": "percent",
                    "type": "node",
                    "data": "top -cn1",
                    "id": f"node/{hostname}",
                    "value": round(100 - cpu_idle_percent,2)
                }

                cpu_idle = {
                    "name": "proxmox_custom_node_cpu_idle",
                    "unit": "percent",
                    "type": "node",
                    "data": "top -cn1",
                    "id": f"node/{hostname}",
                    "value": float(cpu_idle_percent)
                }

                cpu_socket = {
                    "name": "proxmox_custom_node_cpu_size",
                    "unit": "socket",
                    "data": "lscpu",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "value": float(results['socket'])
                }

                cpu_total = {
                    "name": "proxmox_custom_node_cpu_total",
                    "architecture": results['architecture'],
                    "cpu_op_mode": results['cpu_op_mode'],
                    "byte_order": results['byte_order'],
                    "on_line_cpu_list": results['on_line_cpu_list'],
                    "vendor_id": results['vendor_id'],
                    "model_name": results['model_name'],
                    "virtualization": results['virtualization'],
                    "l1d_cache": results['l1d_cache'],
                    "l2_cache": results['l2_cache'],
                    "l3_cache": results['l3_cache'],
                    "unit": "cores",
                    "data": "lscpu",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "socket": results['socket'],
                    "value": float(results['cpu'])
                }

                return [cpu_used, cpu_idle, cpu_socket, cpu_total]
        except:
            return False    

class DiskInfo:
    def update_metrics(self):
        try:
            results_df = subprocess.check_output('df -B1', shell=True, stderr=subprocess.DEVNULL).decode("utf-8").strip()
            lines = results_df.strip().split('\n')
            header = lines[0].split()
            result_list = []
            finall_results = []

            for line in lines[1:]:
                values = line.split()
                entry = dict(zip(header, values))
                result_list.append(entry)

            for entry in result_list:
                finall_results.append({
                    "name": "proxmox_custom_node_disk_used",
                    "filesystem": entry['Filesystem'],
                    "data": "df -h",
                    "unit": "percent",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "mounted": entry['Mounted'],
                    "value": float(entry['Use%'].strip('%'))
                })

                finall_results.append({
                    "name": "proxmox_custom_node_disk_used",
                    "filesystem": entry['Filesystem'],
                    "data": "df -h",
                    "unit": "bytes",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "mounted": entry['Mounted'],
                    "value": float(re.findall(r'\d+', entry['Used'])[0])
                })

                finall_results.append({
                    "name": "proxmox_custom_node_disk_total",
                    "filesystem": entry['Filesystem'],
                    "data": "df -h",
                    "unit": "bytes",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "mounted": entry['Mounted'],
                    "value": float(re.findall(r'\d+', entry['1B-blocks'])[0])
                })

                finall_results.append({
                    "name": "proxmox_custom_node_disk_free",
                    "filesystem": entry['Filesystem'],
                    "data": "df -h",
                    "unit": "bytes",
                    "type": "node",
                    "id": f"node/{hostname}",
                    "mounted": entry['Mounted'],
                    "value": float(re.findall(r'\d+', entry['Available'])[0])
                })
            return finall_results
        except:
            return False

class NodeMemorySize:
    def proxmox_node_memory(self):
        try:
            final_results = []
            output = subprocess.check_output("free -b", shell=True, text=True, stderr=subprocess.DEVNULL)

            mem_line = None
            for line in output.split('\n'):
                if line.startswith('Mem:'):
                    mem_line = line
                    break

            if mem_line:
                mem_info = mem_line.split()

            swap_line = None
            for line in output.split('\n'):
                if line.startswith('Swap:'):
                    swap_line = line
                    break

            if swap_line:
                swap_info = swap_line.split()

            final_results.append({
                "name": "proxmox_custom_node_memory_total",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(mem_info[1])
            })
            final_results.append({
                "name": "proxmox_custom_node_memory_used",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(mem_info[2])
            })
            final_results.append({
                "name": "proxmox_custom_node_memory_free",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",    
                "value": float(mem_info[3])
            })
            final_results.append({
                "name": "proxmox_custom_node_shared_total",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(mem_info[4])
            })
            final_results.append({
                "name": "proxmox_custom_node_buffcache_total",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(mem_info[5])
            })
            final_results.append({
                "name": "proxmox_custom_node_available_total",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(mem_info[6])
            })
            final_results.append({
                "name": "proxmox_custom_node_swap_total",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(swap_info[1])
            })
            final_results.append({
                "name": "proxmox_custom_node_swap_used",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(swap_info[2])
            })
            final_results.append({
                "name": "proxmox_custom_node_swap_free",
                "data": "free -b",
                "type": "node",
                "id": f"node/{hostname}",
                "unit": "bytes",
                "value": float(swap_info[3])
            })
            return final_results
        except:
            return False

class PhysicalMemoryInfo:
    def __init__(self):
        self.missing_fields = ['array_handle', 'error_information_handle', 'total_width', 'data_width', 'form_factor', 'type', 'type_detail', 'speed', 'manufacturer', 'serial_number', 'asset_tag', 'part_number', 'rank', 'configured_memory_speed', 'minimum_voltage', 'maximum_voltage','configured_voltage']

    def fill_missing_fields(self, result):
        for field in self.missing_fields:
            if field not in result:
                result[field] = 'Null'

    def get_physical_mem(self):
        try:
            ram_info_list = []
            ram_info = {}
            no_module_installed_count = 0
            final_results = []

            output = subprocess.check_output("dmidecode --type 17", shell=True, text=True, stderr=subprocess.DEVNULL)
            lines = output.strip().split('\n')

            for line in lines:
                if line.startswith("Handle"):
                    if ram_info:
                        self.fill_missing_fields(ram_info)
                        ram_info_list.append(ram_info)
                        if 'size' in ram_info and ram_info['size'] == 'No Module Installed':
                            no_module_installed_count += 1
                    ram_info = {}
                elif ":" in line:
                    key, value = [s.strip() for s in line.split(":", 1)]
                    transformed_key = (
                        re.sub(r'\([^)]*\)', '', key)
                        .strip()
                        .replace(" ", "_")
                        .lower()
                        .replace("(", "")
                        .replace(")", "")
                        .replace("-", "_")
                    )
                    ram_info[transformed_key] = value

            if ram_info:
                self.fill_missing_fields(ram_info)
                ram_info_list.append(ram_info)
                if 'size' in ram_info and ram_info['size'] == 'No Module Installed':
                    no_module_installed_count += 1

            for idx, result in enumerate(ram_info_list):
                match = re.match(r'(\d+)\s*(\D+)', result.get('size', '0'))
                if match:
                    value = int(match.group(1))
                    unit = match.group(2)
                    size = str(convert_to_bytes(value, unit))
                else:
                    size = str(0)

                final_results.append({
                    "name": "proxmox_custom_physical_memory_info",
                    "locator": result['locator'],
                    "bank_locator": result['bank_locator'],
                    "slot_index": idx,  # <-- thêm index phân biệt slot
                    "array_handle": result['array_handle'],
                    "error_information_handle": result['error_information_handle'],
                    "total_width": result['total_width'],
                    "data_width": result['data_width'],
                    "form_factor": result['form_factor'],
                    "set": result['set'],
                    "type": result['type'],
                    "type_detail": result['type_detail'],
                    "speed": result['speed'],
                    "manufacturer": result['manufacturer'],
                    "serial_number": result['serial_number'],
                    "asset_tag": result['asset_tag'],
                    "part_number": result['part_number'],
                    "rank": result['rank'],
                    "configured_memory_speed": result['configured_memory_speed'],
                    "minimum_voltage": result['minimum_voltage'],
                    "maximum_voltage": result['maximum_voltage'],
                    "configured_voltage": result['configured_voltage'],
                    "id": f"node/{hostname}",
                    "type": "node",
                    "data": "dmidecode --type 17",
                    "unit": "bytes",
                    "value": int(size)
                })

            final_results.append({
                "name": "proxmox_custom_physical_memory_total",
                "type": "node",
                "id": f"node/{hostname}",
                "data": "dmidecode --type 17",
                "unit": "slot",
                "value": len(ram_info_list)
            })
            final_results.append({
                "name": "proxmox_custom_physical_memory_unused",
                "type": "node",
                "id": f"node/{hostname}",
                "data": "dmidecode --type 17",
                "unit": "slot",
                "value": no_module_installed_count
            })
            final_results.append({
                "name": "proxmox_custom_physical_memory_used",
                "type": "node",
                "id": f"node/{hostname}",
                "data": "dmidecode --type 17",
                "unit": "slot",
                "value": len(ram_info_list) - no_module_installed_count
            })
            return final_results

        except Exception as e:
            print(f"Error in get_physical_mem: {e}")
            return False

class ResourceVMInfo:
    def proxmox_virtual_machine_resource(self):
        final_results = []
        cpu_assigned = 0
        memory_assigned = 0
        try:
            hostname ,nodeinfo, cidr, data = api_getdata(PROXMOX_HOST=host, PROXMOX_PORT=port, PASSWORD=passwd, USERNAME=username, REALM=realm)

            node_cidr = {}
            key_name = len(cidr)
            for i in range(key_name):
                node_cidr[f"node_cidr{i + 1}"] = cidr[i].split('/')[0]

            proxmox_node_uptime = {
                "name": "proxmox_custom_api_node_uptime",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "seconds",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['uptime']
            }
            final_results.append(proxmox_node_uptime)

            proxmox_node_uptime = {
                "name": "proxmox_custom_api_node_uptime",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "days",
                "type": "node",
                # **node_cidr,
                "value": round(nodeinfo[0]['uptime'] / 86400)
            }
            final_results.append(proxmox_node_uptime)

            proxmox_node_disk_total = {
                "name": "proxmox_custom_api_node_disk_total",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "bytes",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['maxdisk']
            }
            final_results.append(proxmox_node_disk_total)

            proxmox_node_disk_used = {
                "name": "proxmox_custom_api_node_disk_used",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "bytes",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['disk']
            }
            final_results.append(proxmox_node_disk_used)

            proxmox_node_disk_used = {
                "name": "proxmox_custom_api_node_disk_used",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "percent",
                "type": "node",
                # **node_cidr,
                "value": round(nodeinfo[0]['disk'] / nodeinfo[0]['maxdisk'] * 100, 2)
            }
            final_results.append(proxmox_node_disk_used)

            proxmox_node_cpu_total = {
                "name": "proxmox_custom_api_node_cpu_total",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "cores",
                # **node_cidr,
                "type": "node",
                "value": nodeinfo[0]['maxcpu']
            }
            final_results.append(proxmox_node_cpu_total)

            proxmox_node_memory_used = {
                "name": "proxmox_custom_api_node_memory_used",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "bytes",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['mem']
            }
            final_results.append(proxmox_node_memory_used)

            proxmox_node_memory_used = {
                "name": "proxmox_custom_api_node_memory_used",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "percent",
                "type": "node",
                # **node_cidr,
                "value": round(nodeinfo[0]['mem'] / nodeinfo[0]['maxmem'] * 100, 2)
            }
            final_results.append(proxmox_node_memory_used)

            proxmox_node_memory_free = {
                "name": "proxmox_custom_api_node_memory_free",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "bytes",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['maxmem'] - nodeinfo[0]['mem']
            }
            final_results.append(proxmox_node_memory_free)

            proxmox_node_memory_total = {
                "name": "proxmox_custom_api_node_memory_total",
                "data": "api",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "bytes",
                "type": "node",
                # **node_cidr,
                "value": nodeinfo[0]['maxmem']
            }
            final_results.append(proxmox_node_memory_total)

            for result in data:
                for data in result['qemu']:
                    if data['status'] == 'running':
                        status = 1
                    else:
                        status = 0

                    proxmox_virtual_machine_status = {
                        "name": "proxmox_custom_virtual_machine_status",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "description": "0-Stopped, 1-Running",
                        "type": "qemu",
                        "unit": "status",
                        "value": status,
                    }
                    final_results.append(proxmox_virtual_machine_status)

                    proxmox_virtual_machine_cpu_total = {
                        "name": "proxmox_custom_virtual_machine_cpu_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "type": "qemu",
                        "unit": "cores",
                        "value": int(data['cpus'])
                    }
                    final_results.append(proxmox_virtual_machine_cpu_total)

                    proxmox_virtual_machine_cpu_used = {
                        "name": "proxmox_custom_virtual_machine_cpu_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "cores",
                        "type": "qemu",
                        "value": int(data['cpu'])
                    }
                    final_results.append(proxmox_virtual_machine_cpu_used)
                    
                    proxmox_virtual_machine_cpu_used = {
                        "name": "proxmox_custom_virtual_machine_cpu_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "qemu",
                        "value": int(data['cpu']) / int(data['cpus']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_cpu_used)

                    proxmox_virtual_machine_memory_total = {
                        "name": "proxmox_custom_virtual_machine_memory_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['maxmem'])
                    }
                    final_results.append(proxmox_virtual_machine_memory_total)

                    proxmox_virtual_machine_memory_used = {
                        "name": "proxmox_custom_virtual_machine_memory_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['mem'])
                    }
                    final_results.append(proxmox_virtual_machine_memory_used)

                    proxmox_virtual_machine_memory_used = {
                        "name": "proxmox_custom_virtual_machine_memory_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "qemu",
                        "value": int(data['mem']) / int(data['maxmem']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_memory_used)

                    proxmox_virtual_machine_disk_total = {
                        "name": "proxmox_custom_virtual_machine_disk_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['maxdisk'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_total)

                    proxmox_virtual_machine_disk_used = {
                        "name": "proxmox_custom_virtual_machine_disk_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['disk'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_used)

                    proxmox_virtual_machine_disk_used = {
                        "name": "proxmox_custom_virtual_machine_disk_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "qemu",
                        "value": int(data['disk']) / int(data['maxdisk']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_disk_used)

                    proxmox_virtual_machine_disk_read = {
                        "name": "proxmox_custom_virtual_machine_disk_read",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['diskread'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_read)

                    proxmox_virtual_machine_disk_write = {
                        "name": "proxmox_custom_virtual_machine_disk_write",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['diskwrite'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_write)

                    proxmox_virtual_machine_net_in = {
                        "name": "proxmox_custom_virtual_machine_net_in",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['netin'])
                    }
                    final_results.append(proxmox_virtual_machine_net_in)

                    proxmox_virtual_machine_net_out = {
                        "name": "proxmox_custom_virtual_machine_net_out",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "qemu",
                        "value": int(data['netout'])
                    }
                    final_results.append(proxmox_virtual_machine_net_out)

                    proxmox_virtual_machine_uptime = {
                        "name": "proxmox_custom_virtual_machine_uptime",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "seconds",
                        "type": "qemu",
                        "value": int(data['uptime'])
                    }
                    final_results.append(proxmox_virtual_machine_uptime)

                    proxmox_virtual_machine_uptime = {
                        "name": "proxmox_custom_virtual_machine_uptime",
                        "data": "api",
                        "node": hostname,
                        "id": f"qemu/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "days",
                        "type": "qemu",
                        # **node_cidr,
                        "value": int(data['uptime']) / 86400
                    }
                    final_results.append(proxmox_virtual_machine_uptime)
                
                    if status == 1:
                        cpu_assigned += int(data['cpus'])
                        memory_assigned += int(data['maxmem'])

                for data in result['lxc']:
                    if data['status'] == 'running':
                        status = 1
                    else:
                        status = 0

                    proxmox_virtual_machine_status = {
                        "name": "proxmox_custom_virtual_machine_status",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "description": "0-Stopped, 1-Running",
                        "type": "lxc",
                        "unit": "status",
                        # **node_cidr,
                        "value": status,
                    }
                    final_results.append(proxmox_virtual_machine_status)

                    proxmox_virtual_machine_cpu_total = {
                        "name": "proxmox_custom_virtual_machine_cpu_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "type": "lxc",
                        "unit": "cores",
                        "value": int(data['cpus'])
                    }
                    final_results.append(proxmox_virtual_machine_cpu_total)

                    proxmox_virtual_machine_cpu_used = {
                        "name": "proxmox_custom_virtual_machine_cpu_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "cores",
                        "type": "lxc",
                        "value": int(data['cpu'])
                    }
                    final_results.append(proxmox_virtual_machine_cpu_used)
                    
                    proxmox_virtual_machine_cpu_used = {
                        "name": "proxmox_custom_virtual_machine_cpu_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "lxc",
                        "value": int(data['cpu']) / int(data['cpus']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_cpu_used)

                    proxmox_virtual_machine_memory_total = {
                        "name": "proxmox_custom_virtual_machine_memory_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['maxmem'])
                    }
                    final_results.append(proxmox_virtual_machine_memory_total)

                    proxmox_virtual_machine_memory_used = {
                        "name": "proxmox_custom_virtual_machine_memory_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['mem'])
                    }
                    final_results.append(proxmox_virtual_machine_memory_used)

                    proxmox_virtual_machine_memory_used = {
                        "name": "proxmox_custom_virtual_machine_memory_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "lxc",
                        "value": int(data['mem']) / int(data['maxmem']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_memory_used)

                    proxmox_virtual_machine_disk_total = {
                        "name": "proxmox_custom_virtual_machine_disk_total",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['maxdisk'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_total)

                    proxmox_virtual_machine_disk_used = {
                        "name": "proxmox_custom_virtual_machine_disk_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['disk'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_used)

                    proxmox_virtual_machine_disk_used = {
                        "name": "proxmox_custom_virtual_machine_disk_used",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "percent",
                        "type": "lxc",
                        "value": int(data['disk']) / int(data['maxdisk']) * 100
                    }
                    final_results.append(proxmox_virtual_machine_disk_used)

                    proxmox_virtual_machine_disk_read = {
                        "name": "proxmox_custom_virtual_machine_disk_read",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['diskread'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_read)

                    proxmox_virtual_machine_disk_write = {
                        "name": "proxmox_custom_virtual_machine_disk_write",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['diskwrite'])
                    }
                    final_results.append(proxmox_virtual_machine_disk_write)

                    proxmox_virtual_machine_net_in = {
                        "name": "proxmox_custom_virtual_machine_net_in",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['netin'])
                    }
                    final_results.append(proxmox_virtual_machine_net_in)

                    proxmox_virtual_machine_net_out = {
                        "name": "proxmox_custom_virtual_machine_net_out",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "bytes",
                        "type": "lxc",
                        "value": int(data['netout'])
                    }
                    final_results.append(proxmox_virtual_machine_net_out)

                    proxmox_virtual_machine_uptime = {
                        "name": "proxmox_custom_virtual_machine_uptime",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "seconds",
                        "type": "lxc",
                        "value": int(data['uptime'])
                    }
                    final_results.append(proxmox_virtual_machine_uptime)

                    proxmox_virtual_machine_uptime = {
                        "name": "proxmox_custom_virtual_machine_uptime",
                        "data": "api",
                        "node": hostname,
                        "id": f"lxc/{data['vmid']}",
                        "vps_name": data['name'],
                        "unit": "days",
                        "type": "lxc",
                        "value": int(data['uptime']) / 86400
                    }
                    final_results.append(proxmox_virtual_machine_uptime)
                
                    if status == 1:
                        cpu_assigned += int(data['cpus'])
                        memory_assigned += int(data['maxmem'])

            final_results.append({
                "name": "proxmox_custom_node_cpu_overcommit",
                "data": "api",
                "unit": "cores",
                "type": "node",
                "id": f"node/{hostname}",
                # **node_cidr,
                "value": cpu_assigned
            })

            final_results.append({
                "name": "proxmox_custom_node_cpu_overcommit",
                "data": "api",
                "unit": "percent",
                "type": "node",
                "id": f"node/{hostname}",
                # **node_cidr,
                "value": round((cpu_assigned / (int(nodeinfo[0]['maxcpu']) - 8)) * 100, 2)
            })

            final_results.append({
                "name": "proxmox_custom_node_memory_overcommit",
                "data": "api",
                "unit": "percent",
                "id": f"node/{hostname}",
                # **node_cidr,
                "value": round((memory_assigned / (int(nodeinfo[0]['maxmem']) - (8 * 1024 * 1024 * 1024 ))) * 100, 2)
            })

            final_results.append({
                "name": "proxmox_custom_node_memory_overcommit",
                "data": "api",
                "unit": "bytes",
                "id": f"node/{hostname}",
                # **node_cidr,
                "value": memory_assigned
            })

        except Exception as e:
            final_results = [{
                "name": "proxmox_error",
                "role": sensor_name,
                "module": "api authentication failure",
                "message": str(e).replace('"', "'"),
                "value": 1
            }]
        return final_results

class CPUTemperature:
    @classmethod
    def update_metrics(cls):
        temperature_output = subprocess.check_output("sensors|grep 'high'|grep 'Core'|cut -d '+' -f2|cut -d '.' -f1|sort -nr|sed -n 2p", shell=True, text=True, stderr=subprocess.DEVNULL).strip()
        cpu_temperature = int(temperature_output) if temperature_output else None

        if cpu_temperature is not None:
            return [{
                "name": "proxmox_custom_node_temperature",
                "data": "lm-sensors",
                "unit": "celsius",
                "id": f"node/{hostname}",
                "value": cpu_temperature
            }]
        else:
            return False

class PveStorageInfo:
    def pvesm_status(self):
        try:
            output = subprocess.check_output("pvesm status", shell=True, text=True, stderr=subprocess.DEVNULL)
            lines = output.splitlines()
            header = lines[0].split()
            keys = [key.lower() for key in header]
            data = []
            final_results = []

            for line in lines[1:]:
                values = line.split()
                item = {}
                
                for i, key in enumerate(keys):
                    transformed_key = re.sub(r'\([^)]*\)', '', key).strip().replace("%", "percent").lower()
                    item[transformed_key] = values[i]

                data.append(item)

            for result in data:
                if result['status'] == 'active':
                    status = 1
                else:
                    status = 0

                if result['percent']:
                    percent = str(result['percent']).strip('%')

                if 'N/A' in result['percent']:
                    percent = 0

                final_results.append({
                    "name": "proxmox_custom_storage_status",
                    "data": "pvesm status",
                    "type": "node",
                    "unit": "status",
                    "storage_name": result['name'],
                    "id": f"node/{hostname}",
                    "type": result['type'],
                    "value": float(status)
                })

                final_results.append({
                    "name": "proxmox_custom_storage_total",
                    "data": "pvesm status",
                    "type": "node",
                    "unit": "bytes",
                    "storage_name": result['name'],
                    "id": f"node/{hostname}",
                    "type": result['type'],
                    "value": float(result['total'])
                })
                
                final_results.append({
                    "name": "proxmox_custom_storage_used",
                    "data": "pvesm status",
                    "type": "node",
                    "unit": "bytes",
                    "storage_name": result['name'],
                    "id": f"node/{hostname}",
                    "type": result['type'],
                    "value": float(result['used'])
                })
                final_results.append({
                    "name": "proxmox_custom_storage_available",
                    "data": "pvesm status",
                    "type": "node",
                    "role": "storage",
                    "unit": "bytes",
                    "storage_name": result['name'],
                    "id": f"node/{hostname}",
                    "type": result['type'],
                    "value": float(result['available'])
                })
                
                final_results.append({
                    "name": "proxmox_custom_storage_used",
                    "data": "pvesm status",
                    "type": "node",
                    "unit": "percent",
                    "storage_name": result['name'],
                    "id": f"node/{hostname}",
                    "type": result['type'],
                    "value": float(percent)
                })
            return final_results
        except:
            return False

class Uptime:
    @classmethod
    def update_metrics(cls):
        try:
            final_results = []
            uptime_output = subprocess.check_output("uptime -s", shell=True, text=True, stderr=subprocess.DEVNULL).strip()
            uptime_datetime = datetime.datetime.strptime(uptime_output, "%Y-%m-%d %H:%M:%S")
            uptime_seconds = (datetime.datetime.now() - uptime_datetime).total_seconds()
            uptime_days = round(uptime_seconds / 86400)

            final_results.append({
                "name": "proxmox_custom_node_uptime",
                "data": "uptime -s",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "seconds",
                "type": "node",
                "value": uptime_seconds
            })

            final_results.append({
                "name": "proxmox_custom_node_uptime",
                "data": "uptime -s",
                "node": hostname,
                "id": f"node/{hostname}",
                "unit": "days",
                "type": "node",
                "value": uptime_days
            })
            return final_results
        except:
            return False

def write_prometheus_metrics(sensor_name, results):
    output_path = f"/var/lib/node_exporter/textfile_collector/{sensor_name}.prom"

    try:
        with open(output_path, "w") as f:
            for metric in results:
                if not isinstance(metric, dict):
                    continue

                name = metric.get("name", "unknown_metric")
                value = metric.get("value", 0)

                labels = {k: v for k, v in metric.items() if k not in ["name", "value"]}
                label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())

                if label_str:
                    f.write(f'{name}{{{label_str}}} {value}\n')
                else:
                    f.write(f'{name} {value}\n')
        print(f"[✓] Ghi Prometheus metrics vào {output_path}")
    except Exception as e:
        print(f"[✗] Lỗi ghi Prometheus: {e}")

if __name__ == '__main__':
    import socket
    import traceback
    from config_loader import load_config, get_str, write_prometheus_metrics

    sensor_name = "pve"
    final_results = []

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)
        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})

        if not sensor_cfg.get("enable", False):
            exit(0)

        host = sensor_cfg.get("ip") or socket.gethostbyname(socket.gethostname())
        port = sensor_cfg.get("port", 8006)
        user_full = sensor_cfg.get("user", "root@pam")
        passwd = sensor_cfg.get("password")
        username, realm = user_full.split("@")
        hostname = socket.gethostname()
        globals().update(locals())

        metric_functions = [
            ("memory", lambda: NodeMemorySize().proxmox_node_memory()),
            ("cpu_socket", lambda: CPUSocketSize().proxmox_node_cpu()),
            ("vm_resource", lambda: ResourceVMInfo().proxmox_virtual_machine_resource()),
            ("storage", lambda: PveStorageInfo().pvesm_status()),
            ("uptime", lambda: Uptime.update_metrics()),
            ("cpu_temp", lambda: CPUTemperature.update_metrics()),
            ("load_avg", lambda: CPULoadAverage.update_metrics()),
            ("phys_mem", lambda: PhysicalMemoryInfo().get_physical_mem())
        ]

        for name, func in metric_functions:
            try:
                metrics = func()
                if isinstance(metrics, list):
                    final_results.extend(metrics)
                else:
                    final_results.append(metrics)
            except Exception as e:
                final_results.append({
                    "name": "proxmox_error",
                    "role": sensor_name,
                    "module": name,
                    "message": str(e).replace('"', "'"),
                    "value": 1
                })
                traceback.print_exc()

    except Exception as e:
        final_results = [{
            "name": "proxmox_error",
            "role": sensor_name,
            "module": "initialization",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()

    try:
        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)
    except Exception as e:
        print(f"❌ Failed to write prom file: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)