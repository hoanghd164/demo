import json
import subprocess
import requests
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

class GetResults:
    def snmpv3_results(username, md5, des, target_ipaddr, port, index):
        try:
            return subprocess.check_output(
                f'snmpwalk -v3 -a md5 -A {md5} -x des -X {des} -l authPriv -u {username} {target_ipaddr}:{port} {index}',
                shell=True,
                stderr=subprocess.DEVNULL,
                timeout=5
            )
        except:
            return False

    def rest_api_nxos(target_ipaddr, api_username, api_password, run_command):
        url = f"http://{target_ipaddr}/ins"
        payload = json.dumps(
            {
              "ins_api": {
                "version": "1.0",
                "type": "cli_show",
                "chunk": "0",
                "sid": "1",
                "input": run_command,
                "output_format": "json"
              }
            }
        )

        headers = {
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, auth=(api_username, api_password), headers=headers, data=payload, timeout=5) 
        json_data = (response.text)
        json_data = json.loads(json_data)
        return json_data


class fortigate_vpn_tunnel:
    def vpn_tunnel_up_count():
        split_string = ' = INTEGER: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.1.1.0').decode("utf-8").strip('\n').split(split_string)[1]
        if results:
            return results
        else:
            return False
    
    def vpn_tunnel_phase1_name():
        ls_vpn_tunnel_phase1_name = []
        split_string = ' = STRING: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.2').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                tunnel_phase1_name = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_phase1_name = {
                    index1 + '.' + index2: tunnel_phase1_name
                }
                ls_vpn_tunnel_phase1_name.append(vpn_tunnel_phase1_name)
            return ls_vpn_tunnel_phase1_name
        else:
            return False

    def vpn_tunnel_phase2_name():
        ls_vpn_tunnel_phase2_name = []
        split_string = ' = STRING: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.3').decode("utf-8").strip('\n').split('\n')

        if results:
            for x in results:
                tunnel_phase2_name = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_phase2_name = {
                    index1 + '.' + index2: tunnel_phase2_name
                }
                ls_vpn_tunnel_phase2_name.append(vpn_tunnel_phase2_name)
            return ls_vpn_tunnel_phase2_name
        else:
            return False
        
    def vpn_tunnel_status():
        ls_vpn_tunnel_status = []
        split_string = ' = INTEGER: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.20').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                tunnel_status = int((x.split(split_string))[1].strip('"'))

                if tunnel_status == 2:
                    tunnel_status = 1
                elif tunnel_status == 1:
                    tunnel_status = 0
                else:
                    tunnel_status = 3

                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_status = {
                    index1 + '.' + index2: tunnel_status
                }
                ls_vpn_tunnel_status.append(vpn_tunnel_status)
            return ls_vpn_tunnel_status
        else:
            return False
        
    def vpn_tunnel_remote_gateway():
        ls_vpn_tunnel_remote_gateway = []
        split_string = ' = IpAddress: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.4').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                remote_gateway = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_remote_gateway = {
                    index1 + '.' + index2: remote_gateway
                }
                ls_vpn_tunnel_remote_gateway.append(vpn_tunnel_remote_gateway)

            return ls_vpn_tunnel_remote_gateway
        else:
            return False

    def vpn_tunnel_local_gateway():
        ls_vpn_tunnel_local_gateway = []
        split_string = ' = IpAddress: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.6').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                local_gateway = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_local_gateway = {
                    index1 + '.' + index2: local_gateway
                }
                ls_vpn_tunnel_local_gateway.append(vpn_tunnel_local_gateway)
            return ls_vpn_tunnel_local_gateway
        else:
            return False

    def vpn_tunnel_in_traffic():
        ls_vpn_tunnel_in_traffic = []
        split_string = ' = Counter64: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.18').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                in_traffic = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_in_traffic = {
                    index1 + '.' + index2: in_traffic
                }
                ls_vpn_tunnel_in_traffic.append(vpn_tunnel_in_traffic)
            return ls_vpn_tunnel_in_traffic
        else:
            return False

    def vpn_tunnel_out_traffic():
        ls_vpn_tunnel_out_traffic = []
        split_string = ' = Counter64: '
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.12.2.2.1.19').decode("utf-8").strip('\n').split('\n')
        if results:
            for x in results:
                out_traffic = (x.split(split_string))[1].strip('"')
                len_index = len((x.split(split_string))[0].split('.'))
                index1 = (x.split(split_string))[0].split('.')[len_index-1]
                index2 = (x.split(split_string))[0].split('.')[len_index-2]

                vpn_tunnel_out_traffic = {
                    index1 + '.' + index2: out_traffic
                }
                ls_vpn_tunnel_out_traffic.append(vpn_tunnel_out_traffic)
            return ls_vpn_tunnel_out_traffic
        else:
            return False

    def vpn_tunnel_summary():
        try:
            ls_vpn_tunnel_info = []
            ls_vpn_tunnel_phase1_name = fortigate_vpn_tunnel.vpn_tunnel_phase1_name()
            ls_vpn_tunnel_phase2_name = fortigate_vpn_tunnel.vpn_tunnel_phase2_name()
            ls_vpn_tunnel_status = fortigate_vpn_tunnel.vpn_tunnel_status()
            ls_vpn_tunnel_local_gateway = fortigate_vpn_tunnel.vpn_tunnel_local_gateway()
            ls_vpn_tunnel_remote_gateway = fortigate_vpn_tunnel.vpn_tunnel_remote_gateway()
            ls_vpn_tunnel_in_traffic = fortigate_vpn_tunnel.vpn_tunnel_in_traffic()
            ls_vpn_tunnel_out_traffic = fortigate_vpn_tunnel.vpn_tunnel_out_traffic()
            vpn_tunnel_up_count = fortigate_vpn_tunnel.vpn_tunnel_up_count()

            for dict_vpn_tunnel_phase1_name in ls_vpn_tunnel_phase1_name:
                for index,phase1_name in dict_vpn_tunnel_phase1_name.items():
                    for dict_vpn_tunnel_phase2_name in ls_vpn_tunnel_phase2_name:
                        for y,z in dict_vpn_tunnel_phase2_name.items():
                            if index == y:
                                phase2_name = z

                    for dict_vpn_tunnel_status in ls_vpn_tunnel_status:
                        for y,z in dict_vpn_tunnel_status.items():
                            if index == y:
                                status = z

                    for dict_vpn_tunnel_remote_gateway in ls_vpn_tunnel_remote_gateway:
                        for y,z in dict_vpn_tunnel_remote_gateway.items():
                            if index == y:
                                remote_gateway = z

                    for dict_vpn_tunnel_local_gateway in ls_vpn_tunnel_local_gateway:
                        for y,z in dict_vpn_tunnel_local_gateway.items():
                            if index == y:
                                local_gateway = z

                    for dict_vpn_tunnel_in_traffic in ls_vpn_tunnel_in_traffic:
                        for y,z in dict_vpn_tunnel_in_traffic.items():
                            if index == y:
                                in_traffic = z

                    for dict_vpn_tunnel_out_traffic in ls_vpn_tunnel_out_traffic:
                        for y,z in dict_vpn_tunnel_out_traffic.items():
                            if index == y:
                                out_traffic = z

                                vpn_tunnel_info = {
                                    'index': index,
                                    'tunnel_active': vpn_tunnel_up_count,
                                    'phase1_name': phase1_name,
                                    'phase2_name': phase2_name,
                                    'local_gateway': local_gateway,
                                    'remote_gateway': remote_gateway,
                                    'status': status,
                                    'description': phase1_name,
                                    'in_traffic': in_traffic,
                                    'out_traffic': out_traffic,
                                    'total_traffic': int(in_traffic) + int(out_traffic)
                                }
                                ls_vpn_tunnel_info.append(vpn_tunnel_info)
            return ls_vpn_tunnel_info
        except:
            return False
        
class fortigate_resource:
    def cpu_used():
        results =  GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.4.1.3.0').decode("utf-8").strip('\n').split(' = Gauge32: ')[1]
        if results:
            return results
        else:
            return False

    def memory_used():
        results = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.13.2.1.1.4.1').decode("utf-8").strip('\n').split(' = Gauge32: ')[1]
        if results:
            return results
        else:
            return False 

    def resource_summary():
        cpu_used = fortigate_resource.cpu_used()
        memory_used = fortigate_resource.memory_used()
        if cpu_used and memory_used:
            ls_fortigate_resource = [{
                'cpu_used': cpu_used,
                'memory_used': memory_used
            }]
            return ls_fortigate_resource
        else:
            return False

class fortigate_ha:
    def ha_status():
        try:
            return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.13.2.1.1.12.2').decode("utf-8").strip('\n').split(' = INTEGER: ')[1]
        except:
            return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.13.2.1.1.12').decode("utf-8").strip('\n').split(' = INTEGER: ')[1]

    def ha_role():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.3.2.1.1.4.1').decode("utf-8").strip('\n').split(' = INTEGER: ')[1]

    def ha_summary():
        ha_status = fortigate_ha.ha_status()
        ha_role = fortigate_ha.ha_role()
        
        ls_fortigate_ha = [{
            'ha_status': ha_status,
            'ha_role': ha_role
        }]

        return ls_fortigate_ha

class fortigate_system:
    def system_uptime():
        system_uptime = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.1.3.0').decode("utf-8").strip('\n').split(' = ')[1].split(' ')
        system_uptime_second = system_uptime[1].strip('()')
        fortigate_uptime_day = system_uptime[2].split('.')[0]
        return system_uptime_second, fortigate_uptime_day

    def system_name():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.1.5.0').decode("utf-8").strip('\n').split(' = STRING: ')[1].strip('"')

    def system_description():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.1.1.0').decode("utf-8").strip('\n').split(' = STRING: ')[1].strip('"')

    def system_mode():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.3.2.1.1.3.1').decode("utf-8").strip('\n').split(' = INTEGER: ')[1]

    def system_serial():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.100.1.1.1.0').decode("utf-8").strip('\n').split(' = STRING: ')[1].strip('"')

    def system_firmware_version():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.4.1.12356.101.4.1.1.0').decode("utf-8").strip('\n').split(' = STRING: ')[1].strip('"')

    def system_summary():
        system_name = fortigate_system.system_name()
        system_description = fortigate_system.system_description()
        system_mode = fortigate_system.system_mode()
        system_serial = fortigate_system.system_serial()
        system_firmware_version = fortigate_system.system_firmware_version()
        
        ls_fortigate_system = [{
            'system_uptime_second': fortigate_system.system_uptime()[0],
            'fortigate_uptime_day': fortigate_system.system_uptime()[1],
            'system_name': system_name,
            'system_description': system_description,
            'system_mode': system_mode,
            'system_serial': system_serial,
            'system_firmware_version': system_firmware_version
        }]

        return ls_fortigate_system

class fortigate_interface:
    def interface_name():
        ls_interface_name = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.31.1.1.1.1').decode("utf-8").strip('\n').split('\n')

        for x in val:
            len_index = len(x.split(' = STRING: ')[0].split('.'))
            index = (x.split(' = STRING: ')[0].split('.'))[len_index - 1]
            interface_name = x.split(' = STRING: ')[1].strip('"')
            interface_name = interface_name.replace(' ','-')

            interface_name = {
                index: interface_name
            }
            ls_interface_name.append(interface_name)

        return ls_interface_name

    def interface_status():
        ls_interface_status = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.2.2.1.8').decode("utf-8").strip('\n').split('\n')

        for x in val:
            len_index = len(x.split(' = INTEGER: ')[0].split('.'))
            index = (x.split(' = INTEGER: ')[0].split('.'))[len_index - 1]
            interface_status = x.split(' = INTEGER: ')[1]
            
            if int(interface_status) != 1:
                interface_status = str(0)

            interface_status = {
                index: interface_status
            }
            ls_interface_status.append(interface_status)

        return ls_interface_status

    def interface_alias():
        ls_interface_alias = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.31.1.1.1.18').decode("utf-8").strip('\n').split('\n')
        for x in val:
            index = (x.split(' = ')[0].split('.'))[-1]
            for interface_alias in x.split(' = ')[1].split('\n'):
                if 'STRING' in interface_alias:
                    interface_alias = interface_alias.split('STRING: ')[1].strip('"')
            
            if interface_alias.strip('"') == '':
                interface_alias = 'No description'

            interface_alias = {
                index: interface_alias
            }
            ls_interface_alias.append(interface_alias)
        return ls_interface_alias

    def interface_in_traffic():
        ls_interface_in_traffic = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.31.1.1.1.6').decode("utf-8").strip('\n').split('\n')

        for x in val:
            len_index = len(x.split(' = Counter64:')[0].split('.'))
            index = (x.split(' = Counter64:')[0].split('.'))[len_index - 1]
            interface_status = x.split(' = Counter64:')[1]
            interface_status = {
                index: interface_status
            }
            ls_interface_in_traffic.append(interface_status)

        return ls_interface_in_traffic

    def interface_out_traffic():
        ls_interface_out_traffic = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.31.1.1.1.10').decode("utf-8").strip('\n').split('\n')

        for x in val:
            len_index = len(x.split(' = Counter64:')[0].split('.'))
            index = (x.split(' = Counter64:')[0].split('.'))[len_index - 1]
            interface_status = x.split(' = Counter64:')[1]
            interface_status = {
                index: interface_status
            }
            ls_interface_out_traffic.append(interface_status)

        return ls_interface_out_traffic

    def interface_speed():
        ls_interface_speed = []
        val = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '1.3.6.1.2.1.31.1.1.1.15').decode("utf-8").strip('\n').split('\n')

        for x in val:
            len_index = len(x.split(' = Gauge32: ')[0].split('.'))
            index = (x.split(' = Gauge32: ')[0].split('.'))[len_index - 1]
            interface_speed = x.split(' = Gauge32: ')[1]
            interface_speed = {
                index: interface_speed
            }
            ls_interface_speed.append(interface_speed)

        return ls_interface_speed

    def interface_summary():
        ls_interface_info = []
        ls_interface_name = fortigate_interface.interface_name()
        ls_interface_status = fortigate_interface.interface_status()
        ls_interface_alias = fortigate_interface.interface_alias()
        ls_interface_speed = fortigate_interface.interface_speed()
        ls_interface_in_traffic = fortigate_interface.interface_in_traffic()
        ls_interface_out_traffic = fortigate_interface.interface_out_traffic()

        for dict_interface_name in ls_interface_name:
            for index,name in dict_interface_name.items():
                for dict_interface_status in ls_interface_status:
                    for y,z in dict_interface_status.items():
                        if index == y:
                            status = z

                for dict_interface_alias in ls_interface_alias:
                    for y,z in dict_interface_alias.items():
                        if index == y:
                            alias = z

                for dict_interface_speed in ls_interface_speed:
                    for y,z in dict_interface_speed.items():
                        if index == y:
                            speed = z

                for dict_interface_in_traffic in ls_interface_in_traffic:
                    for y,z in dict_interface_in_traffic.items():
                        if index == y:
                            in_traffic = z.strip(' ')

                for dict_interface_out_traffic in ls_interface_out_traffic:
                    for y,z in dict_interface_out_traffic.items():
                        if index == y:
                            out_traffic = z.strip(' ')

                            ls_interface_info.append({
                                'name': name,
                                'index': index,
                                'status': status,
                                'speed': speed,
                                'description': alias,
                                'in_traffic': in_traffic,
                                'out_traffic': out_traffic,
                                'total_traffic': int(in_traffic) + int(out_traffic),
                            })
        return ls_interface_info

class fortigate_exporter:
    def ftg_interface(self):
        try:
            list_fortigate_interface = []
            for dict_interface_summary in fortigate_interface.interface_summary():
                interface_name = dict_interface_summary['name']
                interface_description = dict_interface_summary['description']
                interface_status = dict_interface_summary['status']
                interface_speed = dict_interface_summary['speed']
                interface_index = dict_interface_summary['index']
                in_traffic = dict_interface_summary['in_traffic']
                out_traffic = dict_interface_summary['out_traffic']

                list_fortigate_interface.append({
                    "name": "fortigate_interface_status",
                    "role": "interface",
                    "target_ipaddr": target_ipaddr,
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "interface_name": interface_name,
                    "description": interface_description,
                    "speed": interface_speed,
                    "index": interface_index,
                    "value": float(interface_status)
                })

                list_fortigate_interface.append({
                    "name": "fortigate_interface_speed",
                    "role": "interface",
                    "target_ipaddr": target_ipaddr,
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "interface_name": interface_name,
                    "description": interface_description,
                    "index": interface_index,
                    "value": float(interface_speed)
                })

                list_fortigate_interface.append({
                    "name": "fortigate_interface_traffic",
                    "role": "interface",
                    "target_ipaddr": target_ipaddr,
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "interface_name": interface_name,
                    "description": interface_description,
                    "index": interface_index,
                    "type_traffic": 'in',
                    "value": float(in_traffic)
                })

                list_fortigate_interface.append({
                    "name": "fortigate_interface_traffic",
                    "role": "interface",
                    "target_ipaddr": target_ipaddr,
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "interface_name": interface_name,
                    "description": interface_description,
                    "index": interface_index,
                    "type_traffic": 'out',
                    "value": float(out_traffic)
                })

            return list_fortigate_interface
        except:
            return False
    
    def ftg_vpntunnel(self):
        try:
            list_fortigate_vpn_tunnel = []
            total_tunnel_active = 0

            for dict_fortigate_vpn_tunnel in fortigate_vpn_tunnel.vpn_tunnel_summary():
                interface_name = dict_fortigate_vpn_tunnel['phase1_name'].replace(' ', '-')
                interface_status = dict_fortigate_vpn_tunnel['status']
                tunnel_active = dict_fortigate_vpn_tunnel['tunnel_active']
                in_traffic = dict_fortigate_vpn_tunnel['in_traffic']
                out_traffic = dict_fortigate_vpn_tunnel['out_traffic']
                total_tunnel_active += float(tunnel_active)

                list_fortigate_vpn_tunnel.append({
                    "name": "fortigate_vpn_tunnel_status",
                    "role": "vpn_tunnel",
                    "target_ipaddr": target_ipaddr,
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "phase1_name": dict_fortigate_vpn_tunnel['phase1_name'],
                    "phase2_name": dict_fortigate_vpn_tunnel['phase2_name'],
                    "index": dict_fortigate_vpn_tunnel['index'],
                    "local_gateway": dict_fortigate_vpn_tunnel['local_gateway'],
                    "remote_gateway": dict_fortigate_vpn_tunnel['remote_gateway'],
                    "value": float(interface_status)
                })

                list_fortigate_vpn_tunnel.append({
                    "name": "fortigate_vpn_tunnel_traffic",
                    "role": "vpn_tunnel",
                    "target_ipaddr": target_ipaddr,
                    "description": "Fortigate VPN Tunnel Traffic",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "phase1_name": dict_fortigate_vpn_tunnel['phase1_name'],
                    "phase2_name": dict_fortigate_vpn_tunnel['phase2_name'],
                    "index": dict_fortigate_vpn_tunnel['index'],
                    "local_gateway": dict_fortigate_vpn_tunnel['local_gateway'],
                    "remote_gateway": dict_fortigate_vpn_tunnel['remote_gateway'],
                    "type_traffic": "in",
                    "value": float(in_traffic)
                })

                list_fortigate_vpn_tunnel.append({
                    "name": "fortigate_vpn_tunnel_traffic",
                    "role": "vpn_tunnel",
                    "target_ipaddr": target_ipaddr,
                    "description": "Fortigate VPN Tunnel Traffic",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "phase1_name": dict_fortigate_vpn_tunnel['phase1_name'],
                    "phase2_name": dict_fortigate_vpn_tunnel['phase2_name'],
                    "index": dict_fortigate_vpn_tunnel['index'],
                    "local_gateway": dict_fortigate_vpn_tunnel['local_gateway'],
                    "remote_gateway": dict_fortigate_vpn_tunnel['remote_gateway'],
                    "type_traffic": "out",
                    "value": float(out_traffic)
                })

            list_fortigate_vpn_tunnel.append({
                "name": "fortigate_tunnel_active",
                "role": "vpn_tunnel",
                "target_ipaddr": target_ipaddr,
                "description": "Fortigate Tunnel Active (Total)",
                "snmp_port": str(snmp_port),
                "tag": tag,
                "vendor": vendor,
                "site": project,
                "value": total_tunnel_active
            })

            return list_fortigate_vpn_tunnel
        except:
            return False
            
    def ftg_resource(self):
        try:
            for dict_fortigate_resource in fortigate_resource.resource_summary():
                memory_percent_used = {
                    "name": "memory_percent_used",
                    "role": "resource",
                    "target_ipaddr": target_ipaddr,
                    "description": "Memory Percent Used",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "value": float(dict_fortigate_resource['memory_used'])
                }

                cpu_percent_used = {
                    "name": "cpu_percent_used",
                    "role": "resource",
                    "target_ipaddr": target_ipaddr,
                    "description": "CPU Percent Used",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "value": float(dict_fortigate_resource['cpu_used'])
                }
            return [memory_percent_used, cpu_percent_used]
        except:
            return False

    def ftg_ha(self):
        try:
            for dict_fortigate_ha in fortigate_ha.ha_summary():
                fortigate_ha_status = {
                    "name": "fortigate_ha_status",
                    "role": "high_availability",
                    "target_ipaddr": target_ipaddr,
                    "description": "Fortigate HA Status",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "value": float(dict_fortigate_ha['ha_status'])
                }

                fortigate_ha_role = {
                    "name": "fortigate_ha_role",
                    "role": "high_availability",
                    "target_ipaddr": target_ipaddr,
                    "description": "Fortigate HA Role",
                    "snmp_port": str(snmp_port),
                    "tag": tag,
                    "vendor": vendor,
                    "site": project,
                    "value": float(dict_fortigate_ha['ha_role'])
                }
            return [fortigate_ha_status, fortigate_ha_role]
        except:
            return False

    def ftg_uptime(self):
        for dict_fortigate_uptime in fortigate_system.system_summary():
            fortigate_uptime_second = {
                "name": "fortigate_uptime_day",
                "role": "uptime",
                "target_ipaddr": target_ipaddr,
                "description": "Fortigate Uptime Second",
                "snmp_port": str(snmp_port),
                "tag": tag,
                "vendor": vendor,
                "site": project,
                "unit": "second",
                "value": float(dict_fortigate_uptime['system_uptime_second'])
            }
            fortigate_uptime_day = {
                "name": "fortigate_uptime_second",
                "role": "uptime",
                "target_ipaddr": target_ipaddr,
                "description": "Fortigate Uptime Day",
                "snmp_port": str(snmp_port),
                "tag": tag,
                "vendor": vendor,
                "site": project,
                "unit": "day",
                "value": float(dict_fortigate_uptime['fortigate_uptime_day'])
            }
        return [fortigate_uptime_second, fortigate_uptime_day]

if __name__ == '__main__':
    try:
        final_results = []
        final_errors = []
        sensor_name = 'fortigate'

        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", [])
        ftg = fortigate_exporter()

        for target in targets:
            if not target.get("enable", False):
                continue

            try:
                target_ipaddr = target['ip']
                project = target.get('site', 'default')
                snmp_port = target.get('snmp_port', 161)
                username = target['username']
                md5 = target['md5']
                des = target['des']
                vendor = target.get('vendor', 'fortigate')
                tag = target.get('tag', 'default')

                globals().update(locals())

                final_results.extend(ftg.ftg_uptime())
                final_results.extend(ftg.ftg_resource())
                final_results.extend(ftg.ftg_vpntunnel())
                final_results.extend(ftg.ftg_interface())
                final_results.extend(ftg.ftg_ha())

            except Exception as e:
                final_errors.append({
                    "name": "fortigate_error",
                    "ip": target.get("ip"),
                    "site": target.get("site", ""),
                    "vendor": target.get("vendor", ""),
                    "tag": target.get("tag", ""),
                    "message": str(e).replace('"', "'"),
                    "value": 1
                })

        final_results.extend(final_errors)
        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)

    except Exception as e:
        print(f"❌ {sensor_name} failed: {e}", file=sys.stderr)
        sys.exit(1)