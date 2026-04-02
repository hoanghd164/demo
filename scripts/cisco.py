from jsonpath_ng import parse
import re
import os
import json
import requests
import subprocess
import sys

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

class cisco_system_info:
    def vendor():
        system_info = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.1.1.0').decode("utf-8").split(':')[1].strip(' "').split(',')
        vendor = system_info[1].strip(' ').split(' ')
        if len(vendor) == 2:
            vendor = vendor[1].strip('()').lower()
            job = 'nexus_device'
            type_device = 'nxos'
        elif len(vendor) == 3:
            vendor= vendor[0].lower()
            job = 'cisco_device'
            type_device = 'cisco'
        elif len(vendor) == 5:
            vendor= vendor[0].lower()
            job = 'cisco_device'
            type_device = 'cisco'

        return job,type_device,vendor

    def cpu_utilization():
        try:
            return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.4.1.9.9.109.1.1.1.1.8.1').decode("utf-8").split(': ')[1].strip('\n')
        except:
            return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'.1.3.6.1.4.1.9.2.1.56').decode("utf-8").split(': ')[1].strip('\n')

    def memory_utilization():
        mem_used = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.4.1.9.9.48.1.1.1.5.1').decode("utf-8").split(': ')[1].strip('\n')
        mem_free = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.4.1.9.9.48.1.1.1.6.1').decode("utf-8").split(': ')[1].strip('\n')
        mem_utilization = round(int(mem_used)/(int(mem_free)+(int(mem_used)))*100)
        return mem_utilization   

    def uptime():
        return GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port, '.1.3.6.1.6.3.10.2.1.3').decode("utf-8").split(': ')[1].strip('\n')
    
class nxos_system_info:
    def nxos_resources(self):
        ls_nxos_resources = [] 
        run_command = "show system resources"
        json_data = GetResults.rest_api_nxos(target_ipaddr, api_username, api_password, run_command)
        jsonpath_expression = parse('ins_api[*].outputs.output.body')

        for match in jsonpath_expression.find(json_data):
            val = (match.value)

        if val['cpu_state_idle'] != '':
            cpu_idle = val['cpu_state_idle']
            cpu_percent_used = 100 - float(cpu_idle)
            cpu_percent_used = round(cpu_percent_used)

        if val['memory_usage_total'] != '':
            memory_total = val['memory_usage_total']

        if val['memory_usage_used'] != '':
            memory_used = val['memory_usage_used']

        if val['memory_usage_free'] != '':
            memory_free = val['memory_usage_free']

        if val['current_memory_status'] == 'OK':
            memory_status = 1
        else:
            memory_status = 0

        memory_percent_used = 100 * (1 - (memory_free / memory_total))
        memory_percent_used = round(memory_percent_used)

        nxos_resources = {'nxos_resources': {
            'cpu_idle': cpu_idle,
            'cpu_percent_used': cpu_percent_used,
            'memory_total': memory_total,
            'memory_used': memory_used,
            'memory_free': memory_free,
            'memory_percent_used': memory_percent_used,
            'memory_status': memory_status
            }
        }
        ls_nxos_resources.append(nxos_resources)
        return ls_nxos_resources  

class nxos_vpc_detail:
    def vpc_status(self):
        run_command = "show vpc"
        json_data = GetResults.rest_api_nxos(target_ipaddr, api_username, api_password, run_command)
        jsonpath_expression = parse('ins_api[*].outputs.output.body')
        jsonpath_expression_port_status = parse('ins_api[*].outputs.output.body.TABLE_vpc.ROW_vpc')
        jsonpath_expression_peerlink = parse('ins_api[*].outputs.output.body.TABLE_peerlink.ROW_peerlink')
        ls_vpc_status1 = []
        ls_vpc_status2 = []
        ls_val_port_status = []
        ls_vpc_peerlink_status = []

        for match in jsonpath_expression.find(json_data):
            val = (match.value)

        for match_port_status in jsonpath_expression_port_status.find(json_data):
            val_port_status = (match_port_status.value)
            check_list = type(val_port_status) is dict
            if check_list == True:
                ls_val_port_status.append(val_port_status)
            else:
                ls_val_port_status = val_port_status

        for match_peerlink in jsonpath_expression_peerlink.find(json_data):
            val_peerlink = (match_peerlink.value)
            vpc_peerlink_id = int(val_peerlink['peer-link-id'])
            vpc_interface_peerlink = (val_peerlink['peerlink-ifindex'])
            vpc_interface_peerlink_status = int(val_peerlink['peer-link-port-state'])
            
            i = {'peerlink_info': {
                'vpc_peerlink_id': vpc_peerlink_id,
                'vpc_interface_peerlink': vpc_interface_peerlink,
                'vpc_interface_peerlink_status': vpc_interface_peerlink_status
            }}
            ls_vpc_peerlink_status.append(i)

            for vpc_port_stt in ls_val_port_status:
                if vpc_port_stt['vpc-ifindex'] != '':
                    i = {'vpc_id_' + str(vpc_port_stt['vpc-id']): {  # Convert vpc-id to string
                        'vpc_interface': vpc_port_stt['vpc-ifindex'],
                        'vpc_interface_status': int(vpc_port_stt['vpc-port-state'])
                    }}
                ls_vpc_status1.append(i)

        if val['vpc-domain-id'] != '':
            vpc_domain_id = val['vpc-domain-id']
            if vpc_domain_id == 'not configured':
                vpc_domain_id = 0
        else:
            vpc_domain_id = 0

        if val['vpc-peer-status'] == 'peer-ok':
            vpc_peer_status = 1
        else:
            vpc_peer_status = 0

        if val['vpc-peer-keepalive-status'] == 'peer-alive':
            vpc_peer_keepalive_status = 1
        else:
            vpc_peer_keepalive_status = 0

        if val['vpc-role'] != '':
            vpc_role = val['vpc-role']
            if vpc_role == 'primary-secondary':
                vpc_role = 1
            elif vpc_role == 'primary':    
                vpc_role = 1
            elif vpc_role == 'secondary-primary':
                vpc_role = 2
            elif vpc_role == 'secondary':
                vpc_role = 2
            elif vpc_role == 'none-established':
                vpc_role = 0  
        else:
            vpc_role = 0

        if val['num-of-vpcs'] != '':
            num_of_vpcs = val['num-of-vpcs']
        else:
            num_of_vpcs = 0

        nxos_vpc_info = {'nxos_vpc_info': {
            'vpc_domain_id': vpc_domain_id,
            'vpc_role': vpc_role,
            'num_of_vpcs': num_of_vpcs,
            'vpc_peer_keepalive_status': vpc_peer_keepalive_status,
            'vpc_peer_status': vpc_peer_status  
        }}
        
        ls_vpc_status2.append(nxos_vpc_info)
        return ls_vpc_status1, ls_vpc_status2

class nxos_and_cisco_interface:
    def nxos_interface_getvalue(self):
        ifName = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.31.1.1.1.1').decode("utf-8").strip('\n')
        ifStatus = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.2.2.1.8').decode("utf-8").strip('\n')
        ifspeed = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.2.2.1.5').decode("utf-8").strip('\n')
        ifTraffic_in = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.31.1.1.1.6').decode("utf-8").strip('\n')
        ifTraffic_out = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.31.1.1.1.10').decode("utf-8").strip('\n')
        ifDescription_oid = GetResults.snmpv3_results(username, md5, des, target_ipaddr, snmp_port,'1.3.6.1.2.1.31.1.1.1.18').decode("utf-8").strip('\n')
        return ifName, ifStatus, ifspeed, ifTraffic_in, ifTraffic_out, ifDescription_oid

    def ifName(self, val):
        vendor=cisco_system_info.vendor()[1]
        ls_oid_and_default_interface_name = []
        ls_oid_and_custom_interface_name = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                interface_oid = val[0].split('.')[11]
                interface_name = re.split('-|/',val[3])
                default_interface_name = val[3].strip('"')

                if len(interface_name) == 1:
                    interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[0])
                    fist_interface_name = interface_name[0]
                    last_interface_name = interface_name[1]
                    custom_interface_name = fist_interface_name.lower() + last_interface_name.lower()

                elif len(interface_name) == 2:
                    fist_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[0])[0]
                    if len(re.findall(r'[A-Za-z]+|\d+',interface_name[1])) == 2:
                        last_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[1])[0]
                        last_interface_name = last_interface_name + re.findall(r'[A-Za-z]+|\d+',interface_name[1])[1]
                    else:
                        last_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[1])[0]
                    custom_interface_name = fist_interface_name.lower() + last_interface_name.lower()

                elif len(interface_name) == 3 and vendor == 'nxos':
                    between_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[1])[0]
                    last_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[2])[0]
                    if int(between_interface_name) == 1:
                        fist_interface_name = 'sfp'
                    else:
                        fist_interface_name = re.findall(r'[A-Za-z]+|\d+',interface_name[0])[0]

                    custom_interface_name = fist_interface_name.lower() + last_interface_name.lower()
                else:
                    custom_interface_name = "".join(interface_name).strip('"')

                oid_and_custom_interface_name = {
                    interface_oid: custom_interface_name
                }  
                ls_oid_and_custom_interface_name.append(oid_and_custom_interface_name)

                oid_and_defaut_interface_name = {
                    interface_oid: default_interface_name
                }  
                ls_oid_and_default_interface_name.append(oid_and_defaut_interface_name)

        return ls_oid_and_custom_interface_name, ls_oid_and_default_interface_name

    def ifStatus(self, val):
        ls_interface_status = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                oid_infterface = val[0].split('.')
                oid_infterface = oid_infterface[10]
                status =int(val[3])
                
                if status == 2:
                    status = 0
                else:
                    status = 1

                interface_status = {
                    oid_infterface: status
                }

                ls_interface_status.append(interface_status)

        return ls_interface_status 

    def ifTraffic_in(self, val):
        ls_in_traffic = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                val_spit = val[0].split('.')
                interface_oid = val_spit[11]
                val_in_traffic = val[3]

                in_traffic = {
                    interface_oid: val_in_traffic
                }
                ls_in_traffic.append(in_traffic)
        return ls_in_traffic

    def ifTraffic_out(self, val):
        ls_out_traffic = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                val_spit = val[0].split('.')
                interface_oid = val_spit[11]
                val_out_traffic = val[3]

                out_traffic = {
                    interface_oid: val_out_traffic
                }
                ls_out_traffic.append(out_traffic)
        return ls_out_traffic

    def ifspeed(self, val):
        ls_ifspeed = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                val_spit = val[0].split('.')
                interface_oid = val_spit[10]
                interface_speed = int(val[3])

                ifspeed = {
                    interface_oid: interface_speed
                }
                ls_ifspeed.append(ifspeed)
        return ls_ifspeed

    def ifDescription(self, val):
        ls_description = []
        for x in val.split('\n'):
            if x != '':
                val = x.split(' ')
                oid_infterface = val[0].split('.')[11]
                i, j = 3, 15
                val[i : j] = [' '.join(val[i : j])]
                description = list(val)[3]

                if description != '':
                    description = description.strip('"')
                else:
                    description = "No description"

                description = {
                    oid_infterface: description
                }
                ls_description.append(description)

        return ls_description 

    def interface_info(self):
        data = nxos_and_cisco_interface.nxos_interface_getvalue(self)
        ls_ifName_default = nxos_and_cisco_interface.ifName(self,data[0])[1]
        ls_ifStatus = nxos_and_cisco_interface.ifStatus(self,data[1])
        ls_ifspeed = nxos_and_cisco_interface.ifspeed(self,data[2])
        ls_ifTraffic_in = nxos_and_cisco_interface.ifTraffic_in(self,data[3])
        ls_ifTraffic_out = nxos_and_cisco_interface.ifTraffic_out(self,data[4])
        ls_ifDescription = nxos_and_cisco_interface.ifDescription(self,data[5])
        ls_interface_eth_info = []

        for dict_ifName in ls_ifName_default:
            for oid, name in dict_ifName.items():
                for z in ls_ifStatus:
                    for x,y in z.items():
                        if x == oid:
                            status = y

                for z in ls_ifName_default:
                    for x,y in z.items():
                        if x == oid:
                            default_interface_name = y

                for z in ls_ifspeed:
                    for x,y in z.items():
                        if x == oid:
                            speed = y

                for z in ls_ifTraffic_in:
                    for x,y in z.items():
                        if x == oid:
                            in_traffic = y

                for z in ls_ifDescription:
                    for x,y in z.items():
                        if x == oid:
                            description = y

                for z in ls_ifTraffic_out:
                    for x,y in z.items():
                        if x == oid:
                            out_traffic = y
                            ls_interface_eth_info.append({'cisco_interface_info': {name: {
                                'default_interface_name': default_interface_name,
                                'oid': oid,
                                'status': status,
                                'speed': speed,
                                'in_traffic': in_traffic,
                                'out_traffic': out_traffic,
                                'description': description
                            }}})
        return ls_interface_eth_info

class cisco_exporter:
    def cisco_resource(self):
        resource = []
        resource.append({
                "name": "cisco_memory_percent_used",
                "role": "resource",
                "tag": tag,
                "vendor": vendor,
                "target_ipaddr": target_ipaddr,
                "snmp_port": str(snmp_port),
                "site": project,
                "unit": "percent",
                "value": float(cisco_system_info.memory_utilization())
            })
        
        resource.append({
                "name": "cisco_cpu_percent_used",
                "role": "resource",
                "tag": tag,
                "vendor": vendor,
                "target_ipaddr": target_ipaddr,
                "snmp_port": str(snmp_port),
                "site": project,
                "unit": "percent",
                "value": float(cisco_system_info.cpu_utilization())
            })
        return resource

    def nxos_resources(self):
        final_result = []
        nxos_resources = nxos_system_info.nxos_resources(self)

        for x in nxos_resources:
            final_result.append({
                'name': 'nxos_cpu_idle',
                'role': 'resource',
                "tag": tag,
                'vendor': vendor,
                'target_ipaddr': target_ipaddr,
                'site': project,
                'api_port': str(api_port),
                'value': float(x['nxos_resources']['cpu_idle'])
            })

            final_result.append({
                'name': 'nxos_cpu_percent_used',
                'role': 'resource',
                "tag": tag,
                'vendor': vendor,
                'target_ipaddr': target_ipaddr,
                'site': project,
                'api_port': str(api_port),
                'value': float(x['nxos_resources']['cpu_percent_used'])
            })

            final_result.append({
                'name': 'nxos_memory_total',
                'role': 'resource',
                "tag": tag,
                'vendor': vendor,
                'target_ipaddr': target_ipaddr,
                'site': project,
                'api_port': str(api_port),
                'value': float(x['nxos_resources']['memory_total'])
            })

            final_result.append({
                'name': 'nxos_memory_used',
                'role': 'resource',
                "tag": tag,
                'vendor': vendor,
                'target_ipaddr': target_ipaddr,
                'site': project,
                'api_port': str(api_port),
                'value': float(x['nxos_resources']['memory_used'])
            })

            final_result.append({
                'name': 'nxos_memory_free',
                'role': 'resource',
                "tag": tag,
                'vendor': vendor,
                'target_ipaddr': target_ipaddr,
                'site': project,
                'api_port': str(api_port),
                'value': float(x['nxos_resources']['memory_free'])
            })

        return final_result

    def cisco_nxos_uptime(self):
        nxos_uptime = []
        nxos_uptime.append({
                "name": "cisco_uptime_second",
                "role": "uptime",
                "tag": tag,
                "vendor": vendor,
                "target_ipaddr": target_ipaddr,
                "snmp_port": str(snmp_port),
                "site": project,
                "unit": "second",
                "value": float(cisco_system_info.uptime())
            })
        
        nxos_uptime.append({
                "name": "cisco_uptime_day",
                "role": "uptime",
                "tag": tag,
                "vendor": vendor,
                "target_ipaddr": target_ipaddr,
                "snmp_port": str(snmp_port),
                "site": project,
                "unit": "day",
                "value": float(round(float(cisco_system_info.uptime())/86400))
            })

        return nxos_uptime

    def cisco_interface(self):
        data = nxos_and_cisco_interface.interface_info(self)
        final_result = []
        for x in data:
            for key, value in x['cisco_interface_info'].items():
                final_result.append({
                    'name': 'cisco_interface_status',
                    'role': 'interface',
                    "tag": tag,
                    'target_ipaddr': target_ipaddr,
                    'site': project,
                    'vendor': vendor,
                    'snmp_port': str(snmp_port),
                    'interface_name': key,
                    'description': value['description'],
                    'value': float(value['status'])
                })

                final_result.append({
                    'name': 'cisco_interface_speed',
                    'role': 'interface',
                    "tag": tag,
                    'target_ipaddr': target_ipaddr,
                    'site': project,
                    'vendor': vendor,
                    'snmp_port': str(snmp_port),
                    'interface_name': key,
                    'description': value['description'],
                    'speed': float(value['speed'])
                })

                final_result.append({
                    'name': 'cisco_interface_traffic',
                    'role': 'interface',
                    "tag": tag,
                    'target_ipaddr': target_ipaddr,
                    'site': project,
                    'vendor': vendor,
                    'snmp_port': str(snmp_port),
                    'interface_name': key,
                    'description': value['description'],
                    'unit': 'bytes',
                    'value': float(value['in_traffic'])
                })

                final_result.append({
                    'name': 'cisco_interface_traffic',
                    'role': 'interface',
                    "tag": tag,
                    'target_ipaddr': target_ipaddr,
                    'site': project,
                    'vendor': vendor,
                    'snmp_port': str(snmp_port),
                    'interface_name': key,
                    'description': value['description'],
                    'unit': 'bytes',
                    'value': float(value['out_traffic'])
                })

        return final_result

    def nxos_vpc_info(self):
        data = nxos_vpc_detail.vpc_status(self)
        final_result = []

        final_result.append({
            'name': 'nxos_vpc_domain_id',
            'role': 'vpc',
            "tag": tag,
            'target_ipaddr': target_ipaddr,
            'site': project,
            'vendor': vendor,
            'api_port': str(api_port),
            'value': float(data[1][0]['nxos_vpc_info']['vpc_domain_id'])
        })

        final_result.append({
            'name': 'nxos_vpc_role',
            'role': 'vpc',
            "tag": tag,
            'target_ipaddr': target_ipaddr,
            'site': project,
            'vendor': vendor,
            'api_port': str(api_port),
            'value': float(data[1][0]['nxos_vpc_info']['vpc_role'])
        })

        final_result.append({
            'name': 'nxos_total_vpc_member',
            'role': 'vpc',
            "tag": tag,
            'target_ipaddr': target_ipaddr,
            'site': project,
            'vendor': vendor,
            'api_port': str(api_port),
            'value': float(data[1][0]['nxos_vpc_info']['num_of_vpcs'])
        })

        final_result.append({
            'name': 'nxos_vpc_peer_keepalive_status',
            'role': 'vpc',
            "tag": tag,
            'target_ipaddr': target_ipaddr,
            'site': project,
            'vendor': vendor,
            'api_port': str(api_port),
            'value': float(data[1][0]['nxos_vpc_info']['vpc_peer_keepalive_status'])
        })

        final_result.append({
            'name': 'nxos_vpc_peer_status',
            'role': 'vpc',
            "tag": tag,
            'target_ipaddr': target_ipaddr,
            'site': project,
            'vendor': vendor,
            'api_port': str(api_port),
            'value': float(data[1][0]['nxos_vpc_info']['vpc_peer_status'])
        })

        return final_result

    def nxos_vpc_member_status(self):
        nxos_vpc_member_status = []
        data = nxos_vpc_detail.vpc_status(self)[0]
        for i in data:
            for vpc_id, values in i.items():
                vpc_id=vpc_id.split('_')[-1]
                vpc_interface = values['vpc_interface']
                vpc_interface_status = values['vpc_interface_status']
                nxos_vpc_member_status.append({ 
                    'name': 'vpc',
                    'role': 'vpc',
                    "tag": tag,
                    'target_ipaddr': target_ipaddr,
                    'site': project,
                    'vendor': vendor,
                    'api_port': str(api_port),
                    'vpc_id': vpc_id,
                    'vpc_member_interface': vpc_interface,
                    'value': float(vpc_interface_status)
                })
        return nxos_vpc_member_status

if __name__ == '__main__':
    try:
        sensor_name = 'cisco'
        final_results = []
        final_errors = []

        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", [])
        cisco = cisco_exporter()

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
                vendor = target.get('vendor', '').lower()
                tag = target.get('tag', 'default')

                api_username = target.get('api_username')
                api_password = target.get('api_password')
                api_port = target.get('api_port', 80)

                globals().update(locals())

                if 'nxos' in vendor:
                    final_results.extend(cisco.cisco_nxos_uptime())
                    final_results.extend(cisco.nxos_resources())
                    final_results.extend(cisco.nxos_vpc_info())
                    final_results.extend(cisco.nxos_vpc_member_status())

                if 'cisco' in vendor:
                    final_results.extend(cisco.cisco_nxos_uptime())
                    final_results.extend(cisco.cisco_resource())
                    final_results.extend(cisco.cisco_interface())

            except Exception as e:
                final_errors.append({
                    "name": "cisco_error",
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