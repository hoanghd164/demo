#!/usr/bin/env python3
import os
import sys
import socket
import socks
import ssl
import concurrent.futures
from datetime import datetime, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.x509 import DNSName, ExtensionNotFound
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    get_bool,
    get_int,
    write_prometheus_metrics
)

DESCRIPTION_BASE = (
    "(-1) Connection error, (-2) SSL handshake error, (-3) Other exceptions, "
    "(-4) Certificate CN/SAN mismatch, (-5) SNI not recognized by server, "
    "(-6) Timeout, (-7) DNS resolution failed, (-9) Unexpected internal error"
)

def is_hostname_valid(cert, hostname):
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san = ext.value.get_values_for_type(DNSName)
        return hostname in san or any(hn.startswith("*.") and hostname.endswith(hn[1:]) for hn in san)
    except ExtensionNotFound:
        return hostname == get_common_name(cert)

def get_common_name(cert):
    try:
        return cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    except Exception:
        return None

class HostInfo:
    def __init__(self, cert, peername, hostname, url, http_status=None):
        self.cert = cert
        self.peername = peername
        self.hostname = hostname
        self.url = url
        self.http_status = http_status

def get_certificate(domain, ip, port, proxy_enabled, proxy_host, proxy_port, timeout=5):
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        sock = socks.socksocket()
        if proxy_enabled:
            sock.set_proxy(socks.HTTP, proxy_host, proxy_port)
        sock.settimeout(timeout)
        sock.connect((ip, port))

        try:
            tls_sock = context.wrap_socket(sock, server_hostname=domain)

            # Get cert
            cert_der = tls_sock.getpeercert(binary_form=True)
            cert = x509.load_der_x509_certificate(cert_der)

            # Send HTTP GET to get status
            try:
                req = f"GET / HTTP/1.1\r\nHost: {domain}\r\nConnection: close\r\n\r\n"
                tls_sock.sendall(req.encode())
                resp = b""
                while True:
                    chunk = tls_sock.recv(4096)
                    if not chunk:
                        break
                    resp += chunk
                status_line = resp.decode(errors="ignore").splitlines()[0]
                status_code = int(status_line.split(" ")[1])
            except Exception:
                status_code = -3

            tls_sock.close()
            return HostInfo(cert, (ip, port), domain, domain, status_code)
        except ssl.SSLError as e:
            if 'unrecognized name' in str(e).lower():
                return HostInfo(None, (ip, port), domain, domain, -5)
            raise e
    except Exception:
        return None

def get_metric_info(hostinfo):
    ip_address = hostinfo.peername[0]
    hostname = hostinfo.hostname

    if hostinfo.cert is None:
        return {
            "name": "http_status",
            "url": hostinfo.url,
            "ip": ip_address,
            "description": DESCRIPTION_BASE,
            "value": hostinfo.http_status or -5
        }

    now = datetime.now(timezone.utc)
    not_after = hostinfo.cert.not_valid_after_utc
    days_left = (not_after - now).days

    if not is_hostname_valid(hostinfo.cert, hostname):
        return {
            "name": "http_status",
            "url": hostinfo.url,
            "ip": ip_address,
            "description": DESCRIPTION_BASE,
            "value": -4
        }

    return {
        "name": "http_status",
        "url": hostinfo.url,
        "ip": ip_address,
        "description": DESCRIPTION_BASE,
        "value": hostinfo.http_status if hostinfo.http_status else days_left
    }

def check_target(item, proxy_enabled, proxy_host, proxy_port, timeout):
    domain, ip, port, url = item
    try:
        hostinfo = get_certificate(domain, ip, port, proxy_enabled, proxy_host, proxy_port, timeout)
        return get_metric_info(hostinfo) if hostinfo else {
            "name": "http_status", "url": url, "ip": ip,
            "description": DESCRIPTION_BASE, "value": -2
        }
    except socket.timeout:
        return {"name": "http_status", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -6}
    except socket.gaierror:
        return {"name": "http_status", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -7}
    except Exception:
        return {"name": "http_status", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -9}

def run_checks(targets, proxy_enabled, proxy_host, proxy_port, timeout):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(check_target, (url, ip, 443, url), proxy_enabled, proxy_host, proxy_port, timeout)
            for url, ips in targets.items() for ip in ips
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    return results

if __name__ == '__main__':
    sensor_name = "http_status"
    final_results = []

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        timeout = int(sensor_cfg.get("timeout", 5))
        targets = {t['url']: t['ip'] for t in sensor_cfg.get("targets", [])}

        proxy_enabled = get_bool("PROXY_SERVER_ENABLE", False)
        proxy_host = get_str("PROXY_SERVER_HOST", "")
        proxy_port = get_int("PROXY_SERVER_PORT", 3128)

        try:
            final_results.extend(run_checks(targets, proxy_enabled, proxy_host, proxy_port, timeout))
        except Exception as e:
            final_results.append({
                "name": "http_status_error",
                "message": str(e).replace('"', "'"),
                "value": 1
            })

    except Exception as e:
        final_results = [{
            "name": "http_status_error",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)