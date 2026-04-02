#!/usr/bin/env python3
import os
import socket
import ssl
import socks
import sys
import concurrent.futures
from datetime import datetime, timezone
from cryptography import x509
from cryptography.x509 import DNSName, ExtensionNotFound
from cryptography.x509.oid import NameOID
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    get_bool,
    get_int,
    get_sensor_config,
    write_prometheus_metrics
)

DESCRIPTION_BASE = "(-1) Connection error, (-2) SSL handshake error, (-3) Other exceptions, (-4) Certificate CN/SAN mismatch, (-5) SNI not recognized by server, (-6) Timeout, (-7) DNS resolution failed, (-9) Unexpected internal error)"

def is_hostname_valid(cert, hostname):
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san = ext.value.get_values_for_type(DNSName)
        return hostname in san or any(hn.startswith("*.") and hostname.endswith(hn[1:]) for hn in san)
    except ExtensionNotFound:
        cn = get_common_name(cert)
        return hostname == cn or (cn and cn.startswith("*.") and hostname.endswith(cn[1:]))

def get_common_name(cert):
    try:
        return cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    except Exception:
        return None

class HostInfo:
    def __init__(self, cert, peername, hostname, url):
        self.cert = cert
        self.peername = peername
        self.hostname = hostname
        self.url = url

def get_certificate(domain, ip, port, proxy_enabled, proxy_host, proxy_port, timeout=5):
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        sock = socks.socksocket() if proxy_enabled else socket.socket()
        if proxy_enabled:
            sock.set_proxy(socks.HTTP, proxy_host, proxy_port)
        sock.settimeout(timeout)
        sock.connect((ip, port))

        tls_sock = context.wrap_socket(sock, server_hostname=domain)
        cert_der = tls_sock.getpeercert(binary_form=True)
        tls_sock.close()
        cert = x509.load_der_x509_certificate(cert_der)
        return HostInfo(cert=cert, peername=(ip, port), hostname=domain, url=domain)
    except ssl.SSLError as e:
        if 'unrecognized name' in str(e).lower():
            return HostInfo(cert=None, peername=(ip, port), hostname=domain, url=domain)
        return None
    except Exception:
        return None

def get_ssl_info(item, proxy_enabled, proxy_host, proxy_port, timeout):
    domain, ip, port, url = item
    try:
        sock = socks.socksocket() if proxy_enabled else socket.socket()
        if proxy_enabled:
            sock.set_proxy(socks.HTTP, proxy_host, proxy_port)
        sock.settimeout(timeout)
        if sock.connect_ex((ip, port)) != 0:
            return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -1}
        sock.close()

        hostinfo = get_certificate(domain, ip, port, proxy_enabled, proxy_host, proxy_port, timeout)
        if not hostinfo:
            return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -2}

        if hostinfo.cert is None:
            return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -5}

        not_after = hostinfo.cert.not_valid_after_utc
        now_utc = datetime.now(timezone.utc)
        days_until_expiration = (not_after - now_utc).days

        if not is_hostname_valid(hostinfo.cert, domain):
            return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -4}

        return {
            "name": "static_ssl_checker",
            "url": url,
            "ip": ip,
            "description": DESCRIPTION_BASE,
            "value": days_until_expiration,
        }

    except socket.timeout:
        return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -6}
    except socket.gaierror:
        return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -7}
    except Exception:
        return {"name": "static_ssl_checker", "url": url, "ip": ip, "description": DESCRIPTION_BASE, "value": -9}

if __name__ == '__main__':
    sensor_name = "static_ssl_checker"
    final_results = []

    try:
        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = get_sensor_config().get("http_status", {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        timeout = sensor_cfg.get("timeout", 5)
        targets = sensor_cfg.get("targets", [])

        check_list = [
            (item["url"], ip, 443, item["url"])
            for item in targets
            for ip in item.get("ip", [])
        ]

        proxy_enabled = get_bool("PROXY_SERVER_ENABLE", False)
        proxy_host = get_str("PROXY_SERVER_HOST", "localhost")
        proxy_port = get_int("PROXY_SERVER_PORT", 3128)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(get_ssl_info, item, proxy_enabled, proxy_host, proxy_port, timeout)
                for item in check_list
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        final_results.append(result)
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

    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/tmp").split(":")
    write_prometheus_metrics(prom_dirs, final_results, sensor_name)