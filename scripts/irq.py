#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
- irqmon_cpu_jiffies_total{cpu,mode}
- irqmon_softirq_total{cpu,type}
- irqmon_interrupts_total{irq,cpu}
- irqmon_irq_desc_info{irq,desc}
- irqmon_irq_affinity_info{irq,cpus}
- irqmon_ksoftirqd_cpu_seconds_total{cpu,pid}
- irqmon_ksoftirqd_info{cpu,pid,allowed_cpus}

The main() function returns list[dict|str] in the format required by write_prometheus_metrics().
"""

import os
import re
import sys
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics,
)

try:
    CLK_TCK = os.sysconf(os.sysconf_names.get("SC_CLK_TCK"))
except Exception:
    CLK_TCK = 100


def add_metric(results, name, labels: dict, value):
    item = {"name": name, "value": value}
    for k, v in labels.items():
        item[k] = v
    results.append(item)


# ---------- /proc/stat ----------
def parse_proc_stat():
    modes = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice"]
    out = {}
    try:
        with open("/proc/stat") as f:
            for line in f:
                if not line.startswith("cpu"):
                    continue
                parts = line.split()
                if parts[0] == "cpu":  # aggregate
                    continue
                m = re.match(r"cpu(\d+)", parts[0])
                if not m:
                    continue
                cpu = m.group(1)
                vals = {}
                for i, mode in enumerate(modes, start=1):
                    if i < len(parts):
                        try:
                            vals[mode] = int(parts[i])
                        except Exception:
                            vals[mode] = 0
                out[cpu] = vals
    except Exception:
        pass
    return out


# ---------- /proc/softirqs ----------
def parse_softirqs():
    data = {}
    try:
        with open("/proc/softirqs") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        if not lines:
            return data
        ncpu = len([h for h in lines[0].split() if h.upper().startswith("CPU")])
        for ln in lines[1:]:
            if ":" not in ln:
                continue
            name, rest = ln.split(":", 1)
            nums = [int(x) for x in rest.split() if x.isdigit()]
            if len(nums) < ncpu:
                nums += [0] * (ncpu - len(nums))
            data[name] = nums[:ncpu]
    except Exception:
        pass
    return data


# ---------- /proc/interrupts ----------
def parse_interrupts():
    data = {}
    try:
        lines = [ln.rstrip() for ln in open("/proc/interrupts") if ln.strip()]
    except Exception:
        return {}
    if not lines:
        return {}
    ncpu = sum(1 for h in lines[0].split() if h.upper().startswith("CPU"))
    for ln in lines[1:]:
        if ":" not in ln:
            continue
        left, right = ln.split(":", 1)
        irq = left.strip()
        parts = right.split()
        counts = []
        for x in parts[:ncpu]:
            try:
                counts.append(int(x))
            except Exception:
                counts.append(0)
        desc = " ".join(parts[ncpu:]) if len(parts) > ncpu else ""
        data[irq] = {"percpu": counts, "desc": desc}
    return data


def read_affinity(irq):
    p1 = f"/proc/irq/{irq}/smp_affinity_list"
    p2 = f"/proc/irq/{irq}/smp_affinity"
    for p in (p1, p2):
        try:
            return open(p).read().strip()
        except Exception:
            continue
    return ""


# ---------- ksoftirqd ----------
def find_ksoftirqd():
    res = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            comm = open(f"/proc/{pid}/comm").read().strip()
        except Exception:
            continue
        if comm.startswith("ksoftirqd/"):
            m = re.match(r"ksoftirqd/(\d+)", comm)
            if m:
                res.append({"pid": pid, "cpu": m.group(1)})
    return res


def read_pid_stat(pid):
    try:
        data = open(f"/proc/{pid}/stat").read()
    except Exception:
        return None
    lpar = data.find("(")
    rpar = data.rfind(")")
    if lpar == -1 or rpar == -1 or rpar < lpar:
        return None
    after = data[rpar + 1 :].strip().split()
    if len(after) < 13:
        return None
    try:
        utime_j = int(after[11])
        stime_j = int(after[12])
        return utime_j, stime_j
    except Exception:
        return None


def read_pid_allowed(pid):
    try:
        for ln in open(f"/proc/{pid}/status"):
            if ln.startswith("Cpus_allowed_list:"):
                return ln.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""


def build_metrics(prefix="irqmon_"):
    """
    Thu thập số liệu và trả về list các metric (dict) cho write_prometheus_metrics().
    """
    results = []

    # CPU jiffies
    cpu_modes = parse_proc_stat()
    for cpu, modes in cpu_modes.items():
        for mode, val in modes.items():
            add_metric(results, f"{prefix}cpu_jiffies_total", {"cpu": cpu, "mode": mode}, val)

    # Softirqs
    soft = parse_softirqs()
    if soft:
        ncpu = len(next(iter(soft.values())))
        for t, arr in soft.items():
            for i in range(ncpu):
                add_metric(results, f"{prefix}softirq_total", {"cpu": str(i), "type": t}, arr[i])

    # Interrupts + desc + affinity
    intr = parse_interrupts()
    for irq, info in intr.items():
        for i, val in enumerate(info.get("percpu", [])):
            add_metric(results, f"{prefix}interrupts_total", {"irq": irq, "cpu": str(i)}, val)

    for irq, info in intr.items():
        desc = info.get("desc")
        if desc:
            add_metric(results, f"{prefix}irq_desc_info", {"irq": irq, "desc": desc}, 1)

    for irq in intr.keys():
        aff = read_affinity(irq)
        if aff:
            add_metric(results, f"{prefix}irq_affinity_info", {"irq": irq, "cpus": aff}, 1)

    # ksoftirqd
    threads = find_ksoftirqd()
    for th in threads:
        pid, cpu = th["pid"], th["cpu"]
        st = read_pid_stat(pid)
        if not st:
            continue
        total_secs = (st[0] + st[1]) / float(CLK_TCK)
        add_metric(results, f"{prefix}ksoftirqd_cpu_seconds_total", {"cpu": cpu, "pid": pid}, f"{total_secs:.6f}")

        allowed = read_pid_allowed(pid)
        add_metric(results, f"{prefix}ksoftirqd_info", {"cpu": cpu, "pid": pid, "allowed_cpus": allowed or ""}, 1)

    return results


def main():
    return build_metrics(prefix="irqmon_")


if __name__ == '__main__':
    sensor_name = "irq"
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        load_config(project)
        final_results = main()
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)
    except Exception as e:
        error_metrics = [{
            "name": "ps_error",
            "role": "ps",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()
        fallback_dirs = ["/tmp"]
        write_prometheus_metrics(fallback_dirs, error_metrics, sensor_name)
        sys.exit(1)