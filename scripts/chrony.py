#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collect chrony metrics for Prometheus textfile (node_exporter).

Exposed metrics (per NTP server `remote`):

- chrony_source_leap_status{remote}
- chrony_source_stratum{remote}
- chrony_source_poll_interval_seconds{remote}
- chrony_source_root_delay_seconds{remote}
- chrony_source_root_dispersion_seconds{remote}
- chrony_source_offset_seconds{remote}
- chrony_source_peer_delay_seconds{remote}
- chrony_source_peer_dispersion_seconds{remote}
- chrony_source_response_time_seconds{remote}
- chrony_source_total_tx{remote}
- chrony_source_total_rx{remote}
- chrony_source_total_valid_rx{remote}

main() returns list[dict|str] in the format required by write_prometheus_metrics().
"""

import os
import sys
import subprocess
import traceback

# Allow importing config_loader from parent directory (same pattern as irq script)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics,
)


def run_chronyc(args):
    """Run chronyc and return stdout as a string, raise on error."""
    result = subprocess.run(
        ["chronyc"] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"chronyc {' '.join(args)} failed: {result.returncode}, stderr={result.stderr.strip()}"
        )
    return result.stdout


def parse_ntpdata_all():
    """
    Run `chronyc ntpdata` (without arguments) and parse all NTP sources.

    Returns:
        list[dict]: one dict per NTP source, with keys like:
            - "Remote address"
            - "Leap status"
            - "Stratum"
            - "Poll interval"
            - "Root delay"
            - "Root dispersion"
            - "Offset"
            - "Peer delay"
            - "Peer dispersion"
            - "Response time"
            - "Total TX"
            - "Total RX"
            - "Total valid RX"
            ... and others.
    """
    try:
        out = run_chronyc(["ntpdata"])
    except Exception as e:
        print(f"# chrony_exporter: error running 'chronyc ntpdata': {e}")
        return []

    # chronyc ntpdata prints multiple blocks separated by a blank line
    blocks = out.strip().split("\n\n")
    sources = []

    for block in blocks:
        data = {}
        for line in block.splitlines():
            if ":" not in line:
                continue
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            data[key] = val
        if data:
            sources.append(data)

    return sources


def float_from_field(val):
    """
    Extract the first float in strings like:
      '-0.000037068 seconds'
      '0.039185 seconds'
    """
    if not val:
        return None
    first = val.split()[0]
    try:
        return float(first)
    except ValueError:
        return None


def int_from_field(val):
    """
    Extract the first int in strings like:
      '3'
      '8 (256 seconds)'
    """
    if not val:
        return None
    first = val.split()[0]
    try:
        return int(first)
    except ValueError:
        return None


def leap_status_to_int(val):
    """
    Map chrony leap status to integer:

      Normal           -> 0
      Not synchronised -> 1
      anything else    -> 2 (unknown/other)
    """
    if not val:
        return 2
    if val == "Normal":
        return 0
    if val == "Not synchronised":
        return 1
    return 2


def add_metric(results, name, labels, value):
    """
    Append a single metric entry to `results` in the format expected by
    write_prometheus_metrics().

    Args:
        results (list[dict]): accumulator list
        name (str): metric name
        labels (dict): labels like {"remote": "10.10.16.199"}
        value (int|float|str): metric value
    """
    item = {"name": name, "value": value}
    for k, v in labels.items():
        item[k] = v
    results.append(item)


def build_metrics():
    """
    Collect chrony metrics and return a list of metric dicts.

    Logic:
    - Use `chronyc ntpdata` (without arguments) to get all NTP sources.
    - For each source:
        * remote label = first token of "Remote address" (IP or hostname)
        * Always export: chrony_source_leap_status{remote}
        * Export TX/RX/valid_RX (even if source is broken)
        * If valid_RX > 0: export detailed metrics (stratum, poll interval,
          offsets, delays, etc.).
    """
    results = []

    sources = parse_ntpdata_all()
    if not sources:
        # No sources or chronyc failed
        return results

    for data in sources:
        # Remote address might look like "10.10.16.199 (0A0A10C7)" or "some-hostname (....)"
        ra = data.get("Remote address", "").split()
        if ra:
            remote = ra[0]
        else:
            remote = "[UNSPEC]"

        labels = {"remote": remote}

        # Leap status (always exported)
        leap_int = leap_status_to_int(data.get("Leap status"))
        add_metric(results, "chrony_source_leap_status", labels, leap_int)

        # TX/RX/valid RX (export even for "broken" sources for debugging)
        valid_rx = int_from_field(data.get("Total valid RX"))
        tx = int_from_field(data.get("Total TX"))
        rx = int_from_field(data.get("Total RX"))

        if tx is not None:
            add_metric(results, "chrony_source_total_tx", labels, tx)
        if rx is not None:
            add_metric(results, "chrony_source_total_rx", labels, rx)
        if valid_rx is not None:
            add_metric(results, "chrony_source_total_valid_rx", labels, valid_rx)

        # If there is no valid RX, the server never replied properly.
        # In that case we skip detailed metrics (offset/delay/etc.) to avoid noise.
        if valid_rx is None or valid_rx == 0:
            continue

        # -------- Healthy / usable NTP source: export detailed metrics --------

        # Stratum
        stratum = int_from_field(data.get("Stratum"))
        if stratum is not None:
            add_metric(results, "chrony_source_stratum", labels, stratum)

        # Poll interval: 2^poll seconds
        poll = int_from_field(data.get("Poll interval"))
        if poll is not None:
            poll_seconds = float(2 ** poll)
            add_metric(results, "chrony_source_poll_interval_seconds", labels, poll_seconds)

        # Root delay
        val = float_from_field(data.get("Root delay"))
        if val is not None:
            add_metric(results, "chrony_source_root_delay_seconds", labels, val)

        # Root dispersion
        val = float_from_field(data.get("Root dispersion"))
        if val is not None:
            add_metric(results, "chrony_source_root_dispersion_seconds", labels, val)

        # Offset
        val = float_from_field(data.get("Offset"))
        if val is not None:
            add_metric(results, "chrony_source_offset_seconds", labels, val)

        # Peer delay
        val = float_from_field(data.get("Peer delay"))
        if val is not None:
            add_metric(results, "chrony_source_peer_delay_seconds", labels, val)

        # Peer dispersion
        val = float_from_field(data.get("Peer dispersion"))
        if val is not None:
            add_metric(results, "chrony_source_peer_dispersion_seconds", labels, val)

        # Response time
        val = float_from_field(data.get("Response time"))
        if val is not None:
            add_metric(results, "chrony_source_response_time_seconds", labels, val)
    return results


def main():
    return build_metrics()


if __name__ == "__main__":
    sensor_name = "chrony"
    prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")

    try:
        project = os.environ.get("PROJECT", "staging")
        load_config(project)
        final_results = main()
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)

    except Exception as e:
        error_metrics = [{
            "name": "chrony_error",
            "role": "chrony",
            "message": str(e).replace('"', "'"),
            "value": 1
        }]
        traceback.print_exc()
        write_prometheus_metrics(prom_dirs, error_metrics, sensor_name)
        sys.exit(1)
