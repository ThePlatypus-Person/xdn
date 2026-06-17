#!/usr/bin/env python3
"""
graph.py — Plot p50/p99 latency over time from k6 results,
with switchover markers from switchovers.log.

Usage:
    python3 graph.py results.json switchovers.log

Dependencies:
    pip install matplotlib numpy
"""

import sys
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────

BUCKET_SIZE_MS = 1000  # 1 second buckets
PORTS = {"2300": "primary", "2301": "backup1", "2302": "backup2"}
SHOW_P99 = False

COLORS = {
    "primary": ("#2196F3", "#0D47A1"),   # (p50 color, p99 color)
    "backup1": ("#4CAF50", "#1B5E20"),
    "backup2": ("#FF9800", "#E65100"),
}

# ── Parse k6 results.json (streaming to avoid loading full file) ───────────────

def parse_k6(path):
    """
    Returns dict: port -> list of (timestamp_ms, duration_ms)
    Only includes http_req_duration metric points.
    """
    data = defaultdict(list)
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if obj.get("type") != "Point":
                continue
            if obj.get("metric") != "http_req_duration":
                continue

            tags = obj.get("data", {}).get("tags", {})
            port = tags.get("port")
            if port not in PORTS:
                continue

            time_str = obj["data"]["time"]
            # Parse ISO 8601 timestamp → Unix ms
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            ts_ms = int(dt.timestamp() * 1000)
            duration_ms = obj["data"]["value"]

            req_type = tags.get("type", "read")
            data[port].append((ts_ms, duration_ms, req_type))

    return data

# ── Parse switchovers.log ──────────────────────────────────────────────────────

def parse_switchovers(path):
    """
    Returns list of (timestamp_ms, role).
    """
    switchovers = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) != 2:
                    continue
                ts_ms = int(parts[0])
                role = parts[1]
                switchovers.append((ts_ms, role))
    except FileNotFoundError:
        print(f"Warning: {path} not found, skipping switchover markers")
    return switchovers

# ── Bucket requests and compute percentiles ────────────────────────────────────

def bucket_percentiles(points, bucket_ms):
    """
    Takes list of (ts_ms, duration_ms), returns:
    (bucket_times_s, p50_values, p99_values)
    where bucket_times_s is relative seconds from start.
    """
    if not points:
        return [], [], []

    start_ms = min(ts for ts, _, _ in points)
    buckets = defaultdict(list)

    for ts_ms, dur, _ in points:
        bucket = ((ts_ms - start_ms) // bucket_ms) * bucket_ms
        buckets[bucket].append(dur)

    sorted_buckets = sorted(buckets.keys())
    times_s = [b / 1000.0 for b in sorted_buckets]
    p50 = [np.percentile(buckets[b], 50) for b in sorted_buckets]
    p99 = [np.percentile(buckets[b], 99) for b in sorted_buckets]

    return times_s, p50, p99

# ── Plot ───────────────────────────────────────────────────────────────────────

def plot(k6_data, switchovers, req_type, output_file):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharex=True)
    fig.suptitle(f"XDN Prototype — {req_type.capitalize()} Latency over Time (p50 / p99)", fontsize=14, fontweight="bold")

    # Compute y max across all ports for this req_type
    y_max = 0
    for port in ["2300", "2301", "2302"]:
        points = k6_data.get(port, [])
        if not points:
            continue
        subset = [(ts, dur, t) for ts, dur, t in points if t == req_type]
        if not subset:
            continue
        _, p50, p99 = bucket_percentiles(subset, BUCKET_SIZE_MS)
        if SHOW_P99 and p99:
            y_max = max(y_max, max(p99))
        elif p50:
            y_max = max(y_max, max(p50))
    y_max *= 1.1

    if req_type == "read":
        y_max = 8
    '''
    else:
        y_max = 50
    '''

    global_start_ms = None

    # First pass: find global start time for consistent x-axis
    for port in ["2300", "2301", "2302"]:
        points = k6_data.get(port, [])
        if points:
            start = min(ts for ts, _, _ in points)
            if global_start_ms is None or start < global_start_ms:
                global_start_ms = start

    if global_start_ms is None:
        print("No data found in results.json")
        sys.exit(1)

    # Find global end time (last request across all ports)
    global_end_ms = max(
        max(ts for ts, _, _ in points)
        for points in k6_data.values()
        if points
    )

    # Filter switchovers to only those within the k6 run
    switchovers = [
        (ts, role) for ts, role in switchovers
        if global_start_ms <= ts <= global_end_ms
    ]
    print(f"  {len(switchovers)} switchover events within k6 window")

    for col, port in enumerate(["2300", "2301", "2302"]):
        role = PORTS[port]
        p50_color, p99_color = COLORS[role]
        points = k6_data.get(port, [])
        ax = axes[col]

        if not points:
            ax.set_title(f":{port} ({role}) — no data")
            continue

        subset = [(ts, dur, t) for ts, dur, t in points if t == req_type]
        offset_s = (min(ts for ts, _, _ in points) - global_start_ms) / 1000.0

        if subset:
            times_s, p50, p99 = bucket_percentiles(subset, BUCKET_SIZE_MS)
            times_s = [t + offset_s for t in times_s]
            ax.plot(times_s, p50, color=p50_color, linewidth=1.5,
                    linestyle="-", label="p50")
            if SHOW_P99:
                ax.plot(times_s, p99, color=p99_color, linewidth=1.5,
                        linestyle="--", label="p99")
                ax.fill_between(times_s, p50, p99, alpha=0.08, color=p50_color)

        for ts_ms, sw_role in switchovers:
            if sw_role != role:
                continue
            x = (ts_ms - global_start_ms) / 1000.0
            ax.axvline(x=x, color="red", linewidth=1.2, linestyle=":", alpha=0.8)
            ax.text(x + 0.5, y_max * 0.85, "switch", color="red", fontsize=7, alpha=0.8)

        ax.set_title(f":{port} ({role})", fontweight="bold")
        ax.set_ylabel(f"latency (ms)")
        ax.set_xlabel("Time (seconds)")
        ax.legend(loc="upper right", fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0, top=y_max)

    # Legend for switchover line
    switch_patch = mpatches.Patch(color="red", alpha=0.8, label="container switchover")
    fig.legend(handles=[switch_patch], loc="lower center", ncol=1, fontsize=9)

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Graph saved to {output_file}")

# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 graph.py results.json switchovers.log")
        sys.exit(1)

    k6_path = sys.argv[1]
    sw_path = sys.argv[2]

    print(f"Parsing {k6_path} ...")
    k6_data = parse_k6(k6_path)
    total = sum(len(v) for v in k6_data.values())
    print(f"  {total} requests parsed across {len(k6_data)} ports")

    print(f"Parsing {sw_path} ...")
    switchovers = parse_switchovers(sw_path)
    print(f"  {len(switchovers)} switchover events found")

    print("Plotting reads...")
    plot(k6_data, switchovers, "read", "reads.png")

    print("Plotting writes...")
    plot(k6_data, switchovers, "write", "writes.png")
