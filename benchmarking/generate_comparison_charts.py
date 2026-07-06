#!/usr/bin/env python3
"""Charts for the Layer-2 static-vs-adaptive comparison (Roadmap Step 8).

Reads comparison_results.json ({arm: {"<hz>hz": metrics}}) and renders the
load-axis figures: authentication success, keys-never-disclosed, disclosure-queue
pressure, and slot duration — one line per arm (fixed_fast / fixed_slow /
adaptive). These are Figures 3-4 of the paper (plus supporting plots).

    python3 benchmarking/generate_comparison_charts.py
"""
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Prefer the user-owned results dir; fall back to benchmarks/.
CANDIDATES = [
    "results/comparison/comparison_results.json",
    "benchmarks/comparison/comparison_results.json",
]
OUT_DIR = "plots/comparison"

ARM_STYLE = {
    "fixed_fast": ("#d62728", "o", "Fixed (T=1s)"),
    "fixed_slow": ("#1f77b4", "s", "Fixed (T=8s)"),
    "adaptive":   ("#2ca02c", "D", "Adaptive"),
}


def load_data():
    for path in CANDIDATES:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f), path
    print("[!] No comparison_results.json found. Run comparison_sweep.py first.")
    sys.exit(1)


def loads_of(arm_data):
    return sorted(int(k.replace("hz", "")) for k in arm_data)


def series(arm_data, metric):
    xs = loads_of(arm_data)
    ys = [arm_data[f"{x}hz"][metric] for x in xs]
    return xs, ys


def plot_metric(data, metric, title, ylabel, fname, warnline=None):
    plt.figure(figsize=(10, 6))
    for arm, (color, marker, label) in ARM_STYLE.items():
        if arm not in data:
            continue
        xs, ys = series(data[arm], metric)
        plt.plot(xs, ys, marker=marker, color=color, linewidth=2,
                 markersize=8, label=label)
    if warnline is not None:
        plt.axhline(y=warnline, color="gray", linestyle="--", alpha=0.6,
                    label="overflow / loss onset")
    plt.title(title, fontweight="bold")
    plt.xlabel("Offered load λ (messages/sec)")
    plt.ylabel(ylabel)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    plt.savefig(f"{OUT_DIR}/{fname}", bbox_inches="tight", dpi=300)
    plt.close()


def main():
    data, path = load_data()
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"[*] Charting {path}")

    # ★ Figure 4 — the headline availability result.
    plot_metric(data, "verified_batches",
                "Authentication Success vs Offered Load",
                "Authenticated messages", "auth_vs_load.png")
    # ★ Figure 3 — the failure mode the adaptive controller prevents.
    plot_metric(data, "keys_never_disclosed",
                "Irrecoverable Key Loss vs Offered Load",
                "Keys never disclosed", "keys_lost_vs_load.png", warnline=1)
    # Supporting: pipeline pressure and drops.
    plot_metric(data, "peak_disc_q",
                "Peak Disclosure-Queue Occupancy vs Offered Load",
                "Peak disclosure queue (entries)", "disc_queue_vs_load.png")
    plot_metric(data, "broadcast_queue_drops",
                "Broadcast-Queue Drops vs Offered Load",
                "Dropped broadcasts", "queue_drops_vs_load.png")
    # Slot-duration response (adaptive ramps; fixed arms flat).
    plot_metric(data, "avg_t_ms",
                "Slot Duration vs Offered Load",
                "Avg slot duration T_i (ms)", "slot_duration_vs_load.png")

    print(f"[*] Charts written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
