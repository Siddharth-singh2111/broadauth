#!/usr/bin/env python3
"""Charts for Layer-1 controller characterization (Roadmap Figure 1).

Reads controller_results.json and plots the steady-state slot duration T_i as a
function of the injected D_i, one line per B_i level. Shows the control law's
threshold (C_i > 0.75 → ramp to T_max) and its bang-bang / no-interior-
equilibrium behavior.

    python3 benchmarking/generate_controller_charts.py
"""
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

CANDIDATES = [
    "results/controller_sweep/controller_results.json",
    "benchmarks/controller_sweep/controller_results.json",
]
OUT_DIR = "plots/controller"


def load_data():
    for path in CANDIDATES:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f), path
    print("[!] No controller_results.json found. Run controller_sweep.py first.")
    sys.exit(1)


def main():
    data, path = load_data()
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"[*] Charting {path}")

    # Group by B_i level.
    by_bi = {}
    for v in data.values():
        by_bi.setdefault(v["b_i"], []).append((v["d_i"], v["steady_t_ms"]))

    plt.figure(figsize=(10, 6))
    for bi in sorted(by_bi):
        pts = sorted(by_bi[bi])
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        plt.plot(xs, ys, marker="o", linewidth=2, markersize=8, label=f"B_i = {bi}")

    plt.title("Controller Response: Steady-State Slot Duration vs D_i",
              fontweight="bold")
    plt.xlabel("Injected queue pressure D_i")
    plt.ylabel("Steady-state slot duration T_i (ms)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend(title="Injected latency")
    plt.savefig(f"{OUT_DIR}/controller_response.png", bbox_inches="tight", dpi=300)
    plt.close()

    print(f"[*] Charts written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
