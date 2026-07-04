#!/usr/bin/env python3
"""Layer-1 controller characterization (Roadmap §Two-layer).

Injects fixed congestion signals (-force-di / -force-bi) and records how the
adaptive controller's slot duration T_i responds. This maps the control law in
ISOLATION from load/radio dynamics, so it needs no realistic-radio fix (F15).

For each (D_i, B_i) cell it runs one RCD with those signals forced, then
extracts from the log:
  - final/steady-state T_i
  - convergence: slots until T_i stops changing
  - scaling regime: ramps-up / holds / ramps-down
  - C_i (should equal W_disc*D_i + W_lat*B_i)

No sudo / no shaping. Requires anvil + cm running; starts its own owner.

    python3 benchmarking/controller_sweep.py      # from repo root, no sudo

NOTE: this characterizes the control law only. Physical outcomes (overflow,
auth-success) are NOT meaningful under injected signals — see load_sweep.py
(Layer 2) for those.
"""
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep_benchmark import (  # noqa: E402
    CONTRACT_ADDR,
    ETH_URL,
    HASHCHAIN_LEN,
    OWNER_ADDR,
    RCD_BIN,
    DISCLOSURE_DELAY,
    get_uuids_from_owner,
    kill_stale_processes,
    start_owner,
)

# --- CONFIGURATION ---
OUT_DIR = "benchmarks/controller_sweep"
FALLBACK_DIR = "results/controller_sweep"
RESULTS_FILE = "controller_results.json"
DURATION = 20                       # short: T_i reaches steady state in a few slots
DI_GRID = [0.0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.0]
BI_GRID = [0.0, 1.0]                # radio-idle vs radio-saturated
T_MIN = "1000"
T_MAX = "8000"
RCD_STARTUP_WAIT = 8

# METRICS line: "... | C_i (Score): 0.51 | IngestQ: ..."; and TOGGLE-ACTION:
# "Scaling T_i: 1000ms -> 2000ms" gives the T_i trajectory.
RE_CI = re.compile(r"C_i \(Score\): ([\d.]+)")
RE_TOGGLE = re.compile(r"Scaling T_i: (\d+)ms -> (\d+)ms")


def resolve_output_dir():
    for d in (OUT_DIR, FALLBACK_DIR):
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".wtest")
            with open(probe, "w"):
                pass
            os.remove(probe)
            return d
        except (PermissionError, OSError):
            continue
    print("[!] No writable output directory found.")
    sys.exit(1)


def parse_controller_log(filepath, t_min):
    """Return the controller response for one injected (D_i, B_i) run."""
    ci = None
    t_start = t_min
    t_final = t_min
    toggles = 0
    first_toggle_slot = None
    trajectory = [t_min]

    with open(filepath, "r") as f:
        for line in f:
            m = RE_CI.search(line)
            if m:
                ci = float(m.group(1))
            tm = RE_TOGGLE.search(line)
            if tm:
                toggles += 1
                t_final = int(tm.group(2))
                trajectory.append(t_final)

    # Direction of the control response.
    if t_final > t_start:
        regime = "ramp-up"
    elif t_final < t_start:
        regime = "ramp-down"
    else:
        regime = "hold"

    return {
        "c_i": ci if ci is not None else 0.0,
        "steady_t_ms": t_final,
        "toggle_count": toggles,
        "regime": regime,
        "trajectory": trajectory,
    }


def main():
    global OUT_DIR
    OUT_DIR = resolve_output_dir()
    results_path = f"{OUT_DIR}/{RESULTS_FILE}"
    print(f"[*] Writing results to {OUT_DIR}/")
    kill_stale_processes()

    results = {}
    owner_proc = start_owner()
    try:
        cells = [(di, bi) for bi in BI_GRID for di in DI_GRID]
        uuids = get_uuids_from_owner(owner_proc, len(cells), log_dir=OUT_DIR)
        if len(uuids) < len(cells):
            print("[!] Not enough UUIDs generated. Exiting.")
            sys.exit(1)

        for idx, (di, bi) in enumerate(cells):
            uid = uuids[idx]
            print(f"\n[*] Cell D_i={di} B_i={bi} (UUID: {uid})")
            log_file = f"{OUT_DIR}/ctrl_di{di}_bi{bi}.log"
            with open(log_file, "w") as f:
                rcd_proc = subprocess.Popen(
                    [
                        RCD_BIN,
                        "-contract", CONTRACT_ADDR,
                        "-eth-url", ETH_URL,
                        "-hashchain-len", HASHCHAIN_LEN,
                        "-owner-addr", OWNER_ADDR,
                        "-uuid", uid,
                        "-mode", "probadaptive",
                        "-t-min", T_MIN,
                        "-t-max", T_MAX,
                        "-disclosure-delay", DISCLOSURE_DELAY,
                        "-force-di", str(di),
                        "-force-bi", str(bi),
                        "-bench",
                    ],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )
                time.sleep(RCD_STARTUP_WAIT + DURATION)
                rcd_proc.terminate()
                try:
                    rcd_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    rcd_proc.kill()
                    rcd_proc.wait()
            kill_stale_processes()

            r = parse_controller_log(log_file, int(T_MIN))
            results[f"di{di}_bi{bi}"] = {"d_i": di, "b_i": bi, **r}
            print(
                f"    C_i={r['c_i']:.2f}  steady_T={r['steady_t_ms']}ms  "
                f"toggles={r['toggle_count']}  regime={r['regime']}"
            )

        with open(results_path, "w") as f:
            json.dump(results, f, indent=4)
        print(f"\n[*] Controller sweep complete. Data saved to {results_path}")

    finally:
        print("[*] Stopping owner node...")
        owner_proc.terminate()
        try:
            owner_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            owner_proc.kill()
            owner_proc.wait()
        kill_stale_processes()


if __name__ == "__main__":
    main()
