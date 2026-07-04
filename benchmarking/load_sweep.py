#!/usr/bin/env python3
"""Load-driven sweep (Roadmap Step 4).

Sweeps the *offered load* (-traffic-hz) instead of packet loss. This is the
experiment's correct independent variable: the controller reacts to local
backpressure (D_i, B_i), not to loss, so load is what causally drives it.

No sudo / no network shaping — the load sweep varies offered load only; packet
loss is a separate secondary axis. Requires anvil + cm running and a deployed
contract; the script starts its own owner and rcd per load point.

Also the calibration vehicle for -ingest-cap (Step 3): the `peak_ingest_q`
column reports the raw pre-flush backlog per load, from which the right cap is
chosen so D_i spans ~0.1 -> 1.0 over the load range.

    python3 benchmarking/load_sweep.py      # from repo root, no sudo
"""
import json
import os
import subprocess
import sys
import time

# Reuse the shared plumbing (constants + owner/uuid/parse/kill helpers).
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
    parse_sweep_log,
    start_owner,
)

# --- CONFIGURATION ---
# Preferred output dir; falls back to a user-owned path if benchmarks/ is
# root-owned (it becomes root-owned after a sudo loss-sweep, and the load
# sweep runs without sudo).
LOAD_DIR = "benchmarks/load_sweep"
FALLBACK_DIR = "results/load_sweep"
RESULTS_FILE = f"{LOAD_DIR}/load_results.json"
DURATION = 45                 # per load point; enough to reach steady state
LOAD_HZ = [2, 5, 10, 20, 40, 80]  # offered-load sweep (messages/sec)
ITERATIONS = 1
T_MIN = "1000"
T_MAX = "8000"
# Calibration cap. Large enough that D_i does not clip, so `peak_ingest_q`
# reports the true backlog for every load point. The recommended production
# cap is then derived from that data.
INGEST_CAP = "1000"
# anvil is run with a short block time for this sweep, so the owner's
# storeAdaptiveKey tx confirms quickly and the rcd's synchronous chain
# pre-fetch (rcd.Start) returns fast.
RCD_STARTUP_WAIT = 8

METRIC_KEYS = [
    "avg_t_ms",
    "peak_di",
    "peak_bi",
    "avg_batch_size",
    "security_drops",
    "verified_batches",
    "empty_batches",
    "peak_ingest_q",
    "peak_disc_q",
    "peak_disc_q_frac",
    "keys_never_disclosed",
    "broadcast_queue_drops",
]


def _resolve_output_dir():
    """Return a writable output dir, preferring LOAD_DIR, else FALLBACK_DIR."""
    for d in (LOAD_DIR, FALLBACK_DIR):
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


def main():
    global LOAD_DIR, RESULTS_FILE
    LOAD_DIR = _resolve_output_dir()
    RESULTS_FILE = f"{LOAD_DIR}/load_results.json"
    print(f"[*] Writing results to {LOAD_DIR}/")
    kill_stale_processes()
    results = {}
    owner_proc = start_owner()

    try:
        total_runs = len(LOAD_HZ) * ITERATIONS
        uuids = get_uuids_from_owner(owner_proc, total_runs, log_dir=LOAD_DIR)
        if len(uuids) < total_runs:
            print("[!] Not enough UUIDs generated. Exiting.")
            sys.exit(1)

        uuid_index = 0
        for hz in LOAD_HZ:
            print(f"\n========================================")
            print(f"  PHASE: {hz} Hz offered load ({ITERATIONS} trials)")
            print(f"========================================")

            phase = {k: [] for k in METRIC_KEYS}

            for run in range(ITERATIONS):
                uid = uuids[uuid_index]
                uuid_index += 1
                print(f"  [*] Trial {run + 1}/{ITERATIONS} @ {hz} Hz (UUID: {uid})")

                log_file = f"{LOAD_DIR}/load_{hz}hz_run_{run + 1}.log"
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
                            "-traffic-hz", str(hz),
                            "-ingest-cap", INGEST_CAP,
                            "-bench",
                        ],
                        stdout=f,
                        stderr=subprocess.STDOUT,
                    )

                    # No network shaping to apply — just let it run.
                    time.sleep(RCD_STARTUP_WAIT + DURATION)

                    rcd_proc.terminate()
                    try:
                        rcd_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        rcd_proc.kill()
                        rcd_proc.wait()

                kill_stale_processes()

                metrics = parse_sweep_log(log_file)
                for k in METRIC_KEYS:
                    phase[k].append(metrics[k])

            averaged = {
                k: (sum(v) / len(v) if v else 0) for k, v in phase.items()
            }
            results[f"{hz}hz"] = averaged

            print(f"\n  [AGGREGATE @ {hz} Hz]")
            print(f"  -> Avg Slot Duration:        {averaged['avg_t_ms']:.0f} ms")
            print(f"  -> Peak Ingest Backlog:      {averaged['peak_ingest_q']:.1f} pkts")
            print(f"  -> Peak D_i:                 {averaged['peak_di']:.2f}")
            print(f"  -> Peak B_i:                 {averaged['peak_bi']:.2f}")
            print(f"  -> Avg Batch Size:           {averaged['avg_batch_size']:.1f}")
            print(f"  -> Authenticated Messages:   {averaged['verified_batches']:.1f}")
            print(f"  -> Empty Batches:            {averaged['empty_batches']:.1f}")
            print(f"  -> Keys Never Disclosed:     {averaged['keys_never_disclosed']:.1f}")
            print(f"  -> Broadcast-Queue Drops:    {averaged['broadcast_queue_drops']:.1f}")

        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=4)
        print(f"\n[*] Load sweep complete. Data saved to {RESULTS_FILE}")

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
