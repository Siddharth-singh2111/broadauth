#!/usr/bin/env python3
"""Layer-2 static-vs-adaptive comparison (Roadmap Step 5 / F18).

The core experiment: sweep offered load and compare three slot-timing policies
under the F15 auth-channel model (auth throttled at -radio-bps, data unthrottled).
All three use identical probabilistic encoding — only the slot-duration policy
differs, isolating the adaptive controller as the single variable:

  fixed_fast : t-min = t-max = 1000 ms   (low latency, overflows early)
  fixed_slow : t-min = t-max = 8000 ms   (overflow-safe, high latency)
  adaptive   : t-min 1000 .. t-max 8000  (the proposed controller)

Records per (arm, load): authenticated messages, keys-never-disclosed,
empty-batches, peak disclosure-queue, broadcast-queue drops, peak D_i/B_i.
Thesis (Figures 3-4): fixed_fast's disclosure pipeline overflows as load rises
while adaptive holds the stable region much further out; fixed_slow is safe but
pays constant high latency.

No sudo (F15 needs no shaping). Requires anvil + cm; starts its own owner.

    python3 benchmarking/comparison_sweep.py      # from repo root
"""
import json
import os
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
    parse_sweep_log,
    start_owner,
)

# --- CONFIGURATION ---
OUT_DIR = "benchmarks/comparison"
FALLBACK_DIR = "results/comparison"
RESULTS_FILE = "comparison_results.json"
DURATION = 30
LOAD_HZ = [20, 70, 120, 170, 220]         # straddles the fixed/adaptive knees
ARMS = {
    "fixed_fast": ("1000", "1000"),
    "fixed_slow": ("8000", "8000"),
    "adaptive":   ("1000", "8000"),
}
RADIO_BPS = "250"                          # auth-channel budget (F15)
INGEST_CAP = "64"                          # from Step-4 calibration
RCD_STARTUP_WAIT = 8

METRIC_KEYS = [
    "avg_t_ms", "peak_di", "peak_bi", "avg_batch_size",
    "verified_batches", "empty_batches", "security_drops",
    "peak_ingest_q", "peak_disc_q", "keys_never_disclosed",
    "broadcast_queue_drops",
]


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


def main():
    global OUT_DIR
    OUT_DIR = resolve_output_dir()
    results_path = f"{OUT_DIR}/{RESULTS_FILE}"
    print(f"[*] Writing results to {OUT_DIR}/")
    kill_stale_processes()

    results = {arm: {} for arm in ARMS}
    owner_proc = start_owner()
    try:
        runs = [(arm, hz) for arm in ARMS for hz in LOAD_HZ]
        uuids = get_uuids_from_owner(owner_proc, len(runs), log_dir=OUT_DIR)
        if len(uuids) < len(runs):
            print("[!] Not enough UUIDs generated. Exiting.")
            sys.exit(1)

        for idx, (arm, hz) in enumerate(runs):
            t_min, t_max = ARMS[arm]
            uid = uuids[idx]
            print(f"\n[*] {arm} @ {hz} Hz (t-min={t_min} t-max={t_max}, UUID {uid})")
            log_file = f"{OUT_DIR}/{arm}_{hz}hz.log"
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
                        "-t-min", t_min,
                        "-t-max", t_max,
                        "-disclosure-delay", DISCLOSURE_DELAY,
                        "-traffic-hz", str(hz),
                        "-ingest-cap", INGEST_CAP,
                        "-radio-bps", RADIO_BPS,
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

            m = parse_sweep_log(log_file)
            row = {k: m[k] for k in METRIC_KEYS}
            results[arm][f"{hz}hz"] = row
            print(
                f"    auth={row['verified_batches']:.0f}  "
                f"keys_lost={row['keys_never_disclosed']:.0f}  "
                f"empty={row['empty_batches']:.0f}  "
                f"peak_discQ={row['peak_disc_q']:.0f}  "
                f"qDrops={row['broadcast_queue_drops']:.0f}"
            )

        with open(results_path, "w") as f:
            json.dump(results, f, indent=4)
        print(f"\n[*] Comparison sweep complete. Data saved to {results_path}")

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
