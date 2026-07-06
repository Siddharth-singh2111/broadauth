import subprocess
import time
import re
import os
import sys
import json
import threading
from typing import List, Dict

# --- CONFIGURATION ---
SWEEP_DIR = "benchmarks/sweep"
RESULTS_FILE = f"{SWEEP_DIR}/sweep_results.json"
DURATION = 100
LOSS_RATES = [0, 20, 40, 60]
ITERATIONS = 1

# Binaries & Shared Arguments
RCD_BIN = "./bin/rcd"
OWNER_BIN = "./bin/owner"
ETH_URL = "http://0.0.0.0:8545"
CONTRACT_ADDR = "0x5FbDB2315678afecb367f032d93F642f64180aa3"

OWNER_ADDR = "0.0.0.0:10102"
OWNER_PORT = "10102"
OWNER_PRIV_KEY = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
CM_ADDR = "0.0.0.0:10101"
HASHCHAIN_LEN = "1024"
DISCLOSURE_DELAY = "2"

# RCD pre-fetches its hashchain synchronously in Start() (see internal/rcd/rcd.go
# Start). With anvil's `-block-time 12` the owner's storeAdaptiveKey
# transaction takes up to ~12 s to mine before the chain TCP response returns.
# Wait long enough to cover that before applying network shaping, otherwise
# shaping would drop the chain-fetch packets.
RCD_STARTUP_WAIT = 15

# Regex Parsers
RE_DI = re.compile(r"D_i \(Queue\): ([\d\.]+)")
RE_BI = re.compile(r"B_i \(Latency\): ([\d\.]+)")
RE_TOGGLE = re.compile(r"Scaling T_i: \d+ms -> (\d+)ms")
RE_TI = re.compile(r"\| T_i: (\d+)ms")  # actual per-slot T_i on the METRICS line
RE_BATCH = re.compile(r"Batch of (\d+) packets")
RE_DROP = re.compile(r"\[SECURITY\] Dropped")
# Fix 1: count actual authenticated messages (the N in "N messages
# authenticated"), not verification events. The previous RE_SUCCESS regex
# ticked on every BF unpack — even when 0 messages matched — and reported a
# misleading "verified_batches" count. Bug-A fix in rcd.go now emits SUCCESS
# only when N > 0 and BATCH-EMPTY otherwise; we count both separately.
RE_AUTHED = re.compile(r"Prob-Adaptive Batch Verification: (\d+) messages authenticated")
RE_BATCH_EMPTY = re.compile(r"\[BATCH-EMPTY\] Prob-Adaptive Batch Verification")
# Step 1 (F2/F12): pipeline-health outcomes surfaced on the METRICS line.
RE_INGESTQ = re.compile(r"IngestQ: (\d+)")         # raw ingest backlog (D_i numerator)
RE_DISCQ = re.compile(r"DiscQ: (\d+)/(\d+)")       # disclosure-queue occupancy / cap
RE_KEYSLOST = re.compile(r"KeysLost: (\d+)")       # cumulative keys never disclosed
RE_QDROPS = re.compile(r"QDrops: (\d+)")           # cumulative broadcast-queue drops

# --- macOS network shaping (dnctl + pfctl / dummynet) ---
# Linux's tc/netem doesn't exist on Darwin. The broadcast-auth data plane is
# UDP on port 8888 (internal/broadcast/udp_broadcast.go DefaultUDPConfig); we
# shape only that, leaving eth RPC (8545) and the cm/owner control channels
# untouched.
DNCTL = "/usr/sbin/dnctl"
PFCTL = "/sbin/pfctl"
DN_PIPE = "1"
UDP_PORT = "8888"


def _run(cmd: str, check: bool = False, inp: str | None = None):
    return subprocess.run(
        cmd,
        shell=True,
        check=check,
        input=inp,
        text=True if inp is not None else None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def reset_network():
    """Remove any dummynet/pf shaping we installed and restore defaults."""
    _run(f"{DNCTL} -q flush")
    _run(f"{PFCTL} -f /etc/pf.conf")
    _run(f"{PFCTL} -d")


def kill_stale_processes():
    """Kill any leftover RCD / owner processes from a prior interrupted run.

    Without this, a stale RCD keeps broadcasting on UDP/8888 alongside the new
    sweep's RCD. They receive each other's data and HMACs but can't verify
    them (different UUID, different hashchain), so the log shows 'two RCDs
    starting' and authentication never succeeds. Anvil and cm are
    user-managed — never touch them.
    """
    _run("pkill -f 'BroadAuth/bin/rcd'")
    _run("pkill -f 'BroadAuth/bin/owner'")
    time.sleep(1)  # let the OS reap them and release UDP/8888


def setup_directories():
    if not os.path.exists(SWEEP_DIR):
        os.makedirs(SWEEP_DIR)
    reset_network()
    kill_stale_processes()


def start_owner() -> subprocess.Popen[str]:
    print(f"[*] Starting Owner Node on port {OWNER_PORT}...")
    cmd = [
        OWNER_BIN,
        "-eth-url",
        ETH_URL,
        "-contract",
        CONTRACT_ADDR,
        "-private-key",
        OWNER_PRIV_KEY,
        "-cm-addr",
        CM_ADDR,
        "-disclosure-delay",
        DISCLOSURE_DELAY,
        "-hashchain-len",
        HASHCHAIN_LEN,
        "-port",
        OWNER_PORT,
    ]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    time.sleep(2)
    if proc.poll() is not None:
        print("[!!!] Owner failed to start.")
        sys.exit(1)
    return proc


def get_uuids_from_owner(
    owner_proc: subprocess.Popen[str], count: int, log_dir: str = SWEEP_DIR
) -> List[str]:
    print(f"[*] Waiting for {count} UUIDs from Owner (Make sure CM is running!)...")
    uuids: List[str] = []
    # log_dir lets a caller (e.g. the sudo-free load sweep) redirect the owner
    # log to a user-writable directory instead of the root-owned SWEEP_DIR.
    owner_log_file = open(f"{log_dir}/owner.log", "w")

    start_time = time.time()
    # Increased timeout to allow generating 70 UUIDs safely
    while time.time() - start_time < 60:
        if owner_proc.stdout is None:
            break
        line = owner_proc.stdout.readline()
        if not line:
            break

        owner_log_file.write(line)
        owner_log_file.flush()
        line = line.strip()

        if re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", line
        ):
            uuids.append(line)
            if len(uuids) >= count:
                break

    def drain_owner_log() -> None:
        if owner_proc.stdout:
            for l in owner_proc.stdout:
                owner_log_file.write(l)
                owner_log_file.flush()
        owner_log_file.close()

    threading.Thread(target=drain_owner_log, daemon=True).start()

    if len(uuids) < count:
        print(f"[!] Warning: Only captured {len(uuids)} UUIDs. Expected {count}.")
    else:
        print(f"[*] Successfully captured {len(uuids)} UUIDs.")
    return uuids


def apply_network_chaos(loss: int):
    reset_network()
    if loss > 0:
        frac = loss / 100.0
        # Configure a dummynet pipe with the desired packet-loss rate.
        _run(f"{DNCTL} pipe {DN_PIPE} config plr {frac}", check=True)
        # Route only UDP/8888 (in & out, any interface) through the pipe.
        rules = (
            f"dummynet in quick proto udp from any to any port {UDP_PORT} pipe {DN_PIPE}\n"
            f"dummynet out quick proto udp from any to any port {UDP_PORT} pipe {DN_PIPE}\n"
        )
        _run(f"{PFCTL} -f -", check=True, inp=rules)
        _run(f"{PFCTL} -e")
        print(
            f"    [+] Applied Selective UDP Throttle: Pure {loss}% packet drop on port {UDP_PORT} (dummynet)"
        )
    else:
        print("    [+] Network is clean (0% drop)")


def parse_sweep_log(filepath: str) -> Dict:
    current_t = 1000
    t_history = []
    di_history = []
    bi_history = []
    batch_sizes = []
    drops = 0
    authenticated = 0  # total messages authenticated (sum of N across SUCCESS lines)
    empty_batches = 0  # BF unpacked but matched no buffered messages
    peak_ingest_q = 0  # peak raw ingest backlog = D_i numerator (Step 3/4 calibration)
    peak_disc_q = 0    # peak disclosure-queue occupancy (Step 1 / F2)
    disc_q_cap = 0     # disclosure-queue capacity (from the log, for the fraction)
    keys_never_disclosed = 0  # cumulative KeysLost (Step 1 / F12)
    broadcast_queue_drops = 0  # cumulative QDrops (Step 1 / F12)

    with open(filepath, "r") as f:
        for line in f:
            # Prefer the actual T_i logged on the METRICS line (robust for
            # no-toggle fixed arms); fall back to toggle reconstruction.
            ti_match = RE_TI.search(line)
            if ti_match:
                current_t = int(ti_match.group(1))

            di_match = RE_DI.search(line)
            if di_match:
                di_history.append(float(di_match.group(1)))
                t_history.append(current_t)

            bi_match = RE_BI.search(line)
            if bi_match:
                bi_history.append(float(bi_match.group(1)))

            toggle_match = RE_TOGGLE.search(line)
            if toggle_match:
                current_t = int(toggle_match.group(1))

            batch_match = RE_BATCH.search(line)
            if batch_match:
                batch_sizes.append(int(batch_match.group(1)))

            if RE_DROP.search(line):
                drops += 1

            authed_match = RE_AUTHED.search(line)
            if authed_match:
                authenticated += int(authed_match.group(1))

            if RE_BATCH_EMPTY.search(line):
                empty_batches += 1

            ingestq_match = RE_INGESTQ.search(line)
            if ingestq_match:
                peak_ingest_q = max(peak_ingest_q, int(ingestq_match.group(1)))

            discq_match = RE_DISCQ.search(line)
            if discq_match:
                peak_disc_q = max(peak_disc_q, int(discq_match.group(1)))
                disc_q_cap = int(discq_match.group(2))

            keyslost_match = RE_KEYSLOST.search(line)
            if keyslost_match:
                # cumulative in the log → max is the final total
                keys_never_disclosed = max(keys_never_disclosed, int(keyslost_match.group(1)))

            qdrops_match = RE_QDROPS.search(line)
            if qdrops_match:
                broadcast_queue_drops = max(broadcast_queue_drops, int(qdrops_match.group(1)))

    return {
        "avg_t_ms": sum(t_history) / len(t_history) if t_history else 1000,
        "peak_di": max(di_history) if di_history else 0.0,
        "peak_bi": max(bi_history) if bi_history else 0.0,
        "avg_batch_size": sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0,
        "security_drops": drops,
        # Key preserved for chart-script compatibility but holds total
        # authenticated messages now (Fix 1), not unpack events.
        "verified_batches": authenticated,
        "empty_batches": empty_batches,
        # Step 1 (F2/F12): pipeline-overflow outcomes.
        "peak_ingest_q": peak_ingest_q,
        "peak_disc_q": peak_disc_q,
        "peak_disc_q_frac": (peak_disc_q / disc_q_cap) if disc_q_cap else 0.0,
        "keys_never_disclosed": keys_never_disclosed,
        "broadcast_queue_drops": broadcast_queue_drops,
    }


def main():
    if os.geteuid() != 0:
        print(
            "[!] ERROR: This script modifies network interfaces and must be run as root (sudo)."
        )
        sys.exit(1)

    setup_directories()
    results = {}
    owner_proc = start_owner()

    try:
        total_runs = len(LOSS_RATES) * ITERATIONS
        uuids = get_uuids_from_owner(owner_proc, total_runs)

        if len(uuids) < total_runs:
            print("[!] Not enough UUIDs generated. Exiting.")
            sys.exit(1)

        uuid_index = 0

        for loss in LOSS_RATES:
            print(f"\n========================================")
            print(f"  PHASE: {loss}% PACKET LOSS ({ITERATIONS} Trials)")
            print(f"========================================")

            phase_metrics = {
                "avg_t_ms": [],
                "peak_di": [],
                "peak_bi": [],
                "avg_batch_size": [],
                "security_drops": [],
                "verified_batches": [],
                "empty_batches": [],
                "peak_ingest_q": [],
                "peak_disc_q": [],
                "peak_disc_q_frac": [],
                "keys_never_disclosed": [],
                "broadcast_queue_drops": [],
            }

            for run in range(ITERATIONS):
                uid = uuids[uuid_index]
                uuid_index += 1

                print(f"  [*] Trial {run + 1}/{ITERATIONS} (UUID: {uid})")
                reset_network()

                log_file = f"{SWEEP_DIR}/probadaptive_loss_{loss}_run_{run + 1}.log"

                with open(log_file, "w") as f:
                    rcd_proc = subprocess.Popen(
                        [
                            RCD_BIN,
                            "-contract",
                            CONTRACT_ADDR,
                            "-eth-url",
                            ETH_URL,
                            "-hashchain-len",
                            HASHCHAIN_LEN,
                            "-owner-addr",
                            OWNER_ADDR,
                            "-uuid",
                            uid,
                            "-mode",
                            "probadaptive",
                            "-t-min",
                            "1000",
                            "-t-max",
                            "8000",
                            "-disclosure-delay",
                            DISCLOSURE_DELAY,
                            "-bench",
                        ],
                        stdout=f,
                        stderr=subprocess.STDOUT,
                    )

                    # RCD pre-fetches hashchain synchronously in Start();
                    # anvil block mining takes ~12 s, so wait 15 s before
                    # applying shaping (so chain-fetch packets aren't
                    # caught by the dummynet pipe).
                    time.sleep(RCD_STARTUP_WAIT)
                    apply_network_chaos(loss)

                    try:
                        time.sleep(DURATION - RCD_STARTUP_WAIT)
                    except KeyboardInterrupt:
                        rcd_proc.terminate()
                        rcd_proc.wait(timeout=5)
                        raise

                    rcd_proc.terminate()
                    try:
                        rcd_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        rcd_proc.kill()
                        rcd_proc.wait()

                # Safety: belt-and-braces — confirm the RCD really is gone
                # before the next iteration starts (otherwise the next run's
                # RCD shares UDP/8888 with the previous one and the auth
                # pipeline silently breaks).
                kill_stale_processes()

                # Parse the individual run
                metrics = parse_sweep_log(log_file)
                for k in phase_metrics.keys():
                    phase_metrics[k].append(metrics[k])

            # Calculate mathematical averages for the phase
            averaged_metrics = {
                k: sum(v) / len(v) if len(v) > 0 else 0
                for k, v in phase_metrics.items()
            }
            results[f"{loss}%"] = averaged_metrics

            # Print aggregated phase summary
            print(f"\n  [AGGREGATE SUMMARY: {loss}% LOSS]")
            print(f"  -> Avg Slot Duration: {averaged_metrics['avg_t_ms']:.0f}ms")
            print(f"  -> Avg Peak Queue Pressure: {averaged_metrics['peak_di']:.2f}")
            print(f"  -> Avg Peak Latency: {averaged_metrics['peak_bi']:.2f}")
            print(
                f"  -> Avg Batch Size: {averaged_metrics['avg_batch_size']:.1f} packets"
            )
            print(
                f"  -> Avg Authenticated Messages: {averaged_metrics['verified_batches']:.1f}"
            )
            print(
                f"  -> Avg Empty Batches:           {averaged_metrics['empty_batches']:.1f}"
            )
            print(
                f"  -> Avg Late-Arrival Drops:      {averaged_metrics['security_drops']:.1f}"
            )
            print(
                f"  -> Avg Peak Disclosure-Q:       {averaged_metrics['peak_disc_q']:.1f}"
                f" ({averaged_metrics['peak_disc_q_frac'] * 100:.0f}% of cap)"
            )
            print(
                f"  -> Avg Keys Never Disclosed:    {averaged_metrics['keys_never_disclosed']:.1f}"
            )
            print(
                f"  -> Avg Broadcast-Queue Drops:   {averaged_metrics['broadcast_queue_drops']:.1f}"
            )

        reset_network()
        print("\n[*] Network restored to normal.")

        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=4)
        print(f"[*] Full sweep completed. Aggregated data saved to {RESULTS_FILE}")

    finally:
        print("[*] Stopping Owner Node...")
        owner_proc.terminate()
        try:
            owner_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            owner_proc.kill()
            owner_proc.wait()
        reset_network()
        kill_stale_processes()


if __name__ == "__main__":
    main()
