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
# Fix 2 (now superseded by Bug-B fix in rcd.go but kept generous for safety):
# the throttled UDP broadcaster (internal/broadcast/udp_broadcast.go) takes
# ~len(msg)/50 seconds per send. With 3 message types contending for the same
# mutex, real round-trip latency was ~9 s. With the new broadcast worker queue
# AND sched.Index captured at send time (rcd.go: buildAdaptiveDataAtSendTime),
# the wall-time-to-receiver is now milliseconds, so a 30-slot cutoff at
# T_min=1000 ms gives 30 s of headroom — far beyond what should now be needed.
DISCLOSURE_DELAY = "30"

# Regex Parsers
RE_DI = re.compile(r"D_i \(Queue\): ([\d\.]+)")
RE_BI = re.compile(r"B_i \(Latency\): ([\d\.]+)")
RE_TOGGLE = re.compile(r"Scaling T_i: \d+ms -> (\d+)ms")
RE_BATCH = re.compile(r"Batch of (\d+) packets")
RE_DROP = re.compile(r"\[SECURITY\] Dropped")
# Fix 1: parse the actual N from "N messages authenticated" instead of
# counting verification *events*. The previous "verified_batches" counter
# would tick on every BF unpack even when 0 messages matched (every batch was
# in fact authenticating zero, masking a broken pipeline behind a "[SUCCESS]"
# log line). We now sum N across SUCCESS lines, and count BATCH-EMPTY events
# separately so empty batches stay visible.
RE_AUTHED = re.compile(r"Prob-Adaptive Batch Verification: (\d+) messages authenticated")
RE_BATCH_EMPTY = re.compile(r"\[BATCH-EMPTY\] Prob-Adaptive Batch Verification")

# --- macOS network shaping (dnctl + pfctl / dummynet) ---
# Linux `tc`/`netem` does not exist on macOS. We emulate packet loss with
# dummynet pipes attached via pf. The broadcast-auth data plane is UDP on
# port 8888 (see internal/broadcast/udp_broadcast.go); we shape only that so
# the eth RPC (8545) and cm/owner control channels stay untouched.
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


def setup_directories():
    if not os.path.exists(SWEEP_DIR):
        os.makedirs(SWEEP_DIR)
    reset_network()


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


def get_uuids_from_owner(owner_proc: subprocess.Popen[str], count: int) -> List[str]:
    print(f"[*] Waiting for {count} UUIDs from Owner (Make sure CM is running!)...")
    uuids: List[str] = []
    owner_log_file = open(f"{SWEEP_DIR}/owner.log", "w")

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

    with open(filepath, "r") as f:
        for line in f:
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

    return {
        "avg_t_ms": sum(t_history) / len(t_history) if t_history else 1000,
        "peak_di": max(di_history) if di_history else 0.0,
        "peak_bi": max(bi_history) if bi_history else 0.0,
        "avg_batch_size": sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0,
        "security_drops": drops,
        # NOTE: the JSON key stays "verified_batches" for chart-script
        # compatibility, but it now holds the total *authenticated messages*,
        # not verification events. See Fix 1.
        "verified_batches": authenticated,
        "empty_batches": empty_batches,
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

                    time.sleep(5)  # Let RCD fetch hashchain
                    apply_network_chaos(loss)

                    try:
                        time.sleep(DURATION - 5)
                    except KeyboardInterrupt:
                        rcd_proc.terminate()
                        raise

                    rcd_proc.terminate()
                    rcd_proc.wait()

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
                f"  -> Avg Authenticated Batches: {averaged_metrics['verified_batches']:.1f}"
            )
            print(
                f"  -> Avg Prevented Forgeries: {averaged_metrics['security_drops']:.1f}"
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
            owner_proc.wait(timeout=2)
        except:
            owner_proc.kill()
        reset_network()


if __name__ == "__main__":
    main()
