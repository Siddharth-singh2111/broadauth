#!/usr/bin/env python3
"""Validate macOS dummynet packet-loss shaping on the broadcast-auth data path.

The sweep injects loss on UDP port 8888 via dnctl/pfctl (dummynet). Linux uses
tc/netem; this is the macOS port. Run this once (with sudo) before the long
sweep to confirm shaping actually drops packets on THIS machine:

    sudo python3 benchmarking/shape_selftest.py

It mirrors internal/broadcast: a listener on 0.0.0.0:8888 and a sender that
broadcasts to 255.255.255.255:8888, measuring delivery at 0% and 80% configured
loss. If the 80% case still delivers ~100%, dummynet is not catching the path
and the sweep results would be meaningless.
"""
import os
import socket
import subprocess
import sys
import time

DNCTL = "/usr/sbin/dnctl"
PFCTL = "/sbin/pfctl"
DN_PIPE = "1"
UDP_PORT = 8888
N = 400


def _run(cmd, check=False, inp=None):
    return subprocess.run(
        cmd, shell=True, check=check, input=inp,
        text=True if inp is not None else None,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def reset():
    _run(f"{DNCTL} -q flush")
    _run(f"{PFCTL} -f /etc/pf.conf")
    _run(f"{PFCTL} -d")


def shape(loss_pct):
    reset()
    if loss_pct > 0:
        _run(f"{DNCTL} pipe {DN_PIPE} config plr {loss_pct / 100.0}", check=True)
        rules = (
            f"dummynet in quick proto udp from any to any port {UDP_PORT} pipe {DN_PIPE}\n"
            f"dummynet out quick proto udp from any to any port {UDP_PORT} pipe {DN_PIPE}\n"
        )
        _run(f"{PFCTL} -f -", check=True, inp=rules)
        _run(f"{PFCTL} -e")


def measure():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    rx.bind(("0.0.0.0", UDP_PORT))
    rx.settimeout(0.05)

    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    received = 0
    for i in range(N):
        tx.sendto(f"pkt-{i}".encode(), ("255.255.255.255", UDP_PORT))
        time.sleep(0.002)
    time.sleep(0.2)
    while True:
        try:
            rx.recvfrom(2048)
            received += 1
        except socket.timeout:
            break
    rx.close()
    tx.close()
    return received


def main():
    if os.geteuid() != 0:
        print("[!] Must run as root: sudo python3 benchmarking/shape_selftest.py")
        sys.exit(1)
    try:
        for loss in (0, 80):
            shape(loss)
            time.sleep(0.3)
            got = measure()
            pct = 100.0 * got / N
            print(f"  configured loss={loss:>3}%  ->  delivered {got}/{N} ({pct:.0f}%)")
        print("\n[*] If the 80% row delivered far fewer than the 0% row, shaping works.")
    finally:
        reset()
        print("[*] Network restored.")


if __name__ == "__main__":
    main()
