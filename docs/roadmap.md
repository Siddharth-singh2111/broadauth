# BroadAuth — Project Roadmap to Publication

Single source of truth for finishing the Prob-Adaptive Inf-TESLA++ paper.
Companion document: [`paper-corrections.md`](paper-corrections.md) tracks the
individual issues (F-items and bug fixes) and their status.

**Working rules**
- One concern per step, one code region per step.
- A test gate after every step (see [Test discipline](#test-discipline)).
- Commit + push after each green step so any regression bisects to one concern.
- Go steps and Python-harness steps touch different files → they can't conflict.

---

## 1. Strategic framing

### 1a. The reframe: load-driven, not loss-driven
The controller (`C_i = W_disc·D_i + W_lat·B_i`) reacts to **local backpressure**
(queue pressure `D_i`, radio latency `B_i`). A one-way broadcast sender **cannot
observe packet loss** (no ACK/NACK), so packet loss was never a valid independent
variable — the old loss sweeps measured a non-relationship (flat / non-monotonic
curves).

**Change:** the experiment's independent variable becomes **offered load /
load-factor ρ = λ/μ** (arrival rate ÷ radio service rate). Packet loss is demoted
to a secondary, open-loop robustness axis.

The causal chain we actually measure:

```
offered load ↑ → queue pressure D_i ↑ → controller lengthens T_i
→ fewer disclosures/sec → overflow avoided → authentication preserved
```

### 1b. Scope decision: Path A
Finish as an **availability / congestion-control paper** for broadcast
authentication. Security rests on citing TESLA / Inf-TESLA++. The formal-security
work (receiver-side `T_max` enforcement, two-node, Theorem 1) is **Path B**,
deferred and stated as an honest limitation.

### 1c. Positioning vs Inf-TESLA++ (the prior work)
Inf-TESLA++'s toggle (`C = R/L`, fraction of empty slots) selects the
**cryptographic mode** (deterministic ↔ probabilistic) on a **fixed cadence**.
Ours selects the **slot duration** (cadence) on a graded closed loop. They are
**orthogonal and composable** — ours adds the axis they don't have. Full
comparison in [Section 4](#4-inf-tesla-comparison-the-papers-central-positioning).

---

## 2. Current state (done)

The mechanism works end-to-end: the toggle fires, authentication succeeds,
macOS network shaping works. What remains is experiment design + the comparison,
not plumbing.

- **Controller correctness:** weights queue-dominant (F1), single B_i baseline
  (F3), EWMA latency so B_i can recover (F6).
- **Concurrency / plumbing:** traffic/slot loop decoupled (F8), slot-counter
  double-advance fixed (F8.5), broadcast worker + priority queues
  (control vs data), and Bugs X/Y/Z/AA/BB/CC (queue starvation, disclosure
  ordering, cold-start chain pre-fetch, stale-process contamination, log typo,
  radio-bandwidth starvation).
- **Harness:** macOS `dnctl`/`pfctl` shaping, stale-process kill, honest
  auth-accounting parser (`RE_AUTHED` + `empty_batches`).
- **Step 0 cleanup:** removed the chat demo cluster (`cmd/chat`, `internal/tx`,
  `internal/rx`, `unix_epoch`), Counter scaffolding, and the orphaned
  `getUnverifiedMessages`.

---

## 3. The ordered steps

Legend: **[Go]** validated by a short single-RCD run (no sudo). **[Py]**
validated by a `sudo` sweep. ★ = decisive for the paper.

| # | Step | Files | How to test | Closes |
|---|---|---|---|---|
| 0 | **Dead-code cleanup** ✅ done | — | build + test | cleanup |
| 1 | **Surface outcome metrics** — overflow, keys-never-disclosed, disclosure-queue occupancy, authenticated-count, empty-batches into logs + JSON **[Go]** | `internal/rcd/rcd.go`, parser | 30 s run, grep new lines, check JSON keys | F2, F12 |
| 2 | **`-traffic-hz` flag** — the load knob (replace hardcoded 100 ms ticker) **[Go]** | `cmd/rcd/main.go`, `internal/rcd/rcd.go` | run 2 rates, confirm ingest cadence | reframe A1 |
| 3 | **Recalibrate `ingestQueueCap`** for D_i dynamic range (consider `-ingest-cap` flag) **[Go]** | `internal/rcd/rcd.go` | 2-point run, D_i spans 0.1→1.0 not pinned | F4 |
| 4 | **Load-sweep harness** — X-axis = traffic-hz / ρ, reuse dummynet + kill scaffolding **[Py]** | new `benchmarking/load_sweep.py` | JSON keyed by load | reframe A2 |
| 5 | ★ **Static-vs-adaptive comparison** — baseline = fixed-T probabilistic at **T_min (fast)** and **T_max (slow)**; adaptive = probadaptive. Produces the Inf-TESLA++ comparison ([Sec 4](#4-inf-tesla-comparison-the-papers-central-positioning)). **[Py]** | harness | multiple series; overflow/auth diverge | **F18** |
| 5b | *(optional, faithful baseline)* **Implement `C = R/L` mode selector** (empty-slot fraction → deterministic/probabilistic) for an as-published Inf-TESLA++ arm — currently only static `-mode` flags exist **[Go]** | `internal/rcd/rcd.go`, `cmd/rcd/main.go` | run, confirm mode switches on occupancy | faithful comparison |
| 6 | **Multi-iteration + variance/CIs** (ITERATIONS > 1, mean ± stddev) **[Py]** | harness | JSON has stddev | F21 |
| 7 | **Transient step-response** — step the load mid-run, plot D_i(t)/T_i(t)/queue(t) **[Py]** | harness + per-slot logs | time-series shows rise→settle | verifies F7 |
| 8 | **Bloom-compression metric + chart script** for load-on-X (+ the comparison plots) **[Py]** | `generate_sweep_charts.py`, parser | charts render | F27, part of F |
| 9 | *(conditional)* **Finer control law** if response curves too blocky from ×2/÷2 **[Go]** | `internal/rcd/rcd.go` `selectDuration` | smoother response curve | F13 |
| 10 | **Tuner: implement or delete the claim** — the "offline gradient-ascent tuner" doesn't exist; build it (calibrate cap/weights per device) or cut it from contributions/§VI | `internal/rcd/`, paper | — | F25 |
| 11 | **Paper rewrite** — load-driven thesis, new figures, fix Eq. 2, reconcile §VIII, notation, Fig-6 granularity, limitations up front | paper | — | F5/F20/F26/F28/F29 |

**Dependencies:** 1→2→3 sequential (Go, distinct functions). 4 needs 1–3.
5 needs 4. 5b independent (can slot before or after 5). 6–8 need 5. 9 conditional
on 5/7. 10–11 last.

---

## 4. Inf-TESLA++ comparison (the paper's central positioning)

This is the analysis layer produced by **Step 5** (plus optional Step 5b), not
extra experiments.

### 4a. The two mechanisms

| | Inf-TESLA++ `C = R/L` | Ours `C_i = W_disc·D_i + W_lat·B_i` |
|---|---|---|
| Controls | cryptographic *mode* (encoding) | slot *duration* (cadence) |
| Output | binary (det ↔ prob) | graded T_i ∈ [T_min, T_max] |
| Senses | channel occupancy (empty-slot fraction) | disclosure/queue pressure + radio latency |
| Loop | open-loop w.r.t. sender backlog | closed-loop feedback |
| Threshold | single static η | deadband 0.25 / 0.75, tunable weights |
| Prevents disclosure-queue overflow? | **No** (cadence fixed) | **Yes** (lengthens slots to drain) |
| Auth latency | fixed (`d × T_fixed`) | variable (rises under load) |

**They control different axes** — frame ours as an *added axis*, composable on
top of theirs, not a replacement.

### 4b. The trade-off frame
Inf-TESLA++'s fixed cadence forces one operating point with a built-in dilemma:
- **Fixed-fast slot** (~1 s): low auth latency, but disclosure pipeline
  **overflows** under load → permanent key loss.
- **Fixed-slow slot** (their 12 s beacon): overflow-safe, but auth latency
  ~24 s (`d=2`) → unusable for real-time.

The adaptive claim: **our controller rides the frontier** — fast-slot latency at
low load, slow-slot stability at high load. Every graph should tell this story,
so the baseline is the **fixed-T family** (run T_min and T_max), not one curve.

### 4c. Per-metric comparison (baseline = fixed-T probabilistic)

| Graphed metric | Inf-TESLA++ (fixed T) | Ours (adaptive T) | Demonstrates |
|---|---|---|---|
| Slot duration T_i vs load | flat | rises 1→8 s | adaptivity exists |
| D_i queue pressure vs load | rises → saturates → overflow | rises then **bounded** | loop caps backlog |
| B_i latency vs load | not sensed | sensed (saturates — needs F15) | richer 2-factor signal |
| Congestion signal | C=R/L binary → mode | C_i graded → cadence | different philosophy |
| ★ Overflow / keys-never-disclosed vs load | overflow at ρ_fixed → **key loss** | deferred to ρ_adaptive > ρ_fixed | **core result** |
| ★ Auth success rate vs load | collapses past ρ_fixed | maintained further | **core result #2** |
| Batch size / Bloom compression vs load | bounded by fixed T × rate | grows as slots lengthen | timing amplifies batching |
| Comm overhead per authenticated msg vs load | constant disclosure rate | fewer disclosures/sec | *mechanism* extending stability |
| Auth latency vs load | constant | **rises** | the honest trade-off |
| Stability region (max ρ before key loss) | ρ_fixed | ρ_adaptive > ρ_fixed | quantifies the gain |
| *(secondary)* Auth vs packet loss | degrades open-loop | degrades open-loop | neither reacts to loss (state honestly) |

The two ★ rows are the paper; the rest is supporting evidence or honest cost.

### 4d. Honest caveats
- Partly apples-to-oranges (mode vs cadence) — frame as an added axis.
- **Show the auth-latency cost** — adaptive buys stability by lengthening slots;
  present it dominating the *latency-vs-stability frontier*, don't hide it.
- B_i saturation and loss-being-open-loop must be stated.

---

## 5. The figure set

1. Load → D_i, T_i response *(Steps 1–3)*
2. Transient step response D_i(t)/T_i(t)/queue(t) *(Step 7)*
3. ★ Overflow / keys-lost vs load — fixed vs adaptive *(Step 5)*
4. ★ Auth success vs load — fixed vs adaptive *(Step 5)*
5. Stability region: ρ where each variant starts dropping keys *(Steps 5–6)*
6. Bloom compression vs load *(Step 8)*
7. *(secondary)* Auth success vs packet loss at fixed load

Figures 3 and 4 are the paper; the rest is support.

---

## Test discipline

- **Every step:** `go vet ./internal/rcd/` + `go build -race ./internal/rcd/` +
  `make` stay green before moving on.
- **Go steps (1–3, 5b, 9):** short single-RCD run, no sudo.
- **Python steps (4–8):** `sudo python3 benchmarking/<script>.py`, then analyze
  the JSON.
- Commit + push after each green step.

---

## 6. Path B — deferred (only for a security paper / stronger venue)

- **F9/F10/F11** — receiver reads on-chain `T_max`, enforces real-time bound.
- **F16/F17** — two-node / multi-node testbed.
- **F22/F23** — fix Theorem 1 direction (`T_min` vs `T_max`) + machine-checked proof.
- **F24** — adversary / DoS evaluation.
- **F15** — realistic radio model (also needed for a strong venue regardless of path).

---

## 7. Execution recommendation

Do **1 → 2 → 3** (Go, cheap to validate) to get the load knob working and D_i
responsive, then **4 → 5** to produce the two decisive figures and the
Inf-TESLA++ comparison. **Stop and evaluate after Step 5** — if fixed-vs-adaptive
diverges, the thesis holds and Steps 6–11 are polish; if not, rethink before
investing further. Run **5b** only if a reviewer would demand the literal
as-published `C = R/L` baseline.
