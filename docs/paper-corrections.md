# Paper ↔ Code corrections (Prob-Adaptive Inf-TESLA++)

Tracking the fixes identified from the audit of `paper_edit.pdf` against this
codebase. Code fixes are tagged `Fn` and reference `internal/rcd/rcd.go` unless
noted. "Paper" items cannot be changed here (PDF) and must be edited in the
manuscript source.

## Status

| Fix | What | Where | Status |
|-----|------|-------|--------|
| F1 | Weights set to queue-dominant `0.70/0.30` (matches Eq. 4) | `rcd.go` consts | ✅ done |
| F2 | `D_i` = ingest backlog (documented); paper Eq. 2 wording must change | `rcd.go` `calculateTimeCongestion` | ✅ code / ⬜ paper |
| F3 | Single `B_i` baseline (20 ms); removed divergent 50 ms path | `rcd.go` | ✅ done |
| F4 | `Q_cap` is a named constant; fixed the `//100` comment | `rcd.go` `ingestQueueCap` | ✅ done |
| F6 | EWMA latency so `B_i` can recover | `rcd.go` `observeBroadcastLatency` | ✅ done |
| F12 | Disclosure-queue overflow counter + `[DISCLOSURE-OVERFLOW]` log | `rcd.go` `Metrics.DisclosureDrops` | ✅ done |
| F8 | Split traffic generation and slot control into independent goroutines | `rcd.go` `trafficLoop`/`slotLoop` | ✅ done |
| F8.5 | Fix double slot-counter advance: `AdaptiveSlotSource.Ticker()` was called twice, so slot advanced at 2×T_i rate (halving effective disclosure delay). `disclosureWorker` now polls `GetSlot()`. | `rcd.go` `disclosureWorker` | ✅ done |
| Fix 1 | Honest verification logging: `[SUCCESS]` only when N>0, `[BATCH-EMPTY]` otherwise. Python parser sums actual N from `N messages authenticated`, separate `empty_batches` counter. Previously every empty BF unpack counted as a "verified batch" — masking that 0 messages had ever actually been authenticated. | `rcd.go`, `sweep_benchmark.py` | ✅ done |
| Fix 2 | Raise sweep `DISCLOSURE_DELAY` from 2 to 10. Cutoff = delay × T_min was 2s while broadcaster latency is ~2s+ per send, so every data message arrived past its security cutoff and was dropped. Long-term fix is F15 or capturing sched.Index at send time, but the test-config bump lets the existing architecture authenticate at all. | `sweep_benchmark.py` | ✅ done |
| Bug A | Receiver iterates ALL `unverifiedMsgs` against the BF instead of a ±1 slot window. Data msgs' `sched.Index` (capture time) can lag the flush slot by many slots, so the narrow window never matched. The BF determines membership by content; slot lookup was a wrong optimization. | `rcd.go` `handleMessage` (Probabilistic + ProbAdaptive) | ✅ done |
| Bug B | Capture `sched.Index` at **send time** (worker dequeue), not generation time. The throttled broadcaster injected ~9 s between r.broadcast and wire; that gap is now milliseconds, so the receiver's `[SECURITY] arrived too late` cutoff is no longer falsely triggered. | `rcd.go` `broadcastWorker` + `buildAdaptiveDataAtSendTime` | ✅ done |
| Bug C | Add `broadcastQueue` + dedicated worker. All four broadcast sites (data, HMAC, disclosure, deterministic-HMAC) enqueue and never block on the radio. `trafficLoop` now runs at full 50 Hz and `messageBuffer` accumulates → `D_i` finally has a real signal, so the toggle can actually fire when the system is overloaded. | `rcd.go` `broadcastWorker`, `enqueueBroadcast`, `r.broadcast`, `broadcastDeterministic`, `broadcastAdaptive`, `flushBatch`, `flushAdaptiveBatch`, `disclosureWorker` | ✅ done |
| - | `DISCLOSURE_DELAY` bumped to 30 as defense-in-depth alongside Bug-B fix (cutoff should no longer be load-bearing now that send-time slot capture works). | `sweep_benchmark.py` | ✅ done |
| F5 | `storeAdaptiveKey` signature notation | paper Eq. 1 | ⬜ paper |
| F20 | Fig. 6 shows 7 of 11 loss levels | paper figure | ⬜ paper |
| F22 | Theorem 1 bound direction (`T_min`, not `T_max`) | paper §V-A | ⬜ paper/theory |
| F26 | "Permanent doubling regime" vs sub-ceiling avg `T_i` | paper §VIII | ⬜ paper (after re-run) |
| F27 | Bloom "compression" framing at ~2 pkts/batch | paper §VII-F | ⬜ paper |
| F28 | Limitations belong up front as scope, not late retraction | paper §VIII | ⬜ paper |
| F29 | Mode is a static `-mode` flag, not dynamic `C=R/L` selection | paper §IV | ⬜ paper |

## Paper edits required (detail)

- **F2 — redefine `D_i`.** Eq. 2 calls `Q_len` "the number of pending disclosure
  events." The implementation (and the genuine backpressure signal) is the
  **ingest / pre-flush backlog of unauthenticated packets**, not the disclosure
  channel (which is drained every slot and effectively never fills). State
  `D_i = Q_len/Q_cap` where `Q_len` is the ingest backlog and `Q_cap` is the
  documented per-device authentication-backlog budget (`ingestQueueCap`).

- **F1 — weights.** Code now matches the paper's `W_disc=0.70, W_lat=0.30`. The
  *previously reported* numbers (e.g. `C_i≈0.83`) were produced by the inverted
  `0.30/0.70`; all figures/tables must be regenerated after re-running.

- **F3 — `B_i` baseline.** Use `L_base = 20 ms` everywhere (the 50 ms variant is
  removed from the code).

- **F5 — signature.** Eq. 1 lists 7 loosely-named args; the contract has 8:
  `storeAdaptiveKey(_rcd,_index,_key,_startTime,_endTime,_disclosurDelay,_tMin,_tMax)`.
  Align the manuscript notation.

- **F20 — Fig. 6.** Re-plot with all 11 loss levels (or state the subset).

- **F22 — Theorem 1.** The TESLA safe-packet test bounds the sender's progress
  with the **shortest** admissible slot (`T_min`), not `T_max`. Re-derive; the
  current direction looks permissive (unsafe). Tie the proof to the implemented
  receiver check once F9/F10 land.

- **F26 — §VIII narrative.** "Stays above 0.75 / permanently doubling" cannot
  coexist with avg `T_i≈3800 ms` that never reaches the 8000 ms ceiling under the
  multiplicative law. Resolve after the re-run; report instantaneous `C_i`, not
  just per-level averages.

- **F27 — Bloom filter.** At 1.8–2.7 packets/batch the filter is net overhead.
  Either raise occupancy (depends on F8 decoupling) or drop the "compression"
  framing.

- **F28 — limitations.** The throttled-loopback artifact (B_i pinned, curves flat
  w.r.t. loss) and the absence of a static-vs-adaptive comparison currently read
  as a late retraction of the headline. Move into scope up front.

- **F29 — mode selection.** The prototype fixes the mode via `-mode`; it does not
  implement the baseline's dynamic `C=R/L` switch. Say so.

## Remaining code work (not yet done)

- **F7** feedback sign · **F9** receiver reads `getAdaptiveKey` and enforces a
  real-time bound · **F10** on-chain tamper check · **F11** sender reads bounds
  from contract · **F13** control-law refinement.
- **F16** two independent nodes · **F18** static-vs-adaptive comparison · **F21**
  variance/CIs · **F24** adversary model.
- **F14** sparse feedback channel · **F15** realistic radio model · **F17**
  multi-node population · **F23** formal proof · **F25** implement or remove the
  "gradient-ascent tuner" contribution.

## Validation note

Tier-0 changes alter controller behavior (weights, recoverable `B_i`). The
results in `benchmarks/sweep/` and `plots/sweep/` are now stale. Re-run:

```bash
sudo python3 benchmarking/sweep_benchmark.py
venv/bin/python benchmarking/generate_sweep_charts.py
```
