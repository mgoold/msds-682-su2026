# Extra Credit Submission

Three extra-credit items, each with its own reproducible evidence. **None of
this replaces the required Confluent work** — the base assignment's own complete
2,000-message-per-strategy comparison remains in `results/producer_benchmark.csv`
with its report in `report.md`.

| Item | Artifacts |
|---|---|
| +1 Deterministic local replay | `extra_credit/local_replay.py`, `tests/test_local_replay.py`, `evidence/xc_local_replay_report.json` |
| +1 AI-assisted engineering review | `extra_credit/AI_REVIEW.md`, `extra_credit/review_evidence.py`, `evidence/xc_review_evidence.json` |
| +1 Advanced evaluation & observability | `extra_credit/analyze_variability.py`, `results/producer_benchmark_run{1,2,3}.csv`, `results/xc_latency_by_run.png`, `evidence/xc_variability_report.json` |

Reproduce everything:

```bash
python -m pytest tests/ -q                       # 13 tests
python extra_credit/local_replay.py              # +1 replay
python extra_credit/review_evidence.py           # +1 review evidence
python extra_credit/analyze_variability.py \
    --inputs results/producer_benchmark_run1.csv \
             results/producer_benchmark_run2.csv \
             results/producer_benchmark_run3.csv # +1 observability
```

---

## +1 Deterministic local replay

A **credential-free** dry-run harness that reuses the assignment's exact event
contract (`TripEvent`, `make_trip_events`, `event_key`, `serialize_event`) and
the same produce / `poll(0)` / `flush` control flow, but sends every message to
an in-memory sink instead of Confluent Cloud.

### Proof of determinism

`logical_fingerprint()` computes a SHA-256 over the full serialized key+value
sequence. Two generations from seed 682 produce byte-identical streams:

```
fingerprint_run_1          d25a2900d850e1afd764685811ff71e09da2e77ebafa91232554f5c28594f92e
fingerprint_run_2          d25a2900d850e1afd764685811ff71e09da2e77ebafa91232554f5c28594f92e
same_seed_reproducible     true
fingerprint (seed 683)     45b45ba1391f21d77f3f61881238c15aa7ea63f8fe63495d16d61f255e4d368f
different_seed_differs     true
```

The second check matters: without it, a generator that ignored its seed entirely
would still "pass" a same-seed comparison. Six tests cover this
(`tests/test_local_replay.py`), including field-level equality so a hash
collision cannot produce a false pass, per-strategy control-flow counts, and a
check that one `trip_id` always lands on a single partition.

### ⚠️ These numbers are a harness check, not Kafka performance

The harness reports:

| Strategy | Delivered | poll calls | flush calls | Harness time |
|---|---:|---:|---:|---:|
| async | 2,000 | 2,000 | 4 | 0.0037 s |
| sync_style | 2,000 | 0 | 2,000 | 0.0037 s |

**The two strategies are indistinguishable locally — and that is the point.**
With no broker, no TLS handshake, and no network round trip, there is no latency
for batching to hide, so `flush()` costs essentially nothing and the ~163×
advantage async shows on real Confluent Cloud disappears entirely. This harness
validates *control flow and reproducibility*; it says nothing about performance,
and its timings must never be compared with `results/producer_benchmark.csv`.

The control-flow counts are the useful signal: async flushes **4** times (once
per 500-message batch) while sync_style flushes **2,000** times (once per
message) — exactly the structural difference the benchmark measures.

---

## +1 AI-assisted engineering review

Full write-up in **[`AI_REVIEW.md`](AI_REVIEW.md)**. Summary of the two decisions,
each backed by measurement rather than argument:

| | **Accepted** | **Rejected** |
|---|---|---|
| Suggestion | Cap delivery samples on `len(delivery_samples) < 10`, not on `delivered_count` | Pre-serialize events outside the batch timer to isolate delivery |
| Evidence | Boundary simulation: first draft retained **9**, second **11**, accepted gate **10**; shipped `DeliveryTracker` verified at **10** | Serialization measured at **0.88 µs/event** = **0.24 %** of the fastest async batch, vs **3.43 %** run-to-run variability |
| Deciding factor | Hard requirement violated in both directions | Bias ~14× smaller than the noise it competes with |

Both my own earlier drafts of the cap were wrong, in opposite directions — which
is precisely why the boundary was measured instead of reasoned about. The
rejection is quantitative: the change would correct a systematic error an order
of magnitude below the random variation already present in the measurement.

---

## +1 Advanced evaluation and observability

**Three additional independent comparisons**, each 2,000 messages per strategy
(12,000 messages total), each with a distinct `--run-id` and `--output` CSV so no
result overwrites another.

### Results

| Strategy | p50 batch latency | p95 batch latency | Mean throughput | Delivered | Failed | Remaining |
|---|---:|---:|---:|---:|---:|---:|
| **async** | 0.195 s | 1.499 s | 2,026.06 msg/s | 6,000 | 0 | 0 |
| **sync_style** | 39.98 s | 42.21 s | 12.44 msg/s | 6,000 | 0 | 0 |

Pooled async advantage: **162.9×**.

### Run-to-run variability

| Strategy | Per-run mean throughput | Spread | Std dev | Coefficient of variation |
|---|---|---:|---:|---:|
| async | 2080.58, 1947.75, 2049.86 | 132.83 | 69.54 | **3.43 %** |
| sync_style | 12.21, 12.59, 12.52 | 0.38 | 0.20 | **1.63 %** |

The coefficient of variation is the comparable statistic here, because the two
strategies differ by two orders of magnitude in absolute throughput.

### Explaining the noise

**1. Async p95 is 7.7× its p50 — driven entirely by connection warm-up.** Every
run's batch 1 pays TLS handshake, SASL authentication, and topic metadata fetch:

| Run | Batch 1 (warm-up) | Batches 2–4 (steady state) |
|---|---:|---|
| run1 | 346.57 msg/s | 2602.40, 2777.88, 2595.47 |
| run2 | 333.64 msg/s | 2411.27, 2480.33, 2565.76 |
| run3 | 339.50 msg/s | 2738.29, 2592.30, 2529.34 |

The warm-up penalty is strikingly **consistent** (333–347 msg/s, a 4 % band) —
it is a reproducible fixed cost, not random noise. Steady state runs ~7.5× faster.
Because each run contributes only four async batches, one warm-up batch per run
is enough to pull p95 far above p50. The p50 is the honest steady-state figure;
the p95 is dominated by that one-time cost. **This is why reporting a single mean
would be misleading** — the distribution is bimodal, not noisy around a center.

**2. Async is twice as variable as sync_style (3.43 % vs 1.63 %) — which is the
opposite of the naive expectation.** Sync_style is *more* stable because it is
latency-bound: each of its 500 sequential round trips averages out, so a batch
converges tightly on ~40 s. Async completes a batch in ~0.19 s, so the same
absolute jitter — a few tens of milliseconds of network or broker scheduling
variation — is a far larger *fraction* of a much smaller number. Fast
measurements are noisier measurements.

**3. A real connectivity failure occurred and is preserved as evidence.** During
the first attempt at run 2, the async producer hit four consecutive 30-second
SSL handshake timeouts against the bootstrap endpoint and never connected:

```
%4|FAIL|rdkafka#producer-1| Connection setup timed out in state SSL_HANDSHAKE
                            (after 30027ms in state SSL_HANDSHAKE)
```

All 2,000 async messages went undelivered (`remaining_after_flush` climbing
500 → 2,000, with a meaningless "16.5 msg/s" that reflects timeout duration
rather than delivery). The sync_style half of the same invocation then succeeded
normally, because `main()` constructs a separate `Producer` per strategy.

The benchmark's own validation caught this and refused to certify the run —
`Incomplete benchmark: expected 8 valid rows; wrote 8 rows with 4 invalid rows` —
so I re-ran it. The failed CSV is kept at
`evidence/xc_failed_run_connectivity.csv` **as evidence, not as one of the three
runs.** Two lessons: cloud benchmarks fail in ways unrelated to the code under
test, and a completion check (`batch_delivered == 500`, `batch_failed == 0`,
`remaining_after_flush == 0`) is what separates a measurement from a number.

**4. Uncontrolled variables remain.** These runs share one laptop, one network
path, one cluster, one region, and one time window of a single afternoon. They
quantify *short-term* variability only. They do not capture time-of-day effects,
cross-region latency, competing cluster tenants, or broker-side load — so the
±3.4 % figure is a floor on real-world variability, not a bound on it.

### Secret-free configuration snapshot

`evidence/xc_variability_report.json` embeds the connection snapshot
(topic, bootstrap host, security protocol, SASL mechanism, and `has_username` /
`has_password` booleans). The analyzer **asserts** that no credential key name
appears anywhere in the snapshot before writing, so the check is enforced rather
than assumed.

### Plot

`results/xc_latency_by_run.png` — per-batch latency for all three runs on a log
axis, solid/dashed/dotted per run and coloured per strategy. The log scale is
necessary: a linear axis cannot show 0.19 s and 40 s in one frame.
