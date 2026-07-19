# Extra Credit +1: AI-Assisted Engineering Review

This goes beyond the required disclosure in `AI_USAGE.md`. It documents a
substantive engineering review of my submitted code, with **one suggestion
accepted and one rejected, each supported by measured evidence**.

Every number below is reproducible:

```bash
python extra_credit/review_evidence.py       # writes evidence/xc_review_evidence.json
python -m pytest tests/ -q                   # 13 tests, all passing
```

---

## 1. Strategic timing: when I asked for the review, and why then

I ran the review at a specific point: **after all 11 code blocks were
implemented and `pytest` passed, but before I generated the final Confluent
evidence.**

That timing was deliberate.

- **Earlier would have been premature.** A review of half-written code produces
  suggestions about code that is about to change. I would have spent effort
  evaluating advice that no longer applied.
- **Later would have been wasted.** Once the 2,000-message-per-strategy runs are
  done and the report is written, accepting any suggestion that touches
  measurement semantics means re-running the whole benchmark. Reviewing *before*
  the expensive runs meant a rejected suggestion cost me nothing and an accepted
  one could be validated cheaply.
- **A passing test suite gave me a safety net.** With 7 tests green, I could
  evaluate any proposed change by applying it and re-running — a concrete,
  non-AI check rather than an opinion.

**Scope I asked for:** correctness of the delivery-tracking bookkeeping, and
whether the benchmark measures what it claims to measure. I deliberately did
*not* ask for general style feedback, because style suggestions are unfalsifiable
and would have diluted the review.

---

## 2. Suggestion ACCEPTED — delivery-sample cap gates on the wrong quantity

### The suggestion

> `DeliveryTracker.callback` caps retained evidence samples using the cumulative
> `delivered_count`. The requirement is "retain at most 10 samples." Gate on the
> length of the sample list instead: `if len(self.delivery_samples) < 10`.

### Why this is a real defect, not a style preference

`delivered_count` and `len(delivery_samples)` are different quantities that only
coincide before the cap engages. Using the counter makes the boundary
off-by-one, and the direction of the error is not obvious by inspection — which
is exactly why I measured it instead of reasoning about it.

### Evidence (`evidence/xc_review_evidence.json`)

Simulating 25 deliveries through each candidate gate:

| Gate condition | Samples retained | Meets "at most 10"? |
|---|---:|---|
| `delivered_count < 10` *(my first draft)* | **9** | ✗ retains too few |
| `len(delivery_samples) <= 10` *(my second draft)* | **11** | ✗ exceeds the cap |
| `len(delivery_samples) < 10` *(accepted)* | **10** | ✓ |
| **Shipped `DeliveryTracker`** | **10** | ✓ |

Both of my own drafts were wrong, in opposite directions. The evidence script
also drives the *actual submitted* `DeliveryTracker`, confirming the shipped
code retains exactly 10 — so this table verifies the code I am submitting, not
just a reimplementation of it.

### Why I accepted it

The requirement is a hard bound, the fix is one character, and the measurement
is unambiguous. There is no trade-off to weigh: 9 and 11 both violate the spec
and 10 satisfies it.

### How I verified it independently of the AI

I did not take the claim on trust. I wrote `cap_variant()` to reimplement all
three gates and count retention directly, then separately drove the real
`DeliveryTracker` to confirm the shipped behavior matches. The script **fails
loudly** (`SystemExit`) if the accepted fix does not retain exactly 10, or if
either rejected draft unexpectedly passes — so the evidence cannot silently rot.

---

## 3. Suggestion REJECTED — pre-serialize events outside the batch timer

### The suggestion

> `run_strategy` calls `event_key()` and `serialize_event()` *inside* the timed
> region, so each batch measures serialization **plus** delivery. Pre-serialize
> all events into `(key, value)` byte pairs before starting the timer, so the
> benchmark isolates delivery latency.

### Why it is superficially attractive

The reasoning is sound in principle. The benchmark's stated purpose is comparing
*delivery* strategies, and any CPU work inside the timer is measurement noise
attributable to neither strategy. If serialization were expensive, this would
bias the fast strategy (async) more than the slow one, because it is a fixed
cost against a much smaller denominator.

### Evidence (`evidence/xc_review_evidence.json`)

I measured serialization for a full 500-event batch, 30 repetitions:

| Metric | Value |
|---|---|
| Median per 500-message batch | **0.00044 s** (0.44 ms) |
| Per event | **0.88 µs** |
| Share of my fastest **async** batch (0.184 s) | **0.24 %** |
| Share of my fastest **sync_style** batch (38.68 s) | **0.0011 %** |

### Why I rejected it

1. **The bias is below the noise floor — and I measured both sides.** The
   serialization bias is **0.24 %** of the fastest async batch. Across three
   independent 2,000-message runs, async throughput varied with a coefficient of
   variation of **3.43 %** (`evidence/xc_variability_report.json`). The bias is
   therefore roughly **14× smaller than the run-to-run noise it would be
   competing with**. Removing a systematic error an order of magnitude below
   random variation does not make the conclusion more trustworthy — it only makes
   the code more complex.
2. **It cannot affect the finding.** The async-vs-sync gap is roughly 200×. A
   0.24 % correction to the *faster* side moves that ratio by a fraction of a
   percent. No conclusion in my report depends on it.
3. **It would diverge from the reference control flow.** The assignment asks me
   to reproduce the Demo 02 produce/poll/flush pattern, where serialization
   happens per message inside the loop. Hoisting it would make my benchmark
   measure a control flow that differs from the one the demos teach and that
   Demo 02D explicitly foregrounds.
4. **It would cost memory at scale.** Pre-serializing 2,000 events holds every
   payload in memory simultaneously. Harmless here, but it trades a real
   resource for an unmeasurable gain — the wrong direction for a pattern meant
   to generalize.

**The deciding question was "is the effect larger than the noise?" — and the
answer was measured, not argued.** Had serialization been, say, 15 % of an async
batch, I would have accepted the change.

---

## 4. Independent accuracy check

For each suggestion I used a check that does not depend on the AI:

| Claim | How I checked it without the AI |
|---|---|
| The cap is off by one | Reimplemented all three gates, counted retention, and drove the real `DeliveryTracker` |
| Serialization biases the timer | Timed it directly, 30 repetitions, and compared against my own observed batch latencies |
| The fix does not break anything | Re-ran the full suite: 13 tests pass (7 base + 6 replay) |
| The benchmark still satisfies the contract | Every batch row still shows 500 delivered, 0 failed, 0 remaining |

I also cross-checked one AI claim against my own data. During the session the
assistant predicted async batch 1 would be slower due to connection warm-up. My
CSV confirms this independently: **305.2 msg/s for batch 1 versus 2624.93,
2715.16, and 2649.55** for batches 2–4. A prediction that survives contact with
my own measurements is worth more than one I simply accepted.

---

## 5. Recovery action actually taken

The review was not failure-free, and neither was the wider session.

**Failure 1 — an incomplete conclusion.** The assistant told me there was no
`.env.example` in my folder and that I should create `.env` from scratch. That
contradicted what I knew: I had already configured Confluent for the demos.

- **Warning sign:** advice that conflicted with facts about my own setup.
- **Recovery:** I widened the search explicitly — *"look in the larger repo
  outside this folder and see if I have the .env and credentials I need"* —
  which surfaced both `handouts/.env.example` and a populated
  `msds682-demos/.env`. Following the original advice would have had me re-enter
  credentials needlessly.

**Failure 2 — a technically-passing but misleading chart.** The first `plot_rows`
produced fractional x-ticks (batch "1.5" is meaningless) and a linear y-axis that
crushed the sync_style line flat against zero, visually implying it delivered
nothing when it had delivered all 2,000 messages.

- **Warning sign:** I looked at the rendered PNG and it **contradicted my own
  CSV**, which showed sync_style delivering 500 messages per batch at ~12.7
  msg/s.
- **Recovery:** switched to a logarithmic y-axis and derived integer ticks from
  the data, then **re-rendered and re-inspected** rather than assuming the fix
  worked. Both series are now legible and the ~205× gap is visible as a
  vertical offset.

**Stop condition I set in advance.** I would stop using AI on a block and fall
back if any of these occurred: (a) two consecutive suggestions that failed the
test suite, (b) advice contradicting the starter source or the assignment spec,
or (c) explanations that repeated without adding detail. My non-AI fallbacks
were the four worked Demo 02 scripts, the assertions in
`tests/test_producer_logic.py` — which pin the required `poll`/`flush` counts
exactly — and the `confluent-kafka`, Pydantic, and matplotlib documentation. The
`FakeProducer` test double was the single most useful non-AI reference, because
its `poll()` and `flush()` implementations show precisely what those calls do to
the pending-callback queue.

---

## 6. Summary

| | Accepted | Rejected |
|---|---|---|
| **Suggestion** | Gate sample cap on list length | Pre-serialize outside the batch timer |
| **Evidence type** | Boundary test (9 / 11 / 10 retained) | Timing measurement (0.24 % of batch) |
| **Deciding factor** | Hard requirement violated in both directions | Effect an order of magnitude below noise |
| **Verified by** | `review_evidence.py` + 13 passing tests | 30-repetition timing vs. observed latencies |

The pattern I applied to both: **turn the suggestion into a measurable question,
measure it, and let the number decide.** The accepted change was worth one
character; the rejected one would have added complexity and memory cost to
correct a bias I could not detect.
