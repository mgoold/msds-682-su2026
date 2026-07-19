# Assignment 1 AI Assistance Disclosure

## 1. Tool, task, and decision to use AI

- Tool/model: Claude Code (Claude Opus 4.8), run locally in VS Code with access
  to this repository.
- Date used: 2026-07-19
- Assignment task it assisted with: 
  (a) planning the order in which to implement
  the six starter files, 
  (b) step-by-step guidance while I completed the eleven
  `CODE START HERE` blocks, 
  (c) code review of each block I wrote, 
  (d) writing the two `analyze_results.py` blocks after I asked for them directly.  I couldn't for love or money imagine what the loop structure should be.
  (e) two chart readability fixes, and 
  (f) drafting this report and disclosure.

- What I already understood or knew how to do before using AI: 
  * the Kafkaproducer concepts from Demo 02 — topics, key/value bytes, delivery callbacks,
  the difference between `poll(0)` and `flush()`, and why sync-style flushing is
  slow. I had the four worked Demo 02 scripts as references and understood what
  each producer strategy was supposed to do. I was comfortable with general
  Python, but rusty on specific library idioms I do not use often, particularly
  `csv.DictReader` handling and matplotlib configuration.
  * Why AI was appropriate and high-leverage at this point: the conceptual content
  was already covered by the demos; what slowed me down was mapping the demo
  code onto a differently-structured starter (different attribute names,
  batching, a single `run_strategy` instead of two functions) and recalling
  library syntax. Using AI for orientation and review let me spend my time on
  the parts that carry the learning — the producer control flow and the
  benchmark logic — rather than on lookup.
  * How I would approach the task without AI: 
    * Honestly, I'm not sure how long it would have taken me to do without AI.  The hangup for me personally isn't kafka; it's remembering all the ticky-tack python items like reading a file and writing it to rows.  Some things like the use of pydantic and matplotlib are more recent from practicums, but some small things are a year out of practice.  So you end up using AI b/c it's more efficient than searching online &c.

## 2. Prompt, request, and context

I asked the assistant to examine `handouts/assignment1_mgoold2/src` and tell me
the order the files needed to be worked on, then to write a `pipeline_overview.md`
tutorial explaining how the pipeline fits together. I then asked it to guide me
through completing the to-be-completed code blocks, with an explicit constraint:
**"Don't write code for me, just take me step by step through what to do, one
step at a time."** I later relaxed that constraint for specific blocks, asking
"show me the logic to do this" for the benchmark batching, and "show me the code
for these things — I understand the content, but I don't remember python details
like these" for the matplotlib block.

Context I provided: the repository itself (the starter `src/` files, the
`tests/`, `README.md`, `requirements.txt`, and the `handouts/demo02*` reference
scripts). For each block I pasted my own draft implementation back and asked
"does this look correct?" I did not provide credentials; the assistant read
`.env` key *names* and confirmed values were non-empty, but the API key and
secret values were never displayed or shared.

## 3. AI output and accuracy review

Concrete assistance provided:

- A dependency-ordered build plan (`producer_common.py` first, then sync, async,
  serialization, compare, analyze) with the reasoning for that order.
- Per-block guidance naming the exact demo lines to model each block on, and
  flagging where the starter deliberately differs from the demo.
- Code review of each block I wrote, which found several real bugs (listed in
  section 4).
- Full implementations of `load_and_validate_rows` and `plot_rows` after I asked
  for them, with a line-by-line explanation of the Python idioms.
- Two chart enhancements: integer x-ticks and a logarithmic y-axis.
- Environment setup (venv, dependencies) and running the benchmark.

Claims or suggestions I independently checked:

- That the starter's `DeliveryTracker` uses different attribute names than the
  demo's (`delivered_count` / `failed_messages` / `delivery_samples` versus
  `delivered` / `failed`).
- That `run_strategy` must branch internally rather than being split into two
  functions like the demo.
- That `batch_index` needed to be 1-based.
- That async would substantially outperform sync-style.

How I checked their accuracy:

- I read the starter source directly. The `failed_count` property at
  `producer_common.py` line 54 does `len(self.failed_messages)`, which confirms
  the required attribute name independently of anything the AI said.
- I read the already-written `main()` in `producer_compare.py`, which loops
  `for strategy in ("async", "sync_style")` and calls `run_strategy` once per
  strategy — confirming the single-function design is forced by code I am not
  allowed to modify.
- I read `tests/test_producer_logic.py`, which builds rows with
  `range(1, 5)`, confirming 1-based indexing, and asserts `flush_calls == 4` for
  sync versus `flush_calls == 1` and `poll_calls == 4` for async.
- I ran `python -m pytest -q` (7 passed) and then the real Confluent runs.

Inaccurate, unsupported, or uncertain parts I found:

- The assistant initially told me there was no `.env.example` in the assignment
  folder and that I would "need to create `.env` from scratch." That was an
  incomplete conclusion drawn from looking only at the assignment folder. When I
  asked it to search the wider repository, it found both
  `handouts/.env.example` and a fully populated `msds682-demos/.env` that I had
  already configured. The original advice would have had me re-enter
  credentials unnecessarily.
- The first version of `plot_rows` it wrote produced a technically-passing but
  poor chart: matplotlib auto-scaled the x-axis to fractional batch numbers
  (1.0, 1.5, 2.0) which are meaningless, and the linear y-axis compressed the
  sync-style line flat against zero, visually implying sync delivered nothing
  when it had in fact delivered all 2,000 messages.
- The `load_and_validate_rows` spec was genuinely ambiguous about what
  "sequential" rows means; the assistant flagged this as my decision rather than
  asserting one reading, and I chose the stricter check.

## 4. My decisions, changes, and understanding

Suggestions I accepted:

- The build order, and the guidance to complete `producer_common.py` fully
  before touching the four programs.
- The `DeliveryTracker` attribute names and the ten-sample cap.
- The `run_strategy` structure: one batch loop, snapshot counters before each
  batch, branch on strategy inside the loop, append one row per batch.
- The two chart fixes (integer ticks, log scale).

Why I accepted them: in each case I verified the claim against the starter
source or the test suite rather than taking it on trust, as described in
section 3. The `run_strategy` structure in particular is not a matter of taste —
the already-written `main()` makes it the only design that works.

Suggestions I rejected or changed:

- I did **not** follow the demo's two-function structure (`run_async` and
  `run_sync_style` as separate functions) even though I initially proposed it,
  once I confirmed from `main()` that the starter requires a single branching
  function.
- For the "sequential rows" ambiguity I chose the stricter check — verifying
  `batch_index` runs 1..n with no gaps — rather than the simpler count-only
  check the assistant recommended starting with, because the TODO explicitly
  says "sequential."
- I rejected the suggestion to copy `msds682-demos/.env` into the assignment
  folder, and used a symlink instead so the credential lives in exactly one
  place and cannot drift out of sync.

Work I completed myself after reviewing the output:

I wrote nine of the eleven blocks myself (`TripEvent`, `DeliveryTracker`,
`load_producer_config`, `make_trip_event`, `event_key`, `serialize_event`,
`run_sync_style`, `run_async`, `run_serialization_demo`, and `run_strategy`),
using the demo files as my reference and the AI for review. Several bugs in my
drafts were caught in that review and I fixed each one myself:

- `__init__` created `self.failed` while `callback` and the `failed_count`
  property both used `self.failed_messages` — an `AttributeError` at runtime.
- My sample cap `if self.delivered_count < 10` stored only 9 samples; my next
  attempt `if len(self.delivery_samples) <= 10` stored 11. The correct guard is
  `< 10` on the list length.
- I called `load_dotenv_for_demo()`, which does not exist in the starter
  (`load_dotenv_for_assignment()` does).
- My first `run_sync_style` omitted the `for event in events:` header entirely,
  so `event` was undefined.
- My first `run_strategy` used the demo's `TOPIC_NAME` constant (not defined in
  this file), iterated the full `events` list instead of `batch_events` in the
  sync branch, returned from inside the batch loop so only one row was produced,
  and computed elapsed time as `batch_start - start` with a redundant second
  timer, which was always negative.

The assistant wrote `load_and_validate_rows` and `plot_rows` at my request, and
applied the two chart fixes. I reviewed both, chose the stricter sequential
check, and confirmed the behavior through the test suite and by inspecting the
rendered PNG.

* How I know I can explain the submitted code and conclusions without relying on
the AI response: I can trace any message end to end — `make_trip_event` produces
a validated `TripEvent`, `event_key` and `serialize_event` turn it into key and
value bytes, `produce()` queues it, `poll(0)` or `flush()` drains the queue and
triggers the callback, and the callback increments the counters that become the
CSV row and the plotted point. I can explain why sync-style is ~205× slower
here, why async batch 1 is slower than batches 2–4, why `batch_delivered` must
be a per-batch delta rather than a running total, and why the final `flush()` is
mandatory. Each of those explanations is one I verified against my own data, not
one I am repeating.
* The main reason though is that I use the time I save from writing these assignments by hand to go and review the actual code and concepts, and ask more questions of Claude beyond those required.

## 5. Failure recovery and fallback

- Did AI give an incorrect answer, repeat itself, or fail to solve the problem?
  Yes, twice, as described in section 3: the incomplete `.env` conclusion, and
  the first `plot_rows` chart that was technically correct but misleading to
  read.
- What warning sign told me to stop or change course? For the `.env` issue, the
  advice conflicted with what I knew about my own setup — I had already
  configured Confluent for the demos, so being told to start from scratch did
  not match reality. For the chart, I looked at the rendered PNG and saw a red
  line lying flat on the axis, which contradicted my CSV, where sync-style
  clearly delivered 500 messages per batch at ~12.7 messages per second.
- How I changed the prompt, supplied better context, narrowed the task, or used
  a test/documentation/debugging aid: for the credentials I widened the search
  explicitly — "look in the larger repo outside this folder and see if I have
  the .env and credentials I need" — which surfaced the existing file. For the
  chart I had the fixes applied and then re-rendered and re-inspected the image
  rather than trusting that it was correct. Throughout, `pytest` was my primary
  non-AI check: it exercises every block against a `FakeProducer` with no
  credentials, so it catches control-flow errors independently.
- If AI still could not solve it, what non-AI method I used: my fallbacks were
  the four Demo 02 scripts (which are worked, correct references for every
  producer pattern), the assertions in `tests/test_producer_logic.py` (which
  specify the required `poll`/`flush` counts exactly), and the library
  documentation. The `FakeProducer` in the test file was especially useful — its
  `poll()` and `flush()` implementations show precisely what those calls do to
  the pending-callback queue.
- Stop condition: I treated a block as done only when the relevant test passed
  and I could explain the code without referring back to the conversation.

## 6. Verification evidence

- `python -m pytest -q` → **7 passed**, run twice: once after completing all
  eleven blocks, and again after the chart changes to confirm no regression.
  This suite is credential-free and covers determinism
  (`test_same_seed_replays_same_serialized_events`), the flush/poll patterns
  (`test_sync_and_async_have_expected_flush_patterns`), byte serialization
  (`test_serialization_demo_produces_utf8_json_bytes`), the per-batch benchmark
  rows (`test_benchmark_records_one_completed_row_per_batch`), argument
  validation, credential exclusion
  (`test_safe_config_report_excludes_credentials`), and the analyzer
  (`test_analyzer_validates_and_plots_complete_evidence`).
- Real Confluent runs: Demo 02A, 02B, and 02D each reported 4 attempted, 4
  delivered, 0 failed, 0 remaining after flush.
- Benchmark: `Wrote 8 valid rows to results/producer_benchmark.csv`. All eight
  rows show `batch_delivered = 500`, `batch_failed = 0`,
  `remaining_after_flush = 0`. `main()` would have aborted otherwise, so this
  output is itself a check on my per-batch delta arithmetic — a running total
  would have recorded 1000 on batch 2 and failed the run.
- Manual check: I inspected the rendered `results/producer_benchmark.png`. The
  first version failed my reading of it (sync-style flat at zero, fractional
  x-ticks); after the log-scale and integer-tick fixes I re-rendered and
  confirmed both series are legible and the ~205× gap is visible.
- Cross-check of a specific AI claim: the assistant asserted async batch 1 would
  be slower due to connection warm-up. My CSV confirms it independently — 305.2
  messages per second for async batch 1 versus 2624.93, 2715.16, and 2649.55 for
  batches 2–4.

## Declaration

I used AI to improve efficiency rather than replace learning. I reviewed the AI-assisted material, understand and can explain the submitted code and analysis, verified the result independently, and remain responsible for this
submission.

- Student name: Mark Goold
- Date: 2026-07-19
