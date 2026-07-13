# Assignment 1 Student Starter

This starter matches the required files and commands in the official
[Assignment 1 specification](https://wgu9.github.io/msds-682-su2026/#/handouts/assignment01). Complete every block between
`CODE START HERE` and `CODE ENDS HERE`; do not remove the markers, docstrings,
or explanatory comments.

The course Demo 02 files are worked references that teach the required producer
behaviors. This starter is the assignment scaffold: implement it, run the
larger required benchmark, generate its fixed artifacts, and submit this folder.
Do not replace the starter with unchanged demo files.

## 1. Set up Python 3.11.14

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 2. Configure Confluent Cloud

Copy `.env.example` to `.env`, then fill in your own Kafka credentials. Never
commit or submit `.env`.

The four programs use the same topic. The default is
`msds682.demo01.trip-events.v1`; change `ASSIGNMENT1_TOPIC_NAME` only if your
Demo 01 topic has a different name.

## 3. Complete and test the starter

Run the credential-free tests before using Confluent:

```bash
python -m pytest -q
```

Before implementation, contract-only tests may pass while tests that exercise
the marked code blocks fail. All tests must pass after you complete the starter.
They do not replace the required real Confluent runs.

## 4. Run Demo 02A-02D assignment programs

Run all commands from this starter's top-level directory:

```bash
python src/producer_sync.py --run-id assignment1
python src/producer_async.py --run-id assignment1
python src/producer_compare.py --run-id assignment1 --messages 2000 --batch-size 500 --seed 682
python src/analyze_results.py --input results/producer_benchmark.csv --output results/producer_benchmark.png
python src/producer_serialization.py --run-id assignment1
```

The programs write the required secret-free reports to `evidence/` and the
benchmark data and graph to `results/`.

The sync-style benchmark flushes after every message. The 2,000-message base
run is designed to complete in about 20 minutes or less on a typical course
setup, but cloud and network latency vary. Start before the due-date evening.

## 5. Write the report and disclose AI assistance

Copy `REPORT_TEMPLATE.md` to `report.md` and complete every section.

- If you did not use AI assistance, select `No` in `report.md`.
- If you used any AI assistance, select `Yes`, copy
  `AI_USAGE_TEMPLATE.md` to `AI_USAGE.md`, and submit the completed log.
- Use the template to explain why AI was appropriate, how you checked accuracy,
  and how you recovered or would fall back if AI failed or became repetitive.
- The disclosure is required when AI is used. It does not itself earn extra
  credit. The optional AI-review point has additional evidence requirements in
  the assignment specification.

## 6. Package the submission

Rename the folder to `assignment1_<usf_username>`, remove `.env`, `.venv`, and
cache files, and submit `assignment1_<usf_username>.zip` to Canvas. Do not
remove the completed source files, reports, benchmark CSV, graph, or report.
Use the official assignment's ASCII submission tree and checkbox list before
uploading.
