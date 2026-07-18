# Assignment 1 Report

## Student and run information

- Name:
- USF username:
- Run ID:
- Topic:
- Seed:
- Messages per strategy:
- Batch size:

## AI assistance disclosure

- I used AI assistance for this assignment: **Yes / No**
- If Yes, I included a completed `AI_USAGE.md`: **Yes / No / Not applicable**

## Demo 02A: sync-style producer

Summarize the run and link to `evidence/demo02a_report.json`. Explain why
flushing after every message is easy to understand but normally slow.

## Demo 02B: asynchronous producer

Summarize the run and link to `evidence/demo02b_report.json`. Explain how
`poll(0)` and the final `flush()` are used.

## Demo 02C: performance benchmark

Embed or link to `results/producer_benchmark.png`. In at least 150 words,
address all five analysis prompts in the assignment specification and support
your conclusions with your CSV results.

## Demo 02D: validation and serialization

Link to `evidence/demo02d_report.json`, include one sample serialized event,
and explain the Python model to JSON string to UTF-8 bytes path.

## Producer-code understanding

1. What configuration is required to create the producer, and why must it stay
   outside source code?

2. What does the delivery callback record for a success and for a failure?

3. What is the difference between `poll(0)` and `flush()` in these programs?

4. Why is one final `flush()` required before the asynchronous script exits?

## Limitations and cleanup

Explain why this run is not a universal Kafka capacity claim. Confirm that you
removed credentials from the submission and stopped or deleted unused cloud
resources.
