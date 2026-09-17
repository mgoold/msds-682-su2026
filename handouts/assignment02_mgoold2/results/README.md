# Processing output

The consumer phases create:

- `processed_events.jsonl`: 8 first-run records plus 4 resumed records
- `replayed_events.jsonl`: 12 records from the separate replay group

Each line is a secret-free JSON processing result. Do not edit generated
results to conceal an incomplete run.
