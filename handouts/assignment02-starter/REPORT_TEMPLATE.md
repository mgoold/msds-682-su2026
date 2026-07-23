# Assignment 2 Report

## Student and run

- USF username:
- Run ID:
- Topic:
- Base consumer group:
- Replay consumer group:

## Evidence summary

| Phase | Expected | Observed | Evidence file |
|---|---:|---:|---|
| API seed | 12 acknowledged |  | `evidence/api_seed_report.json` |
| First consumer run | 8 processed and committed |  | `evidence/consumer_first_run.json` |
| Same-group resume | 4 new records processed and committed |  | `evidence/consumer_resume_run.json` |
| Separate-group replay | 12 replayed |  | `evidence/consumer_replay_run.json` |

Confirm that the first and resume sequence-number sets are disjoint and their
union is 0 through 11:

## Analysis

Write at least 150 words addressing the required process-before-commit,
failure-boundary, resume, offset-reset, replay, and component-responsibility
questions:

[Write here.]

## Short answers

1. What is a poll loop, and why must it have visible stop conditions here?

   [Answer.]

2. What exactly is stored by a Kafka consumer offset commit?

   [Answer.]

3. Why is producer acknowledgement different from consumer offset commit?

   [Answer.]

4. Why must deserialization and Pydantic validation happen before commit?

   [Answer.]

## AI assistance status

Did you use AI assistance for any submitted code, debugging, analysis, writing,
or testing?

- [ ] No
- [ ] Yes; `AI_USAGE.md` is included.

If Yes, confirm that `AI_USAGE.md` lists every tool/model and every submitted
area it assisted:

- [ ] Confirmed

## Extra credit claimed

- [ ] None
- [ ] Two-member consumer group
- [ ] Native asyncio consumer extension
- [ ] AI-assisted engineering review

List supporting files:

## Credential safety and cleanup

- [ ] `.env` and credentials are excluded.
- [ ] Evidence contains no secrets.
- [ ] Unused Confluent resources and keys were deleted; any retained resource
      is still needed for the course and is being monitored.

Brief cleanup note:
