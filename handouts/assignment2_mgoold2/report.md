# Assignment 2 Report

## Student and run

- USF username: mgoold2
- Run ID: `assignment2`
- Topic: `msds682.assignment02.trip-events-api-avro.v1`
- Base consumer group: `msds682.assignment02.mgoold2.base.v1.assignment2`
- Replay consumer group: `msds682.assignment02.mgoold2.base.v1.assignment2.replay`

## Evidence summary

| Phase | Expected | Observed | Evidence file |
|---|---:|---:|---|
| API seed | 12 acknowledged | 12 | `evidence/api_seed_report.json` |
| First consumer run | 8 processed and committed | 8 | `evidence/consumer_first_run.json` |
| Same-group resume | 4 new records processed and committed | 4 | `evidence/consumer_resume_run.json` |
| Separate-group replay | 12 replayed | 12 | `evidence/consumer_replay_run.json` |

Confirm that the first and resume sequence-number sets are disjoint and their
union is 0 through 11:
* first processed sequence: {0, 1, 2, 4, 5, 6, 9, 10}
* resume processed sequence: {3, 7, 8, 11}
* intersection is null.
* union is 0 through 11 with no gaps --> every event processed just once.

## Analysis

Write at least 150 words addressing the required process-before-commit,
failure-boundary, synchronous commit-result checking, resume, offset-reset,
replay, and component-responsibility questions:

### Why the consumer processes and writes output before committing.
* Answer:
* the first thing to understand is that "commit" in the kafka context only means "bookmark or record an offset as having been processed". The offset is effectively the index of a message relative to the start of the message queue that is being processed.  To commit also thus has the sense that work on the message (processing, writing) is complete. Therefore, committing is done last because, if the processing or writing fails, it would be false/pointless to commit its offset.
* related to this, kafka operates with the understanding that it is OK to attempt to process and or write the same message multiple times.  It's unacceptable, to commit if either of those steps fail though, because in that case you'd imply the the data had been captured and dealt with properly, which would be false, and the downstream systems would never know.
### What can happen if the application fails before or after the commit?
* Answer:
  * Before: the record will be redelivered and reprocessed, at the cost of having something duplicated, but you don't lose the data.
  * After: the commit already succeeded and was recorded as a placeholder for the consumer.  So when the app restarts it will feed that placeholder to the consumer, and the processing of messages should start where they left off, from the last committed offset before the app crash.
### Why does a synchronous commits returned partition results still need checking.
* Answer: so what asynchronous=False does is block/stop the sending of messages until the broker responds.  But that doesn't mean the broker succeeded; a broker response just means it did *something* .  So before you continue you have to evaluate the broker's error messages to verify that it did its more recent work successfully.  You also have to check the position of the message.offset(), the position where that record already lives in the log, vs committed_partition.offset, bookmark the broker is updating right now, in response to your commit call, to make sure the bookmark is landing in the correct place.
### Why does the same group resume?
Answer: because offsets are stored per consumer group, not per consumer instance.  If you use a fresh group_id, you'll trigger a fresh reread of the same data.  
### Why auto.offset.reset does not normally reset an existing group?
* Because it is triggered by not being able to find a valid offset -- like if you used a brand new group id.  But for an existing group, you normally have a valid preexisting offset, so the auto.offset.reset doesn't get triggered.

### Why does replay use a separate group and explicit assignment override?
* Answer: so if your goal is to reprocess data that you've already committed past, you first need to use a new group id, as discussed above.  If you try to replay data you've committed past on an existing group kafka won't do it.
* But even with a new group, there are corner cases where doing replay even with the "earliest" parameter may not work, such as if the group id were accidentally re-used.  So to really force it you have to to use the "force_beginning" option in an explicit override.

### How FastAPI, Pydantic, Avro, Schema Registry, Kafka, the producer, and the consumer have different responsibilities.
* Answer:
   * FastAPI: turns the POST call into an http response.
   * Pydantic: handles data validation,type, and shape enforcement.
   * Avro: handles format and schema definition for an event's bytes, and confers the serialization that is transmitted by kafka.
   * Schema Registry: the external (to kakfa) store of Avro schemas.  Because it is external to kafka, both producers and consumers can look up schema from it to confirm the schema or use it to encode/decode.  The encoding and decoding used by producers and consumers can stay synched because they both reference this external store.
   * kafka: kafka is the broker and manages the logs.  That is its whole job.  It assigns offsets to published records, stores them per partition, and separately tracks each consumer group's committed offset.  It doesn't know about any of these other parts in the ecosystem.
   * producer: sends a serialized event (per the Avro schema) to the broker, and waits for a confirmation from it and then returns a success message.
   * consumer: polls kafka broker for raw byte events, then deserializes them per the Avro schema. Then it validates the deserialized content with Pydantic, and writes the result.  If all that succceeds, it commits and updates its bookmark.

## Short answers

1. What is a poll loop, and why must it have visible stop conditions?
Answer: a consumer of data from a kafka broker operates on a (poll) loop, asking the broker if it has anything new to process.  It has thus has to have visible stop conditions because if there are none it will loop indefinitely, wasting resources.  The stop conditions are "visible" in the sense that they are explicitly defined in the consumer functions as checks, typically on idle or run timeouts, or on a max number of messages.  When the condition is met, it fires a stop_reason string that gets included in reports, again making them "visible".

2. What exactly is stored by a Kafka consumer offset commit?
Answer: for each (consumer group, topic, partition), a commit stores a single integer value which is the offset (not the index) of the **next message** (not the last message completed) that group should read from that partition.  That's all it is. 

3. Why is producer acknowledgement different from consumer offset commit?
Answer:
* a producer acknowledgement is nothing but a signal that from the producer noting that the broker acknowledged that it has accepted and persisted a record.
* a consumer offset commit is the integer value of the offset per (group, topic, partition) triplet noting the next message that should be processed.

4. Why must deserialization and Pydantic validation happen before commit?
Answer: this is kind of the bookend to the initial question.  You mustn't commit until  deserialization and Pydantic validation succeed because once you commit, you'll never be able to have the broker redo the same commit.  The broker will commit any time you tell it to, even if deserialization and Pydantic validation failed, so you have to be sure and get it right before you ask for the commit.

## AI assistance status

Did you use AI assistance for any submitted code, debugging, analysis, writing,
or testing?

- [ ] No
- [x] Yes; `AI_USAGE.md` is included.

If Yes, confirm that `AI_USAGE.md` lists every tool/model and every submitted
area it assisted:

- [x] Confirmed

## Extra credit claimed

- [ ] None
- [ ] Two-member consumer group
- [ ] Native asyncio consumer extension
- [x] AI-assisted engineering review

List supporting files:

### For AI-assisted engineering review.
#### Accepted Suggestion:
##### Writeup: 
* Claude pointed out that although these lines:
```
message_key_str = message_key.decode("utf-8")
if message_key_str != event.trip_id:
    raise ValueError(f"{message_key_str} is not equal to event.trip_id")
```
... raise when `message_key_str != event.trip_id` , there are no actual corresponding pytests to flag this inequality, so it is a good idea to add such tests.  It added 4 pytests and ran them in an ablation suite.  Here are the results of running those tests, running each of the 4 conditions (the right hand column) one at a time.  The main thing to notice is that the center column "Provided suite" of 11 original tests pass at every step, meaning that there was nothing in the original pytest suite to catch these errors until now.  Good catch, claude; you may have cake and pie. 

| State | Provided suite | New review guards |
|---|---|---|
| Baseline | 11 passed | 4 passed |
| Key comparison disabled | **11 passed** | 1 failed, 3 passed |
| Missing-key guard disabled | **11 passed** | 1 failed, 3 passed |
| run_id filter disabled | **11 passed** | 1 failed, 3 passed |
| Restored | 11 passed | 4 passed |

##### Related files:

- [`tests/test_review_guards.py`](tests/test_review_guards.py) — four
  credential-free tests covering the key-verification and run-ID guards
- [`extra_credit/mutation_check.py`](extra_credit/mutation_check.py) — harness
  that disables one guard at a time and records which suite detects it
- [`evidence/xc_review_evidence.json`](evidence/xc_review_evidence.json) —
  generated results; reproduce with `python extra_credit/mutation_check.py`


#### Rejected Suggestion:
* 


## Credential safety and cleanup

- [ ] `.env` and credentials are excluded.
- [ ] Evidence contains no secrets.
- [ ] Unused Confluent resources and keys were deleted; any retained resource
      is still needed for the course and is being monitored.

Brief cleanup note:
