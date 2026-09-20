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
- [x] Two-member consumer group
- [x] Native asyncio consumer extension
- [x] AI-assisted engineering review

### For Two-member consumer group:
#### Evidence:
* Based on the description given in the rubric, Claude implemented this and output the following evidence:
```
member-1: partitions [0, 1]  processed 6  seqs [3, 6, 7, 9, 10, 11]  stop=run_covered
member-2: partitions [2]     processed 6  seqs [0, 1, 2, 4, 5, 8]    stop=run_covered

combined sequence numbers        : [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
covers_intended_run              : True
no_partition_shared_concurrently : True

partition 0: member-1 from 1.5836s to 3.011s
partition 1: member-1 from 1.5836s to 3.011s
partition 2: member-2 from 1.5871s to 3.011s
```
The interpretation this is as follows:
* two different threads assigned to members 1 & 2, respectively, operated over the same time interval from 1.5836s to 3.011s.  They each three stop conditions — idle timeout, run timeout, and "the run is covered."
* they used a new group `assignment2.xc-two-members`, which was distinct from the base an replay groups.
* no-overlap: the script timestamps every assign and revoke on a shared clock, reconstructs ownership windows, and checks every pair for time overlap. The overlap list came back empty.
* you can see that the sequence numbers 0 through 11 occur exactly once, with no duplicates.

### Related Files:
* extra_credit/two_member_group.py
* evidence/xc_two_member_group.json

### For Native asyncio consumer extension

#### Evidence
* Claude provided the following evidence of its work:
```
client               : confluent_kafka.aio.AIOConsumer
deserializer         : confluent_kafka.schema_registry.avro.AsyncAvroDeserializer
processed            : 12   stop_reason: max_messages
sequence numbers     : [0…11]
assignment ready in  : 1.7621 s
assignments (offsets): [[-2, -2, -2]]
cleanup              : {'unsubscribe': 'ok', 'close': 'ok'}  -> clean: True

sync identities  : 12      async identities : 12
identical        : True    missing: []   extra: []

graded before/after : {'base': {0:3, 1:3, 2:6}, 'replay': {0:3, 1:3, 2:6}}  -> untouched
```
#### Requested Criteria

* **Real assignment readiness.** The receive budget doesn't open when the consumer starts — it opens the first time assignment_ready is set, which took 1.76 seconds here. That number is published in the report. Without this, a slow group join on a cold cluster would silently eat the time budget and the run would look like it found no data.

* **Finite poll and time limits.** Four separate bounds, all recorded in a bounds block: a 12-message cap, a 45-second assignment timeout, a 45-second post-assignment receive deadline, and a 1-second poll timeout. It stopped on max_messages, so none of the timeouts had to fire.

* **Schema-aware validation.** AsyncAvroDeserializer wired with my own avro_dict_to_event, so Block 3 runs inside the async path exactly as it does in the synchronous one. Then isinstance against TripEventV1, and the UTF-8 key compared to event.trip_id.

* **Correct cleanup.** unsubscribe and close are each awaited under their own timeout, with results recorded rather than swallowed. A cleanup failure is captured but never replaces the error that actually stopped the run.

* **Equivalence.** The 12 identities it accepted are identical to the 12 in results/processed_events.jsonl from the first and resume runs — nothing missing, nothing extra. Identity is run_id:sequence:trip_id, deliberately excluding partition and offset, since those legitimately differ between a two-phase synchronous read and a single async pass.

#### Related Files
* [`extra_credit/async_consumer.py`](extra_credit/async_consumer.py) — the bounded
  AIOConsumer pass, with assignment readiness, the four limits, and bounded cleanup
* [`evidence/xc_async_consumer.json`](evidence/xc_async_consumer.json) — generated
  evidence including the identity equivalence comparison

### For AI-assisted engineering review.
#### Accepted Suggestion: Add pytests for event.trip_id inequality.
##### Writeup: 
* Claude pointed out that although these lines:
```
message_key_str = message_key.decode("utf-8")
if message_key_str != event.trip_id:
    raise ValueError(f"{message_key_str} is not equal to event.trip_id")
```
... raise when `message_key_str != event.trip_id` , there are no actual corresponding pytests to flag this inequality, so it is a good idea to add such tests.  It added 4 pytests and ran them in an ablation suite.  Here are the results of running those tests, running each of the 3 mutations (in the right hand column) one at a time.  The main thing to notice is that the center column "Provided suite" of 11 original tests pass at every step, meaning that there was nothing in the original pytest suite to catch these errors until now.  Good catch, claude; you may have cake and pie. 

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
* Claude pointed out that `consumer.commit(...)` returns a **list** of `TopicPartition` objects, and that my loop assumes that list holds exactly one
  entry — the receipt for the message I just committed. There is a second form of the call, `commit()` with no `message=` argument, which commits every assigned partition and returns one entry per partition. Under that form my loop would be checking partitions it never committed, so the code is not future-proofed against ever switching to it.
* I rejected future-proofing the code in this way because:
 * `run_consumer.py` only ever calls `consumer.commit(message=message, asynchronous=False)`. That form commits only the message's own partition, so the returned list holds exactly one entry — the one the check is meant to examine. The multi-entry hazard cannot arise in this code as it stands.

 * Here is an example that this is true:
    ```
Evidence (one run, same consumer, same assignment):

    ASSIGNED partitions: [0, 1, 2] (3 total)

    commit(message=...)  while holding 3 partitions
      -> returned 1 entry: [(1, 2)]

    commit()  (no message=) while holding 3 partitions
      -> returned 3 entries: [(0, -1001), (1, 3), (2, -1001)]
    ```

* The consumer held three partitions in both calls. The message= form returned one entry; the no-argument form returned three. The return size therefore follows which partitions were committed, not which are assigned. Since run_consumer.py only ever calls the message= form, `committed` can hold only one entry, and the multi-entry hazard can't happen in this code as it stands now.
* An interesting thing about `[(0, -1001), (1, 3), (2, -1001)]` : it turns out that -1001 is kafka's marker to mean "nothing to commit here", and it is used to pad per-partition entries the list where there is nothing to commit.  I didn't know that.  If my code ever used the the list, it would receive two meaningless -1001 values and the check would raise on both, reporting a failure that didn't happen. --So the potential risk is real, but it is not possible for it to happen now.
 * Additionally, multiplying complexity would multiply the potential for bugs, which outweighs the theoretical benefit of future-proofing.

##### Stop condition and non-AI fallback: 
 * this decision rests on a measurement of one client version (confluent-kafka 2.15.0), not on a guarantee. The stop condition is the existing offset check itself — if it ever raises about a partition the run did not commit, that is the signal the return shape has changed and this rejection no longer holds. The failure is loud rather than silent, which is the safe direction: a wrong commit is never quietly accepted. The fallback is not to ask an AI what changed, but to re-run the probe above against the upgraded client and read the library's release notes.


## Credential safety and cleanup

- [x] `.env` and credentials are excluded.
- [x] Evidence contains no secrets.
- [x] Unused Confluent resources and keys were deleted; any retained resource
      is still needed for the course and is being monitored.

Brief cleanup note: all of the listed cleanup items are completed.
