# Final Project Ideas

These examples are starting points, not assigned topics. You may adapt one,
combine parts of several, or propose a different project. Choose a problem that
you can explain clearly and reduce to one complete Kafka path with an observable
result. A smaller system that another person can review and verify is stronger
than a large design that remains unfinished. Each idea below includes one
possible bounded AI component; you may replace it with another disclosed and
evaluated AI-assisted workflow. Before submitting the proposal, review the
[Final Project requirements](#/handouts/final-project), the
[Proposal Template](#/handouts/final-project-proposal-template), and the
[10-Point Proposal Rubric](#/handouts/final-project-proposal-rubric).

## Project directions

**1. Wikimedia Recent Changes Monitor · realtime · low to medium difficulty.**
Build a monitoring tool for editors, researchers, or moderators using the
[Wikimedia EventStreams service](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service).
A producer identifies itself with a User-Agent, reads the public
`recentchange` server-sent event stream, discards canary events, validates a
small schema, and publishes selected changes to Kafka keyed by wiki or page; a
Python consumer computes windowed counts by wiki, page, or change type and
produces an alert or compact report. For the bounded AI element, one option is
to classify a small labeled subset or summarize only the evidence in selected
events, then measure classification quality or unsupported claims. Keep a short
cached JSONL sample so the same pipeline can be replayed when the live feed or
network is unavailable.

**2. USGS Earthquake Alert and Situation Summary · near-realtime · medium
difficulty.** Create an alerting service for a traveler, emergency-planning
team, or local news desk using the
[USGS Earthquake GeoJSON feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).
A poller checks one official feed at its documented cadence, validates each
record, and publishes new or revised events to Kafka keyed by USGS event ID; it
uses the event's `updated` value so later revisions are not silently dropped. A
consumer applies transparent magnitude and location rules and writes an alert
artifact. For the bounded AI element, one option is to turn the structured facts
into a short situation summary, tested for unsupported claims and numeric
accuracy. Use saved API responses as a deterministic replay path, and do not
depend on an earthquake occurring during the demonstration.

**3. Citi Bike Station Availability Monitor · hybrid · medium difficulty.**
Design an operations tool for a bike-share dispatcher or commuter using the
realtime GBFS feeds and historical trip files linked from the
[Citi Bike System Data page](https://citibikenyc.com/system-data). Validate and
publish station-status snapshots to Kafka keyed by station ID, maintain the
latest availability state, and emit a CSV, alert stream, or dashboard-ready
file for stations that are nearly empty or full; a small historical sample can
provide context or replay data. For the bounded AI element, one option is to
explain a proposed rebalancing action from the calculated station facts; the
evaluation checks whether the explanation preserves those facts. Limit the
first version to one `station_information` and `station_status` feed pair, one
borough or station subset, and cached snapshots for offline use.

**4. NYC Taxi Demand Replay and Anomaly Report · batch replay · medium
difficulty.** Build a demand-monitoring pipeline for a transportation analyst
using a small fixed sample from one monthly Parquet file on the
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
A replay producer validates and emits trips in timestamp order at an
accelerated rate, Kafka events are keyed by pickup zone, and a consumer computes
bounded time-window metrics such as trip count, fare, or tip rate before writing
an anomaly report or dashboard-ready dataset. For the bounded AI element, one
option is to explain already detected anomalies; the evaluation compares every
explanation with the underlying metrics. Do not download many months or make a
live dashboard the minimum success path; a fixed sample and a reproducible
output file are enough.

**5. MovieLens Streaming Recommendations · batch replay · medium to high
difficulty.** Create a small recommendation service for a movie application
using the education-sized
[MovieLens Latest Small dataset](https://grouplens.org/datasets/movielens/latest/).
Validate and replay a bounded set of ratings as user events, publish them to
Kafka keyed by user ID, and maintain simple state such as recent ratings, movie
popularity, or item-to-item co-occurrence before producing a top-N
recommendation artifact. For the bounded AI element, one option is to explain a
recommendation using only the computed evidence, with tests for unsupported
movie facts and consistency with the ranking. Cite GroupLens, follow the dataset
README and usage license, and do not redistribute data unless those terms
permit it.

**6. Application Reliability Monitor · realtime synthetic · low to medium
difficulty.** Build an incident-monitoring tool for a developer or site
reliability engineer. A seeded Python generator produces service name, latency,
status code, and error events; a producer validates them and publishes to Kafka
keyed by service, while a consumer calculates fixed-window error rates and
latency summaries before writing an alert and incident evidence file. For the
bounded AI element, one option is to generate a short incident summary using
only those calculated metrics, then evaluate it against deliberately injected
faults and known ground truth. The
[OpenTelemetry documentation](https://opentelemetry.io/docs/) provides
real-world context for logs, metrics, and traces, but installing a full
OpenTelemetry stack is not required. Preserve the seed and generated JSONL so
the run is exactly replayable.

**7. Game Telemetry and Cheat Detection · realtime synthetic · medium
difficulty.** Create a live-operations tool for the analyst of one clearly
defined game mode. A seeded Python generator produces a small schema of login,
match, score, and optional purchase events; events are validated and published
to Kafka keyed by player ID, and a consumer maintains a leaderboard or flags
impossible scores before writing a review queue. For the bounded AI element, one
option is to explain each flagged case using only its rule results and event
history, with injected cheat cases providing labeled evaluation data. Keep one
target user, one game mode, one primary output, and a deterministic replay so
the project demonstrates a verifiable operational result instead of only a toy
event generator.

**8. Inventory CDC Monitor · realtime · advanced alternative.** Build a
low-stock monitor for an inventory operator using one PostgreSQL inventory
table. With the
[Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html),
inserts, updates, and deletes flow through Kafka Connect into Kafka keyed by
item ID; a consumer materializes current inventory state and writes a low-stock
alert artifact. For the bounded AI element, one option is to summarize or
prioritize alerts from the computed state, evaluated against a fixed script of
known database mutations. This direction is advanced because PostgreSQL,
logical decoding permissions, Kafka Connect, and the connector must all work.
Validate that path early, restrict the project to one table and one output, and
keep a deterministic replay of the same change-event contract as the offline
fallback.

## Keep the proposal course-sized

- Name one target user, one useful output, one minimum end-to-end result, and
  the applicable review path.
- Show where validation, Kafka, state or windowing, AI, and evaluation enter the
  data flow; extra technologies do not earn extra proposal points.
- Test data access early. For a cloud-based core path, plan full reviewer access;
  for a non-cloud path, plan a locally runnable sample or replay demo. A local
  fallback for a cloud path is recommended when practical.
- Record the owner, link, license, and usage limits of your chosen source; each
  of these datasets has its own terms, and the final package documents them in
  `DATA_SOURCE.md`.
- For synthetic data, document the generator, seed, schema, and injected test
  cases in `DATA_SOURCE.md` so another person can reproduce the evaluation.
