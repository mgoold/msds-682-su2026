# Demo 07: Real-Time Pricing, Delayed Outcomes, and Model Evaluation

- **Lecture:** Lecture 7, State, Features, and Real-Time ML
- **Python:** 3.11.14
- **Kafka client:** `confluent-kafka[avro,schemaregistry]==2.15.0`
- **ML library:** `scikit-learn==1.9.0`
- **Validation:** `pydantic==2.13.4`
- **Routing:** open-source OSRM Route service or an explicit offline fixture
- **Cloud platform:** your Confluent Cloud environment

> ##### KEY CONCEPT
>
> A real-time prediction is not the end of an ML system. A later outcome
> provides the label needed to evaluate the prediction, compare model versions,
> and recommend which version better meets the business target.

## 1. Objective, expected outcome, and 55 to 70 minute route

Demo 07 answers one business question:

> How can we set a fare so that realized markup per trip is close to 20%?

The finished pipeline turns four trip requests into two comparable quote
versions, joins those quotes with delayed outcomes, and produces one
evidence-based model selection recommendation.

By the end of Demo 07, you should be able to:

1. separate route features, cost prediction, and pricing policy;
2. explain why `rule-v1` is a baseline and `ridge-v2` is a trained model;
3. follow output acknowledgement before input offset commit;
4. join fare quotes with delayed outcomes by `trip_id`; and
5. compare model versions against one business metric.

The complete teaching loop is:

<div class="handout-flow" role="group" aria-label="Demo 07 teaching loop">
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Offline</span>
    <strong>07A · Train</strong>
    <p>Deterministic examples produce one validated, versioned Ridge artifact.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Real-time</span>
    <strong>07B–07C · Quote</strong>
    <p>Trip requests become route features and two comparable fare quotes.</p>
  </section>
  <div class="handout-flow-arrow" aria-hidden="true">→</div>
  <section class="handout-flow-card">
    <span class="handout-flow-phase">Delayed evaluation</span>
    <strong>07D–07F · Learn</strong>
    <p>Outcomes join with quotes, produce evaluations, and support one selection recommendation.</p>
  </section>
</div>

| Step | Demo | Question | Time |
|---:|---|---|---:|
| 1 | 07A | How is a model trained, versioned, and exported? | 8 minutes |
| 2 | 07B | What event starts the online pipeline? | 5 minutes |
| 3 | 07C | How do rule-v1 and ridge-v2 produce comparable quotes? | 15 minutes |
| 4 | 07D | What delayed label arrives after a trip completes? | 5 minutes |
| 5 | 07E | How are quotes joined with outcomes safely? | 15 minutes |
| 6 | 07F | Which version is closer to the business target? | 8 minutes |

## 2. Relationship to Demo 06

Demo 07 reuses the processing discipline established in Demo 06:

```text
consume -> validate -> compute or infer -> produce
        -> output ack -> commit input offset
```

Demo 06 uses one input topic to derive a stateless order metric and emphasizes
resume and replay. Demo 07 keeps that correctness boundary and adds two model
versions, delayed trip outcomes, a bounded join by `trip_id`, and business
evaluation. The conceptual sequence is intentional; the runtime resources are
independent.

| Demo 06 established | Demo 07 adds |
|---|---|
| Stateless input-to-output transformation | Route features and versioned pricing |
| Output acknowledgement before input commit | The same correctness boundary for quotes and evaluations |
| Consumer-group resume and replay | Two model-version groups receive the same requests |
| Kafka offsets record processing progress | Join state pairs quotes with delayed outcomes |

## 3. Direct prerequisites

> **Start directly with Demo 07.** It creates its own data, topics, schemas,
> consumer groups, offsets, and model artifact. No Demo 01–06 resource or
> running process is required.

| Requirement | 07A and 07F | 07B–07E |
|---|---:|---:|
| Python 3.11.14 and published requirements | Required | Required |
| Confluent Cloud Kafka cluster | No | Required |
| Schema Registry enabled for Avro | No | Required |
| Kafka and Registry credentials in `.env` | No | Required |
| Existing Demo 07 topics or records | No | No; 07B creates and seeds them |
| Outbound HTTPS | No | Only 07C and 07D in OSRM mode |
| Demo 06 topic, data, or process | No | No |

07A is standalone. 07F is credential-free but not standalone: by default it
reads `outputs/runs/<run-id>/demo07e/report.json`. Run 07B–07E first, or pass
`--evaluation-report` explicitly.

## 4. Business problem and system overview

### Business question and teaching hypotheses

The service must quote one **upfront fare** before a trip begins. When a
`TripRequestV1` first arrives, it contains the request time plus pickup and
dropoff coordinates. A routing provider then returns estimated distance and
duration; those quote-time estimates are not post-trip measurements. The
synthetic realized distance, duration, and fulfillment cost arrive later in a
`TripOutcomeV1`.

Demo 07 asks:

> Which pricing method keeps realized markup closest to the 20% target?

The comparison tests three teaching hypotheses:

1. a trained cost model plus an explicit markup policy can outperform a fixed
   fare heuristic;
2. a controlled comparison requires the same trip requests, route features,
   and delayed outcomes; fixture mode guarantees that controlled input, while
   OSRM mode demonstrates live integration; and
3. an online prediction is not enough: the delayed outcome closes the
   evaluation loop.

This is a simplified teaching problem, not a claim that Ridge is the best
production pricing model.

### Terms and assumptions

| Term | Meaning in Demo 07 |
|---|---|
| `fare` | The fixed upfront amount quoted to the customer; the demo does not reprice the completed trip |
| `actual cost` | Synthetic realized trip fulfillment cost: fixed operating cost plus distance-related cost, time-related cost, and small noise |
| `profit` | `fare - actual cost` |
| `realized markup` | `profit / actual cost` |
| `fixture` | A predefined pickup/dropoff pair and offline route measurement stored in the course code for reproducible runs; it is not a third-party API call |
| `selection recommendation` | The version preferred by the published comparison rule; the demo does not deploy it |

`actual cost` may conceptually include driver time and mileage compensation,
but the demo does not model a separate driver-payout transaction. During the
comparison, both pricing versions produce counterfactual quotes for the same
trip. After a version is reviewed and selected, a normal production path would
expose only one official fare.

### Canonical end-to-end specification

Use this compact specification to keep the business story, units, code, and
classroom explanation aligned. `demo07_common.py` remains the executable SSOT
for numeric constants.

<ol class="handout-pipeline-list">
  <li>
    <span>Offline model preparation</span>
    <strong>Historical examples → train and validate ridge-v2 → save the versioned artifact</strong>
  </li>
  <li>
    <span>Real-time quote path</span>
    <strong>TripRequest → route features → rule-v1 or ridge-v2 → FareQuote</strong>
  </li>
  <li>
    <span>Delayed evaluation</span>
    <strong>TripOutcome + FareQuote → keyed join → PricingEvaluation → selection recommendation</strong>
  </li>
</ol>

Training and model selection happen outside the real-time request path. Route
estimation, inference, and fare-quote publication are the real-time path.

#### Step by step: 07A through 07F

| Step | Input and operation | Output |
|---|---|---|
| 07A: prepare model | Generate 160 deterministic synthetic historical rows; fit Ridge on 120 and validate on 40 | Validated `ridge-cost-v2.json` artifact |
| 07B: publish requests | Publish four predefined public pickup/dropoff coordinate pairs; a request has no route estimate | Four `TripRequestV1` records |
| 07C: route and quote | Use explicit fixture lookup or OSRM; convert provider meters/seconds to miles/minutes; run both pricing versions | Eight `FareQuoteV1` records |
| 07D: publish outcomes | Recreate the same requests, obtain the same declared route type, then apply stable post-trip adjustments | Four synthetic `TripOutcomeV1` records |
| 07E: join and evaluate | Collect the complete bounded set, join quote and outcome by `trip_id`, publish output, wait for acknowledgement, then commit inputs | Eight `PricingEvaluationV1` records |
| 07F: compare | Compute each version's mean absolute error from the 20% realized-markup target | One model-selection recommendation |

#### Units used throughout

| Quantity | Unit and representation |
|---|---|
| Pickup and dropoff | Latitude/longitude in decimal degrees |
| Provider route distance | Meters; converted using `1 mile = 1609.344 meters` |
| Provider route duration | Seconds; converted using `1 minute = 60 seconds` |
| Published route features | Miles rounded to `0.01`; minutes rounded to `0.1` |
| Money in code and Kafka contracts | Integer cents with currency `USD`; prose equations display dollars |
| Event timestamps | Timezone-aware UTC timestamps |
| Markup | Percent of actual cost; model-comparison error is reported in percentage points |

#### Canonical formulas

Let \(D_e\) be estimated miles, \(T_e\) estimated minutes, \(D_a\) synthetic
actual miles, \(T_a\) synthetic actual minutes, \(F\) fare in USD, and \(C_a\)
synthetic actual fulfillment cost in USD.

The rule baseline is:

$$
F_{\text{rule-v1}}
= 3.50 \times D_e + 0.20 \times T_e
$$

The trained candidate predicts cost, then applies the pricing policy. The
equation presents dollars for readability; the JSON artifact stores its
intercept and coefficients in cents.

$$
\widehat{C}
= \beta_0 + aD_e + bT_e,
\qquad
F_{\text{ridge-v2}} = 1.20 \times \widehat{C}
$$

The classroom outcome generator applies stable factors derived from
`trip_id`:

$$
D_a = s_DD_e,\quad s_D \in [0.98,1.06],
\qquad
T_a = s_TT_e,\quad s_T \in [0.96,1.14]
$$

It then produces the delayed teaching label:

$$
C_a
= 5.00 + 0.75D_a
+ 20.00\left(\frac{T_a}{60}\right) + \varepsilon,
\qquad
\varepsilon \in [-0.50,0.50]
$$

Finally:

$$
\text{realized markup}
= \frac{F-C_a}{C_a},
\qquad
\operatorname{MAE}_{\text{markup}}
= \frac{1}{N}\sum_{i=1}^{N}
\left|\text{realized markup}_i-20\%\right|
$$

#### Important assumptions

- The four online requests use predefined public coordinates, not randomly
  generated locations or real passenger data.
- Fixture mode is deterministic. OSRM mode is an integration demonstration;
  its public route response can change and is not treated as a live-traffic
  production SLA.
- Fare is an upfront fixed quote. The completed trip is not repriced.
- `actual miles`, `actual minutes`, and `actual cost` are synthetic realized
  teaching labels, not GPS observations, driver payout, or accounting truth.
- In fixture mode, both versions receive the same requests, route features,
  and delayed outcomes. Their quotes are counterfactual candidates for
  comparison. OSRM mode is not the controlled benchmark because separate
  public calls can return different measurements.
- Four trips verify the pipeline but cannot justify a production model
  decision.
- Ridge is a simplified trained baseline. Demo 07 recommends a version but
  does not promote or deploy it.
- 07E uses bounded in-memory state and at-least-once processing. It is not a
  durable production join or an exactly-once pipeline.

The event flow branches before it joins:

```text
TripRequestV1
  |-- 07C: routing estimate -> candidate FareQuoteV1 records
  `-- 07D: independent synthetic completion source -> TripOutcomeV1

FareQuoteV1 + TripOutcomeV1
  -> 07E bounded stateful join
  -> PricingEvaluationV1
```

07D is not a consumer of the requests topic. It independently recreates the
same declared synthetic requests for this teaching run. A production system
would normally receive outcomes from trip operations or vehicle telemetry.

### Four topics and their owners

`demo07_common.py` is the SSOT for the topic names and business constants.
Estimated distance, estimated duration, and fare stay together in one quote
event; there are no separate mileage and duration topics.

<div class="handout-topic-grid">
  <section class="handout-topic-card">
    <span class="handout-topic-role">Input event</span>
    <strong>Trip requests</strong>
    <code class="handout-topic-name">msds682.demo07.ml-trip-requests-avro.v1</code>
    <p><code>07B source</code> → <code>07C pricing groups</code></p>
    <p><code>TripRequestV1</code> · key <code>trip_id</code></p>
  </section>
  <section class="handout-topic-card">
    <span class="handout-topic-role">Prediction event</span>
    <strong>Fare quotes</strong>
    <code class="handout-topic-name">msds682.demo07.ml-fare-quotes-avro.v1</code>
    <p><code>07C pricing processors</code> → <code>07E evaluator</code></p>
    <p><code>FareQuoteV1</code> · key <code>trip_id</code></p>
  </section>
  <section class="handout-topic-card">
    <span class="handout-topic-role">Delayed label</span>
    <strong>Trip outcomes</strong>
    <code class="handout-topic-name">msds682.demo07.ml-trip-outcomes-avro.v1</code>
    <p><code>07D outcome source</code> → <code>07E evaluator</code></p>
    <p><code>TripOutcomeV1</code> · key <code>trip_id</code></p>
  </section>
  <section class="handout-topic-card">
    <span class="handout-topic-role">Evaluation event</span>
    <strong>Pricing evaluations</strong>
    <code class="handout-topic-name">msds682.demo07.ml-pricing-evaluations-avro.v1</code>
    <p><code>07E evaluator</code> → evidence or monitoring consumers</p>
    <p><code>PricingEvaluationV1</code> · key <code>quote_id</code></p>
  </section>
</div>

The first three topics represent the request, prediction, and delayed business
outcome. The fourth topic preserves the per-model comparison as a replayable
evaluation event.

### How route estimates and the synthetic outcome work

At request arrival, `TripRequestV1` contains coordinates but no distance or
duration. 07C passes those coordinates to one explicitly selected provider:

- `fixture` performs an offline lookup of four predefined coordinate pairs and
  their course-owned distance/duration measurements; and
- `osrm` calls the public open-source
  [OSRM Route service](https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md)
  with the driving profile.

07B does not generate arbitrary random locations. The shared code owns four
predefined public San Francisco pickup/dropoff pairs. A `run_id` creates stable
trip IDs and timestamps, while the coordinate pairs remain fixed. This avoids
unroutable or cross-water random examples and gives both pricing versions the
same request events.

OSRM returns the distance and duration for its profile-weighted fastest route.
It uses road-network speeds and routing weights, not straight-line distance
divided by one uniform average speed. The public endpoint is useful for a small
integration demonstration, but this course does not treat it as a live-traffic
ETA service or a production availability guarantee.

Historical training data use a fixed seed. Classroom outcomes use stable
`trip_id`-based adjustments. In `fixture` mode, the same inputs therefore
reproduce the same route features and labels. In `osrm` mode, the adjustments
are still deterministic, but the public routing response may change with its
map data or service behavior.

07D does not observe GPS or wait for the displayed trip duration. After the
quotes have been produced, the instructor runs this independent bounded source.
It calls the same declared routing mode, varies the resulting distance and
duration slightly to simulate a completed trip, and then computes:

```text
realized fulfillment cost
= fixed operating cost
+ distance-related cost
+ time-related cost
+ small deterministic noise
```

The later data-and-scale and business-target sections publish the exact ranges
and formulas.

### Tools and outputs by stage

| Stage | Primary tools | Output |
|---|---|---|
| 07A model preparation | Python, scikit-learn Ridge, fixed seed | Validated JSON model artifact |
| 07B request source | Predefined coordinates, Pydantic, Avro, Schema Registry, Kafka producer | `TripRequestV1` |
| 07C route estimate | Explicit `fixture` lookup or public OSRM Route API | Estimated miles and minutes |
| 07C pricing | Rule formula or validated Ridge artifact | `FareQuoteV1` |
| 07D outcome source | Same routing mode, deterministic adjustments, synthetic cost formula | `TripOutcomeV1` |
| 07E bounded join | Kafka consumer, Avro/Pydantic validation, in-memory dictionaries | `PricingEvaluationV1` |
| 07F comparison | Python reads the secret-free 07E JSON report | Model-selection recommendation |

### What the final selection recommendation means

07F compares both versions on the same four delayed outcomes. It recommends
the version with lower mean absolute deviation from the 20% markup target. 07F
reads the secret-free 07E evaluation report; it is not another Kafka consumer.
The recommendation summarizes this finite teaching comparison. It does not
deploy a model, change live traffic, or reprice any trip.

In a production ML system, moving an approved model version into live service
is often called **model promotion**. That separate governance and deployment
step is outside Demo 07.

## 5. Download and setup

- [Download `demo07-student.zip`](handouts/demo07-student.zip)

Extract it, enter its one top-level folder, and run:

```bash
uv venv --python 3.11.14 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
pytest -q
```

Then copy `.env.example` to `.env` and fill the Kafka and Schema Registry
credentials. `.env` is ignored and must never be published or submitted.

## 6. Data and scale

Training and validation labels, trip requests, fixture measurements, and
outcome adjustments are synthetic teaching data. Fixture mode is deterministic.
OSRM mode instead obtains live public route measurements, which may change with
the service's map data or behavior; the later outcome adjustments remain
synthetic.

| Data | Count | Purpose |
|---|---:|---|
| Training examples | 120 | Fit the Ridge cost model |
| Validation examples | 40 | Measure held-out cost prediction error |
| Online trip requests | 4 | Keep the live classroom run small |
| Quote versions per trip | 2 | Compare rule-v1 and ridge-v2 on the same fixture routes |
| Delayed outcomes | 4 | Provide realized cost labels from the same route mode |
| Evaluation events | 8 | One result per trip and model version |

The synthetic variation is bounded and reproducible:

| Stage | Distance | Duration | Cost noise |
|---|---|---|---|
| Historical training/validation | Estimated: 0.8–14.0 miles; actual: 97%–108% of estimate | Estimate varies with distance and synthetic traffic; actual: 94%–116% of estimate | −60 to +60 cents |
| Four delayed classroom outcomes | Actual: 98%–106% of the selected route estimate | Actual: 96%–114% of the selected route estimate | −50 to +50 cents |

The historical rows use seed `682`. Each classroom outcome uses a stable hash
of `trip_id`. These choices create repeatable labels for teaching; they are not
empirical claims about real trip-error distributions.

Four outcomes are enough to verify the event flow and comparison logic. They
are not enough to justify a production model decision. The synthetic cost
process is intentionally linear, so this example is expected to be favorable
to a small linear model.

Each `TripRequestV1` uses these fields:

| Contract field | Meaning |
|---|---|
| `requested_at` | Request date and time |
| `pickup` | Pickup latitude and longitude |
| `dropoff` | Destination latitude and longitude |

At request arrival, the contract has coordinates but no route measurements.
The selected provider receives the pickup and dropoff and returns estimated
distance and duration in one response. Fixture mode performs a predefined
offline lookup; OSRM mode performs an HTTPS routing request.

The first deterministic fixture is:

```text
estimated distance = 2.00 miles
estimated duration = 15.0 minutes
```

$$
\text{rule-v1 fare}
= 3.50 \times 2.00 + 0.20 \times 15.0
= 10.00\text{ USD}
$$

## 7. The business target

The course uses **20% markup on cost**:

$$
\text{realized markup}
= \frac{\text{fare} - \text{actual cost}}{\text{actual cost}}
$$

$$
\text{target fare}
= 1.20 \times \text{predicted cost}
$$

This is not the same as a 20% profit margin:

$$
\text{profit margin}
= \frac{\text{fare} - \text{cost}}{\text{fare}},
\qquad
\text{fare}_{20\%\text{ margin}}
= \frac{\text{cost}}{1 - 0.20}
= 1.25 \times \text{cost}
$$

Demo 07 consistently uses markup because the business question defines profit
relative to cost.

Each evaluation also records whether the absolute markup error is within
**2 percentage points** of the target. This diagnostic does not select the
recommended version; 07F compares mean absolute markup error.

### Rule v1

The transparent baseline is the original heuristic:

$$
F_{\text{rule-v1}}
= 3.50 \cdot D + 0.20 \cdot T
$$

Here, \(F_{\text{rule-v1}}\) is the estimated fare in USD, \(D\) is estimated
distance in miles, and \(T\) is estimated duration in minutes.

It is versioned and reproducible, but it is not ML and does not explicitly
target 20% markup.

### Ridge v2

The candidate first predicts trip cost:

$$
\widehat{C}
= \beta_0 + a \cdot D + b \cdot T,
\qquad
F_{\text{ridge-v2}}
= 1.20 \times \widehat{C}
$$

Here, \(\widehat{C}\) is predicted trip cost and \(F_{\text{ridge-v2}}\) is the
estimated fare after applying the 20% markup policy. The Ridge model is fitted
from historical synthetic examples. The artifact records the feature names,
coefficients, intercept, training seed, library version, and validation MAE.

### Optional advanced discussion: why Ridge?

This discussion is not required to run Demo 07. The model is deliberately
small so the course can focus on the end-to-end streaming ML lifecycle.

Distance and duration are usually correlated, but they are not identical. A
short congested route can take longer than a longer freeway route. That
variation helps the model distinguish distance-related cost from time-related
cost. If duration were almost a fixed multiple of distance, the model could
estimate their combined effect but could not reliably identify how much cost
belongs to each feature.

| Method | Behavior with correlated features | Demo 07 interpretation |
|---|---|---|
| Ordinary linear regression | Coefficients can change substantially across similar samples | Simple and valid, but potentially less stable |
| Lasso regression | May force one correlated coefficient to exactly zero | Not preferred because both distance and time are intended cost drivers |
| Ridge regression | Shrinks large coefficients and usually keeps both features | A stable, explainable teaching choice |

Ridge minimizes prediction error plus an L2 penalty:

$$
\text{Ridge loss}
= \sum_i\left(C_i-\widehat{C}_i\right)^2
+ \lambda\left(a^2+b^2\right)
$$

The penalty stabilizes coefficient selection; it does not prove that the
coefficients are causal, and it does not guarantee that they are positive.
Demo 07 fixes the scikit-learn regularization strength at `alpha=1.0` so every
student runs the same deterministic comparison. It does not tune a large
hyperparameter search or claim that Ridge is the best production pricing
model.

In a production model, engineers might scale features, tune the regularization
strength, add nonnegative coefficient constraints, or keep known accounting
costs as explicit rules and train ML only on the remaining cost. Those choices
are intentionally outside this demo.

<aside class="handout-callout handout-callout-important">
  <h5>IMPORTANT NOTE</h5>
  <p>The synthetic realized cost is:</p>
  <div class="math-display-source">
  \[
  C_{\text{actual}}
  = 5 + 0.75 \cdot D_{\text{actual}}
  + 20 \times \left(\frac{T_{\text{actual}}}{60}\right)
  + \varepsilon
  \]
  </div>
  <p>Here, \(\varepsilon\) is small deterministic noise generated from the fixed course seed.</p>
  <p>The distance component is intentional. If the label used only fixed cost and hourly time, the correct learned distance coefficient would be approximately zero.</p>
</aside>

## 8. Topic runtime and consumer design

The business overview defines each topic and owner. The classroom runtime uses:

- one partition per topic for a clear ordered demonstration;
- Avro values with TopicNameStrategy subjects;
- manual Kafka offset commits in both processors;
- one request consumer group per model version; and
- one evaluator group consuming quote and outcome topics.

Because rule-v1 and ridge-v2 use different consumer groups, both receive every
request and produce one quote per trip. The two-version path exists for the
classroom comparison. A normal serving path would run one approved version, so
each new request would produce one official fare quote.

## 9. Why Architecture A is implemented

### A: one routing and pricing processor

```text
trip request -> one routing-provider response with miles and minutes -> fare quote
```

- 2 topics on the online quote path: requests and quotes;
- 1 active pricing processor after a version is selected and deployed;
- 1 routing call per trip in that steady-state design;
- no online join state.

The classroom comparison deliberately runs rule-v1 and ridge-v2 as separate
processors, so each request is routed once per version. Demo 07D makes one
additional routing call per trip to create the synthetic delayed outcome.

### B: split mileage and duration

```text
trip request -> mileage topic  --+
             -> duration topic --+-> join -> fare quote
```

- 4 topics on the online quote path: requests, mileage, duration, and quotes;
- 3 processors;
- potentially 2 routing calls per trip;
- join state, timeout, duplicate, and late-event policy.

B is technically possible, but it creates an artificial join because one
routing-provider response already owns both fields. It is kept as a design
comparison, not as the runnable baseline.

The meaningful stateful join is:

```text
fare quote + delayed trip outcome -> business evaluation
```

### Streaming join versus database join

Demo 07 implements the streaming path because Lecture 7 is about keyed state:

| Question | Kafka event-driven join | Database join |
|---|---|---|
| Where does unmatched state live? | Stream processor state | Database tables |
| When is evaluation produced? | When the bounded expected set is complete in 07E; a continuous production processor could emit per matched pair | When a query, trigger, or scheduled job runs |
| Best fit | Low-latency evaluation events and downstream reactions | Historical audit, reporting, and model training |
| Included in Demo 07? | Yes, as a bounded in-memory teaching join | No; production architecture alternative only |

A production system can use both: publish immediate evaluation events, then
sink those events to a database or warehouse for historical analysis.

No ksqlDB, Kafka Streams, Flink, or Spark is required. The runnable baseline
uses Python `confluent-kafka`. The finite evaluator uses in-memory dictionaries
only to make state visible.

## 10. Run Demo 07

Choose one unique run ID and one routing mode, then reuse both:

```bash
RUN_ID=lec7-demo07-yourname
ROUTING_MODE=fixture
```

`fixture` is the recommended comparison mode because both pricing versions and
the outcome generator receive identical, reproducible route features. Use
`osrm` only when demonstrating live routing integration. In OSRM mode, 07C
routes each request once per version and 07D routes it once again, so repeated
responses are not guaranteed to be identical.

07C and 07D must use the same declared mode; mixing modes creates invalid label
lineage. The code never silently falls back between providers. The public OSRM
endpoint is used only for this small bounded teaching run: 12 calls total for
four trips, two quote versions, and four outcomes. It has no production service
guarantee.

### 07A: train ridge-v2

**Objective:** Train and export one transparent, versioned cost model.

**Why:** Online inference needs a validated artifact whose inputs, coefficients,
training lineage, and held-out error can be inspected.

**Done when:** `ridge-cost-v2.json` and a secret-free report exist, and the
artifact contains the two named route features plus validation MAE.

```bash
python demo07a_train_cost_model.py --run-id "$RUN_ID"
```

Expected terminal lines:

```text
Trained ridge-v2 on 120 records; validation MAE=... cents
Model artifact: outputs/runs/$RUN_ID/demo07a/ridge-cost-v2.json
Secret-free report: outputs/runs/$RUN_ID/demo07a/report.json
```

The artifact is validated JSON, not an executable pickle.

### 07B: create topics and publish requests

**Objective:** Create the four Demo 07 topics and publish the same four
deterministic trip requests used by both pricing versions.

**Why:** A fair model comparison begins with identical input events and visible
broker acknowledgements.

**Done when:** All four requests are acknowledged and all four topics are
verified.

```bash
python demo07b_produce_trip_requests.py \
  --run-id "$RUN_ID" \
  --count 4 \
  --create-topics
```

Expected terminal lines:

```text
Published 4 requests to msds682.demo07.ml-trip-requests-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07b/report.json
```

### 07C: publish two comparable quote versions

**Objective:** Consume every request once per model version and publish one
governed fare quote per version.

**Why:** Separate consumer groups let the baseline and candidate see the same
requests while preserving the acknowledgement-before-commit boundary.

**Done when:** `rule-v1` and `ridge-v2` each acknowledge four quotes, for eight
quotes total, and each processor commits only after its output acknowledgement.

#### Run 1: rule-v1 baseline

```bash
python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" \
  --pricing-method rule-v1 \
  --max-messages 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
rule-v1 published 4 quotes to msds682.demo07.ml-fare-quotes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07c-rule-v1/report.json
```

#### Run 2: ridge-v2 candidate

```bash
python demo07c_confluent_fare_quote_processor.py \
  --run-id "$RUN_ID" \
  --pricing-method ridge-v2 \
  --model-artifact "outputs/runs/$RUN_ID/demo07a/ridge-cost-v2.json" \
  --max-messages 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
ridge-v2 published 4 quotes to msds682.demo07.ml-fare-quotes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07c-ridge-v2/report.json
```

Each quote processor follows:

```text
consume request
-> validate Avro and Pydantic contract
-> call routing provider once
-> execute pricing version
-> produce quote
-> wait for broker acknowledgement
-> commit Kafka input offset
```

Here, `commit` means a **consumer offset commit**. It does not mean a Git
commit, and a Kafka producer does not commit these input offsets.

### 07D: publish delayed outcomes

**Objective:** Simulate and publish the delayed trip outcomes that conceptually
arrive after pricing.

**Why:** A quote is a prediction. Only a later outcome supplies the label needed
to measure realized markup.

07D does not consume the requests topic, observe a vehicle, or sleep for the
simulated duration. It independently recreates the same synthetic requests,
uses the same declared routing mode as 07C, and applies stable `trip_id`-based
realization adjustments.

**Done when:** Four outcomes are acknowledged for the same four `trip_id`
values and the same routing mode used by 07C.

Run this after both quote versions:

```bash
python demo07d_produce_trip_outcomes.py \
  --run-id "$RUN_ID" \
  --count 4 \
  --routing-mode "$ROUTING_MODE"
```

Expected terminal lines:

```text
Published 4 delayed outcomes to msds682.demo07.ml-trip-outcomes-avro.v1
Secret-free report: outputs/runs/$RUN_ID/demo07d/report.json
```

### 07E: join and evaluate

**Objective:** Collect and validate the complete bounded set of quotes and
outcomes, join them by `trip_id`, and publish one evaluation event per trip and
model version.

**Why:** Subscribing to two topics does not perform a join. The evaluator must
hold bounded keyed state until the expected set contains both quote versions
and one outcome for every trip. It then emits all eight evaluations. A
continuous production join could emit each eligible pair as it arrives; this
finite classroom implementation deliberately makes the complete state visible
first.

**Done when:** Eight pairs produce eight acknowledged evaluations, then the
evaluator commits progress for both input topics.

```bash
python demo07e_confluent_quote_outcome_evaluator.py \
  --run-id "$RUN_ID" \
  --expected-trips 4
```

Expected terminal lines:

```text
Published 8 evaluations to msds682.demo07.ml-pricing-evaluations-avro.v1
Model summary: {...}
Secret-free report: outputs/runs/$RUN_ID/demo07e/report.json
```

### 07F: compare and recommend

**Objective:** Compare both versions against the 20% realized-markup target and
make a reproducible model selection recommendation.

**Why:** A model version should be selected from shared outcome evidence and a
declared business metric, not from its name or training status.

The primary comparison metric is:

$$
\operatorname{MAE}_{\text{markup}}
= \frac{1}{N}\sum_{i=1}^{N}
\left|\text{realized markup}_{i} - 20\%\right|
$$

The report expresses this error in percentage points.

**Done when:** The comparison uses the same four outcomes, reports both errors,
and recommends whichever version has lower error under the published selection
rule.

```bash
python demo07f_compare_models.py --run-id "$RUN_ID"
```

Expected terminal lines:

```text
Recommended version: ridge-v2 | baseline error=... pp | candidate error=... pp
Recommendation: prefer ridge-v2 for this teaching comparison
Secret-free report: outputs/runs/$RUN_ID/demo07f/report.json
```

## 11. Classroom boundary and production gaps

This is a complete **teaching loop**, not a production-complete ML platform.
In production, the quote processor would poll continuously and each new request
would produce a quote. The classroom scripts stop after fixed counts so every
record, offset, and output remains inspectable.

| Layer | Coverage |
|---|---|
| Event source and ingestion | Complete for the demo |
| Kafka event log | Complete |
| Contract governance | Complete |
| Stateless route feature transformation | Complete |
| Stateful stream processing | Partial: bounded in-memory quote-outcome join |
| Online model inference | Complete for one small trained model |
| Prediction event | Complete |
| Outcome and delayed labels | Complete |
| Monitoring and drift | Partial: finite metrics, no continuous monitor |
| Evaluation and retraining | Partial: initial training and evaluation, no retraining loop |
| Model versioning and selection | Partial: artifact plus finite teaching recommendation |

The processing pattern is **at least once**, not exactly once. Output
acknowledgement before input commit protects against lost output, but a crash
after the output acknowledgement and before the input commit can produce a
duplicate after restart. The bounded evaluator expects one complete, unique
set of classroom events; it does not implement durable deduplication or
late/missing-event recovery.

Not covered: durable join state, event-time and late-event policy, continuous
monitoring, model registry and rollback, controlled deployment, automatic
retraining, or a production routing service.

## 12. Evidence and common mistakes

Secret-free reports are written under:

```text
outputs/runs/$RUN_ID/
├── demo07a/report.json
├── demo07a/ridge-cost-v2.json
├── demo07b/report.json
├── demo07c-rule-v1/report.json
├── demo07c-ridge-v2/report.json
├── demo07d/report.json
├── demo07e/report.json
└── demo07f/report.json
```

Common mistakes:

- calling 20% markup a 20% profit margin;
- calling rule-v1 an ML model;
- committing an input before its output is acknowledged;
- calling acknowledgement-before-commit exactly-once processing;
- assuming subscribing to two topics automatically performs a join;
- treating the bounded in-memory join as production durable state; or
- publishing `.env`, API keys, or raw credentials.

## 13. Completion checklist

- [ ] Credential-free tests pass.
- [ ] Ridge artifact has two named features and a validation MAE.
- [ ] Four request events are acknowledged.
- [ ] rule-v1 and ridge-v2 each publish four quotes.
- [ ] Four delayed outcomes are acknowledged.
- [ ] The evaluator joins by `trip_id`, publishes eight evaluations, and then commits.
- [ ] The comparison reports both errors and recommends the lower-error version.
- [ ] You can explain the Architecture A/B trade-off and name the remaining production gaps.

## 14. Cleanup

After class:

1. stop every Python process;
2. inspect the four Demo 07 topics and four Schema Registry subjects;
3. save only secret-free reports you need;
4. delete the Demo 07 cloud resources if they are no longer needed; and
5. continue monitoring Confluent Cloud cost and credits.
