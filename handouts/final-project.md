# Final Project: Reproducible Streaming Data Product

Build a small Kafka-based streaming data product that solves one clear problem,
produces one useful result, and includes one bounded AI-related element or
AI-assisted workflow.

The goal is not to use the largest number of tools. The goal is to build one
complete path that another person can understand, review, and verify.

## Milestones

| Deliverable | Course weight | Deadline |
|---|---:|---|
| Project proposal | 10% | Tuesday, August 4, 2026 at 11:59 PM PDT |
| Written report and code | 20% | Friday, August 14, 2026 at 11:59 PM PDT |
| Presentation | 20% | Timing and format will be confirmed on Canvas |
| **Final Project total** | **50%** | |

Canvas is the official submission platform.

Late policy for project deliverables:

- Up to 1 day late: 10% deduction.
- Up to 2 days late: 20% deduction.
- Day 3 or later: not accepted and receives zero credit.

## Core requirements

Your project must show this minimum path:

```text
Data source or deterministic replay
                |
                v
Validated event contract
                |
                v
Producer, poller, connector, or replay script
                |
                v
Kafka topic or topics
                |
                v
Consumer or stream processor
                |
                v
Useful output
                |
                v
Validation or evaluation evidence
```

The project must:

1. Define a clear problem, target user, and useful output.
2. Document the data source, access rules, schema, and limitations.
3. Use a coherent Kafka event path with visible contracts and ownership.
4. Produce a real artifact such as an alert, metric, report, score, structured
   file, dashboard-ready dataset, or validated event.
5. Include one bounded AI-related element or AI-assisted workflow.
6. Include repeatable tests, metrics, acceptance checks, or an evaluation
   harness.
7. Provide a complete review path: full reviewer access when cloud services are
   part of the core path, or a locally runnable minimum demo for a non-cloud
   path.

Batch, realtime, and hybrid inputs are all acceptable. A historical file may
enter Kafka record by record through a producer or replay script. A notebook
that reads the entire file and produces one chart without a streaming path is
not sufficient.

## Platform and review path

You may use:

- Confluent Cloud;
- GCP or another cloud provider;
- a local Kafka-compatible setup on one computer.

No specific cloud provider is required. Use the review path that matches the
project:

- **Cloud-based core path:** Give the course reviewer full access to every
  required service and resource for review. Include the code, pinned
  dependencies, resource and configuration inventory, setup steps, sample
  input, expected output, validation evidence, and cleanup instructions. The
  reviewer is not required to reproduce cloud-only services on a local
  computer.
- **Non-cloud path:** Provide a minimum end-to-end demo that the course reviewer
  can run locally with pinned dependencies, sample or replay data, one clear run
  command, expected output, and validation evidence.

A local or cached fallback for a cloud project is strongly recommended when
practical, but it is not required when the submitted cloud review path is fully
accessible and verifiable.

If the project depends on cloud services, provide:

- code and pinned dependencies;
- a resource inventory with required topics, buckets, endpoints, and services;
- full course-reviewer access to the required services and resources;
- sample data and a minimum success path;
- expected output and cleanup instructions;
- a local or cached fallback when practical.

Do not place credentials in the submitted code or documents. Keep secrets
outside source code, exclude `.env`, and provide a blank `.env.example` when
environment variables are required. Grant cloud access through the provider's
normal account, project, or role mechanism rather than sharing a personal
password or secret.

Additional requirements by project type:

| Project type | Required review evidence |
|---|---|
| Cloud-based core path | Full reviewer access, code, resource/configuration inventory, setup, sample input/output, validation evidence, cleanup |
| Non-cloud/local path | Pinned dependencies, one run command, sample or replay data, output, validation |
| External API | Credential setup, rate-limit notes, cached sample, deterministic replay |
| Machine learning | Training and prediction data, preparation code, training code, model artifact, inference code, metrics |
| Dashboard | Startup and access steps, expected view, data path, screenshot or fallback output |

## AI requirement

Every project must include one bounded AI-related element or AI-assisted
workflow. Examples include classification, summarization, RAG, evaluation,
memory, or disclosed and verified AI-assisted development.

Show:

- the task AI owns;
- representative input and output;
- what you accepted or rejected;
- how you verified the result;
- known limitations and a fallback method.

AI may accelerate the project, but it may not replace your ownership of the
design, code, testing, or explanation.

## Project proposal

Use the [Final Project Proposal Template](#/handouts/final-project-proposal-template)
and review the [10-Point Proposal Rubric](#/handouts/final-project-proposal-rubric).
The proposal is a feasibility contract, not a finished implementation.
If you need a starting point, review the
[Final Project Ideas](#/handouts/final-project-ideas).

Submit one PDF named:

```text
final_project_proposal_<usf_username>.pdf
```

For a two-person team, submit one shared proposal PDF that lists both students.
Name the file:

```text
final_project_proposal_<username1>_<username2>.pdf
```

If Canvas has not linked the students as a group, both students upload the same
PDF. List both students and their planned responsibilities. Contributions to
the proposal and final project must remain approximately 50-50, and each
student must be able to explain the complete design.

The proposal must contain no more than **550 words** and no more than **2 pages**.
Ideally, use 1 page. The required architecture sketch is included in the page
limit.

### Required proposal content

1. Problem summary, target user, intended result, and course-sized scope.
2. Planned data source, access limitations, batch, realtime, or hybrid
   classification, and the applicable cloud-access or local-demo review path.
3. Architecture sketch separating the realtime streaming layer from other
   components.
4. Planned tools and packages with one responsibility for each.
5. Minimum end-to-end result, review path, feasibility risks, fallbacks, and
   milestones.
6. Individual or approximately 50-50 team contribution plan, planned AI role,
   and AI-use disclosure.

Every proposal must identify the bounded AI-related element or AI-assisted
workflow planned for the final project. If AI assisted with preparation of the
proposal, also name the tool, task, and verification method. If no AI was used
to prepare the proposal, state that explicitly.

### Proposal rubric: 10 points

The shared proposal rubric contains five 2-point buckets and ten independently
scored 1-point criteria:

1. Problem and intended result.
2. Data source and classification.
3. Architecture sketch.
4. Tools and feasibility.
5. Contribution and AI disclosure.

See the published [10-Point Proposal Rubric](#/handouts/final-project-proposal-rubric)
for the atomic pass conditions.

## Final submission structure

Submit one ZIP file:

```text
final_project_<usf_username>.zip
```

For a two-person team, submit one shared ZIP:

```text
final_project_<username1>_<username2>.zip
```

When opened, the ZIP must contain one top-level folder:

```text
final_project_<username_or_team>/
├── README.md
├── DATA_SOURCE.md
├── AI_USAGE.md
├── requirements.txt
├── .env.example
├── src/
├── data/
│   └── sample_or_replay_data
├── outputs/
│   └── representative_result
├── evaluation/
│   └── validation_or_eval_artifact
└── report.pdf
```

Equivalent organization is acceptable when the README clearly maps every
required item. Exclude credentials, virtual environments, caches, generated
dependency folders, and unrelated large files.

### Final package checklist

- [ ] The README gives setup, one minimum review path, expected output,
      validation, and cleanup.
- [ ] The data source file documents source, owner, link, access, rights,
      schema, rate limits, and replay.
- [ ] The AI usage file explains the AI task, evidence, student decisions,
      verification, and limitations.
- [ ] The code includes ingestion or replay, contracts, Kafka processing,
      output, and evaluation.
- [ ] Sample or replay data is included and contains no private information.
- [ ] A representative output artifact is included.
- [ ] A validation or evaluation artifact is included.
- [ ] Cloud resources and full course-reviewer access are documented when the
      core path is cloud-based.
- [ ] Individual contributions are documented for a two-person team.
- [ ] No credentials, `.env`, caches, or virtual environments are included.

## Collaboration

Projects may be completed individually or in a two-person team. Individual
projects are always allowed.

For a two-person project:

- keep contributions to the proposal and final project approximately 50-50;
- document each person's contributions;
- both students must understand the complete architecture;
- both students must be able to explain the code path, AI usage, and evaluation;
- if Canvas has not linked the team as a group, both students upload the same
  proposal and final ZIP.

## Final presentation

The presentation should explain:

1. the problem and target user;
2. the data and event contract;
3. the streaming architecture;
4. the minimum end-to-end result and review path;
5. the useful output and evaluation evidence;
6. the bounded AI element;
7. limitations, failures, and next steps.

Presentation timing and format will be confirmed on Canvas.
