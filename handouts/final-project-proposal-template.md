# MSDS 682 Final Project Proposal Template

## Submission Requirements

- **Submission:** One PDF through Canvas.
- **Length:** No more than **550 words** and no more than **2 pages**. Ideally, use **1 page**.
- **Architecture sketch:** Required and included within the page limit.
- **Team size:** Individual or a team of up to 2 students.

If you work with a partner, contributions to the entire final project, including
this proposal, must be approximately 50-50. Both partners must contribute to the
proposal and be able to explain the complete project. Clearly describe each
partner's responsibilities below.

Replace the prompts in brackets with concise proposal content.

---

## Project Information

**Project title:** [Concise working title]

**Student name(s):** [Name(s)]

**Project format:** [Individual / Two-person team]

**Contribution plan:**

[For an individual project, state that you own all components. For a two-person
team, describe each partner's responsibilities and explain how the overall
contribution, including this proposal, will remain approximately 50-50.]

## 1. Problem Summary

[What specific problem will the project address? Who needs the result, and what
useful output or decision will the project support? Keep the proposed scope
small enough to complete and demonstrate during this course.]

## 2. Planned Data Source and Classification

- **Data source and official URL:** [Source name and link]
- **Data owner:** [Organization or project]
- **Classification:** [Batch / Realtime / Hybrid]
- **Why this classification applies:** [One concise explanation]
- **Access and limitations:** [API key, rate limit, license, cost, privacy, or
  availability concerns]
- **Review path:** [For a cloud-based core path, explain how the course reviewer
  will receive full access to the required services and resources. For a
  non-cloud path, name the cached sample, fixture, or deterministic replay used
  by the locally runnable minimum demo.]

## 3. Architecture Sketch

Insert a compact diagram showing the complete data flow. Clearly distinguish
the following two areas.

### Realtime Data Streaming Layer

Show the applicable path and label the direction of data movement:

```text
data source or generator
→ producer, poller, or replay process
→ Kafka topic(s) and event key
→ schema contract and validation
→ consumer and/or stream processor
→ output event or artifact
```

### Other Components

Show any other components that the project needs and how they connect to the
streaming layer. Examples include:

- data generation and simulation details;
- data preparation, transformation, or ETL;
- machine-learning models or inference services;
- analytical dashboards, reports, or notebooks;
- databases or file storage;
- external APIs, tools, or services.

Label which parts are batch, realtime, or hybrid. The diagram may be simple,
but every component and arrow should have a clear purpose.

## 4. Planned Tools and Packages

[List the main tools and packages and map each one to a project responsibility.
Examples might include Python 3.11, `confluent-kafka`, Pydantic, pandas,
scikit-learn, FastAPI, or a visualization package. Include only tools you
currently expect to use.]

## 5. Feasibility Risks and Plan

- **Minimum end-to-end result:** [What is the smallest complete result that can
  be inspected? State whether the reviewer reaches it through full cloud access
  or a locally runnable minimum demo.]
- **Primary risks:** [Data access, credentials, cost, time, technical
  complexity, data quality, privacy, or integration risks]
- **Fallback plan:** [How will you reduce scope or use sample/replay data if the
  preferred plan fails? A local fallback for a cloud path is recommended when
  practical, but it is not required when full cloud review access is provided.]
- **Milestones:** [Brief sequence from initial pipeline to final validation]

## 6. AI Element and Disclosure

### Planned bounded AI element (required)

Every final project must include one bounded AI-related element or AI-assisted
workflow, so every proposal must name the one you plan to build:

- **Planned AI element:** [Classification, summarization, RAG, evaluation,
  memory, or disclosed and verified AI-assisted development]
- **Input and output boundary:** [What goes in, what comes out]
- **Verification method:** [Tests, metrics, source checks, failure cases, or
  human review]
- **Fallback:** [What the project does when the AI element is unavailable or
  its output is rejected]

"No AI" is not an option for this section.

### AI use in preparing this proposal (disclosure)

If AI helped you write this proposal, identify:

- the AI tool(s) or model(s);
- which parts AI helped with;
- which parts were completed by the student or team;
- how AI-generated output was verified, tested, rejected, or modified.

Follow the same AI-use disclosure expectations used in the course assignments.
If no AI was used to prepare this proposal, state: **"No AI was used to prepare
this proposal."**
