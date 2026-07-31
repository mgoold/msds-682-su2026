# MSDS 682 Final Project Proposal Rubric

## Scope

The proposal is worth **10 points**. This rubric grades only the proposal. The
final project report, code, evidence, and presentation are evaluated separately
under the final-project rubric.

The proposal has **5 buckets worth 2 points each**. Each bucket contains two
atomic 1-point criteria.

## Atomic Scoring Rule

Score each atomic criterion independently:

- **1 point:** Clear, complete, internally consistent, and feasible.
- **0.5 points:** Present, but materially incomplete, unclear, or only partly
  feasible.
- **0 points:** Missing, contradictory, or not feasible as proposed.

Submission compliance is not a sixth scoring bucket. The submission must be one
PDF, contain no more than 550 words, and contain no more than 2 pages. Only
content within the first 550 words and first 2 pages will be graded. Visual
polish does not earn separate points.

## Rubric

| Bucket | Atomic criterion | Points |
|---|---|---:|
| **1. Problem and Intended Result** | **1.1 Problem summary:** Defines a specific problem and explains who needs the result or why it matters. | 1 |
| | **1.2 Intended result and scope:** Identifies a useful, observable output or supported decision and proposes a course-sized scope. | 1 |
| **2. Data Source and Classification** | **2.1 Planned data source:** Identifies the source, official URL, owner, access method, material limitations, and the applicable review path: full reviewer access for a cloud-based core path or a credible local sample/replay path for a non-cloud project. | 1 |
| | **2.2 Batch/realtime/hybrid classification:** Selects the correct classification and briefly justifies it using the proposed data flow. | 1 |
| **3. Architecture Sketch** | **3.1 Realtime data streaming layer:** Shows the applicable producer/poller/replay path, Kafka topic or event contract, schema validation, consumer or stream processor, and output. | 1 |
| | **3.2 Other components and boundaries:** Shows applicable ML models, dashboards, external tools, data generation, storage, or data-preparation/ETL components and clearly connects them to the streaming layer. | 1 |
| **4. Tools and Feasibility** | **4.1 Planned tools and packages:** Names the main tools/packages and maps each one to a specific project responsibility without unnecessary technology. | 1 |
| | **4.2 Feasibility plan:** Defines a minimum end-to-end result, the applicable cloud-access or local-demo review path, material risks, realistic milestones, and a credible fallback or scope-reduction plan. | 1 |
| **5. Contribution and AI Disclosure** | **5.1 Contribution plan:** For an individual project, clearly states individual ownership. For a two-person project, assigns meaningful responsibilities and presents a credible approximately 50-50 contribution plan covering both the proposal and final project. | 1 |
| | **5.2 AI element and disclosure:** Names the one bounded AI-related element or AI-assisted workflow planned for the final project, with its input/output boundary, verification method, and fallback, and discloses any AI used to prepare the proposal. A planned AI element is required and cannot be omitted; an explicit no-AI statement covers only the proposal-preparation disclosure. | 1 |
|  | **Total** | **10** |

## Scoring Clarifications

- Your proposal is graded on clarity and feasibility, not the number of
  technologies.
- Cloud deployment, additional topics, sophisticated dashboards, or multiple
  AI tools do not earn extra proposal points.
- A cloud-based core path does not need to be reproduced on the reviewer's
  computer when the proposal provides full reviewer access and a complete,
  verifiable review plan. A non-cloud path must include a locally runnable
  minimum demo. A local fallback for a cloud path is recommended when practical.
- A simple architecture can receive full credit when all required boundaries
  and data flows are clear.
- Only information included in the submitted proposal can earn credit; missing
  information will not be inferred from prior conversations, code repositories,
  or planned future work.
- For a two-person project, criterion 5.1 requires a meaningful,
  approximately equal contribution plan. A token role, a highly unequal split,
  or one partner completing the proposal alone does not satisfy the criterion.
- Criterion 5.2 covers two separate things. Naming the planned bounded AI
  element is required and cannot be satisfied by a no-AI statement. Disclosing
  whether AI helped write the proposal is a separate statement, and "No AI was
  used to prepare this proposal" is a valid answer for that part.
