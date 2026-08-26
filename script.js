const pages = {
  "/": {
    title: "Course Overview",
    body: `
      <p class="lede">MSDS 682 equips students to process continuous data streams at scale and in real time.</p>
      <p>This hands-on course teaches students how to process and analyze real-time data streams using Apache Kafka, Python, Confluent tools, and related data engineering workflows. Students will produce and consume Kafka messages, build streaming workflows, interpret streaming outputs, and use GitHub for code management and collaboration.</p>

      <div class="notice">
        Each lecture meets 5:30–7:20 PM PDT. Monday sessions meet on Zoom; Thursday sessions meet in person at 101 Howard, Classroom 529.
      </div>

      <h3>Logistics</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Course</strong><span>MSDS 682-01 (30398), Data Stream Processing</span></div>
        <div class="meta-row"><strong>Term</strong><span>Summer 2026</span></div>
        <div class="meta-row"><strong>Instructor</strong><span>Jeremy W. Gu · <a href="mailto:wgu9@usfca.edu">wgu9@usfca.edu</a></span></div>
        <div class="meta-row"><strong>Course assistant</strong><span>Annie Chiu · <a href="mailto:ychiu14@usfca.edu">ychiu14@usfca.edu</a></span></div>
        <div class="meta-row"><strong>Meeting pattern</strong><span>Mon Zoom · Thu in person, 101 Howard Classroom 529 · 5:30–7:20 PM PDT</span></div>
        <div class="meta-row"><strong>Canvas link</strong><span><a href="https://usfca.instructure.com/courses/1633704" target="_blank" rel="noopener">Course Canvas page</a>; USF login may be required.</span></div>
        <div class="meta-row"><strong>Zoom link</strong><span>Please see the calendar invite or Canvas.</span></div>
        <div class="meta-row"><strong>Piazza</strong><span><a href="https://piazza.com/usfca/summer2026/msds682" target="_blank" rel="noopener">Course Q/A and discussion forum</a>.</span></div>
        <div class="meta-row"><strong>Submissions</strong><span>Canvas is the official submission platform; GitHub is used for code management and collaboration.</span></div>
      </div>

    `
  },
  "/schedule": {
    title: "Schedule",
    body: `
      <p class="lede">Each lecture meets 5:30–7:20 PM PDT. Monday sessions are on Zoom; Thursday sessions are in person at 101 Howard, Classroom 529.</p>
      <div class="table-wrap" tabindex="0" role="region" aria-label="Summer 2026 course schedule">
        <table class="schedule-table">
          <caption class="sr-only">Summer 2026 course schedule</caption>
          <thead>
            <tr>
              <th scope="col">#</th>
              <th scope="col">Date</th>
              <th scope="col">Format</th>
              <th scope="col">Topic</th>
              <th scope="col">Work and deadlines</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td><strong>Mon · Jul 06</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Course Introduction and Environment Setup</td>
              <td>Course introduction and Demo 00 environment setup</td>
            </tr>
            <tr>
              <td>2</td>
              <td><strong>Thu · Jul 09</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Kafka Topics and Producers</td>
              <td>Demo 01 topic creation and Demo 02 producers</td>
            </tr>
            <tr>
              <td>3</td>
              <td><strong>Mon · Jul 13</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Kafka Consumers</td>
              <td>Consumer concepts and Demo 03A–03D on Confluent Cloud</td>
            </tr>
            <tr>
              <td>4</td>
              <td><strong>Thu · Jul 16</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Data Contracts and Streaming Architecture</td>
              <td>Pydantic, Avro, Schema Registry, and Demo 04A–04D; Assignment 1 extended to Jul 21, 11:59 PM PDT</td>
            </tr>
            <tr>
              <td>5</td>
              <td><strong>Mon · Jul 20</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Streaming APIs and Data Schemas Pt. 2</td>
              <td>FastAPI, REST API, and schema-aware producer/consumer integration</td>
            </tr>
            <tr>
              <td>6</td>
              <td><strong>Thu · Jul 23</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Kafka Connect and Stream Processing</td>
              <td>Connectors and demo; Assignment 2 released Jul 23 and due Jul 31, 11:59 PM PDT</td>
            </tr>
            <tr>
              <td>7</td>
              <td><strong>Mon · Jul 27</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Lecture 7A/7B: Final Project and Stateful Stream Processing</td>
              <td>Project scope and proposal contract, followed by joins, delayed outcomes, and stateful correctness</td>
            </tr>
            <tr>
              <td>8</td>
              <td><strong>Thu · Jul 30</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Lecture 8: Project Systems and End-to-End Architecture</td>
              <td>Proposal scope through 2023 examples, an insurance document-to-answer system, and a minimum compute-owner framework</td>
            </tr>
            <tr>
              <td>9</td>
              <td><strong>Mon · Aug 03</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Streaming SQL: Python, ksqlDB, and Flink SQL</td>
              <td>Review Lectures 6–8, then map Demo 07 to two SQL compute owners; project proposal due Tue Aug 4, 11:59 PM PDT</td>
            </tr>
            <tr>
              <td>10</td>
              <td><strong>Thu · Aug 06</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Lecture 10: Review Session and Final Presentation</td>
              <td>Course recap, Demo 07/09 synthesis, final-project checklist, and presentation requirements</td>
            </tr>
            <tr>
              <td>11</td>
              <td><strong>Mon · Aug 10</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Lecture 11A/11B: API Design and Interview Preparation</td>
              <td>Lecture 11A observable async API; Lecture 11B interview preparation and final-project questions</td>
            </tr>
            <tr>
              <td>12</td>
              <td><strong>Thu · Aug 13</strong><span class="table-secondary">5:30–7:20 PM PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Lecture 12: Final Project Presentations</td>
              <td><a href="#/handouts/lec12-final-project-presentations">Lecture 12 slides</a>; <a href="#/handouts/final-presentation">Final Presentation Schedule and Rubrics</a>; report/code due Aug 14, 11:59 PM PDT</td>
            </tr>
          </tbody>
        </table>
      </div>
    `
  },
  "/assignments": {
    title: "Assignments",
    body: `
      <p class="lede">There are two individual assignments and one final project. <a href="https://usfca.instructure.com/courses/1633704" target="_blank" rel="noopener">Canvas</a> is the official submission platform; submit the proposal as one PDF and the assignment/final packages in their required formats.</p>
      <div class="assignment-list">
        <article class="assignment-card">
          <h3>Assignment 1</h3>
          <p>Use real Confluent Cloud Kafka to complete Demo 02A–02D: sync-style producer, async producer, performance comparison, and serialization. The assignment is worth 20% of the course grade and is graded out of 20 base points, with up to 3 extra-credit points. Disclose AI assistance if used.</p>
          <p><span class="tag">Extended deadline: Tue Jul 21, 2026 · 11:59 PM PDT · 20 points + up to 3 extra credit · 20% course weight</span></p>
          <p><a class="download-link" href="#/handouts/assignment01">Open Assignment 1</a> · <a href="handouts/assignment01-starter.zip">Download student starter</a></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2</h3>
          <p>Use an independent real Confluent topic to connect a FastAPI input boundary to Avro and Schema Registry, then implement a bounded consumer with strict validation, process-before-commit, same-group resume, and explicit replay. This is the final homework assignment. It is graded out of 20 base points, with up to 3 extra-credit points.</p>
          <p><span class="tag">Released Thu Jul 23 · Due Fri Jul 31, 2026 · 11:59 PM PDT · 20 points + up to 3 extra credit · 20% course weight</span></p>
          <p><a class="download-link" href="#/handouts/assignment02">Open Assignment 2</a> · <a href="handouts/assignment02-starter.zip">Download student starter</a></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project Proposal</h3>
          <p>Submit one PDF of no more than 550 words and no more than 2 pages; ideally, use 1 page. The proposal is worth 10 points and 10% of the course grade. Projects may be completed individually or in a two-person team. For a team, contributions to the proposal and final project must remain approximately 50-50.</p>
          <div class="milestone-list">
            <div><strong>Proposal</strong><span>Due Tue Aug 4, 2026 · 11:59 PM PDT · 10% course weight</span></div>
            <div><strong>Report/code</strong><span>Due Aug 14, 2026 · 11:59 PM PDT</span></div>
            <div><strong>Presentation</strong><span><a href="#/handouts/final-presentation">Open the schedule and rubrics</a></span></div>
          </div>
          <p><a class="download-link" href="#/handouts/final-project-proposal-template">Open proposal template</a> · <a href="#/handouts/final-project-proposal-rubric">Open 10-point proposal rubric</a> · <a href="#/handouts/final-project">Open full project requirements</a></p>
        </article>
      </div>

      <h3>Submission and collaboration</h3>
      <ul>
        <li><a href="https://usfca.instructure.com/courses/1633704" target="_blank" rel="noopener">Canvas course page</a>; USF login may be required.</li>
        <li><strong>Late policy for homework and project deliverables:</strong> up to 1 day late receives a 10% deduction; up to 2 days late receives a 20% deduction; day 3 or later is not accepted and receives zero credit.</li>
        <li>GitHub is used for code management, collaboration, and portfolio development.</li>
        <li>AI tools, coding agents, open-source resources, and online references are permitted with clear attribution. Students must understand and verify submitted work and use another method when an AI tool cannot resolve the problem reliably.</li>
        <li>For two-person final projects, contributions, including the proposal, must be approximately 50-50. Each student must document contributions and be able to explain the design, code, AI usage, and evaluation results.</li>
      </ul>
    `
  },
  "/syllabus": {
    title: "Syllabus",
    body: `
      <p class="lede">This page summarizes the syllabus and current course announcements. <a href="assets/msds-682-syllabus.pdf" target="_blank" rel="noopener">Download the original PDF syllabus</a>.</p>
      <div class="notice"><strong>Course update effective July 23:</strong> the project proposal is due Tuesday, August 4 at 11:59 PM PDT. The late-work policy below supersedes the earlier no-late-work wording in the original PDF.</div>

      <h3>Course Description</h3>
      <p>This class equips students with the skills necessary to process continuous data streams at scale and in real time. Students will use Apache Kafka, Python, Confluent tools, Git, GitHub, and related data engineering tools.</p>

      <h3>Prerequisites</h3>
      <ul>
        <li>Working knowledge of Python.</li>
        <li>Basic knowledge of SQL, data pipelines, statistics, and data analysis.</li>
        <li>Familiarity with pandas, data visualization, scikit-learn, and machine learning is helpful but not required.</li>
        <li>Comfort using notebooks, scripts, Git, and command-line tools.</li>
        <li>Familiarity with at least one AI coding assistant or coding agent is helpful; AI use must be documented and verified.</li>
      </ul>

      <h3>Course Learning Outcomes</h3>
      <ul>
        <li>Understand the Kafka ecosystem.</li>
        <li>Produce and consume Kafka messages using Python.</li>
        <li>Use Confluent tools for Kafka topic management and demos.</li>
        <li>Build real-time data ingestion and streaming analytics workflows.</li>
        <li>Interpret streaming outputs and extract insights.</li>
        <li>Use Git and GitHub for repositories, issues, pull requests, and collaboration.</li>
        <li>Use AI tools and open-source resources responsibly with proper credit.</li>
        <li>Present and explain technical project work clearly.</li>
      </ul>

      <h3>Assessment</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Attendance and professionalism</strong><span>10%</span></div>
        <div class="meta-row"><strong>Individual assignments</strong><span>40% total; two assignments worth 20% each</span></div>
        <div class="meta-row"><strong>Final project</strong><span>50% total; proposal 10%, written report/code 20%, final presentation 20%</span></div>
        <div class="meta-row"><strong>Exams</strong><span>There are no exams.</span></div>
      </div>

      <h3>Recommended texts</h3>
      <ul>
        <li><em>Kafka in Action</em>, Dylan Scott, Viktor Gamov, Dave Klein.</li>
        <li><em>Kafka: The Definitive Guide</em>, 2nd edition, Gwen Shapira, Todd Palino, Rajini Sivaram, Krit Petty.</li>
      </ul>

      <h3>Policies</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Attendance</strong><span>Mandatory attendance for every lecture.</span></div>
        <div class="meta-row"><strong>Laptops</strong><span>Please keep laptops closed during lecture unless instructed otherwise. During demos, exercises, or Python practice, laptops may be required.</span></div>
        <div class="meta-row"><strong>Late work</strong><span>Homework and project deliverables: up to 1 day late, 10% deduction; up to 2 days late, 20% deduction; day 3 or later, not accepted and zero credit.</span></div>
        <div class="meta-row"><strong>Generative AI</strong><span>AI tools are permitted with appropriate attribution. Students remain responsible for understanding and verifying the work, recognizing unreliable output, and switching prompts, context, tools, or non-AI methods when needed.</span></div>
      </div>
    `
  },
  "/staff": {
    title: "Staff",
    body: `
      <p class="lede">Course staff and office hours.</p>
      <div class="staff-grid">
        <article class="staff-card">
          <h3>Jeremy W. Gu</h3>
          <p>Instructor</p>
          <p><a href="mailto:wgu9@usfca.edu">wgu9@usfca.edu</a></p>
          <p>Office hours: Mondays 7:20–7:50 PM, right after class, on Zoom.</p>
        </article>
        <article class="staff-card">
          <h3>Annie Chiu</h3>
          <p>Course assistant</p>
          <p><a href="mailto:ychiu14@usfca.edu">ychiu14@usfca.edu</a></p>
          <p>Office hours: not currently scheduled; email for assistance.</p>
        </article>
      </div>
    `
  }
};

// Handout manifest. To add a handout: drop a file in `handouts/` and add a row
// here. Markdown handouts (kind: "md") render in-page with syntax-highlighted
// code blocks; HTML handouts (kind: "html") render in-page or in a standalone
// iframe; PDF handouts (kind: "pdf") open the file directly.
// Course-order registry for the Handouts page. This is the single owner of the
// section order and labels; each handout below references one section ID.
const handoutSections = [
  {
    id: "course",
    label: "Start here",
    title: "Course Information",
    summary: "Read the official syllabus before following the lecture materials in order."
  },
  {
    id: "lec1",
    label: "Lecture 1",
    title: "Environment Setup",
    summary: "Prepare and verify the local Python environment used throughout the course."
  },
  {
    id: "lec2",
    label: "Lecture 2",
    title: "Kafka Topics and Producers",
    summary: "Review the lecture slides and supplement, then create a topic, produce records, and complete Assignment 1."
  },
  {
    id: "lec3",
    label: "Lecture 3",
    title: "Kafka Consumers",
    summary: "Learn the consumer model first, then run the bounded Confluent Cloud consumer sequence."
  },
  {
    id: "lec4",
    label: "Lecture 4",
    title: "Data Contracts and Streaming Architecture",
    summary: "Work through Pydantic, Avro, and Schema Registry, followed by the architecture supplement."
  },
  {
    id: "lec5",
    label: "Lecture 5",
    title: "Streaming APIs with FastAPI and Kafka",
    summary: "Review FastAPI, then connect a validated HTTP request to an independent schema-aware Kafka round trip."
  },
  {
    id: "lec6",
    label: "Lecture 6",
    title: "Kafka Connect and Stream Processing",
    summary: "Move data into Kafka with Connect, then validate, derive, acknowledge, commit, resume, and replay."
  },
  {
    id: "lec7a",
    label: "Lecture 7A",
    title: "Final Project",
    summary: "Define the project contract, proposal, bounded AI element, review path, and required evidence."
  },
  {
    id: "lec7b",
    label: "Lecture 7B",
    title: "Stateful Stream Processing",
    summary: "Join fare quotes with delayed outcomes, then compare two pricing methods in Demo 07."
  },
  {
    id: "lec8",
    label: "Lecture 8",
    title: "Project Systems and End-to-End Architecture",
    summary: "Scope one complete project path, learn from 2023 examples, study an insurance document-to-answer system, and choose a compute owner from durable requirements."
  },
  {
    id: "lec9",
    label: "Lecture 9",
    title: "One Problem, Three Compute Owners",
    summary: "Keep Demo 07 fixed while comparing how Python, ksqlDB, and Flink SQL own the same join, evaluation, aggregation, and lifecycle."
  },
  {
    id: "lec10",
    label: "Lecture 10",
    title: "Review Session and Final Presentation",
    summary: "Review the course and prepare the final presentation through event, order, resume, contract, state and time, and output."
  },
  {
    id: "lec11a",
    label: "Lecture 11A",
    title: "API Design for Streaming Systems",
    summary: "Complete Demo 05 with an observable asynchronous API and connect the client contract to the Demo 09 compute owners."
  },
  {
    id: "lec11b",
    label: "Lecture 11B",
    title: "Interview Preparation",
    summary: "Turn the final project into relevant, concise, and verifiable evidence for applications, interviews, follow-up, offers, and resumes."
  },
  {
    id: "lec12",
    label: "Lecture 12",
    title: "Final Project Presentations",
    summary: "Use the published presentation format, 20-point rubric, frozen order, and final class schedule."
  }
];

function lectureIdentity(section) {
  const match = /^lec(\d+)([a-z]?)$/.exec(section.id);
  if (!match) return null;
  const number = Number(match[1]);
  const suffix = match[2].toUpperCase();
  const suffixOrder = suffix ? suffix.charCodeAt(0) - 64 : 0;
  return {
    code: `${number}${suffix}`,
    number,
    suffixOrder
  };
}

const lectureSectionsNewestFirst = handoutSections
  .filter((section) => lectureIdentity(section) !== null)
  .sort((a, b) => {
    const aIdentity = lectureIdentity(a);
    const bIdentity = lectureIdentity(b);
    return bIdentity.number - aIdentity.number
      || aIdentity.suffixOrder - bIdentity.suffixOrder;
  });

// All parts of the newest numbered lecture form one current teaching batch.
const latestLectureNumber = lectureSectionsNewestFirst.length
  ? lectureIdentity(lectureSectionsNewestFirst[0]).number
  : null;
const latestLectureSections = lectureSectionsNewestFirst.filter(
  (section) => lectureIdentity(section).number === latestLectureNumber
);
const earlierHandoutSections = [
  ...lectureSectionsNewestFirst.filter(
    (section) => lectureIdentity(section).number !== latestLectureNumber
  ),
  ...handoutSections.filter((section) => lectureIdentity(section) === null)
];

const handouts = [
  {
    slug: "syllabus",
    section: "course",
    category: "Syllabus",
    title: "Final Syllabus",
    kind: "pdf",
    file: "assets/msds-682-syllabus.pdf",
    createdAt: "Created at 4:36 PM PDT on July 3, 2026",
    lastUpdatedAt: "Last updated at 12:03 AM PDT on July 23, 2026",
    summary: "Official syllabus covering course outcomes, grading, policies, and schedule."
  },
  {
    slug: "lec1-slides",
    section: "lec1",
    category: "Slides",
    title: "Lecture 1: Course Introduction and Environment Setup",
    kind: "external",
    file: "https://docs.google.com/presentation/d/1Nl0_unoyMPKwfC8ZFaqxA1kC1oYU7lm1KPK7MiNrgWU/edit",
    createdAt: "Created at 3:46 PM PDT on July 23, 2026",
    lastUpdatedAt: "Last updated at 3:46 PM PDT on July 23, 2026",
    summary: "Course introduction, learning path, logistics, and the environment used throughout the course."
  },
  {
    slug: "demo00",
    section: "lec1",
    category: "Demo",
    title: "Demo 00: Environment Setup",
    kind: "md",
    file: "handouts/demo00.md",
    createdAt: "Created at 3:17 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 2:06 PM PDT on July 13, 2026",
    summary: "Create the Python environment, install dependencies, and verify the first local run."
  },
  {
    slug: "lec2-slides",
    section: "lec2",
    category: "Slides",
    title: "Lecture 2: Kafka Topics and Producers",
    kind: "external",
    file: "https://docs.google.com/presentation/d/1Z4BnBibYTy6Hwgvfc7IwidpbhqgmyrwSMtXLS--4eBw/edit?slide=id.g3f44cdb5d93_0_0#slide=id.g3f44cdb5d93_0_0",
    createdAt: "Created at 3:46 PM PDT on July 23, 2026",
    lastUpdatedAt: "Last updated at 3:46 PM PDT on July 23, 2026",
    summary: "Primary Lecture 2 slides covering Kafka topics, producers, delivery, and serialization."
  },
  {
    slug: "lec2-topic-vs-table",
    section: "lec2",
    category: "Supplement",
    title: "Lecture 2 Supplement: Kafka Topics and Producers",
    kind: "html",
    file: "handouts/lec2-topic-vs-table.html",
    createdAt: "Created at 1:48 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 3:46 PM PDT on July 23, 2026",
    wide: true,
    standalone: true,
    summary: "Topics, producers, serialization, real Confluent results, and client-side batching."
  },
  {
    slug: "demo01",
    section: "lec2",
    category: "Demo",
    title: "Demo 01: Create a Kafka Topic",
    kind: "md",
    file: "handouts/demo01.md",
    createdAt: "Created at 4:46 PM PDT on July 6, 2026",
    lastUpdatedAt: "Last updated at 2:06 PM PDT on July 13, 2026",
    summary: "Create the shared Confluent Cloud topic safely with AdminClient and secret-free evidence."
  },
  {
    slug: "demo02",
    section: "lec2",
    category: "Demo",
    title: "Demo 02: Kafka Producer",
    kind: "md",
    file: "handouts/demo02.md",
    createdAt: "Created at 3:49 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 3:53 PM PDT on July 23, 2026",
    summary: "Run sync-style and async producers, benchmark delivery, and serialize trip events."
  },
  {
    slug: "assignment01",
    section: "lec2",
    category: "Assignment",
    title: "Assignment 1: Confluent Cloud Kafka Producer Performance Analysis",
    kind: "md",
    file: "handouts/assignment01.md",
    createdAt: "Created at 12:24 PM PDT on July 13, 2026",
    lastUpdatedAt: "Last updated at 1:31 PM PDT on July 13, 2026",
    summary: "Real-Confluent producer benchmark using Demo 02A–02D, plus AI-use disclosure."
  },
  {
    slug: "lec3-consumers",
    section: "lec3",
    category: "Slides",
    title: "Lecture 3: Kafka Consumers",
    kind: "html",
    file: "handouts/lec3-consumers.html",
    createdAt: "Created at 4:09 PM PDT on July 13, 2026",
    lastUpdatedAt: "Last updated at 3:46 PM PDT on July 23, 2026",
    wide: true,
    standalone: true,
    summary: "Consumers, offsets, commits, groups, replay, batching, and native asyncio."
  },
  {
    slug: "demo03",
    section: "lec3",
    category: "Demo",
    title: "Demo 03: Kafka Consumers on Confluent Cloud",
    kind: "md",
    file: "handouts/demo03.md",
    createdAt: "Created at 1:57 PM PDT on July 13, 2026",
    lastUpdatedAt: "Last updated at 2:55 PM PDT on July 16, 2026",
    summary: "Bounded consumers, manual commits, group replay, and native asyncio on Confluent Cloud."
  },
  {
    slug: "lec4-data-contracts",
    section: "lec4",
    category: "Slides",
    title: "Lecture 4: Data Contracts, Avro, and Schema Registry",
    kind: "html",
    file: "handouts/lec4-data-contracts.html",
    createdAt: "Created at 5:06 PM PDT on July 16, 2026",
    lastUpdatedAt: "Last updated at 5:18 PM PDT on July 16, 2026",
    wide: true,
    standalone: true,
    summary: "Schemas, validation layers, Avro, Schema Registry, compatible evolution, Demo 04, and a ridesharing architecture case study."
  },
  {
    slug: "demo04",
    section: "lec4",
    category: "Demo",
    title: "Demo 04: Data Contracts, Avro, and Schema Registry",
    kind: "md",
    file: "handouts/demo04.md",
    createdAt: "Created at 2:35 PM PDT on July 16, 2026",
    lastUpdatedAt: "Last updated at 3:53 PM PDT on July 16, 2026",
    summary: "Pydantic validation, Avro serialization, Schema Registry, and bounded Cloud extensions."
  },
  {
    slug: "lec4-ridesharing-architecture",
    section: "lec4",
    category: "Supplement",
    title: "Lecture 4 Supplement: Ridesharing Streaming Architecture",
    kind: "md",
    file: "handouts/lec4-ridesharing-architecture-supplement.md",
    createdAt: "Created at 2:35 PM PDT on July 16, 2026",
    lastUpdatedAt: "Last updated at 2:55 PM PDT on July 16, 2026",
    wide: true,
    summary: "Ridesharing case study on groups, derived topics, joins, replay, and trade-offs."
  },
  {
    slug: "lec5-streaming-apis",
    section: "lec5",
    category: "Slides",
    title: "Lecture 5: Streaming APIs with FastAPI and Kafka",
    kind: "html",
    file: "handouts/lec5-streaming-apis.html",
    createdAt: "Created at 5:29 PM PDT on July 20, 2026",
    lastUpdatedAt: "Last updated at 4:19 PM PDT on July 23, 2026",
    wide: true,
    standalone: true,
    summary: "FastAPI application boundaries, schema-aware Kafka delivery, consumer processing, Demo 05, and a stateful idling-classification case study."
  },
  {
    slug: "fastapi-recap",
    section: "lec5",
    category: "Recap",
    title: "FastAPI Recap for Demo 05",
    kind: "md",
    file: "handouts/fastapi-recap.md",
    createdAt: "Created at 4:48 PM PDT on July 20, 2026",
    lastUpdatedAt: "Last updated at 4:48 PM PDT on July 20, 2026",
    summary: "A concise review of routes, Pydantic request and response models, status codes, lifespan, and OpenAPI."
  },
  {
    slug: "demo05",
    section: "lec5",
    category: "Demo",
    title: "Demo 05: FastAPI and Schema-aware Kafka Integration",
    kind: "md",
    file: "handouts/demo05.md",
    createdAt: "Created at 4:48 PM PDT on July 20, 2026",
    lastUpdatedAt: "Last updated at 5:06 PM PDT on July 20, 2026",
    wide: true,
    summary: "Local FastAPI contracts plus a bounded, independent Confluent Cloud Avro round trip with expected-result screenshots."
  },
  {
    slug: "lec5-realtime-ml-examples",
    section: "lec5",
    category: "Supplement",
    title: "Lecture 5 Supplement: Kafka for Real-Time Risk and Machine Learning",
    kind: "html",
    file: "handouts/lec5-supplementary-real-time-ml.html",
    createdAt: "Created at 5:17 PM PDT on July 20, 2026",
    lastUpdatedAt: "Last updated at 5:28 PM PDT on July 20, 2026",
    wide: true,
    standalone: true,
    summary: "Three anonymized examples compare real-time Kafka scoring, batch account risk, and account takeover detection."
  },
  {
    slug: "lec6-kafka-connect-stream-processing",
    section: "lec6",
    category: "Slides",
    title: "Lecture 6: Kafka Connect and Stream Processing",
    kind: "html",
    file: "handouts/lec6-kafka-connect-stream-processing.html",
    createdAt: "Created at 11:00 PM PDT on July 22, 2026",
    lastUpdatedAt: "Last updated at 4:29 PM PDT on July 23, 2026",
    wide: true,
    standalone: true,
    summary: "Choose the right integration boundary, operate a managed connector, and prove bounded processing, commit, resume, and replay."
  },
  {
    slug: "demo06",
    section: "lec6",
    category: "Demo",
    title: "Demo 06: Kafka Connect and Bounded Stream Processing",
    kind: "md",
    file: "handouts/demo06.md",
    createdAt: "Created at 11:00 PM PDT on July 22, 2026",
    lastUpdatedAt: "Last updated at 4:25 PM PDT on July 23, 2026",
    wide: true,
    summary: "Managed source integration, schema-aware inspection, output-before-commit processing, resume, and replay."
  },
  {
    slug: "assignment02",
    section: "lec6",
    category: "Assignment",
    title: "Assignment 2: Schema-Aware Kafka Consumer Application",
    kind: "md",
    file: "handouts/assignment02.md",
    createdAt: "Created at 11:01 PM PDT on July 22, 2026",
    lastUpdatedAt: "Last updated at 4:25 PM PDT on July 23, 2026",
    summary: "Independent real-Confluent FastAPI-to-Avro input plus bounded validation, commit, resume, and replay."
  },
  {
    slug: "lec7a-final-project",
    section: "lec7a",
    category: "Slides",
    title: "Lecture 7A: Final Project",
    kind: "html",
    file: "handouts/lec7a-final-project.html",
    createdAt: "Created at 4:06 PM PDT on July 27, 2026",
    lastUpdatedAt: "Last updated at 1:20 PM PDT on August 8, 2026",
    wide: true,
    standalone: true,
    summary: "Define a feasible streaming project, proposal contract, bounded AI element, review path, and visible evidence."
  },
  {
    slug: "final-project",
    section: "lec7a",
    category: "Project",
    title: "Final Project Requirements",
    kind: "md",
    file: "handouts/final-project.md",
    createdAt: "Created at 4:01 PM PDT on July 27, 2026",
    lastUpdatedAt: "Last updated at 3:00 PM PDT on August 8, 2026",
    wide: true,
    summary: "Student-facing contract for the proposal, streaming product, bounded AI element, review path, final package, and presentation."
  },
  {
    slug: "final-project-ideas",
    section: "lec7a",
    category: "Ideas",
    title: "Final Project Ideas",
    kind: "md",
    file: "handouts/final-project-ideas.md",
    createdAt: "Created at 4:05 PM PDT on July 27, 2026",
    lastUpdatedAt: "Last updated at 5:16 PM PDT on July 27, 2026",
    wide: true,
    summary: "Eight course-sized project directions with public or synthetic data, a Kafka path, bounded AI, evaluation, and replay."
  },
  {
    slug: "final-project-proposal-template",
    section: "lec7a",
    category: "Template",
    title: "Final Project Proposal Template",
    kind: "md",
    file: "handouts/final-project-proposal-template.md",
    createdAt: "Created at 3:54 PM PDT on July 27, 2026",
    lastUpdatedAt: "Last updated at 5:07 PM PDT on July 27, 2026",
    wide: true,
    download: {
      filename: "MSDS-682-Final-Project-Proposal-Template.md",
      label: "Download Markdown template (.md)"
    },
    summary: "One-PDF proposal structure covering the problem, data source, streaming architecture, tools, feasibility, contributions, required AI element, and disclosure."
  },
  {
    slug: "final-project-proposal-rubric",
    section: "lec7a",
    category: "Rubric",
    title: "Final Project Proposal Rubric",
    kind: "md",
    file: "handouts/final-project-proposal-rubric.md",
    createdAt: "Created at 3:54 PM PDT on July 27, 2026",
    lastUpdatedAt: "Last updated at 5:16 PM PDT on July 27, 2026",
    wide: true,
    summary: "Student-facing proposal rubric with five 2-point buckets and ten independently scored criteria."
  },
  {
    slug: "final-project-rubric",
    section: "lec7a",
    category: "Rubric",
    title: "Final Written Report and Code Rubric",
    kind: "md",
    file: "handouts/final-project-rubric.md",
    createdAt: "Created at 1:16 PM PDT on August 8, 2026",
    lastUpdatedAt: "Last updated at 2:32 PM PDT on August 8, 2026",
    wide: true,
    summary: "Four 5-point final-package categories: 20 base points plus three optional package-only bonus points."
  },
  {
    slug: "lec7b-stateful-stream-processing",
    section: "lec7b",
    category: "Slides",
    title: "Lecture 7B: Stateful Stream Processing",
    kind: "html",
    file: "handouts/lec7b-stateful-stream-processing.html",
    createdAt: "Created at 4:26 PM PDT on July 23, 2026",
    lastUpdatedAt: "Last updated at 5:31 PM PDT on July 27, 2026",
    wide: true,
    standalone: true,
    summary: "Join fare quotes with delayed outcomes, compare two pricing methods, and define the state, time, recovery, and delivery policies."
  },
  {
    slug: "demo07",
    section: "lec7b",
    category: "Demo",
    title: "Demo 07: Real-Time Pricing, Delayed Outcomes, and Model Evaluation",
    kind: "md",
    file: "handouts/demo07.md",
    createdAt: "Created at 3:38 AM PDT on July 23, 2026",
    lastUpdatedAt: "Last updated at 4:13 PM PDT on July 27, 2026",
    wide: true,
    summary: "Compare a rule baseline with a trained cost model, then join fare quotes to delayed outcomes and evaluate the 20% markup target."
  },
  {
    slug: "lec8-system-design",
    section: "lec8",
    category: "Slides",
    title: "Lecture 8: Project Systems and End-to-End Architecture",
    kind: "html",
    file: "handouts/MSDS682-Lecture8-Three-Part-Index.html",
    createdAt: "Created at 4:46 PM PDT on July 30, 2026",
    lastUpdatedAt: "Last updated at 4:46 PM PDT on July 30, 2026",
    wide: true,
    standalone: true,
    summary: "Course map and classroom entry point: project design through 2023 evidence, an insurance end-to-end case, and a minimum compute-owner framework."
  },
  {
    slug: "lec8-project-design-2023",
    section: "lec8",
    category: "Part 1",
    title: "Lecture 8 Part 1: Project Design Through 2023 Evidence",
    kind: "html",
    file: "handouts/MSDS682-Lecture8-2023-Project-Retrospective.html",
    createdAt: "Created at 4:46 PM PDT on July 30, 2026",
    lastUpdatedAt: "Last updated at 4:46 PM PDT on July 30, 2026",
    wide: true,
    standalone: true,
    summary: "Use one proposal architecture contract and seven sanitized historical examples to identify closed loops, stateful products, and scope gaps."
  },
  {
    slug: "lec8-insurance-end-to-end",
    section: "lec8",
    category: "Part 2",
    title: "Lecture 8 Part 2: Insurance Document Events to Authorized Answers",
    kind: "html",
    file: "handouts/MSDS682-Lecture8-Insurance-RAG-MCP-Reviewed.html",
    createdAt: "Created at 4:46 PM PDT on July 30, 2026",
    lastUpdatedAt: "Last updated at 4:46 PM PDT on July 30, 2026",
    wide: true,
    standalone: true,
    summary: "A complete industrial case with versioned object storage, Kafka change events, idempotent consumers, active knowledge, authorized retrieval, RAG, and MCP."
  },
  {
    slug: "lec8-compute-owner-decisions",
    section: "lec8",
    category: "Part 3",
    title: "Lecture 8 Part 3: Minimum Compute-Owner Decision Framework",
    kind: "html",
    file: "handouts/MSDS682-Lecture8-Stream-Processing-Decision-Guide.html",
    createdAt: "Created at 4:46 PM PDT on July 30, 2026",
    lastUpdatedAt: "Last updated at 4:46 PM PDT on July 30, 2026",
    wide: true,
    standalone: true,
    summary: "Choose a plain consumer, stateful SQL engine, or embedded processing library from state, time, recovery, scale, and ownership requirements; specific products continue in Lecture 9."
  },
  {
    slug: "lec9-compute-owners",
    section: "lec9",
    category: "Slides",
    title: "Lecture 9: One Problem, Three Compute Owners",
    kind: "html",
    file: "handouts/MSDS682-Lecture9-One-Problem-Three-Compute-Owners.html",
    createdAt: "Created at 4:53 PM PDT on August 3, 2026",
    lastUpdatedAt: "Last updated at 4:53 PM PDT on August 3, 2026",
    wide: true,
    standalone: true,
    summary: "Review Lectures 6–8, then compare Python, ksqlDB, and Flink SQL as three owners of the same Demo 07 stateful evaluation contract."
  },
  {
    slug: "demo09",
    section: "lec9",
    category: "Demo",
    title: "Demo 09: One Problem, Three Compute Owners",
    kind: "md",
    file: "handouts/demo09.md",
    createdAt: "Created at 4:53 PM PDT on August 3, 2026",
    lastUpdatedAt: "Last updated at 6:05 PM PDT on August 3, 2026",
    wide: true,
    summary: "Continue Demo 07 with ksqlDB and Flink SQL output topics, then compare both paths with the unchanged Python owner and one independent verifier."
  },
  {
    slug: "lec10-course-recap",
    section: "lec10",
    category: "Slides",
    title: "Lecture 10: Review Session and Final Presentation",
    kind: "html",
    file: "handouts/MSDS682-Lecture10-Course-Recap.html",
    createdAt: "Created at 5:00 PM PDT on August 6, 2026",
    lastUpdatedAt: "Last updated at 5:24 PM PDT on August 6, 2026",
    wide: true,
    standalone: true,
    summary: "Review the course through six concrete streaming decisions, Demo 07 and Demo 09 evidence, and the final-presentation checklist."
  },
  {
    slug: "lec11a-api-design",
    section: "lec11a",
    category: "Slides",
    title: "Lecture 11A: API design for streaming systems",
    kind: "html",
    file: "handouts/MSDS682-Lecture11A-API-Design.html",
    createdAt: "Created at 1:49 PM PDT on August 9, 2026",
    lastUpdatedAt: "Last updated at 5:04 PM PDT on August 10, 2026",
    wide: true,
    standalone: true,
    summary: "Use REST and FastAPI to accept work, Kafka to retain events, and a status resource to report downstream progress."
  },
  {
    slug: "demo11",
    section: "lec11a",
    category: "Demo",
    title: "Demo 11: Observable API for an asynchronous Kafka workflow",
    kind: "md",
    file: "handouts/demo11.md",
    createdAt: "Created at 1:49 PM PDT on August 9, 2026",
    lastUpdatedAt: "Last updated at 4:05 PM PDT on August 9, 2026",
    wide: true,
    summary: "Show why 202 is not completion, then use one request_id and status URL to observe processing and make retries safe."
  },
  {
    slug: "lec11b-interview-preparation",
    section: "lec11b",
    category: "Slides",
    title: "Lecture 11B: Interview preparation",
    kind: "html",
    file: "handouts/MSDS682-Lecture11B-Interview-Preparation.html",
    createdAt: "Created at 4:29 PM PDT on August 10, 2026",
    lastUpdatedAt: "Last updated at 5:13 PM PDT on August 10, 2026",
    wide: true,
    standalone: true,
    summary: "Follow the complete candidate journey from target role and evidence package through applications, interviews, written-offer review, negotiation, and signature."
  },
  {
    slug: "final-presentation",
    section: "lec12",
    category: "Handout",
    title: "Final Presentation Schedule and Rubrics",
    kind: "html",
    file: "handouts/final-presentation.html",
    createdAt: "Created at 2:03 PM PDT on August 8, 2026",
    lastUpdatedAt: "Last updated at 2:52 PM PDT on August 8, 2026",
    wide: true,
    summary: "One concise handout for final-project links, presentation timing, four-category rubric, schedule, and Q&A."
  },
  {
    slug: "lec12-final-project-presentations",
    section: "lec12",
    category: "Slides",
    title: "Lecture 12: Final Project Presentations",
    kind: "html",
    file: "handouts/MSDS682-Lecture12-Final-Project-Presentations.html",
    createdAt: "Created at 3:19 PM PDT on August 13, 2026",
    lastUpdatedAt: "Last updated at 5:21 PM PDT on August 13, 2026",
    wide: true,
    standalone: true,
    summary: "Celebrate the final class, connect Lectures 1–11 into one end-to-end journey, review the published rubric, follow the calendar, and close with an optimistic send-off."
  }
  // PDF example (uncomment and add the file to publish):
  // {
  //   slug: "week1-slides",
  //   section: "lec1",
  //   category: "Slides",
  //   title: "Week 1 Slides",
  //   kind: "pdf",
  //   file: "handouts/week1-slides.pdf",
  //   createdAt: "Created at 9:00 AM PDT on September 1, 2026",
  //   lastUpdatedAt: "Last updated at 9:00 AM PDT on September 1, 2026",
  //   summary: "Lecture slides handout (PDF)."
  // }
];

// Compact chronological entry point for the Handouts page. Local links refer
// to the handout manifest by slug so filenames and routes still have one owner.
const lectureRoadmap = [
  {
    section: "lec1",
    slides: { slug: "lec1-slides", label: "Lecture slides" },
    materials: [
      { slug: "demo00", label: "Demo 00" }
    ]
  },
  {
    section: "lec2",
    slides: { slug: "lec2-slides", label: "Lecture slides" },
    materials: [
      { slug: "lec2-topic-vs-table", label: "Lecture supplement" },
      { slug: "demo01", label: "Demo 01" },
      { slug: "demo02", label: "Demo 02" }
    ]
  },
  {
    section: "lec3",
    slides: { slug: "lec3-consumers", label: "Lecture slides" },
    materials: [
      { slug: "demo03", label: "Demo 03" }
    ]
  },
  {
    section: "lec4",
    slides: { slug: "lec4-data-contracts", label: "Lecture slides" },
    materials: [
      { slug: "demo04", label: "Demo 04" }
    ]
  },
  {
    section: "lec5",
    slides: { slug: "lec5-streaming-apis", label: "Lecture slides" },
    materials: [
      { slug: "fastapi-recap", label: "FastAPI recap" },
      { slug: "demo05", label: "Demo 05" }
    ]
  },
  {
    section: "lec6",
    slides: {
      slug: "lec6-kafka-connect-stream-processing",
      label: "Lecture slides"
    },
    materials: [
      { slug: "demo06", label: "Demo 06" }
    ]
  },
  {
    section: "lec7a",
    slides: {
      slug: "lec7a-final-project",
      label: "Lecture 7A slides"
    },
    materials: [
      { slug: "final-project", label: "Project requirements" },
      { slug: "final-project-ideas", label: "Project ideas" },
      { slug: "final-project-proposal-template", label: "Proposal template" },
      { slug: "final-project-proposal-rubric", label: "Proposal rubric" },
      { slug: "final-project-rubric", label: "Final report + code rubric" }
    ]
  },
  {
    section: "lec7b",
    slides: {
      slug: "lec7b-stateful-stream-processing",
      label: "Lecture 7B slides"
    },
    materials: [
      { slug: "demo07", label: "Demo 07" }
    ]
  },
  {
    section: "lec8",
    slides: {
      slug: "lec8-system-design",
      label: "Lecture 8 overview"
    },
    materials: [
      { slug: "lec8-project-design-2023", label: "Part 1 · Project design + 2023 evidence" },
      { slug: "lec8-insurance-end-to-end", label: "Part 2 · Insurance end to end" },
      { slug: "lec8-compute-owner-decisions", label: "Part 3 · Compute-owner framework" }
    ]
  },
  {
    section: "lec9",
    slides: {
      slug: "lec9-compute-owners",
      label: "Lecture slides"
    },
    materials: [
      { slug: "demo09", label: "Demo 09" }
    ]
  },
  {
    section: "lec10",
    slides: {
      slug: "lec10-course-recap",
      label: "Lecture slides"
    },
    materials: []
  },
  {
    section: "lec11a",
    slides: {
      slug: "lec11a-api-design",
      label: "Lecture 11A slides"
    },
    materials: [
      { slug: "demo11", label: "Demo 11" }
    ]
  },
  {
    section: "lec11b",
    slides: {
      slug: "lec11b-interview-preparation",
      label: "Lecture 11B slides"
    },
    materials: []
  },
  {
    section: "lec12",
    slides: {
      slug: "lec12-final-project-presentations",
      label: "Lecture 12 slides"
    },
    materials: [
      { slug: "final-presentation", label: "Final presentation schedule and rubrics" }
    ]
  }
];

function escapeHtml(value) {
  return value.replace(/[&<>"]/g, (ch) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]
  ));
}

function handoutMetaHtml(h) {
  return [h.createdAt, h.lastUpdatedAt]
    .filter(Boolean)
    .map((line) => `<span>${escapeHtml(line)}</span>`)
    .join("");
}

function handoutDownloadHtml(h) {
  if (!h.download) return "";
  const fallbackName = h.file.split("/").pop();
  const filename = h.download.filename || fallbackName;
  const label = h.download.label || "Download file";
  return `<p class="handout-actions"><a class="download-link" href="${escapeHtml(h.file)}" ` +
    `download="${escapeHtml(filename)}">${escapeHtml(label)}</a></p>`;
}

function handoutListMeta(h) {
  const full = [h.createdAt, h.lastUpdatedAt].filter(Boolean).join(" · ");
  const compact = h.lastUpdatedAt
    ? h.lastUpdatedAt.replace(/^Last updated at .* on /, "Updated ")
    : "";
  return {
    compact,
    full
  };
}

function handoutDestination(h) {
  const opensNewTab = ["external", "pdf"].includes(h.kind);
  return {
    href: opensNewTab ? h.file : `#/handouts/${h.slug}`,
    opensNewTab
  };
}

function handoutCardHtml(h) {
  const { href, opensNewTab } = handoutDestination(h);
  const target = opensNewTab ? ' target="_blank" rel="noopener"' : "";
  const titleId = `handout-title-${h.slug}`;
  const summaryId = `handout-summary-${h.slug}`;
  const targetNoteId = `handout-target-${h.slug}`;
  const describedBy = opensNewTab ? `${summaryId} ${targetNoteId}` : summaryId;
  const listMeta = handoutListMeta(h);
  const arrow = opensNewTab ? "↗" : "→";
  return `
    <li class="handout-card">
      <a class="handout-card-link" href="${href}"${target}
         aria-labelledby="${escapeHtml(titleId)}"
         aria-describedby="${escapeHtml(describedBy)}">
        <span class="handout-type">${escapeHtml(h.category)}</span>
        <div class="handout-copy">
          <h4 class="handout-title" id="${escapeHtml(titleId)}">${escapeHtml(h.title)}</h4>
          <span class="handout-summary" id="${escapeHtml(summaryId)}">${escapeHtml(h.summary)}</span>
          ${opensNewTab ? `<span class="sr-only" id="${escapeHtml(targetNoteId)}">Opens in a new tab.</span>` : ""}
        </div>
        <span class="handout-tail">
          <time class="handout-updated" title="${escapeHtml(listMeta.full)}" aria-label="${escapeHtml(listMeta.full)}">${escapeHtml(listMeta.compact)}</time>
          <span class="handout-arrow" aria-hidden="true">${arrow}</span>
        </span>
      </a>
    </li>`;
}

function handoutSectionHtml(section, { current = false } = {}) {
  const sectionHandouts = handouts.filter((h) => h.section === section.id);
  if (!sectionHandouts.length) return "";
  const cards = sectionHandouts
    .map(handoutCardHtml)
    .join("");
  return `
    <section class="handout-section${current ? " handout-section-current" : ""}" aria-labelledby="handout-section-${escapeHtml(section.id)}">
      <header class="handout-section-head">
        <div class="handout-section-badges">
          <span class="handout-section-label">${escapeHtml(section.label)}</span>
        </div>
        <div>
          <h3 id="handout-section-${escapeHtml(section.id)}">${escapeHtml(section.title)}</h3>
          <p>${escapeHtml(section.summary)}</p>
        </div>
      </header>
      <ol class="handout-list">${cards}</ol>
    </section>`;
}

function latestHandoutGroupHtml(sections) {
  const activeSections = sections.filter(
    (section) => handouts.some((handout) => handout.section === section.id)
  );
  if (!activeSections.length) return "";
  const labels = activeSections.map((section) => section.label).join(" and ");
  const sectionHtml = activeSections
    .map((section) => handoutSectionHtml(section, { current: true }))
    .join("");
  return `
    <div class="handout-latest-group" role="region" aria-label="Latest materials: ${escapeHtml(labels)}">
      <div class="handout-latest-group-head">
        <span class="handout-latest-badge">Latest</span>
        <span class="handout-latest-range">${escapeHtml(labels)}</span>
      </div>
      ${sectionHtml}
    </div>`;
}

function lectureRoadmapLinkHtml(link) {
  const handout = handouts.find((item) => item.slug === link.slug);
  if (!handout) {
    throw new Error(`Lecture roadmap references an unknown handout: ${link.slug}`);
  }

  const { href, opensNewTab } = handoutDestination(handout);
  const target = opensNewTab ? ' target="_blank" rel="noopener"' : "";
  const arrow = opensNewTab ? ' <span aria-hidden="true">↗</span>' : "";
  return `<a href="${escapeHtml(href)}"${target}>${escapeHtml(link.label)}${arrow}</a>`;
}

function lectureRoadmapHtml() {
  const rows = lectureRoadmap.map((lecture) => {
    const section = handoutSections.find((item) => item.id === lecture.section);
    if (!section) {
      throw new Error(`Lecture roadmap references an unknown section: ${lecture.section}`);
    }

    const identity = lectureIdentity(section);
    const links = [lecture.slides, ...lecture.materials]
      .filter(Boolean)
      .map(lectureRoadmapLinkHtml)
      .join("");

    return `
      <li class="lecture-map-row">
        <span class="lecture-map-number" aria-hidden="true">L${identity.code}</span>
        <div class="lecture-map-copy">
          <strong>${escapeHtml(section.label)}: ${escapeHtml(section.title)}</strong>
          <span>${escapeHtml(section.summary)}</span>
        </div>
        <div class="lecture-map-links" aria-label="${escapeHtml(section.label)} materials">${links}</div>
      </li>`;
  }).join("");

  return `
    <section class="lecture-map" aria-labelledby="lecture-map-title">
      <header class="lecture-map-head">
        <p class="lecture-map-eyebrow">Course path</p>
        <h3 id="lecture-map-title">Lecture and Demo Map</h3>
        <p>Start with the lecture, then open the linked demo materials for that class.</p>
      </header>
      <ol class="lecture-map-list">${rows}</ol>
    </section>`;
}

function handoutsListBody() {
  const latest = latestHandoutGroupHtml(latestLectureSections);
  const earlier = earlierHandoutSections.map((section) => handoutSectionHtml(section)).join("");
  const sections = `${latest}${earlier}`;

  return `
    ${lectureRoadmapHtml()}
    <p class="lede handout-library-intro">Complete handout library, newest lecture first.</p>
    <div class="handout-sections">${sections || '<p>Handouts will be posted as the course progresses.</p>'}</div>
  `;
}

const handoutRouteAliases = Object.freeze({
  "lec7-stateful-stream-processing": "lec7b-stateful-stream-processing",
  "lec11-api-design": "lec11a-api-design",
  "final-presentation-requirements": "final-presentation",
  "final-presentation-rubric": "final-presentation",
  "final-presentation-schedule": "final-presentation",
  "final-presentation-peer-reviews": "final-presentation"
});

const fallbackRoute = "/";
const content = document.querySelector("#content");
const skipLink = document.querySelector(".skip-link");
const navLinks = [...document.querySelectorAll(".nav a")];

skipLink.addEventListener("click", (event) => {
  event.preventDefault();
  content.scrollIntoView({ block: "start" });
  content.focus({ preventScroll: true });
});

function parseRoute() {
  const hash = window.location.hash.replace(/^#/, "") || fallbackRoute;
  const detail = hash.match(/^\/handouts\/(.+)$/);
  if (detail) {
    const slug = handoutRouteAliases[detail[1]] || detail[1];
    return { kind: "handout", slug, nav: "/handouts" };
  }
  if (hash === "/handouts") {
    return { kind: "static", route: "/handouts", nav: "/handouts" };
  }
  if (pages[hash]) {
    return { kind: "static", route: hash, nav: hash };
  }
  return { kind: "static", route: fallbackRoute, nav: fallbackRoute };
}

function setActiveNav(navRoute) {
  navLinks.forEach((link) => {
    const active = link.dataset.route === navRoute;
    link.classList.toggle("active", active);
    if (active) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
}

function renderStatic(route) {
  const page = route === "/handouts"
    ? { title: "Handouts", body: handoutsListBody() }
    : pages[route];
  content.className = ["/schedule", "/handouts"].includes(route) ? "content wide" : "content";
  content.innerHTML = `<h2>${page.title}</h2>${page.body}`;
  document.title = `${page.title} - MSDS 682`;
}

function addCopyCodeButtons(root) {
  root.querySelectorAll("pre").forEach((pre) => {
    if (pre.querySelector(".copy-code-button")) return;
    const code = pre.querySelector("code");
    const source = code ? code.innerText : pre.innerText;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "copy-code-button";
    button.textContent = "Copy Code";
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(source);
        button.textContent = "Copied";
      } catch (_) {
        const textarea = document.createElement("textarea");
        textarea.value = source;
        textarea.setAttribute("readonly", "");
        textarea.style.position = "fixed";
        textarea.style.left = "-9999px";
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        textarea.remove();
        button.textContent = "Copied";
      }
      window.setTimeout(() => {
        button.textContent = "Copy Code";
      }, 1400);
    });
    pre.appendChild(button);
  });
}

function headingSlug(text) {
  return text
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "section";
}

function prepareHandoutNavigation(root) {
  const used = new Map();
  root.querySelectorAll("h1, h2, h3, h4").forEach((heading) => {
    if (!heading.id) {
      const base = headingSlug(heading.textContent.trim());
      const count = used.get(base) || 0;
      used.set(base, count + 1);
      heading.id = count ? `${base}-${count + 1}` : base;
    }
  });

  root.querySelectorAll('a[href^="#"]:not([href^="#/"])').forEach((link) => {
    link.addEventListener("click", (event) => {
      const rawId = link.getAttribute("href").slice(1);
      const target = document.getElementById(decodeURIComponent(rawId));
      if (!target || !root.contains(target)) return;
      event.preventDefault();
      const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      target.setAttribute("tabindex", "-1");
      target.scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth", block: "start" });
      target.focus({ preventScroll: true });
    });
  });
}

function protectDisplayMath(source) {
  const lines = source.split(/\r?\n/);
  const protectedLines = [];
  const mathBlocks = [];

  for (let index = 0; index < lines.length; index += 1) {
    const start = lines[index].match(/^(\s*(?:>\s*)*)\$\$\s*$/);
    if (!start) {
      protectedLines.push(lines[index]);
      continue;
    }

    const prefix = start[1];
    const quoted = prefix.includes(">");
    const originalLines = [lines[index]];
    const texLines = [];
    let closed = false;

    while (index + 1 < lines.length) {
      index += 1;
      const line = lines[index];
      originalLines.push(line);
      const normalized = quoted
        ? line.replace(/^\s*(?:>\s*)+/, "")
        : line;
      if (normalized.trim() === "$$") {
        closed = true;
        break;
      }
      texLines.push(normalized);
    }

    if (!closed) {
      protectedLines.push(...originalLines);
      continue;
    }

    const mathIndex = mathBlocks.push(texLines.join("\n").trim()) - 1;
    protectedLines.push(
      `${prefix}<div class="math-display-source" data-math-index="${mathIndex}"></div>`
    );
  }

  return { source: protectedLines.join("\n"), mathBlocks };
}

function protectInlineMath(source) {
  const lines = source.split(/\r?\n/);
  const inlineMathBlocks = [];
  let fenceCharacter = "";

  const protectedLines = lines.map((line) => {
    const fence = line.match(/^\s*(`{3,}|~{3,})/);
    if (fence) {
      const character = fence[1][0];
      if (!fenceCharacter) {
        fenceCharacter = character;
      } else if (fenceCharacter === character) {
        fenceCharacter = "";
      }
      return line;
    }
    if (fenceCharacter) return line;

    return line.replace(/\\\((.+?)\\\)/g, (_match, tex) => {
      const mathIndex = inlineMathBlocks.push(tex.trim()) - 1;
      return `<span class="math-inline-source" data-inline-math-index="${mathIndex}"></span>`;
    });
  });

  return { source: protectedLines.join("\n"), inlineMathBlocks };
}

function restoreDisplayMath(root, mathBlocks) {
  root.querySelectorAll("[data-math-index]").forEach((placeholder) => {
    const index = Number.parseInt(placeholder.dataset.mathIndex, 10);
    if (!Number.isInteger(index) || !mathBlocks[index]) return;
    placeholder.removeAttribute("data-math-index");
    placeholder.textContent = `\\[\n${mathBlocks[index]}\n\\]`;
  });
}

function restoreInlineMath(root, inlineMathBlocks) {
  root.querySelectorAll("[data-inline-math-index]").forEach((placeholder) => {
    const index = Number.parseInt(placeholder.dataset.inlineMathIndex, 10);
    if (!Number.isInteger(index) || !inlineMathBlocks[index]) return;
    placeholder.removeAttribute("data-inline-math-index");
    placeholder.textContent = `\\(${inlineMathBlocks[index]}\\)`;
  });
}

function clearTypesetMath(root) {
  if (window.MathJax && typeof window.MathJax.typesetClear === "function") {
    window.MathJax.typesetClear([root]);
  }
}

async function typesetMath(root) {
  if (!window.MathJax || !window.MathJax.startup) return;

  try {
    await window.MathJax.startup.promise;
    if (typeof window.MathJax.typesetPromise === "function") {
      await window.MathJax.typesetPromise([root]);
    }
  } catch (err) {
    console.warn("MathJax could not typeset this handout.", err);
  }
}

async function renderHandout(slug) {
  const meta = handouts.find((h) => h.slug === slug);
  const backLink = '<p class="back-link"><a href="#/handouts">&larr; All handouts</a></p>';
  content.className = meta && meta.wide ? "content wide" : "content";

  if (!meta || !["md", "html"].includes(meta.kind)) {
    content.innerHTML = `${backLink}<h2>Handout not found</h2><p>This handout is not available.</p>`;
    document.title = "Handout not found - MSDS 682";
    return;
  }

  document.title = `${meta.title} - MSDS 682`;
  content.innerHTML = `${backLink}<div class="handout"><p class="lede">Loading…</p></div>`;

  try {
    if (meta.standalone) {
      content.innerHTML = `${backLink}<div class="standalone-handout-shell">` +
        `<div class="standalone-handout-bar"><div><h2>${escapeHtml(meta.title)}</h2>` +
        `<p class="handout-meta standalone-meta">${handoutMetaHtml(meta)}</p></div>` +
        `<a href="${escapeHtml(meta.file)}" target="_blank" rel="noopener">Open full page</a></div>` +
        `<iframe class="standalone-handout-frame" src="${escapeHtml(meta.file)}" title="${escapeHtml(meta.title)}"></iframe>` +
        `</div>`;
      return;
    }

    const res = await fetch(meta.file, { cache: "no-cache" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const source = await res.text();
    let mathBlocks = [];
    let inlineMathBlocks = [];
    let html = source;
    if (meta.kind === "md") {
      if (window.marked) {
        const protectedDisplayMath = protectDisplayMath(source);
        const protectedInlineMath = protectInlineMath(protectedDisplayMath.source);
        mathBlocks = protectedDisplayMath.mathBlocks;
        inlineMathBlocks = protectedInlineMath.inlineMathBlocks;
        html = window.marked.parse(protectedInlineMath.source);
      } else {
        html = `<pre>${escapeHtml(source)}</pre>`;
      }
    }
    const articleClass = meta.wide ? "handout handout-wide" : "handout";
    content.innerHTML = `${backLink}${handoutDownloadHtml(meta)}` +
      `<article class="${articleClass}">${html}</article>`;
    const article = content.querySelector("article");
    if (article && mathBlocks.length) {
      restoreDisplayMath(article, mathBlocks);
    }
    if (article && inlineMathBlocks.length) {
      restoreInlineMath(article, inlineMathBlocks);
    }
    prepareHandoutNavigation(content);
    if (window.hljs) {
      content.querySelectorAll("pre code").forEach((el) => window.hljs.highlightElement(el));
    }
    addCopyCodeButtons(content);
    await typesetMath(content);
  } catch (err) {
    content.innerHTML = `${backLink}<h2>${escapeHtml(meta.title)}</h2>` +
      `<div class="notice">Could not load this handout (${escapeHtml(String(err.message))}). ` +
      `Markdown handouts require the site to be served over http (local server or GitHub Pages), not opened as a file.</div>`;
  }
}

async function render() {
  clearTypesetMath(content);
  const parsed = parseRoute();
  const isHome = parsed.kind === "static" && parsed.route === "/";
  document.body.classList.toggle("route-home", isHome);
  document.body.classList.toggle("route-internal", !isHome);
  document.body.classList.toggle(
    "route-final-presentation",
    parsed.kind === "handout" && parsed.slug === "final-presentation"
  );
  setActiveNav(parsed.nav);

  if (parsed.kind === "handout") {
    await renderHandout(parsed.slug);
  } else {
    renderStatic(parsed.route);
  }

  content.focus({ preventScroll: true });
}

window.addEventListener("hashchange", render);
render();
