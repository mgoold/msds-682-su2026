const pages = {
  "/": {
    title: "Course Overview",
    body: `
      <p class="lede">MSDS 682 equips students to process continuous data streams at scale and in real time.</p>
      <p>This hands-on course teaches students how to process and analyze real-time data streams using Apache Kafka, Python, Confluent tools, and related data engineering workflows. Students will produce and consume Kafka messages, build streaming workflows, interpret streaming outputs, and use GitHub for code management and collaboration.</p>

      <div class="notice">
        Each lecture meets 5:30–7:20pm PDT. Monday sessions meet on Zoom; Thursday sessions meet in person at 101 Howard, Classroom 529.
      </div>

      <h3>Logistics</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Course</strong><span>MSDS 682-01 (30398), Data Stream Processing</span></div>
        <div class="meta-row"><strong>Term</strong><span>Summer 2026</span></div>
        <div class="meta-row"><strong>Instructor</strong><span>Jeremy W. Gu · <a href="mailto:wgu9@usfca.edu">wgu9@usfca.edu</a></span></div>
        <div class="meta-row"><strong>Course assistant</strong><span>Annie Chiu · <a href="mailto:ychiu14@usfca.edu">ychiu14@usfca.edu</a></span></div>
        <div class="meta-row"><strong>Meeting pattern</strong><span>Mon Zoom · Thu in person, 101 Howard Classroom 529 · 5:30–7:20pm PDT</span></div>
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
      <p class="lede">Each lecture meets 5:30–7:20pm PDT. Monday sessions are on Zoom; Thursday sessions are in person at 101 Howard, Classroom 529.</p>
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
              <td><strong>Mon · Jul 06</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Course Introduction and Environment Setup</td>
              <td>Course introduction and Demo 00 environment setup</td>
            </tr>
            <tr>
              <td>2</td>
              <td><strong>Thu · Jul 09</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Kafka Topics and Producers</td>
              <td>Demo 01 topic creation and Demo 02 producers</td>
            </tr>
            <tr>
              <td>3</td>
              <td><strong>Mon · Jul 13</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Kafka Consumers</td>
              <td>Consumer concepts and Demo 03A–03D on Confluent Cloud</td>
            </tr>
            <tr>
              <td>4</td>
              <td><strong>Thu · Jul 16</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Data Contracts and Streaming Architecture</td>
              <td>Pydantic, Avro, Schema Registry, and Demo 04A–04D; Assignment 1 extended to Jul 21, 11:59pm PDT</td>
            </tr>
            <tr>
              <td>5</td>
              <td><strong>Mon · Jul 20</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Streaming APIs and Data Schemas Pt. 2</td>
              <td>FastAPI, REST API, and schema-aware producer/consumer integration</td>
            </tr>
            <tr>
              <td>6</td>
              <td><strong>Thu · Jul 23</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Kafka Connect and stream processing</td>
              <td>Connectors and demo; Assignment 2 due Jul 25, 11:59pm PDT</td>
            </tr>
            <tr>
              <td>7</td>
              <td><strong>Mon · Jul 27</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Stream processing and final project requirements</td>
              <td>Demo, ksqlDB Pt. 1, joins, tables, streams</td>
            </tr>
            <tr>
              <td>8</td>
              <td><strong>Thu · Jul 30</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>ksqlDB Pt. 2</td>
              <td>Windowing, aggregation, querying basics; project proposal due Aug 01, 11:59pm PDT</td>
            </tr>
            <tr>
              <td>9</td>
              <td><strong>Mon · Aug 03</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Data pipelines / optional Airflow</td>
              <td>Orchestration concepts and an optional Airflow demo</td>
            </tr>
            <tr>
              <td>10</td>
              <td><strong>Thu · Aug 06</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Kafka + AI systems</td>
              <td>RAG, memory, evals, guardrails, project examples</td>
            </tr>
            <tr>
              <td>11</td>
              <td><strong>Mon · Aug 10</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Final review and project workshop</td>
              <td>Course review, project troubleshooting, begin presentations if needed</td>
            </tr>
            <tr>
              <td>12</td>
              <td><strong>Thu · Aug 13</strong><span class="table-secondary">5:30–7:20pm PDT</span></td>
              <td><span class="tag in-person">In person</span><span class="table-secondary">101 Howard · 529</span></td>
              <td>Final class and project presentations</td>
              <td>Presentation timing will be confirmed on Canvas; report/code due Aug 14, 11:59pm PDT</td>
            </tr>
          </tbody>
        </table>
      </div>
    `
  },
  "/assignments": {
    title: "Assignments",
    body: `
      <p class="lede">There are two individual assignments and one final project. <a href="https://usfca.instructure.com/courses/1633704" target="_blank" rel="noopener">Canvas</a> is the official submission platform; submit homework and final-project materials there as ZIP files.</p>
      <div class="assignment-list">
        <article class="assignment-card">
          <h3>Assignment 1</h3>
          <p>Use real Confluent Cloud Kafka to complete Demo 02A–02D: sync-style producer, async producer, performance comparison, and serialization. The assignment is worth 20% of the course grade and is graded out of 20 base points, with up to 3 extra-credit points. Disclose AI assistance if used.</p>
          <p><span class="tag">Extended deadline: Tue Jul 21, 2026 · 11:59pm PDT · 20 points + up to 3 extra credit · 20% course weight</span></p>
          <p><a class="download-link" href="#/handouts/assignment01">Open Assignment 1</a> · <a href="handouts/assignment01-starter.zip">Download student starter</a></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2</h3>
          <p>Individual assignment graded out of 20 points and worth 20% of the course grade. Focus: FastAPI routes, Pydantic models, multiple logical streams, a replay or scheduler entrypoint, and a local processor output.</p>
          <p><span class="tag">Due Jul 25, 2026 · 11:59pm PDT · 20 points · 20% course weight</span></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project</h3>
          <p>Final project worth 50% total: proposal 10%, written report/code 20%, presentation 20%. Projects may be completed individually or in two-person teams; individual projects are always allowed.</p>
          <div class="milestone-list">
            <div><strong>Proposal</strong><span>Due Aug 01, 2026 · 11:59pm PDT</span></div>
            <div><strong>Report/code</strong><span>Due Aug 14, 2026 · 11:59pm PDT</span></div>
            <div><strong>Presentation</strong><span>Timing will be announced on Canvas</span></div>
          </div>
        </article>
      </div>

      <h3>Submission and collaboration</h3>
      <ul>
        <li><a href="https://usfca.instructure.com/courses/1633704" target="_blank" rel="noopener">Canvas course page</a>; USF login may be required.</li>
        <li>Late submissions are not accepted unless prior approval has been granted by the instructor.</li>
        <li>GitHub is used for code management, collaboration, and portfolio development.</li>
        <li>AI tools, coding agents, open-source resources, and online references are permitted with clear attribution. Students must understand and verify submitted work and use another method when an AI tool cannot resolve the problem reliably.</li>
        <li>For two-person final projects, each student must document individual contributions and be able to explain the design, code, AI usage, and evaluation results.</li>
      </ul>
    `
  },
  "/syllabus": {
    title: "Syllabus",
    body: `
      <p class="lede">This page summarizes the final syllabus. <a href="assets/msds-682-syllabus.pdf" target="_blank" rel="noopener">Download the PDF syllabus</a>.</p>

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
        <div class="meta-row"><strong>Late work</strong><span>Late submissions are not accepted unless prior approval has been granted by the instructor.</span></div>
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
          <p>Office hours: Mondays 7:20–7:50pm, right after class, on Zoom.</p>
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
    summary: "Create a topic, produce records, review the lecture slides, and then complete Assignment 1."
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
  }
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
    lastUpdatedAt: "Last updated at 4:36 PM PDT on July 3, 2026",
    summary: "Official syllabus covering course outcomes, grading, policies, and schedule."
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
    lastUpdatedAt: "Last updated at 2:06 PM PDT on July 13, 2026",
    summary: "Run sync-style and async producers, benchmark delivery, and serialize trip events."
  },
  {
    slug: "lec2-topic-vs-table",
    section: "lec2",
    category: "Slides",
    title: "Lecture 2: Kafka Topics and Producers",
    kind: "html",
    file: "handouts/lec2-topic-vs-table.html",
    createdAt: "Created at 1:48 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 4:40 PM PDT on July 13, 2026",
    wide: true,
    standalone: true,
    summary: "Topics, producers, serialization, real Confluent results, and client-side batching."
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
    lastUpdatedAt: "Last updated at 4:36 PM PDT on July 16, 2026",
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
    lastUpdatedAt: "Last updated at 4:59 PM PDT on July 20, 2026",
    wide: true,
    summary: "Local FastAPI contracts plus a bounded, independent Confluent Cloud Avro round trip with expected-result screenshots."
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

function handoutCardHtml(h) {
  const href = h.kind === "pdf" ? h.file : `#/handouts/${h.slug}`;
  const target = h.kind === "pdf" ? ' target="_blank" rel="noopener"' : "";
  const titleId = `handout-title-${h.slug}`;
  const summaryId = `handout-summary-${h.slug}`;
  const targetNoteId = `handout-target-${h.slug}`;
  const describedBy = h.kind === "pdf" ? `${summaryId} ${targetNoteId}` : summaryId;
  const listMeta = handoutListMeta(h);
  const arrow = h.kind === "pdf" ? "↗" : "→";
  return `
    <li class="handout-card">
      <a class="handout-card-link" href="${href}"${target}
         aria-labelledby="${escapeHtml(titleId)}"
         aria-describedby="${escapeHtml(describedBy)}">
        <span class="handout-type">${escapeHtml(h.category)}</span>
        <div class="handout-copy">
          <h4 class="handout-title" id="${escapeHtml(titleId)}">${escapeHtml(h.title)}</h4>
          <span class="handout-summary" id="${escapeHtml(summaryId)}">${escapeHtml(h.summary)}</span>
          ${h.kind === "pdf" ? `<span class="sr-only" id="${escapeHtml(targetNoteId)}">Opens PDF in a new tab.</span>` : ""}
        </div>
        <span class="handout-tail">
          <time class="handout-updated" title="${escapeHtml(listMeta.full)}" aria-label="${escapeHtml(listMeta.full)}">${escapeHtml(listMeta.compact)}</time>
          <span class="handout-arrow" aria-hidden="true">${arrow}</span>
        </span>
      </a>
    </li>`;
}

function handoutSectionHtml(section) {
  const sectionHandouts = handouts.filter((h) => h.section === section.id);
  if (!sectionHandouts.length) return "";
  const cards = sectionHandouts
    .map(handoutCardHtml)
    .join("");
  return `
    <section class="handout-section" aria-labelledby="handout-section-${escapeHtml(section.id)}">
      <header class="handout-section-head">
        <span class="handout-section-label">${escapeHtml(section.label)}</span>
        <div>
          <h3 id="handout-section-${escapeHtml(section.id)}">${escapeHtml(section.title)}</h3>
          <p>${escapeHtml(section.summary)}</p>
        </div>
      </header>
      <ol class="handout-list">${cards}</ol>
    </section>`;
}

function handoutsListBody() {
  const sections = handoutSections.map(handoutSectionHtml).join("");

  return `
    <p class="lede">Materials are grouped in course order. Start with the final syllabus, then follow each lecture section from top to bottom.</p>
    <div class="handout-sections">${sections || '<p>Handouts will be posted as the course progresses.</p>'}</div>
  `;
}

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
    return { kind: "handout", slug: detail[1], nav: "/handouts" };
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
    const html = meta.kind === "html"
      ? source
      : window.marked ? window.marked.parse(source) : `<pre>${escapeHtml(source)}</pre>`;
    const articleClass = meta.wide ? "handout handout-wide" : "handout";
    content.innerHTML = `${backLink}<article class="${articleClass}">${html}</article>`;
    prepareHandoutNavigation(content);
    if (window.hljs) {
      content.querySelectorAll("pre code").forEach((el) => window.hljs.highlightElement(el));
    }
    addCopyCodeButtons(content);
  } catch (err) {
    content.innerHTML = `${backLink}<h2>${escapeHtml(meta.title)}</h2>` +
      `<div class="notice">Could not load this handout (${escapeHtml(String(err.message))}). ` +
      `Markdown handouts require the site to be served over http (local server or GitHub Pages), not opened as a file.</div>`;
  }
}

async function render() {
  const parsed = parseRoute();
  const isHome = parsed.kind === "static" && parsed.route === "/";
  document.body.classList.toggle("route-home", isHome);
  document.body.classList.toggle("route-internal", !isHome);
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
