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
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Date</th>
              <th>Day</th>
              <th>Time</th>
              <th>Mode</th>
              <th>Location</th>
              <th>Topic</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td>Jul 06, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Intro and data streaming</td>
              <td>Course setup and Demo 01</td>
            </tr>
            <tr>
              <td>2</td>
              <td>Jul 09, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Apache Kafka Pt. 1</td>
              <td>Architecture, setup, topics, producers</td>
            </tr>
            <tr>
              <td>3</td>
              <td>Jul 13, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Apache Kafka Pt. 2</td>
              <td>Consumers</td>
            </tr>
            <tr>
              <td>4</td>
              <td>Jul 16, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Apache Kafka Pt. 3</td>
              <td>FastAPI, hands-on demo, Data Schemas Pt. 1, Schema Registry; Assignment 1 due Jul 18, 11:59pm</td>
            </tr>
            <tr>
              <td>5</td>
              <td>Jul 20, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Data Schemas Pt. 2</td>
              <td>REST API, Avro, demo</td>
            </tr>
            <tr>
              <td>6</td>
              <td>Jul 23, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Kafka Connect and stream processing</td>
              <td>Connectors, demo; Assignment 2 due Jul 25, 11:59pm</td>
            </tr>
            <tr>
              <td>7</td>
              <td>Jul 27, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Stream processing and final project requirements</td>
              <td>Demo, ksqlDB Pt. 1, joins, tables, streams</td>
            </tr>
            <tr>
              <td>8</td>
              <td>Jul 30, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>ksqlDB Pt. 2</td>
              <td>Windowing, aggregation, querying basics; project proposal due Aug 01, 11:59pm</td>
            </tr>
            <tr>
              <td>9</td>
              <td>Aug 03, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Data pipelines / Airflow</td>
              <td>Orchestration concepts and Airflow demo</td>
            </tr>
            <tr>
              <td>10</td>
              <td>Aug 06, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Kafka + AI systems</td>
              <td>RAG, memory, evals, guardrails, project examples</td>
            </tr>
            <tr>
              <td>11</td>
              <td>Aug 10, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>Final review and project workshop</td>
              <td>Course review, project troubleshooting, begin presentations if needed</td>
            </tr>
            <tr>
              <td>12</td>
              <td>Aug 13, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Project presentation</td>
              <td>In-person presentations, concluding remarks; report/code due Aug 14, 11:59pm</td>
            </tr>
          </tbody>
        </table>
      </div>
    `
  },
  "/assignments": {
    title: "Assignments",
    body: `
      <p class="lede">There are two individual assignments and one final project. Canvas is the official submission platform; the Canvas link will be provided at the first class. Homework, projects, and final project materials must be submitted as ZIP files on Canvas.</p>
      <div class="assignment-list">
        <article class="assignment-card">
          <h3>Assignment 1</h3>
          <p>Individual assignment worth 20% of the course grade. Focus: Kafka-style producer logic, event schemas, local replay, and performance benchmarking.</p>
          <p><span class="tag">Due Jul 18, 2026 · 11:59pm</span></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2</h3>
          <p>Individual assignment worth 20% of the course grade. Focus: FastAPI routes, Pydantic models, multiple logical streams, a replay or scheduler entrypoint, and a local processor output.</p>
          <p><span class="tag">Due Jul 25, 2026 · 11:59pm</span></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project</h3>
          <p>Final project worth 50% total: proposal 10%, written report/code 20%, presentation 20%. Projects may be completed individually or in two-person teams; individual projects are always allowed.</p>
          <div class="milestone-list">
            <div><strong>Proposal</strong><span>Due Aug 01, 2026 · 11:59pm</span></div>
            <div><strong>Report/code</strong><span>Due Aug 14, 2026 · 11:59pm</span></div>
            <div><strong>Presentation</strong><span>Date TBA</span></div>
          </div>
        </article>
      </div>

      <h3>Submission and collaboration</h3>
      <ul>
        <li>Canvas link: to be provided at the first class.</li>
        <li>Late submissions are not accepted unless prior approval has been granted by the instructor.</li>
        <li>GitHub is used for code management, collaboration, and portfolio development.</li>
        <li>AI tools, coding agents, open-source resources, and online references are permitted with clear attribution and explanation.</li>
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
        <div class="meta-row"><strong>Generative AI</strong><span>Comprehensive use of generative AI tools is permitted with appropriate attribution and explanation.</span></div>
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
          <p>Office hours: N/A.</p>
        </article>
      </div>
    `
  }
};

// Handout manifest. To add a handout: drop a file in `handouts/` and add a row
// here. Markdown handouts (kind: "md") render in-page with syntax-highlighted
// code blocks; HTML handouts (kind: "html") render in-page without Markdown
// parsing; PDF handouts (kind: "pdf") open the file directly.
const handouts = [
  {
    slug: "demo00",
    title: "Demo 00: Environment Setup",
    kind: "md",
    file: "handouts/demo00.md",
    createdAt: "Created at 3:17 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 3:17 AM PDT on July 9, 2026",
    summary: "Step-by-step first local run: create a Python environment, install packages, run the environment check, and inspect the JSON artifact."
  },
  {
    slug: "demo01",
    title: "Demo 01: Create a Kafka Topic",
    kind: "md",
    file: "handouts/demo01.md",
    createdAt: "Created at 4:46 PM PDT on July 6, 2026",
    lastUpdatedAt: "Last updated at 1:32 PM PDT on July 9, 2026",
    summary: "Step-by-step Confluent Cloud topic creation with Python AdminClient, .env credentials, idempotent topic creation, and a safe JSON report."
  },
  {
    slug: "demo02",
    title: "Demo 02: Kafka Producer",
    kind: "md",
    file: "handouts/demo02.md",
    createdAt: "Created at 3:49 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 1:32 PM PDT on July 9, 2026",
    summary: "Producer core demos over the Demo 01 trip topic: sync-style producer, async producer, async-vs-sync comparison, and serialization."
  },
  {
    slug: "lec2-topic-vs-table",
    title: "Lec 2 Lab Supplemental Materials",
    kind: "html",
    file: "handouts/lec2-topic-vs-table.html",
    createdAt: "Created at 1:48 AM PDT on July 9, 2026",
    lastUpdatedAt: "Last updated at 1:32 PM PDT on July 9, 2026",
    wide: true,
    standalone: true,
    summary: "Supplemental Lec 2 slide deck after Demo 02: topic vs table, topic creation, producer behavior, sync vs async, real Confluent results, and serialization."
  },
  {
    slug: "syllabus",
    title: "Final Syllabus",
    kind: "pdf",
    file: "assets/msds-682-syllabus.pdf",
    createdAt: "Created at 4:36 PM PDT on July 3, 2026",
    lastUpdatedAt: "Last updated at 4:36 PM PDT on July 3, 2026",
    summary: "Official Simple Syllabus PDF for MSDS 682-01 Data Stream Processing."
  }
  // PDF example (uncomment and add the file to publish):
  // {
  //   slug: "week1-slides",
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

function handoutsListBody() {
  const cards = handouts.map((h) => {
    const href = h.kind === "pdf" ? h.file : `#/handouts/${h.slug}`;
    const target = h.kind === "pdf" ? ' target="_blank" rel="noopener"' : "";
    const badge = h.kind === "pdf" ? "PDF" : h.kind === "html" ? "HTML" : "Markdown";
    const action = h.kind === "pdf" ? "Download PDF" : "Open handout";
    return `
      <article class="handout-card">
        <div class="handout-card-head">
          <h3><a href="${href}"${target}>${escapeHtml(h.title)}</a></h3>
          <span class="tag">${badge}</span>
        </div>
        <p>${escapeHtml(h.summary)}</p>
        <p class="handout-meta">${handoutMetaHtml(h)}</p>
        <p class="handout-actions"><a class="download-link" href="${href}"${target}>${action}</a></p>
      </article>`;
  }).join("");

  return `
    <p class="lede">Lecture notes, reference sheets, and slides will be posted here. Markdown handouts render in-page with highlighted code; PDF handouts open directly.</p>
    <div class="handout-list">${cards || '<p>Handouts will be posted as the course progresses.</p>'}</div>
  `;
}

const fallbackRoute = "/";
const content = document.querySelector("#content");
const navLinks = [...document.querySelectorAll(".nav a")];

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
  content.className = route === "/schedule" ? "content wide" : "content";
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
