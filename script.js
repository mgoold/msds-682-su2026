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
        <div class="meta-row"><strong>Canvas link</strong><span>To be provided at the first class.</span></div>
        <div class="meta-row"><strong>Zoom link</strong><span>Please see the calendar invite.</span></div>
        <div class="meta-row"><strong>Submissions</strong><span>Canvas is the official submission platform; GitHub is used for code management and collaboration.</span></div>
      </div>

      <h3>Course Themes</h3>
      <ul>
        <li><strong>Kafka ecosystem:</strong> topics, producers, consumers, Confluent tools, and demos.</li>
        <li><strong>Python streaming workflows:</strong> producing, consuming, transforming, and interpreting message streams.</li>
        <li><strong>Software engineering practice:</strong> GitHub repositories, issues, pull requests, and collaboration.</li>
        <li><strong>Responsible technical work:</strong> testing, attribution, communication, and project presentation.</li>
      </ul>

      <h3>Learning Goals</h3>
      <ul>
        <li>Understand the Kafka ecosystem.</li>
        <li>Produce and consume Kafka messages using Python.</li>
        <li>Build real-time data ingestion and streaming analytics workflows.</li>
        <li>Use Git and GitHub for repositories, issues, pull requests, and collaboration.</li>
        <li>Use AI tools and open-source resources responsibly with proper credit.</li>
        <li>Present and explain technical project work clearly.</li>
      </ul>
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
              <td>-</td>
            </tr>
            <tr>
              <td>2</td>
              <td>Jul 09, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>-</td>
            </tr>
            <tr>
              <td>3</td>
              <td>Jul 13, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>-</td>
            </tr>
            <tr>
              <td>4</td>
              <td>Jul 16, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Assignment 1 due Jul 18, 11:59pm</td>
            </tr>
            <tr>
              <td>5</td>
              <td>Jul 20, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>-</td>
            </tr>
            <tr>
              <td>6</td>
              <td>Jul 23, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Assignment 2 due Jul 25, 11:59pm</td>
            </tr>
            <tr>
              <td>7</td>
              <td>Jul 27, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>-</td>
            </tr>
            <tr>
              <td>8</td>
              <td>Jul 30, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Project proposal due Aug 01, 11:59pm</td>
            </tr>
            <tr>
              <td>9</td>
              <td>Aug 03, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>-</td>
            </tr>
            <tr>
              <td>10</td>
              <td>Aug 06, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>-</td>
            </tr>
            <tr>
              <td>11</td>
              <td>Aug 10, 2026</td>
              <td>Monday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag zoom">Zoom</span></td>
              <td>Zoom</td>
              <td>-</td>
            </tr>
            <tr>
              <td>12</td>
              <td>Aug 13, 2026</td>
              <td>Thursday</td>
              <td>5:30–7:20pm</td>
              <td><span class="tag in-person">In person</span></td>
              <td>101 Howard, Classroom 529</td>
              <td>Report/code due Aug 14, 11:59pm; presentation date TBA</td>
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
          <p>Individual assignment worth 20% of the course grade.</p>
          <p><span class="tag">Due Jul 18, 2026 · 11:59pm</span></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2</h3>
          <p>Individual assignment worth 20% of the course grade.</p>
          <p><span class="tag">Due Jul 25, 2026 · 11:59pm</span></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project</h3>
          <p>Final project worth 50% total: proposal 10%, written report/code 20%, presentation 20%.</p>
          <p><span class="tag">Proposal Aug 01 · Report/code Aug 14 · Presentation TBA</span></p>
        </article>
      </div>

      <h3>Submission and collaboration</h3>
      <ul>
        <li>Canvas link: to be provided at the first class.</li>
        <li>Late submissions are not accepted unless prior approval has been granted by the instructor.</li>
        <li>GitHub is used for code management, collaboration, and portfolio development.</li>
        <li>AI tools, coding agents, open-source resources, and online references are permitted with clear attribution and explanation.</li>
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
        <div class="meta-row"><strong>Laptops</strong><span>Please keep laptops closed for in-class lectures unless instructed otherwise, especially during demo or exercise sessions.</span></div>
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
// code blocks; PDF handouts (kind: "pdf") open the file directly.
const handouts = [
  {
    slug: "syllabus",
    title: "Final Syllabus",
    kind: "pdf",
    file: "assets/msds-682-syllabus.pdf",
    date: "Summer 2026",
    summary: "Official Simple Syllabus PDF for MSDS 682-01 Data Stream Processing."
  }
  // PDF example (uncomment and add the file to publish):
  // {
  //   slug: "week1-slides",
  //   title: "Week 1 Slides",
  //   kind: "pdf",
  //   file: "handouts/week1-slides.pdf",
  //   date: "Sep 2026",
  //   summary: "Lecture slides handout (PDF)."
  // }
];

function escapeHtml(value) {
  return value.replace(/[&<>"]/g, (ch) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]
  ));
}

function handoutsListBody() {
  const cards = handouts.map((h) => {
    const href = h.kind === "pdf" ? h.file : `#/handouts/${h.slug}`;
    const target = h.kind === "pdf" ? ' target="_blank" rel="noopener"' : "";
    const badge = h.kind === "pdf" ? "PDF" : "Markdown";
    const action = h.kind === "pdf" ? "Download PDF" : "Open handout";
    return `
      <article class="handout-card">
        <div class="handout-card-head">
          <h3><a href="${href}"${target}>${escapeHtml(h.title)}</a></h3>
          <span class="tag">${badge}</span>
        </div>
        <p>${escapeHtml(h.summary)}</p>
        <p class="handout-meta">${escapeHtml(h.date)}</p>
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

async function renderHandout(slug) {
  const meta = handouts.find((h) => h.slug === slug);
  const backLink = '<p class="back-link"><a href="#/handouts">&larr; All handouts</a></p>';
  content.className = "content";

  if (!meta || meta.kind !== "md") {
    content.innerHTML = `${backLink}<h2>Handout not found</h2><p>This handout is not available.</p>`;
    document.title = "Handout not found - MSDS 682";
    return;
  }

  document.title = `${meta.title} - MSDS 682`;
  content.innerHTML = `${backLink}<div class="handout"><p class="lede">Loading…</p></div>`;

  try {
    const res = await fetch(meta.file, { cache: "no-cache" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const md = await res.text();
    const html = window.marked ? window.marked.parse(md) : `<pre>${escapeHtml(md)}</pre>`;
    content.innerHTML = `${backLink}<article class="handout">${html}</article>`;
    if (window.hljs) {
      content.querySelectorAll("pre code").forEach((el) => window.hljs.highlightElement(el));
    }
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
