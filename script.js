const pages = {
  "/": {
    title: "Course Overview",
    body: `
      <p class="lede">MSDS 682 is a Summer 2026 graduate course site currently being designed. This draft establishes the course structure, schedule format, assignments page, syllabus page, and staff page before public release.</p>
      <p>The course will emphasize clear technical thinking, hands-on work, and careful evaluation. Detailed topics, readings, due dates, and policies will be updated as the course design is finalized.</p>

      <div class="notice">
        This is a private draft site. Course details are placeholders until the syllabus is finalized.
      </div>

      <h3>Logistics</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Course</strong><span>MSDS 682</span></div>
        <div class="meta-row"><strong>Term</strong><span>Summer 2026</span></div>
        <div class="meta-row"><strong>Format</strong><span>Graduate course with lectures, labs, assignments, and a final project</span></div>
        <div class="meta-row"><strong>Meeting pattern</strong><span>To be announced</span></div>
        <div class="meta-row"><strong>Course materials</strong><span>Readings, notebooks, slides, and assignment links will be posted here</span></div>
      </div>

      <h3>Course Themes</h3>
      <ul>
        <li><strong>Applied data science:</strong> turning open-ended problems into measurable workflows.</li>
        <li><strong>Modern AI systems:</strong> using language models, retrieval, tools, and evaluation responsibly.</li>
        <li><strong>Experimentation:</strong> designing comparisons, measuring impact, and communicating uncertainty.</li>
        <li><strong>Production thinking:</strong> reliability, observability, reproducibility, and human review.</li>
      </ul>

      <h3>Learning Goals</h3>
      <ul>
        <li>Frame practical data and AI problems with clear assumptions and success metrics.</li>
        <li>Build reproducible analysis and modeling workflows.</li>
        <li>Evaluate model and system behavior with appropriate quantitative and qualitative evidence.</li>
        <li>Communicate technical decisions clearly to technical and non-technical audiences.</li>
      </ul>
    `
  },
  "/schedule": {
    title: "Schedule",
    body: `
      <p class="lede">The schedule below is a working draft. Dates, readings, labs, and deadlines will be refined as the course design develops.</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Week</th>
              <th>Topic</th>
              <th>Materials</th>
              <th>Due</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td>Course framing and technical workflow</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>2</td>
              <td>Data pipelines, reproducibility, and measurement</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 1 released</td>
            </tr>
            <tr>
              <td>3</td>
              <td>Modeling baselines and evaluation design</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>4</td>
              <td>Experimentation and causal thinking</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 1 due</td>
            </tr>
            <tr>
              <td>5</td>
              <td>LLM applications and retrieval workflows</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 2 released</td>
            </tr>
            <tr>
              <td>6</td>
              <td>Agents, tools, and multi-step task evaluation</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>7</td>
              <td>Reliability, monitoring, and human-in-the-loop systems</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 2 due</td>
            </tr>
            <tr>
              <td>8</td>
              <td>Final project workshops</td>
              <td><span class="tag">TBD</span></td>
              <td>Project proposal due</td>
            </tr>
            <tr>
              <td>9</td>
              <td>Project presentations and review</td>
              <td><span class="tag">TBD</span></td>
              <td>Final project due</td>
            </tr>
          </tbody>
        </table>
      </div>
    `
  },
  "/assignments": {
    title: "Assignments",
    body: `
      <p class="lede">Assignments are placeholders while the course is being designed. Final instructions, starter code, rubrics, and submission links will be added here.</p>
      <div class="assignment-list">
        <article class="assignment-card">
          <h3>Assignment 1: Reproducible Data Workflow</h3>
          <p>Build a clean, documented analysis workflow with clear data assumptions, metrics, and reproducible outputs.</p>
          <p><span class="tag">Draft</span></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2: Evaluation and Experiment Design</h3>
          <p>Design and implement an evaluation plan for a model or AI-assisted workflow, including failure cases and tradeoffs.</p>
          <p><span class="tag">Draft</span></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project</h3>
          <p>Develop a practical data or AI system, evaluate it rigorously, and present technical decisions with evidence.</p>
          <p><span class="tag">Draft</span></p>
        </article>
      </div>
    `
  },
  "/syllabus": {
    title: "Syllabus",
    body: `
      <p class="lede">This syllabus page is a structured placeholder. Final policies should be reviewed before the site is published or shared with students.</p>

      <h3>Course Description</h3>
      <p>MSDS 682 focuses on applied data science and modern AI workflows. Students will practice building reliable technical systems, evaluating outcomes, and communicating decisions with evidence.</p>

      <h3>Prerequisites</h3>
      <ul>
        <li>Working knowledge of Python.</li>
        <li>Basic probability, statistics, and machine learning familiarity.</li>
        <li>Comfort using notebooks, scripts, Git, and command-line tools.</li>
      </ul>

      <h3>Assessment</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Assignments</strong><span>TBD</span></div>
        <div class="meta-row"><strong>Final project</strong><span>TBD</span></div>
        <div class="meta-row"><strong>Participation</strong><span>TBD</span></div>
      </div>

      <h3>Policies</h3>
      <p>Attendance, collaboration, late work, academic integrity, and accessibility policies will be added after course requirements are finalized.</p>
    `
  },
  "/staff": {
    title: "Staff",
    body: `
      <p class="lede">Staff information is a draft and can be expanded with office hours, email, and support channels.</p>
      <div class="staff-grid">
        <article class="staff-card">
          <h3>Jeremy Gu</h3>
          <p>Instructor</p>
          <p>Office hours and contact information to be announced.</p>
        </article>
        <article class="staff-card">
          <h3>Teaching Assistant</h3>
          <p>To be announced</p>
          <p>Course support details will be added before launch.</p>
        </article>
      </div>
    `
  }
};

const fallbackRoute = "/";
const content = document.querySelector("#content");
const navLinks = [...document.querySelectorAll(".nav a")];

function getRoute() {
  const hash = window.location.hash.replace(/^#/, "");
  return pages[hash] ? hash : fallbackRoute;
}

function render() {
  const route = getRoute();
  const page = pages[route];

  content.innerHTML = `<h2>${page.title}</h2>${page.body}`;
  document.title = `${page.title} - MSDS 682`;

  navLinks.forEach((link) => {
    const active = link.dataset.route === route;
    link.classList.toggle("active", active);
    if (active) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });

  content.focus({ preventScroll: true });
}

window.addEventListener("hashchange", render);
render();
