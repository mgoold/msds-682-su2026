# MSDS 682 · Data Stream Processing — Summer 2026

Course website for **MSDS 682-01 (30398), Data Stream Processing** at the University of San Francisco.

A lightweight static course site with no framework and no build step. Sections:

- **Home** — overview and logistics
- **Schedule** — class meetings and major due dates
- **Assignments** — assignment briefs
- **Handouts** — Markdown notes (with highlighted code) and PDF handouts
- **Syllabus** — policies and expectations
- **Staff** — instructor and support

## Structure

```
index.html   Shell: header, nav, content mount
script.js    Page content + hash router + handout renderer
styles.css   Visual system (USF green/gold, minimal)
assets/      usf-logo.svg (swap with the official logo)
handouts/    .md, .html, and .pdf handouts
```

## Adding a handout

1. Drop a file in `handouts/` — a Markdown file (`.md`), HTML file (`.html`), or PDF.
2. Add one row to the `handouts` array in `script.js` and assign its existing
   course-order section. The `handoutSections` registry is the single owner of
   section order and labels; add to it only when introducing a new lecture.

   ```js
   {
     slug: "week2-consumers",       // becomes #/handouts/week2-consumers
     section: "lec2",               // ID from handoutSections
     category: "Slides",            // student-facing type label
     title: "Week 2: Consumers",
     kind: "md",                    // "md" renders in-page, "html" can render in-page or standalone, "pdf" opens the file
     file: "handouts/week2-consumers.md",
     createdAt: "Created at 9:00 AM PDT on July 10, 2026",
     lastUpdatedAt: "Last updated at 9:00 AM PDT on July 10, 2026",
     summary: "One-line description."
   }
   ```

Markdown handouts render in the page with syntax-highlighted code blocks
(via marked + highlight.js, loaded from CDN). Standalone HTML handouts can be
embedded with `standalone: true`. PDF handouts open directly. Within each
section, cards follow their order in the `handouts` array.

The Handouts list derives its compact `Updated …` label from the full
`lastUpdatedAt` value; do not maintain a second short-date field. Because the
site hash belongs to the router, Markdown heading IDs and in-page table-of-
contents scrolling are handled by `prepareHandoutNavigation()` rather than by
changing `window.location.hash`.

Links from one Markdown handout to another must use the registered route
`#/handouts/<slug>`, not a relative `.md` path. If the same Markdown source is
packaged into a student ZIP, its builder owns the small route-to-local-link
translation so the published handout remains the SSOT.

## Local preview

```bash
python3 -m http.server 8000
```

Open the local server URL printed by the command. Serving over HTTP is required:
Markdown handouts are fetched at runtime and will not load from a `file://` path.

## Publishing

To publish on GitHub Pages: repository **Settings → Pages → Deploy from branch →
`main` / root**. The `.nojekyll` file is already present so asset paths are served
as-is.
