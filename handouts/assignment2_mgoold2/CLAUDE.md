# Scope

All work for this assignment happens inside this directory
(`handouts/assignment2_mgoold2/`) and its subdirectories.

Do not read, edit, or create files in the parent repo (`handouts/`, the repo
root, or any other directory above this one) unless the user explicitly asks
for that. Treat this directory as the complete, self-contained workspace for
this assignment.

# Packaging the submission ZIP

The archive is `assignment2_mgoold2.zip` and must open to a single top-level
`assignment2_mgoold2/` folder.

Include, beyond the required starter tree:

- `evidence/xc_review_evidence.json` — generated mutation-check results
- `extra_credit/mutation_check.py` — the harness that regenerates it
- `tests/test_review_guards.py` — the four review tests
- `extra_credit/AI_REVIEW.md` — the accept/reject write-up
- `report.md` and `AI_USAGE.md`

Exclude: `.env`, `.venv/`, `.pytest_cache/`, every `__pycache__/`,
`MSDS682_Assignment 2_Readme.pdf`, and this `CLAUDE.md`. A plain `zip -r`
sweeps all of them in, so pass explicit exclusions and then open the archive
and check before uploading.
