# Repository Audit — 2026-08-22

Full-repo audit run 2026-08-22 (session: audit only, no files changed by the audit itself).
Tests at audit time: 258 passed, 1 skipped. Work each item in a **fresh session**; check it
off here when done. The three standing Deferred items in CLAUDE.md (PR,
`project_architecture/` move, `pytest.ini` header) are excluded — they stay parked.

## A. Broken repository state — fix first

- [ ] 1. Stale cherry-pick sequencer in `.git` — `git status` says "cherry-pick in
      progress"; the todo picks (`e5f29bd`, `38283a2`) are already on the branch.
      Fix with `git cherry-pick --quit` (NOT `--abort`).
- [ ] 2. 14 commits unpushed on `refactor/wire-pipeline` — the whole Phase-4/4b ML
      program exists only locally. Push after clearing item 1.
- [ ] 3. Uncommitted edit in `spark/2026-07-10-fare-prediction-modeling-plan.md` —
      the 2026-08-09 leakage-verification write-up. Commit it.

## B. Vision gaps

- [~] 4. Identity mismatch: repo name + README say **duration**; the locked modeling
      target is **fare** (`fare_capped`); `CASE_STUDY.md` (Jun 3) predates the pivot.
      **Done 2026-08-22:** the CLAUDE.md "Project" section and the README are reoriented
      to fare (title, intro, diagram, `spark/` tree, plus a note explaining the pivot).
      **Remaining:** `CASE_STUDY.md` waits for modeling Phase 5 — see D-005 in
      `notes/decisions.md`. The folder name stays; the README pivot note carries it.
- [ ] 5. GCP successor decision missing: account retired, so Airflow/dbt/Terraform/BQ
      cannot run end-to-end and the §5c cloud run has no target. Record a decision:
      new account, local substitute, or archive-as-reference.
      *Rider (2026-08-22):* the decision settles the "Deployment split" subsection in
      CLAUDE.md — keep it if the GCP design continues, else move it to
      `notes/gcp-reference.md` and keep only the no-keyfiles auth rule.
- [ ] 6. Three workstreams (pipeline docs, ML sweep, dashboard v3) with no priority
      order. Write the order down.

## C. Plan-of-action gaps

- [ ] 7. Branch scope: `refactor/wire-pipeline` now carries the entire ML program.
      Decide how to split future work into smaller branches.
- [ ] 8. Two plans, no single next-step list: CLAUDE.md's phase plan and the spark
      modeling plan don't point at each other. Add cross-links / one "next" pointer.

## D. Implementation drawbacks

- [x] 9. `spark/` mixes course notebooks (`03_test.ipynb` … `09_spark_gcs.ipynb`,
      `Demo Spark notebook.ipynb`, `head.csv`, `tmp/`, `lib/` 39 MB, `local_spark/`)
      with the production `spark/ml/` code. Extend the retire list and prune.
      **Done 2026-09-01.** Pruned exactly that list (39 MB freed, all of it `lib/`).
      `spark/local_spark/pyspark_bigquery_debug.py` also left `SCANNED_FILES` in
      `tests/unit/test_credential_decoupling.py`. The CLAUDE.md retire list is rewritten
      to what actually remains — most of the original entries no longer exist on disk.
      **Left in place, deliberately:** `nyc_taxi_duration_prediction.ipynb`,
      `nyc_taxi_duration_prediction_1.ipynb`, `pyspark_r_equivalent_toolkit.ipynb`. The
      audit never named them, and the first two are the pre-pivot duration work the owner
      still has plans for. Decide them separately.
- [x] 10. `migration_backup/` (7.1 GB) sits inside the working tree. Move it outside
      the repo.
      **Done 2026-09-01.** Moved to the repo's sibling
      `/Applications/saggydev/projects_learning/nyc_taxi_migration_backup/` — same disk,
      so the move was a rename. All 204 `fact_trips` parquet files verified in place.
      `spark/ml/00_prep_spark.py` hard-coded `REPO_ROOT / "migration_backup"`, so the
      path became policy in the new `spark/ml/src/paths.py` (TDD, 7 tests): the
      `MIGRATION_BACKUP_DIR` env var wins, else the sibling directory. The `.dockerignore`
      guard in `test_docker_runtime.py` stays — it costs nothing and still catches a
      restore. Docs updated in CLAUDE.md, `notes/gcp-reference.md`, `MIGRATION_RUNBOOK.md`
      and the modeling plan.
- [ ] 11. Loose root documents (`2026-05-24-Dashboard-development-plan-v3.md`,
      `project-status-phase5.pdf`, `MIGRATION_RUNBOOK.md`, `CASE_STUDY.md`) have no
      home. Relocate to `notes/`.
- [ ] 12. Known model defect unassigned: design matrix rank 25/27 (dummy trap,
      cond ≈ 4e15), coefficients unstable. Assign it a phase in the modeling plan.

## Attack order

1. Items 1–3 (git state) — one short cleanup session.
2. Items 5–6 + 8 — one decision document.
3. Item 4 — docs reconciliation.
4. Items 9–11 — prune + relocate.
5. Resume the modeling plan at MLlib row 2 (Phase 4b §5b); item 12 rides along there.

## Session log — 2026-08-22

- Audited the repository; wrote this file. Tests green: 258 passed, 1 skipped.
- Trimmed `CLAUDE.md` from 350 lines to ~200, against Anthropic's under-200-line target.
  Moved the GCS/BigQuery layout, the test-structure tree, the E2E smoke steps, and the
  Phase 0–4 history to `notes/gcp-reference.md`.
- Reoriented `CLAUDE.md` and `README.md` from duration to fare (part of item 4).
- Created `notes/decisions.md`, the Decision Register, and put its guard rule in
  `CLAUDE.md`. Eight seed entries: D-001 … D-008.
- Nothing committed. The git-state items (1–3) remain the cleanup session's job.
