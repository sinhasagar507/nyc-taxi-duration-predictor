# Decision Register

Settled decisions for this repository. `CLAUDE.md` carries the guard rule that enforces
this file; this file carries the entries.

**Status vocabulary**

- **LOCKED** — a settled decision. Anything that contradicts it stops work until the
  entry is reopened.
- **DEFERRED** — parked work. Do not start it, and do not raise it as a blocker or a
  next step, until the owner asks explicitly.

**Reopening.** A LOCKED entry reopens only on the explicit words "reopen D-NNN" plus a
new reason. A repeated thought is not a new reason. Record every reopening in the entry
itself, with the date and the new reason.

**Format.** Each entry states the Decision, the Why, the Reopen-if condition, and the
Status. The Why is the important field — it is what a future session quotes back.

---

## D-001 — The modeling target is fare, not duration

- **Decision:** the ML model predicts **fare** — target `fare_capped`, the p99-capped
  `fare_amount`. It does not predict trip duration. `trip_duration_min` is a candidate
  *feature*, wired as an on/off ablation, not the target.
- **Why:** the owner confirmed it explicitly ("Its fare prediction") on 2026-07-04. The
  repository folder name (`nyc_taxi_durationprediction`) and the legacy notebooks predate
  the pivot and must not pull the target back.
- **Reopen if:** never expected. The whole `spark/ml/` program assumes this target.
- **Status:** LOCKED (2026-07-04)

## D-002 — The PR into `main` waits for the owner

- **Decision:** the repository owner opens the pull request themselves, on their own
  timing. Compare link when wanted:
  `https://github.com/sinhasagar507/taxi-fare-prediction/compare/main...refactor/wire-pipeline`
- **Why:** nothing in any plan gates on it. Surfacing it as a pending next step reads as
  pressure and misrepresents what the plan requires.
- **Reopen if:** the owner asks for the PR.
- **Status:** DEFERRED (2026-07-30)

## D-003 — `project_architecture/` move stays parked

- **Decision:** leave `project_architecture/` at the repository top level for now. It is
  listed under "Artifacts to retire" in `CLAUDE.md`.
- **Why:** deferred, not forgotten. The owner sets the sequencing.
- **Reopen if:** the owner asks for the move.
- **Status:** DEFERRED (2026-07-30)

## D-004 — The `pytest.ini` header fix stays its own change

- **Decision:** do not fix `[tool:pytest]` → `[pytest]` as part of other work.
- **Why:** the fix makes `testpaths`, `--strict-markers`, and `filterwarnings` take
  effect for the first time. That behavioural change must land alone, so its fallout is
  attributable.
- **Reopen if:** the owner schedules it as its own change.
- **Status:** DEFERRED (2026-07-30)

## D-005 — `CASE_STUDY.md` waits for modeling Phase 5

- **Decision:** leave `CASE_STUDY.md` untouched. Rewrite it to the fare story, or remove
  it, only after **modeling Phase 5** (tune + diagnose) scores the sealed holdout.
- **Why:** the case study's headline number does not exist until the holdout is scored
  once, per the §4a split policy. A rewrite now goes stale the day Phase 5 lands.
- **Reopen if:** modeling Phase 5 lands, or a job-application deadline needs a partial
  write-up sooner. The `ds-writeup` skill generates it when the time comes.
- **Status:** LOCKED (2026-08-22)

## D-006 — The "Deployment split" subsection rides audit item 5

- **Decision:** keep the "Deployment split" subsection in `CLAUDE.md` unchanged until
  audit item 5 records the GCP successor decision. If the GCP design continues, it stays.
  Otherwise it moves to `notes/gcp-reference.md`, and only the no-keyfiles auth rule
  stays in `CLAUDE.md`.
- **Why:** three of its four bullets are safety and build rules that must load
  unconditionally. Moving them early would hide them exactly when they matter. Cutting
  them early would pre-empt a decision that is not made yet.
- **Reopen if:** audit item 5 is decided.
- **Status:** LOCKED (2026-08-22)

## D-007 — Sequential sessions, not worktrees

- **Decision:** run one session per concern, in sequence. Do not create git worktrees for
  parallel work until audit items 1–3 are done.
- **Why:** the stale cherry-pick sequencer lives in `.git`, which every worktree shares.
  Broken repository state leaks into all of them. Fix it once, first.
- **Reopen if:** audit items 1–3 are done and genuinely parallel work appears.
- **Status:** LOCKED (2026-08-22)

## D-008 — Rules live in `CLAUDE.md`, state lives in `notes/`

- **Decision:** `CLAUDE.md` holds only rules that must load in every session. Task state,
  reference material, history, and checklists go to `notes/`, reached by a pointer.
  `CLAUDE.md` must not grow; new state goes to `notes/`.
- **Why:** Anthropic's memory documentation targets under 200 lines per `CLAUDE.md`;
  larger files consume context and reduce adherence. The 2026-08-22 trim took the file
  from 350 lines to about 200 on this principle. A rule must push, because Claude cannot
  fetch a rule it has never seen; state can pull, because a task knows when it needs it.
- **Reopen if:** the official guidance changes.
- **Status:** LOCKED (2026-08-22)
