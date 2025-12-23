# Global Codex Orchestrator – Project Specification

## 1. Purpose and Scope

The **Global Codex Orchestrator** is a *single, machine-level utility repository* that coordinates unattended, AI-assisted maintenance work across a configurable set of local Git repositories.

Its purpose is to:

* Plan and execute code maintenance while the user is unavailable (overnight, roadtrips)
* Refactor notebook-resident logic into maintainable Python modules
* Resolve queued development tasks tracked via **Beads**
* Produce reviewable, auditable outputs (branches, commits, reports)

The orchestrator is **not** a CI system, **not** a data pipeline, and **not** a project manager. It is an execution framework for well-scoped, low-risk development work.

---

## 2. Core Design Decisions

### 2.1 Single Global Repository

* Exactly **one orchestrator repo per machine**
* Targets many independent repos via configuration
* No orchestrator code is vendored into target repos

Rationale: centralized control, simpler upgrades, uniform behavior.

### 2.2 Explicit Runs

All work occurs within an explicitly defined **run**.

* A *run* corresponds to a single session:
  * One run per night window (automated)
  * One run per manual invocation (roadtrip)
* Each run has a stable identifier: `RUN_ID`
* `RUN_ID` is persisted across 45-minute ticks via `current_run.json` in the orchestrator cache (with expiry)
* New triggers during an active run exit loudly (global run lock)
* A run ends at the end of the night window, or when no actionable beads remain for N ticks
* All artifacts (branches, decks, reports) are namespaced by `RUN_ID`

Rationale: bounded reasoning, auditability, clean rollback.

### 2.3 Branch-First Safety Model

* Each repo uses a fresh branch per run:

```text
run/<RUN_ID>
```

* Per-bead sub-branches (`run/<RUN_ID>/<bead-id>`) are not created in v1

* Branches are created from a base branch that existed **before** the run
* The orchestrator **never merges**
* Integration happens only via pull requests created after the run

Rationale: zero risk to mainline branches, explicit human review gate.

### 2.4 Fail Loud, Skip Safely

* Missing tools (`codex`, `bd`) → hard failure
* Dirty working tree → repo skipped
* Detached HEAD → repo skipped
* No retries, no silent fallbacks

Rationale: unattended systems must prefer visibility over progress.

---

## 3. Target Repositories

Target repositories must:

* Be local Git repos
* Use Beads (`.beads/`) for task tracking
* Contain notebooks and/or Python source code

Repositories are enumerated via a structured per-repo config file:

```text
config/repos.toml
```

### 3.1 Repo IDs

* Each repo has a stable `repo_id` (the key used in `config/repos.toml`)
* `repo_id` is used in run decks, cache files, log naming, and repo selection

### 3.2 Repo Groups (Subsets)

Repos are listed into named groups so runs can target subsets (e.g. `fly_navigation_repos`, `automation_repos`, `home_projects`).

In `config/repos.toml`, repo configs live under `repos.<repo_id>`, and groups are declared as lists of `repo_id` values:

```toml
[repos.fly_navigation]
path = "/abs/path/to/fly_navigation"
base_branch = "main"

[repos.lab_automation]
path = "/abs/path/to/lab_automation"
base_branch = "main"

[repo_groups]
fly_navigation_repos = ["fly_navigation"]
automation_repos = ["lab_automation"]
home_projects = ["codex_orchestrator"]
```

Rules:

* Default target set is **all** repos defined under `repos.*`.
* A run may specify one or more `repo_groups` and/or explicit `repo_id` values; the target set is the union.
* A repo may appear in multiple groups.
* If a group references an unknown `repo_id`, the orchestrator must fail loudly before doing any work.

Each repo entry declares:

* `path` (local filesystem path)
* `base_branch` (explicit; never inferred)
* `env` (repo-default conda env name; optional)
* `notebook_roots` (where to look for notebooks)
* `allowed_roots` and `deny_roots` (safety boundaries)
* `validation_commands` (repo-standard; optional)
* `notebook_output_policy` (keep/strip; default strip; optional)

Each repo is treated as an **independent unit of work**.

### 3.3 Repo Ingestion and Contract Overlays

Most target repos will not embed orchestrator-only fields in Beads issue bodies. The orchestrator therefore resolves a per-bead execution contract from repo policy plus an orchestrator-maintained overlay:

```text
config/bead_contracts/<repo_id>.toml
```

The overlay file contains:

* `[defaults]` for repo-wide contract defaults
* `[beads."<BEAD_ID>"]` tables for per-bead overrides

Resolution order (highest to lowest):

1. `beads."<BEAD_ID>"` overrides
2. `[defaults]` values
3. Repo entry defaults from `config/repos.toml`

Rules:

* A repo is considered “ingested” when its repo entry exists and its overlay validates.
* If any required contract field cannot be resolved for a ready bead, that bead is not queued and the planner logs loudly with the next action (edit the overlay file).
* Overrides may only **narrow** `allowed_roots` and may not relax `deny_roots`.
* Overrides referencing an unknown Beads ID must fail loudly to prevent typos from silently changing behavior.

---

## 4. Scheduling Model

### 4.1 Execution Window

Default night window:

* **20:00 – 07:00 local time**

Outside this window, scheduler-triggered (automated) runs do not start and exit immediately before doing any work. Roadtrip/manual runs are not subject to this gate, and runs already in progress are never aborted when the window closes.

### 4.2 Cadence

* Automated cadence: **every 45 minutes**
* Manual cadence (roadtrip mode): defaults to 45 minutes, configurable

Rationale: long enough for meaningful work; short enough to checkpoint frequently.

---

## 5. Planning Model (Decks)

### 5.1 Beads as Source of Truth

* Beads is the authoritative task tracker
* Orchestrator interacts with Beads via `bd` CLI
* At the start of work in each repo, call `beads.set_context(workspace_root=<repo_path>)` before any Beads operations
* Only *ready* (unblocked) Beads issues are eligible for work

### 5.2 First-Run Planning Pass

On the first trigger of a run (per repo):

1. Initialize Beads (`bd init --quiet`)
2. Query ready issues (`bd ready --json`)
3. Detect modified/untracked notebooks
4. Create or tag Beads issues for notebook-refactor work
5. Evaluate the repo against engineering desiderata (including semantic modeling / conceptual DRY) and record findings as a run artifact
6. Snapshot a **run deck**:

```text
<cache>/runs/<RUN_ID>/<repo_id>.deck.<YYYY-MM-DD>.json
```

The deck is an immutable snapshot of what is “on deck” for that run.

Each deck item includes the bead ID + title, the resolved contract used for execution (time budget, env, validations, notebook execution, allowed/deny roots), and baseline validation status.

Rationale: deterministic scope; no mid-run task drift.

### 5.3 Bead Planning Contract and Enforcement

The planning pass must ensure that every on-deck bead is executable under a strict orchestrator contract.

* Each on-deck bead must have a complete orchestrator contract that can be resolved deterministically.
* The contract is resolved from:
  * Repo policy from `config/repos.toml`
  * Contract overlays from `config/bead_contracts/<repo_id>.toml` (`[defaults]` plus optional `beads."<BEAD_ID>"` overrides)
* Beads missing resolvable contract fields are not queued; the planner logs loudly with the next action (edit the overlay file).
* Dependency declaration is required in the Beads dependency graph.

At minimum, the contract must declare:

* Per-bead time budget
* Validation commands (repo standard + bead-specific; what must pass to close)
* Conda environment (repo default + optional per-bead override) and whether environment creation is permitted
* Notebook execution requirement (only when explicitly required)
* Allowed/denied roots for file edits (repo policy + optional per-bead refinement)

The resolved contract is recorded in the run deck so that execution uses an immutable snapshot (no mid-run re-resolution).

The planner records baseline validation status before work so that pre-existing failures are visible and not silently attributed to the run.

On bead close, downstream beads are identified via the Beads dependency graph and updated in their bodies with a short status/context note.

---

## 6. Execution Model

### 6.1 Per-Repo Isolation

* Repos may run in parallel
* Each repo enforces a **single active instance** via filesystem locks

### 6.2 Work Units

Within a run, Codex may:

* Resolve **multiple Beads issues** per repo
* Refactor multiple notebooks

Work is time-bounded and incremental.

### 6.3 Supported Work Types

#### Notebook Refactoring

* Extract reusable code from `.ipynb` into `src/*.py`
* Replace notebook code cells with imports and function calls
* Preserve narrative, plotting, and exploratory cells

Constraints:

* Notebook must remain runnable
* No unrelated reformatting or API churn

#### Beads Issue Resolution

* One or more Beads tasks per run
* Tasks must be locally solvable
* Beads state updated (`in_progress` → `closed`) with notes

### 6.4 Timeboxing and Stopping Rules

* Work is time-bounded per tick and per bead.
* Do not start a new bead if fewer than 15 minutes remain in the tick.
* Cap beads per repo per tick (default 2–3).
* If blocked by tests, environment, or scope ambiguity, stop and write bead updates instead of continuing.

### 6.5 Ownership of Git Operations

* The orchestrator owns all staging and commits.
* Default granularity: one commit per bead.
* Commit messages follow: `beads(<id>): <title>` with `RUN_ID` in the commit body.
* Sync policy before branching: `git fetch` only (no pull/rebase).
* Push policy: no push by default.

### 6.6 Guardrails and Safety Boundaries

* Edits are constrained to explicit allowed roots and explicit denylist roots (e.g., data/results/binaries).
* Diff size is capped per tick (files changed and lines added) to prevent runaway changes.
* Unattended command execution is allowlisted (git read-only, env activation, tests, formatting); no long-running training or simulations.

### 6.7 Testing Policy

* Given/When/Then enforcement is checked via linting or simple pattern checks in tests.
  * Optional repo/bead contract key: `enforce_given_when_then = true` in `config/bead_contracts/<repo_id>.toml`.
  * When enabled, the orchestrator fails the close gate if any **new/modified test files** lack `Given`, `When`, and `Then` markers (line-start markers; case-insensitive; comment prefixes like `#`/`//` allowed).

Example:

```toml
[defaults]
enforce_given_when_then = true
```
* Baseline failing tests are recorded by the planner; beads cannot close unless failures are scoped or fixed.
* Minimum close gate: declared validation commands pass plus at least one behavioral test.

### 6.8 Notebook Policy

* Notebook execution is performed only when explicitly required by the bead contract.
* Output handling is controlled by repo-level policy; strip outputs by default.
* Refactor threshold: reusable logic across datasets/notebooks or large multi-cell logic must be extracted into `src/`.

### 6.9 DRY Escalation and Workflow Awareness

* DRY trigger: similar changes in two or more notebooks must be consolidated into shared modules.
* Workflow escalation: propose Snakemake only for true multi-step dependency graphs.
* Workflow scope constraint: minimal initial workflow; no repo-wide restructure in one run.

---

## 7. Documentation & Audit Trail

### 7.1 Repo-Local Run Reports

Each repo must maintain a markdown run report at:

```text
docs/runs/<RUN_ID>.md
```

The run report must contain:

* Summary
* `RUN_ID` and branch name (`run/<RUN_ID>`)
* Beads issues worked (IDs + titles)
* Notebook refactors (notebooks + extracted code locations)
* Tests/commands executed (declared validation commands)
* Failures or skipped steps (with next action)
* Follow-ups (including downstream bead updates)
* Tool versions (bd, codex, git, python/conda)

### 7.2 Orchestrator Logs

The orchestrator maintains:

* Global run logs
* Per-repo stdout/stderr logs
* Cache-side summaries for quick inspection
* A one-command status summary: `codex-status --run-id <RUN_ID>`
* Failure triage as one line per repo with the next action

Rationale: human-readable provenance.

---

## 8. Concurrency and Resource Control

* Parallelism across repos is configurable (`MAX_PARALLEL`)
* Per-repo execution is strictly serialized
* Optional per-repo timeouts are supported

---

## 9. Model and AI Configuration

### 9.1 Model Choice

All unattended runs use:

* **Model**: `gpt-5.2-codex`
* **Reasoning effort**: `xhigh`

Rationale: favor correctness and coherence over speed.

### 9.2 Execution Mode

* Use `codex exec --full-auto`
* No interactive prompts
* No long-running training or simulations

### 9.3 Enforcement

* Model and reasoning settings are centralized in orchestrator configuration and asserted by the CLI.
* Unattended runs must refuse to run if the configured model or reasoning effort does not match the policy.

### 9.4 Version Pinning

Tool versions are recorded in run reports for reproducibility (bd, codex, git, python/conda).

---

## 10. Manual (“Roadtrip”) Mode

A CLI tool allows invoking the same lifecycle manually:

```bash
codex-roadtrip --hours 3
codex-roadtrip --until "YYYY-MM-DD HH:MM"
```

Characteristics:

* Immediate planning pass
* Repeated execution cycles
* Shared `RUN_ID` across cycles
* Same safety and branching rules as automated runs

---

## 11. Non-Goals

The orchestrator will not:

* Modify data or results
* Perform dependency upgrades unless explicitly tasked
* Merge branches automatically
* Coordinate work across repos

---

## 12. Future Extensions (Explicitly Out of Scope)

* Automatic PR creation
* Distributed or cloud execution
* Cross-repo reasoning or refactors
* Fine-grained scheduling by task type

These may be revisited only after the core system is stable and trusted.
