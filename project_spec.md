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

* A *run* corresponds to a single session (e.g. an evening, a roadtrip)
* Each run has a stable identifier: `RUN_ID`
* All artifacts (branches, decks, reports) are namespaced by `RUN_ID`

Rationale: bounded reasoning, auditability, clean rollback.

### 2.3 Branch-First Safety Model

* Each repo uses a fresh branch per run:

```
run/<RUN_ID>
```

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

Repositories are enumerated via a simple config file:

```
config/repos.txt
```

Each repo is treated as an **independent unit of work**.

---

## 4. Scheduling Model

### 4.1 Execution Window

Default night window:

* **20:00 – 07:00 local time**

Outside this window, automated runs exit immediately.

### 4.2 Cadence

* Automated cadence: **every 45 minutes**
* Manual cadence (roadtrip mode): defaults to 45 minutes, configurable

Rationale: long enough for meaningful work; short enough to checkpoint frequently.

---

## 5. Planning Model (Decks)

### 5.1 Beads as Source of Truth

* Beads is the authoritative task tracker
* Orchestrator interacts only via `bd` CLI
* Only *ready* (unblocked) Beads issues are eligible for work

### 5.2 First-Run Planning Pass

On the first trigger of a run (per repo):

1. Initialize Beads (`bd init --quiet`)
2. Query ready issues (`bd ready --json`)
3. Detect modified/untracked notebooks
4. Create or tag Beads issues for notebook-refactor work
5. Snapshot a **run deck**:

```
<cache>/runs/<RUN_ID>/<repo_id>.deck.<YYYY-MM-DD>.json
```

The deck is an immutable snapshot of what is “on deck” for that run.

Rationale: deterministic scope; no mid-run task drift.

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

---

## 7. Documentation & Audit Trail

### 7.1 Repo-Local Run Reports

Each repo must maintain a markdown run report containing:

* `RUN_ID`
* Branch name (`run/<RUN_ID>`)
* Beads issues worked (IDs + titles)
* Notebooks refactored and code locations
* Tests/commands executed
* Failures or skipped steps

### 7.2 Orchestrator Logs

The orchestrator maintains:

* Global run logs
* Per-repo stdout/stderr logs
* Cache-side summaries for quick inspection

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

* **Model**: `gpt-5.2`
* **Reasoning effort**: `xhigh`

Rationale: favor correctness and coherence over speed.

### 9.2 Execution Mode

* Use `codex exec --full-auto`
* No interactive prompts
* No long-running training or simulations

---

## 10. Manual (“Roadtrip”) Mode

A CLI tool allows invoking the same lifecycle manually:

```
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
