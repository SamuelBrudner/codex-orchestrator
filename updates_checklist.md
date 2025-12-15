# Global Codex Orchestrator – Design Decisions Checklist

This checklist enumerates **explicit design decisions** that must be locked in before treating the orchestrator as production‑worthy. Each item represents a *policy choice*, not an implementation detail. Defaults are recommended to allow forward progress while preserving safety.

---

## 1. Run semantics and persistence

* [ ] **Run boundary definition**

  * What constitutes a single run?
  * Options:

    * One run per night window
    * One run per manual invocation
    * Both, explicitly tracked
  * **Recommended default:** both nightly and manual runs, explicitly tracked

* [ ] **Run ID persistence across ticks**

  * How do 45‑minute launchd ticks reuse the same `RUN_ID`?
  * **Recommended default:** persist `current_run.json` in orchestrator cache with expiry

* [ ] **Run collision policy**

  * What if a new run starts while one is active?
  * **Recommended default:** global run lock; new triggers exit loudly

* [ ] **Run stop conditions**

  * When does a run end?
  * **Recommended default:** end of night window or no actionable beads for N ticks

---

## 2. Repo targeting and configuration

* [ ] **Repo list format**

  * Plain paths vs structured config?
  * **Recommended default:** structured per‑repo config (path, base branch, env, notebook roots, denylist roots)

* [ ] **Base branch policy**

  * How is the base branch selected?
  * **Recommended default:** explicit per repo (e.g., `main`), never implicit

* [ ] **Sync policy before branching**

  * Fetch only vs pull/rebase?
  * **Recommended default:** `git fetch` only; no automatic pulls

* [ ] **Dirty tree handling**

  * Skip, stash, or fail?
  * **Recommended default:** skip repo and log loudly

---

## 3. Branching, PRs, and integration

* [ ] **Branch naming**

  * **Canonical:** `run/<RUN_ID>` (no tool names in branches)

* [ ] **Per‑bead sub‑branches**

  * Are `run/<RUN_ID>/<bead-id>` branches ever created?
  * **Recommended default (v1):** no

* [ ] **Push policy**

  * Does the orchestrator push branches?
  * **Recommended default:** no push by default

* [ ] **PR creation policy**

  * Does the orchestrator open PRs?
  * **Recommended default:** no; manual PRs only

---

## 4. Ownership of git operations

* [ ] **Who stages and commits?**

  * Codex vs orchestrator
  * **Recommended default:** orchestrator owns all git commits

* [ ] **Commit granularity**

  * Per bead, per tick, or per notebook?
  * **Recommended default:** one commit per bead

* [ ] **Commit message format**

  * **Recommended default:** `beads(<id>): <title>` with `RUN_ID` in body

---

## 5. Beads planning contract and enforcement

* [ ] **Planner responsibility**

  * What must the first planning pass guarantee?
  * **Recommended default:** all on‑deck beads include a complete orchestrator contract

* [ ] **Engineering desiderata audit during planning**

  * Should the first planning pass evaluate the repo against explicit software-engineering desiderata (e.g., semantic modeling / conceptual DRY) and record findings as a run artifact?
  * **Recommended default:** yes; report-only by default, with optional creation/tagging of Beads issues for top actionable findings when explicitly enabled

* [ ] **Contract validity enforcement**

  * What if a bead is missing required fields?
  * **Recommended default:** do not queue; log loudly; optionally create a hardening bead

* [ ] **Dependency declaration rule**

  * Graph dependencies vs YAML contract consistency
  * **Recommended default:** require both and verify consistency

* [ ] **Downstream context propagation on close**

  * How are downstream beads identified and updated?
  * **Recommended default:** via Beads dependency graph; update bodies on close

* [ ] **Multi‑bead per tick policy**

  * How many beads may be worked per repo per tick?
  * **Recommended default:** as many as fit within time budget, with per‑bead caps

---

## 6. Timeboxing and stopping rules

* [ ] **Per‑bead time budget**

  * Avoid starting large work late in a tick
  * **Recommended default:** do not start new bead if <15 min remain

* [ ] **Per‑tick work cap**

  * Limit beads per tick
  * **Recommended default:** cap at 2–3 beads

* [ ] **Escalation rule**

  * When should Codex stop and write updates instead of continuing?
  * **Recommended default:** blocked by tests/env/scope → update bead and stop

---

## 7. Conda environment and reproducibility

* [ ] **Canonical env declaration**

  * Repo default vs per‑bead override
  * **Recommended default:** repo default + bead override

* [ ] **Env creation policy**

  * Can envs be created unattended?
  * **Recommended default:** only if explicitly declared in bead contract

* [ ] **Validation command standardization**

  * Standard per repo vs fully per bead
  * **Recommended default:** repo standard + bead‑specific commands

---

## 8. Testing policy

* [ ] **Given / When / Then enforcement**

  * How is this checked?
  * **Recommended default:** lint or simple pattern checks in tests

* [ ] **Baseline failing tests policy**

  * What if tests already fail before the run?
  * **Recommended default:** planner records failures; beads cannot close unless scoped or fixed

* [ ] **Minimum close gate**

  * Absolute minimum verification for closing a bead
  * **Recommended default:** declared validation commands pass + at least one behavioral test

---

## 9. Notebook policy

* [ ] **Notebook execution policy**

  * Execute notebooks or not?
  * **Recommended default:** only when bead explicitly requires it

* [ ] **Output handling policy**

  * Strip outputs, keep outputs, or configurable?
  * **Recommended default:** repo‑level policy; strip by default

* [ ] **Refactor threshold**

  * When must code move from notebook to `src/`?
  * **Recommended default:** reuse across datasets/notebooks or large multi‑cell logic

---

## 10. DRY escalation and workflow awareness

* [ ] **DRY trigger definition**

  * When is duplication severe enough to refactor?
  * **Recommended default:** similar changes in ≥2 notebooks

* [ ] **Workflow escalation policy**

  * When to propose Snakemake vs simple scripts?
  * **Recommended default:** Snakemake only for true multi‑step dependency graphs

* [ ] **Workflow scope constraint**

  * How invasive can workflow introduction be?
  * **Recommended default:** minimal initial workflow; no repo‑wide restructure in one run

---

## 11. Guardrails and safety boundaries

* [ ] **Allowed / denied path policy**

  * What must never be touched?
  * **Recommended default:** explicit denylist (data/results/binaries) + explicit allowed roots

* [ ] **Diff size limits**

  * Prevent runaway changes
  * **Recommended default:** cap files changed and lines added per tick

* [ ] **Command allowlist**

  * Which commands may run unattended?
  * **Recommended default:** git read‑only, env activation, tests, formatting; no training

---

## 12. Run documentation and reporting

* [ ] **Canonical run report location**

  * Where do run reports live in each repo?
  * **Recommended default:** `docs/runs/<RUN_ID>.md`

* [ ] **Run report schema**

  * Required headings
  * **Recommended default:** summary, beads worked, notebook refactors, tests run, failures, follow‑ups

* [ ] **Downstream update logging**

  * Must report downstream bead updates?
  * **Recommended default:** yes

---

## 13. Model and configuration enforcement

* [ ] **Model enforcement mechanism**

  * CLI flags vs centralized profile
  * **Recommended default:** centralized orchestrator profile + CLI assertion

* [ ] **Version pinning**

  * Record tool versions?
  * **Recommended default:** yes; include in run report

---

## 14. Observability and UX

* [ ] **One‑command status summary**

  * How to check progress on return?
  * **Recommended default:** `codex-status --run-id <RUN_ID>`

* [ ] **Failure triage format**

  * How are failures summarized?
  * **Recommended default:** one‑line per repo with next action
