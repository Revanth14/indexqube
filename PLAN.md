# IndexQube Product and Delivery Plan

_Updated September 1, 2026 from the original product blueprint and the current `codex/agent-router-foundation` implementation._

## Product definition

**IndexQube is a local-first control plane for coding agents.**

> Talk to one. Get work done by many.

The user gives a repository-level task to IndexQube. IndexQube owns the task, selects and supervises an agent, protects the workspace, records what happened, verifies the result, and preserves enough canonical context to continue with the same agent or hand work to another one.

IndexQube now has two complementary planes:

1. **Control plane** — tasks, turns, agent processes, routing, session continuity, workspace safety, verification, and user interfaces.
2. **Data plane** — the existing local L7 gateway for provider traffic, prompt optimization, caching, auditing, usage measurement, and upstream failover.

The control plane is the product. The data plane is an optional capability used by agents and advanced deployments. Product work should no longer let proxy optimization displace the end-to-end task experience.

## Invariants

These are release-blocking properties, not aspirations:

1. **IndexQube owns canonical task state.** Native Codex or Claude sessions accelerate continuation but never become the source of truth.
2. **The filesystem and Git describe what exists.** SQLite records what IndexQube observed and decided.
3. **Write safety is deterministic.** OS locks and fencing epochs decide who may mutate a workspace; an LLM never decides this.
4. **Terminal events follow durable state.** A client must not observe success, failure, or cancellation before SQLite contains the same outcome.
5. **An automatic handoff is allowed only when workspace state is understood.** Unknown mutation risk becomes `needs_attention`.
6. **Credentials stay with official clients or request-scoped provider adapters.** IndexQube does not extract subscription tokens or persist provider credentials.
7. **Interfaces are clients of one engine.** CLI, TUI, dashboard, editor integrations, and automation all use the same local control API.
8. **Optimization must be semantics-preserving.** Saving tokens is never more important than retaining instructions, tool results, or provider prompt-cache behavior.

## Current baseline

The original plan described a greenfield project. That is no longer accurate. The repository currently contains the following foundation.

| Area | Status | Current reality |
|---|---|---|
| Local daemon | Shipped foundation | One process runs a loopback model proxy and a separate loopback control API. |
| Canonical task state | Shipped foundation | SQLite stores tasks, turns, route attempts, backend sessions, workspace snapshots, events, and write epochs. |
| Agent runtime | Shipped foundation | Child-process supervision, cancellation, bounded stderr, normalized events, and process-group termination exist. |
| Fake backend | Shipped foundation | Deterministic success, failure, mutation, stale epoch, sleep/cancel, and lost-session scenarios are testable. |
| Workspace safety | Shipped foundation | Git-root identity, per-path dirty-baseline state, authoritative turn deltas, bounded diffs, OS write locks, inherited lock lifetime, fencing checks, and agent-evidence mismatch detection exist. |
| Recovery | Shipped foundation | Interrupted daemon work is reconciled; lost native sessions can be replaced from canonical history when safe. |
| Control API | Shipped foundation | Create, list, inspect, continue, cancel, backend health, assembled task evidence, and replayable/live SSE task events exist. |
| Task CLI | Shipped foundation | `iq task`, `iq tasks`, `iq task status`, `iq task show`, and `iq continue` use the control API and stream normalized events. |
| Codex task backend | Shipped foundation | Read-only execution plus guarded App Server workspace-write execution, durable command/file approvals, event parsing, evidence, native-session resume, and lost-session detection work. |
| Claude task backend | Missing | Claude traffic can use the proxy, but Claude Code is not yet an orchestrated task backend. |
| Routing and handoff | Partial | Backend selection is explicit and a lost session can recover within one backend; there is no policy router or cross-backend handoff. |
| Verification | Missing | Auditing exists, but task-scoped build/test/lint verification and durable verification results do not. |
| User experience | Partial | Durable task listing, evidence inspection, and approval commands work; there is no TUI or active dashboard, and bare `iq` still opens the legacy Claude wrapper. |
| Data plane | Advanced | Claude Messages and OpenAI Responses ingress, provider adapters, streaming, optimization, prompt-cache preservation, LSM/SQLite caches, telemetry, setup, audit, and benchmarking exist. |
| Distributed coordination | Deferred | Redis is absent and is not required for the local product. Kubernetes assets apply to the gateway, not the canonical local task engine. |

The repository's full supported Go validation (`make check`) passes at this baseline.

## Target architecture

```text
                    CLI / TUI / Dashboard / Editor
                                  |
                         Local Control API
                                  |
          +------------------- Orchestrator -------------------+
          | task state | routing | approvals | recovery        |
          | handoff    | events  | verification | retention    |
          +----------+----------------------+------------------+
                     |                      |
             Workspace Guard          Agent Backends
          Git + locks + epochs       Codex | Claude
                     |                      |
                     +------ filesystem ---+
                                  |
                        optional Data Plane
               optimization | cache | audit | usage
                                  |
                         Provider endpoints

              SQLite = canonical durable task history
              Native sessions = disposable continuation cache
              Redis = absent until multi-process coordination is real
```

Use **backend** for a coding-agent executor such as Codex or Claude Code and **provider** for an upstream model API such as Anthropic, OpenAI, Bedrock, or Azure. The code and CLI currently blur these terms; new work should keep them distinct.

## Task evidence read model

`TaskEvidence` is the stable read model shared by CLI, TUI, and dashboard clients. It is assembled from canonical records rather than becoming another source-of-truth table:

```text
TaskEvidence
|- task and turns
|- assistant results and failures
|- normalized commands
|- changed files
|- workspace snapshots
|- backend sessions
|- route attempts and future handoffs
|- future approvals and verification
`- underlying normalized event timeline
```

Clients should render this projection instead of reimplementing joins or parsing backend-specific events.

## Release strategy

The plan is organized by exit gates rather than dates. A phase is complete only when its acceptance tests pass in a real repository, including a repository that is already dirty.

### Gate 0 — Stabilize the foundation

Goal: make the new control-plane branch safe to merge and easy to evaluate.

- [x] Document the first durable task flow in the main README.
- [x] Add a deterministic control-plane smoke flow with optional real-Codex execution.
- [x] Add task listing to the store, API, and CLI so users can rediscover task IDs after a restart.
- [x] Expose turns, sessions, route attempts, snapshots, commands, files, and events through `TaskEvidence`.
- [x] Make `backend` canonical in the task CLI/API while retaining `provider` as a compatibility alias.
- [ ] Merge the foundation branch; authenticated real-Codex write and approval alpha smokes are green.
- Merge only with `make check` and the manual smoke flow green.

Exit criteria:

```text
iq start
iq task --provider fake "hello"
iq task --provider codex "explain this repository"
iq tasks
iq task show TASK
iq continue TASK "go deeper"
iq stop && iq start
iq task show TASK
```

All commands return durable, consistent state and no task becomes undiscoverable.

### Gate 1 — Complete a safe Codex task loop

Goal: one real agent can safely finish useful read and write tasks under IndexQube ownership.

- [x] Enable Codex `workspace_write` with the existing lock and fencing guard inherited by the child process.
- [x] Treat `iq task --write` as the explicit workspace grant while keeping Codex inside its workspace-write sandbox and OS-guarded child lifetime.
- [x] Implement durable App Server approval requests and decisions:
  - backend emits `approval_requested`;
  - task pauses durably as `awaiting_approval`;
  - `iq approvals`, `iq approve`, and `iq deny` record the user's choice before the backend resumes;
  - cancellation, timeout, and restart never silently approve a pending action.
- [x] Persist bounded command evidence and every changed path reported by normalized Codex events.
- [x] Compare per-path pre/post state even when the adapter does not report a file event; persist the authoritative delta and move mismatches to `needs_attention`.
- Make cancellation idempotent and expose a clear final task state.
- Add close/reopen semantics so `open`, `running`, `needs_attention`, and `closed` have user-visible meanings.
- Test dirty baseline preservation, concurrent writer rejection, child-process crash, daemon crash, stale events, and mutation followed by failure.

Exit criteria:

- A user can ask Codex to make a bounded change, approve it, see the diff and commands, cancel safely, restart the daemon, and continue the IndexQube task.
- Pre-existing dirty files are never attributed to the agent or overwritten by recovery logic.
- A failed mutation-capable run is never automatically handed to another backend unless the post-state is proven safe by policy.

### Gate 2 — Add Claude Code and deterministic handoff

Goal: make “one task, multiple workers” real without pretending routing is intelligent yet.

- Implement a Claude Code backend using the same process runner and normalized event contract.
- Keep Claude task execution separate from Anthropic API proxying; the backend may opt into the data plane without depending on it for correctness.
- Support native Claude session creation, continuation, version probing, cancellation, and lost-session recovery.
- Build a canonical handoff packet from:
  - original goal;
  - completed user/assistant turns;
  - current request;
  - current Git/workspace snapshot;
  - changed files and command summaries;
  - latest verification result;
  - bounded failure reason.
- Add explicit commands first: `--backend codex`, `--backend claude`, `iq handoff TASK --to claude`, and task pinning.
- Add deterministic ordered fallback only for classified pre-mutation failures such as unavailable binary, rate limit, provider unavailability, or lost native session.
- Record every route decision and handoff as canonical state.

Exit criteria:

- The same task can continue on Codex, recover from a lost Codex session, and be explicitly handed to Claude without losing task history.
- Automatic fallback occurs only for allowlisted failure classes and never crosses an uncertain write boundary.

### Gate 3 — Make results verifiable

Goal: IndexQube reports evidence, not merely an agent's claim of completion.

- Add durable `verification_runs` and `verification_checks` rather than storing verification only as generic events.
- Detect project checks conservatively from repository files; never invent a destructive or networked command.
- Start with explicit or configured commands, then add safe detection for Go, Node, Python, Rust, and common monorepos.
- Run checks after successful mutation-capable turns, with timeouts and captured exit status.
- Distinguish `agent_succeeded`, `verified`, `verification_failed`, and `verification_skipped`.
- Integrate the existing audit engine as a separate security check with severity and evidence.
- Include verification output in handoff packets and task summaries.

Exit criteria:

- Every completed write task clearly says what was changed, which checks ran, what passed or failed, and whether human attention is required.
- Routing performance data uses verified outcomes, not self-reported agent success.

### Gate 4 — Ship the primary local experience

Goal: make IndexQube feel like one persistent product rather than a collection of subcommands.

- Build the TUI as the first rich client of the control API.
- Required views: task list, active task conversation, backend/health, approvals, changed files, commands, verification, and handoff history.
- Support attach/detach so closing the TUI does not cancel a running task.
- Preserve backward compatibility during transition:
  1. keep `iq claude` as the explicit optimized Claude wrapper;
  2. introduce `iq ui` for the TUI;
  3. after an announced migration, make bare `iq` open the TUI.
- Build the local dashboard only after the TUI proves the API. Serve it from the loopback daemon with the same daemon-scoped authentication.
- Treat the retired browser/extension surfaces as archived experiments, not active deliverables.

Exit criteria:

- A user can start with bare `iq`, submit and continue tasks, handle approvals, inspect evidence, switch agents, detach, and return later without knowing native session IDs.

### Gate 5 — V1 hardening and release

Goal: a dependable local product for Codex and Claude Code.

- Add schema migrations with downgrade-safe backups and corruption diagnostics.
- Add retention, redaction, bounded event payloads, orphan process cleanup, and log rotation.
- Add compatibility fixtures for supported Codex and Claude CLI versions and fail closed on unknown breaking protocols.
- Add installer/update rollback, signed release artifacts, macOS/Linux builds, and `iq doctor` coverage for both task backends.
- Measure end-to-end task latency, handoff count, verification outcome, crash recovery, and opt-in anonymous reliability telemetry.
- Run an alpha on real repositories before calling routing automatic or intelligent.

V1 is:

```text
IndexQube
|- one canonical local task history
|- Codex and Claude Code backends
|- safe read/write execution and approvals
|- native session continuation and canonical recovery
|- explicit routing plus conservative fallback/handoff
|- deterministic workspace safety
|- task-scoped verification and audit evidence
|- TUI, task CLI, and basic local dashboard
`- optional local optimization/data plane
```

## Immediate implementation backlog

Work in this order:

1. **Done:** merge-readiness documentation and a restart-aware control-plane smoke script.
2. **Done:** `iq tasks` and `iq task show TASK`, backed by durable list and `TaskEvidence` APIs.
3. **Done:** Codex workspace-write execution with locked child lifetime and integration coverage.
4. **Done:** durable App Server approval request/response state and CLI commands.
5. **Done:** mutation reconciliation based on per-path pre/post snapshots, independent of adapter events.
6. Idempotent cancellation plus explicit close/reopen task semantics.
7. Verification schema, configured checks, and post-turn verifier for the demo gate.
8. Control API authentication before any browser surface is enabled.
9. Claude Code backend and protocol fixtures.
10. Explicit handoff, task pinning, and conservative failure classification.
11. TUI.
12. V1 hardening, packaging, and alpha feedback.

Items 1–6 complete the single-agent product. Items 7–9 deliver the multi-agent promise. Items 10–12 make it trustworthy and usable.

## Explicitly deferred

Do not put these on the V1 critical path:

- Gemini, Copilot, Grok, local-model, or generic MCP agent backends;
- learned routing, embeddings-based task classification, or autonomous swarms;
- parallel writers, automated merge resolution, or worktree orchestration;
- Redis, Kafka, distributed task workers, or cloud canonical state;
- subscription quota scraping or private API/token extraction;
- cloud dashboard, team tenancy, billing, or organization policy;
- additional custom storage-engine work unless measurements show it blocks the local task experience;
- new provider proxy features unrelated to reliability of Codex/Claude task execution.

Kubernetes and remote gateway deployment may continue as a separate data-plane track, but they do not make the local task control plane distributed and are not V1 acceptance criteria.

## Success measures

The north-star metric is **verified tasks completed without manual agent switching**.

Track locally first:

- task completion and verified completion rate;
- percentage of tasks recovered after daemon or native-session loss;
- unsafe handoffs prevented;
- approval wait time and cancellation success;
- p50/p95 time to first event and total task duration;
- backend failure classes and fallback success;
- tasks resumed after a day or longer;
- token/cost reduction from the data plane without verification regression.

Do not optimize a routing score until there are enough verified outcomes to evaluate it.

## Decision filter

For every proposed feature, ask:

1. Does it make the single IndexQube task experience more complete?
2. Does it preserve canonical state and deterministic workspace safety?
3. Is it required for Codex/Claude V1, or can it wait?
4. Can its behavior be proven with a fake backend or an integration fixture?
5. Does it improve verified completion rather than only adding infrastructure?

If the answer to the first two is no, it is not control-plane work. If the answer to the third is no, defer it until the core loop is excellent.
