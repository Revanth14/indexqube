# IndexQube

**A local-first control plane for coding agents.**

IndexQube gives agent work a durable, permissioned, inspectable home. It keeps
the task alive across terminal exits, daemon restarts, and lost native sessions;
guards workspace writes; records approvals; reconciles what changed; and stores
verification evidence alongside the result.

[Website](https://indexqube.com) · [Product plan](./PLAN.md)

> **Alpha:** the control plane is usable from source, but release packaging and
> public installation are not ready. Treat the commands below as a contributor
> preview, not a stable installation contract.

## Why IndexQube

A coding agent session is useful runtime state, but it is a poor source of
truth. It can disappear, crash after changing files, or report a result that no
longer matches the workspace.

IndexQube owns the longer-lived record:

- the task and every turn;
- the selected backend and native-session lineage;
- explicit workspace-write permission;
- commands, changed paths, approvals, and cancellations;
- pre/post workspace snapshots and evidence mismatches;
- post-task verification checks and their output.

The operating rule is simple: **the agent is temporary; the task is not.**

## What works today

| Capability | Status |
|---|---|
| Durable task creation, listing, continuation, close, and reopen | Shipped foundation |
| Codex read-only and guarded workspace-write execution | Shipped foundation |
| SQLite task history and replayable/live task events | Shipped foundation |
| OS workspace locks, fencing epochs, and dirty-baseline reconciliation | Shipped foundation |
| Durable command/file approvals and retry-safe cancellation | Shipped foundation |
| Changed-file, command, route, snapshot, and session evidence | Shipped foundation |
| Configured recipes plus automatic Go, Node, Python, and Rust verification | Shipped foundation |
| Task-scoped security audit findings with severity and evidence | Shipped foundation |
| Claude Code as an orchestrated task backend and explicit handoff | Planned |
| TUI, authenticated control API, and release installer | Planned |

The existing Claude/OpenAI-compatible local proxy remains available as an
optional data plane. The durable task control plane is the primary product.

## Try the control plane from source

Requirements:

- macOS or Linux;
- Go 1.25 or newer;
- Git;
- the Codex CLI installed and authenticated for real Codex tasks.

Build and start the loopback daemon:

```bash
git clone https://github.com/Revanth14/indexqube.git
cd indexqube
make build
./bin/iq doctor
./bin/iq start
./bin/iq status
```

The daemon starts in the background. Its model proxy listens on
`127.0.0.1:17373`; its task control API listens separately on
`127.0.0.1:17374`.

Create a read-only task in the current Git workspace:

```bash
./bin/iq task --backend codex "explain the retry and cancellation path"
```

Grant one task permission to modify the workspace:

```bash
./bin/iq task --backend codex --write \
  "harden cancellation, add regression coverage, and verify the result"
```

IndexQube streams the run and prints the task ID. The durable record remains
available after the command exits:

```bash
./bin/iq tasks
./bin/iq task status TASK_ID
./bin/iq task show TASK_ID
./bin/iq continue TASK_ID "check the remaining edge cases"
```

Stop the daemon when finished:

```bash
./bin/iq stop
```

By default, state lives under `~/.indexqube`. Set `INDEXQUBE_HOME` to isolate
task history, logs, cache data, sessions, setup backups, and the local anonymous
machine identifier.

## Permissions and approvals

`--write` is an explicit, up-front grant for that IndexQube task. The Codex
child still runs inside its workspace-write sandbox and under IndexQube's OS
workspace lock and fencing epoch.

If the backend requests a command or file-change escalation, IndexQube first
commits the request to SQLite and moves the task to `awaiting_approval`. A user
can then make a durable decision:

```bash
./bin/iq approvals
./bin/iq approve APPROVAL_ID
./bin/iq deny APPROVAL_ID
```

The decision is stored before the backend resumes. Pending requests are
cancelled during daemon recovery and are never silently approved after a
restart.

Cancellation is also durable and safe to repeat:

```bash
./bin/iq cancel TASK_ID
./bin/iq task close TASK_ID
./bin/iq task reopen TASK_ID
```

A cancelled read-only turn returns to `open`. A write turn whose workspace
state cannot be proven safe becomes `needs_attention`. Reopening such a task is
an explicit acknowledgement, not a silent reset.

## Evidence and verification

`iq task show TASK_ID` assembles the task's canonical evidence from SQLite:
turns, backend sessions, route attempts, snapshots, normalized commands,
changed files, approvals, cancellations, verification runs, and the underlying
event timeline.

Changed-file evidence comes from per-path pre/post Git state, including edits
inside an already-dirty baseline. IndexQube compares that authoritative delta
with agent-reported file events. A mismatch is persisted and moves the task to
`needs_attention`.

After a successful write turn, IndexQube first looks for an explicit
`.indexqube/verification.json`. If no recipe exists, it conservatively detects
checks from the changed paths:

| Ecosystem | Detection | Command |
|---|---|---|
| Go | Nearest `go.mod` | `go test -mod=readonly ./...` |
| Node | Nearest `package.json` with a non-empty `test` script | `npm test`, `pnpm test`, `yarn test`, or `bun run test` |
| Python | Nearest project with an explicit pytest signal | `python3 -m pytest -p no:cacheprovider`, or the matching uv/Poetry/PDM runner |
| Rust | Nearest `Cargo.toml` | `cargo test --offline`, with `--locked` when a lockfile exists |

Dependency-manager offline controls are set for every automatic check. Rust
build output uses a temporary target directory, and Python bytecode and pytest
cache writes are disabled.

### Configured recipes

Use an argv array rather than a shell command string:

```json
{
  "version": 1,
  "checks": [
    {
      "name": "API tests",
      "kind": "test",
      "command": ["make", "test"],
      "cwd": "services/api",
      "paths": ["services/api"],
      "timeout_seconds": 180,
      "env": {
        "APP_ENV": "test"
      }
    }
  ]
}
```

Commit the recipe with the project before relying on it:

```bash
git add .indexqube/verification.json
git commit -m "Configure IndexQube verification"
```

Configured recipes are authoritative and replace auto-detection for that
workspace. `paths` entries are workspace-relative prefixes; omit `paths` to run
a check after every successful write turn. `kind` can be `test`, `lint`,
`typecheck`, `build`, `security`, or `custom`. Timeouts default to two minutes
and can be raised to ten minutes. The recipe must be Git-tracked before a turn
starts; IndexQube never executes an untracked recipe.

Recipe parsing is strict. Absolute executables, shell entry points, escaping or
symlinked working directories, protected environment overrides, oversized
commands, and unknown fields fail closed. If an agent creates, edits, deletes,
or renames the recipe during a turn, IndexQube records a configuration failure
instead of executing the new instructions. A reviewed workspace-relative
script can be invoked directly.

### Automatic security audit

Every successful write turn with authoritative changed paths also gets a
separate rule-based security check. IndexQube compares findings in its bounded
pre/post Git-diff evidence, so a finding already present in a dirty baseline is
not attributed to the turn merely because its line number moved. Changed
untracked files, which Git diffs do not contain, are scanned as bounded
current-file evidence and labeled clearly because their content may predate the
turn.

Findings are normalized into durable records with a stable rule ID, severity,
category, scope, path and line when available, redacted evidence, explanation,
and occurrence count. They appear in the control API and `iq task show` rather
than living only in a generated report.

The automatic policy is explicit:

- **high** severity fails verification and moves the task to
  `needs_attention`;
- **medium** and **low** severity produce `verified_with_warnings` while the
  task remains open;
- no findings produces a passed security check.

A truncated stored diff, an oversized file, a symlink, or another unscannable
changed untracked file becomes an audit-coverage warning. This scanner is local
heuristic triage, not proof that code is safe or exploitable; high findings
still require human review before commit or release.

Every executed check has bounded output, a bounded process group, and the
existing workspace lock. If verification changes tracked or unignored Git
workspace state, the run fails closed. A failed check preserves the agent's
completed turn but moves the task to `needs_attention`. When no project test
recipe or supported ecosystem matches, the durable security check still makes
clear what was reviewed rather than implying that tests ran.

Verification executes repository code. Offline package-manager settings prevent
implicit dependency downloads, but they are not a complete OS or network
sandbox; project tests and reviewed custom recipes retain the host access
available to their process. Git-ignored side effects are outside the current
workspace-stability comparison.

## Architecture

```mermaid
flowchart LR
    UI[CLI / future TUI] --> API[Loopback control API]
    API --> ORCH[Orchestrator]
    ORCH --> STORE[(SQLite task store)]
    ORCH --> GUARD[Workspace guard]
    ORCH --> AGENT[Codex backend]
    ORCH --> VERIFY[Verification runner]
    AGENT --> SESSION[Native agent session]
    GUARD --> REPO[Git workspace]
    VERIFY --> REPO

    CLIENT[Claude / OpenAI-compatible client] -. optional .-> PROXY[Local model proxy]
    PROXY -.-> UPSTREAM[Provider API]
```

The control and data planes run in one local daemon today, but they have
separate responsibilities:

- **Control plane:** task state, execution, permissions, approvals, workspace
  safety, recovery, evidence, and verification.
- **Data plane:** provider traffic, prompt optimization, caching, streaming,
  auditing, and optional telemetry.

Official clients retain their own provider credentials. IndexQube does not
extract subscription tokens or persist provider credentials in task state.

## Optional proxy workflow

The proxy's deepest integration is currently Claude Code through Anthropic
Messages. OpenAI-compatible Responses ingress is also available for clients
that can select a local base URL.

```bash
./bin/iq setup claude
./bin/iq claude

./bin/iq setup codex
./bin/iq doctor
```

`iq setup` records backups before changing supported agent configuration, and
`iq unsetup` restores them. This proxy workflow is distinct from using Codex as
the durable `iq task` backend.

For a local security report from an explicitly captured Claude session:

```bash
./bin/iq claude --dump-payloads
./bin/iq audit latest
```

Payload dumping is opt-in. Reports are written locally under
`.indexqube/reports`.

## Development

The active build is Go-only:

```bash
make check       # formatting, vet, unit tests, and both binaries
make test-race   # race-enabled test suite
make build       # bin/iq and bin/indexqube-gateway
make control-smoke
```

Optional real-agent smoke lanes require an installed, authenticated Codex CLI:

```bash
IQ_SMOKE_REAL_CODEX=1 make control-smoke
IQ_SMOKE_REAL_APPROVAL=1 make control-smoke
```

Repository map:

```text
gateway/cmd/iq/                    CLI and local daemon lifecycle
gateway/internal/control/          loopback control API
gateway/internal/orchestrator/     canonical task execution
gateway/internal/taskstore/        SQLite state and evidence
gateway/internal/workspace/        locks, fencing, and Git snapshots
gateway/internal/agent/            agent contracts and backends
gateway/internal/verification/     post-task verification
gateway/internal/securityaudit/    shared rule-based security scanner
gateway/internal/proxy/            optional model traffic data plane
web/                               indexqube.com
PLAN.md                            product gates and implementation order
```

## Next milestones

Work is currently focused on:

1. control API authentication;
2. Claude Code task execution and deterministic handoff;
3. an attachable local TUI;
4. signed release packaging and real-repository alpha feedback.

The acceptance gates and non-negotiable safety invariants live in
[PLAN.md](./PLAN.md).
