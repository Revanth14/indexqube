# IndexQube

Bring-your-own-key AI coding assistant for VS Code with context optimization. IndexQube routes your questions through a Go gateway that deduplicates repeated code context using session-aware diffs, so you burn fewer tokens on every turn.

Your provider key is stored in VS Code SecretStorage and is sent only for the active request to your configured gateway, which forwards it to the provider and never persists it.

---

## Requirements

- **VS Code** 1.85 or later
- **IndexQube Gateway** running locally ([see below](#running-the-gateway))
- An API key from **Anthropic**, **OpenAI**, **Azure OpenAI**, or **AWS Bedrock**

---

## Quick Start (Beta)

### 1. Install the VSIX

Open the Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`) and run:

```
Extensions: Install from VSIX...
```

Select the `indexqube-x.x.x.vsix` file you received.

### 2. Run the Gateway

The gateway is a single Go binary. No external services required.

**From source:**

```bash
git clone https://github.com/Revanth14/indexqube
cd indexqube
make build-gateway         # produces bin/indexqube-gateway
./bin/indexqube-gateway
```

**Or run without building:**

```bash
cd gateway
go run ./cmd/gateway
```

Confirm it is running:

```bash
curl http://localhost:8080/healthz
# {"status":"ok"}
```

### 3. Configure the Extension

Open VS Code Settings (`Cmd+,`) and search for **IndexQube**:

| Setting | Default | Description |
|---|---|---|
| `indexqube.gatewayUrl` | `http://localhost:8080` | URL of your local gateway. |
| `indexqube.provider` | `anthropic` | LLM provider: `anthropic`, `openai`, `azure`, `bedrock`. |
| `indexqube.model` | `claude-sonnet-4-6` | Model ID to use. |
| `indexqube.contextMode` | `activeFile` | What editor context to include: `selection`, `activeFile`, `openEditors`, `workspace`. |
| `indexqube.memoryMode` | `workspace` | `workspace` reuses one session per workspace (enables deduplication); `isolated` uses a fresh session per request. |
| `indexqube.privacyMode` | `standard` | `standard` allows localhost and HTTPS remote gateways; `localOnly` restricts to localhost only. |
| `indexqube.maxTokens` | `4096` | Maximum output tokens. |
| `indexqube.temperature` | `0` | Sampling temperature (0–2). |
| `indexqube.maxContextBytes` | `120000` | Total context size cap sent to the gateway. |
| `indexqube.maxFileBytes` | `60000` | Per-file context size cap. |
| `indexqube.maxWorkspaceFiles` | `30` | Files sampled in workspace context mode. |
| `indexqube.contextExcludePatterns` | `[]` | Glob patterns excluded from context (e.g. `["**/*.generated.ts"]`). |
| `indexqube.projectMemory` | `""` | Optional rules prepended to every request as a system instruction. |

### 4. Save Your Provider Key

Run the command:

```
IndexQube: Set Provider Key
```

Your key is stored in VS Code SecretStorage — encrypted at rest by the OS keychain, never written to disk as plaintext, and never logged.

### 5. Open the Chat Panel

Click the IndexQube icon in the Activity Bar, or run:

```
IndexQube: Open Chat
```

Ask a question. The first turn attaches your active file and sends it through the gateway (warmup). On subsequent turns with the same file unchanged, the gateway replaces the full file body with a tiny marker. When you edit the file, only the changed lines are sent as a unified diff.

---

## Context Modes

| Mode | What is included |
|---|---|
| `selection` | Only the currently selected text. |
| `activeFile` | The full active editor file (up to `maxFileBytes`). |
| `openEditors` | All open editor tabs. |
| `workspace` | A sample of workspace files filtered by relevance. |

Files that match sensitive patterns (`.env`, `.pem`, private key files, lock files, generated directories) are automatically excluded regardless of mode.

---

## Reading the Optimizer Receipt

After each response, the chat shows an optimizer receipt:

```
Optimizer: diff  •  42 tokens saved  (1 block pruned)
```

Modes:
- **warmup** — first turn for a file; full content sent, session populated.
- **unchanged** — file unchanged; full body replaced with a single-line marker.
- **diff** — file changed; only the diff sent.
- **none** — no code fences found; prompt sent as-is.

---

## Privacy and Key Handling

- Provider keys are stored using VS Code SecretStorage (OS keychain integration).
- Keys are sent only on the active request to the configured gateway and are never stored by the gateway.
- The gateway never logs or persists keys.
- `localOnly` privacy mode blocks all non-localhost gateway URLs — useful if you want to guarantee the gateway cannot be misconfigured to point at a remote HTTPS endpoint.
- Context redaction: files matching sensitive path patterns are silently skipped. Content containing secrets (private key headers, `sk-` style tokens, AWS access keys) is blocked before sending.

---

## Commands

| Command | Description |
|---|---|
| `IndexQube: Open Chat` | Open the chat sidebar. |
| `IndexQube: Set Provider Key` | Securely save your provider key. |
| `IndexQube: Pick Model` | Choose a known model for the configured provider or enter a custom model ID. |
| `IndexQube: Check Gateway` | Ping the gateway and show latency. |
| `IndexQube: Reset Workspace Session` | Start a fresh session (clears server-side diff history for this workspace). |
| `IndexQube: Open Settings` | Jump to IndexQube settings. |

---

## Known Limitations (Beta)

- **Streaming only.** Non-streaming (`stream: false`) requests are not supported. All responses stream token by token.
- **No tool/function calling.** Only plain text completions.
- **No image input.** Multimodal requests are not supported.
- **Single workspace session.** In `workspace` memory mode, all conversations in a VS Code window share one session key. Open a new window for a clean slate.
- **No conversation history persistence.** Closing the panel clears the chat UI. The gateway session (diff history) persists until its 2-hour TTL expires or you reset it.
- **Azure and Bedrock not fully validated.** Anthropic and OpenAI are the tested providers for beta. Azure and Bedrock adapters exist but have not been end-to-end tested with real credentials.
- **Workspace context mode samples up to 30 files.** Large monorepos may miss relevant files. Use `activeFile` or `openEditors` for more predictable context.
- **Context is not ranked by relevance.** Files are included in open-order, not by semantic similarity to your question.

---

## Troubleshooting

**"Gateway offline" in the chat panel**

1. Check the gateway is running: `curl http://localhost:8080/healthz`
2. Confirm `indexqube.gatewayUrl` matches the address the gateway is listening on.
3. Run `IndexQube: Check Gateway` from the Command Palette for a latency ping.

**"Provider key invalid" error**

Run `IndexQube: Set Provider Key` and re-enter your key. Keys are never shown after storage — re-enter to replace.

**Chat is slow to start streaming**

The first token latency includes gateway processing + provider TTFB. Subsequent tokens stream continuously. Latency varies by provider and model.

**Context shows 0 bytes**

Make sure an editor tab is open and `contextMode` is set to `activeFile` or `selection`. In `selection` mode, you must have text selected before sending.

---

## Reporting Issues

File issues at: https://github.com/Revanth14/indexqube/issues

Include:
- VS Code version
- IndexQube version
- Gateway version (`curl http://localhost:8080/healthz` output)
- Whether the issue is with context, streaming, key storage, or the optimizer
