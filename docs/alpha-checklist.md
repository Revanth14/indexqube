# IndexQube Path A — Alpha QA Checklist

Run these checks top-to-bottom before tagging a release or sharing with external testers.
Each item has a clear pass criterion. Mark `[x]` as you go.

---

## 1. Gateway Setup

- [ ] **Build** — `make build-gateway` completes without errors; binary exists at `bin/indexqube-gateway`.
- [ ] **Start** — `./bin/indexqube-gateway` (or `make dev`) starts without fatal log lines.
- [ ] **Health** — `curl -s http://localhost:8080/healthz` returns `{"status":"ok"}` with HTTP 200.
- [ ] **Readiness** — `curl -s http://localhost:8080/readyz` returns `{"status":"ready"}` with HTTP 200.
- [ ] **Admin metrics** — `curl -s http://localhost:9100/metrics` returns Prometheus text with `iq_` prefixed metrics.
- [ ] **Diagnostics** — `curl -s http://localhost:8080/v1/diagnostics` returns JSON with `"status":"ok"`, `"pruning_enabled":true`, and a `history` block.

---

## 2. Extension Install

- [ ] **Package** — `make package-extension` produces `dist/indexqube-extension.zip` without errors.
- [ ] **Load unpacked** — In Chrome `chrome://extensions`, enable Developer Mode, click "Load unpacked", select `extension/`. No errors or warnings in the extension card.
- [ ] **Icon** — Extension icon is visible in the Chrome toolbar at 16px and 32px; no broken-image placeholder.
- [ ] **Popup opens** — Clicking the toolbar icon opens the popup without a blank screen or console errors.
- [ ] **Options page opens** — Right-clicking the icon → "Options" (or clicking the options link) opens the full settings page.

---

## 3. Extension Settings

- [ ] **Gateway URL saved** — Set Gateway URL to `http://localhost:8080`, click Save. Reopen popup; value persists.
- [ ] **Health check** — Click "Check" in popup. Status changes to green "Online at http://localhost:8080 (X ms)".
- [ ] **Diagnostics panel** — After "Check" succeeds, the diagnostics row appears showing pruning state, contract version, and history stats. No raw code or session keys visible.
- [ ] **Enabled toggle** — Uncheck "Enabled", observe the toggle saves immediately. Re-check and save.
- [ ] **Session key** — Click "Reset" next to Session key. A new random hex key appears and saves. The previous key is gone.
- [ ] **Options: Reset all sessions** — Open Options page, click "Forget local sessions". Confirm session summary resets to `Global: xxxx...xxxx` with 0 conversation scopes.

---

## 4. Path A — Optimize Flow (ChatGPT or Claude)

Open https://chatgpt.com or https://claude.ai. Ensure the extension is enabled and the popup shows the gateway as online.

### 4a. Natural language only — no optimization

- [ ] Type a plain sentence like `What is the difference between a goroutine and a thread?` and press Enter or click Send.
- [ ] **Pass**: message sends normally. No status toast appears (the content script intentionally skips natural-language prompts and never calls the gateway). Popup last request does not change from its previous state.

### 4b. First code paste — warmup

- [ ] Paste the following into the composer (no code fences):
  ```
  src/server.go
  package main

  import "fmt"

  func main() {
      fmt.Println("hello")
  }
  ```
  Then add a question: `Does this look correct?` and send.
- [ ] **Pass**: A brief status toast appears ("Optimizing..." or similar). After send, popup last request shows mode `warmup`. Tokens saved = 0 (first time). The message received by the LLM has the code wrapped in a fenced block (visible in the conversation).

### 4c. Unchanged repeat — diff suppressed

- [ ] Paste the **same code block and question** again and send.
- [ ] **Pass**: Popup last request shows mode `unchanged`. Tokens saved > 0. The sent message is significantly shorter than the original (diff marker replaces the body).

### 4d. One-line change — diff applied

- [ ] Change one line of the pasted code (e.g., change `"hello"` to `"hello world"`) and send.
- [ ] **Pass**: Popup last request shows mode `diff`. Tokens saved > 0. The sent message is a short unified diff, not the full file.

### 4e. Copy receipt

- [ ] After any optimized request, click "Copy" in the Last request row.
- [ ] **Pass**: Status shows "Copied". Paste the clipboard into a text editor and verify it looks like:
  ```
  IndexQube saved X estimated input tokens.
  Mode: diff
  Bytes: X -> Y
  Blocks: 1 seen, 1 pruned, 0 skipped
  ```
  No prompt text or code content in the pasted receipt.

---

## 5. Project Memory

- [ ] Open Options page. Enter project memory text like `Always use Go error wrapping with fmt.Errorf("%w", err).`
- [ ] Send a code prompt.
- [ ] **Pass**: The LLM response applies or references the rule (qualitative check — diagnostics does not expose project memory content and gateway logs do not currently record it).

---

## 6. Offline / Degraded Behavior

- [ ] **Stop the gateway** (`Ctrl-C` on the gateway process).
- [ ] Click "Check" in the popup.
- [ ] **Pass**: Status shows red "Offline at http://localhost:8080". Diagnostics panel is hidden (not shown as "0 entries").
- [ ] **Type a prompt and send** in the LLM chat.
- [ ] **Pass**: The message sends normally through the browser without hanging. IndexQube falls back gracefully — it does **not** block the submit.
- [ ] Popup last request may show mode `error` with a gateway-unavailable message. No sensitive content in the error.

---

## 7. Session Isolation

- [ ] Open two separate ChatGPT conversations (two different `/c/<id>` URLs) in the same browser.
- [ ] Send the same code block from conversation A, then from conversation B.
- [ ] **Pass**: Both show mode `warmup` on first send. The history from conversation A does not bleed into conversation B's diff comparison (second send from B is still `warmup`, not `unchanged`).

---

## 8. Reset Flows

- [ ] In the popup, click "Reset all" (session + usage).
- [ ] **Pass**: Session key changes, usage counters reset to 0, last request shows "Idle".
- [ ] Send the same code block again.
- [ ] **Pass**: Mode is `warmup` (a new session key means the gateway treats this as a new tenant; prior in-memory history for the old key persists until TTL/eviction, but this new session starts clean).

---

## Acceptance Criteria Summary

| Scenario | Expected mode |
|---|---|
| Natural language only | no gateway request |
| First code paste | `warmup` |
| Same code, unchanged | `unchanged` |
| Same code, one line changed | `diff` |
| Gateway offline | `error` (prompt still sends) |
