# IndexQube Chrome Extension MVP

This is the first browser-path proof for IndexQube. It intercepts sends on ChatGPT and Claude, sends the prompt to the local gateway, replaces the composer text with the optimized payload, then lets the page submit normally.

## Local Install

1. Start the gateway:

   ```bash
   cd gateway
   go run ./cmd/gateway
   ```

2. Open Chrome:

   ```text
   chrome://extensions
   ```

3. Enable **Developer mode**.
4. Click **Load unpacked**.
5. Select this `extension/` directory.
6. Open the extension popup and confirm the gateway URL is `http://localhost:8080`.

After changing extension files, click **Reload** on the extension card in `chrome://extensions`, then refresh ChatGPT/Claude.

Natural-language prompts are submitted unchanged and skip the gateway unless project memory or explicit context settings are enabled. When a browser prompt mixes a question with pasted raw code, the gateway keeps the question as plain text and fences only the code region before pruning.

## Test Flow

1. Open ChatGPT or Claude.
2. Paste a prompt with repeated code context.
3. Send it once to establish session history.
4. Send a modified version with the same session.
5. The status toast should stay quiet unless code context is actually pruned or an error occurs.

The popup exposes:

- Enable/disable toggle.
- Gateway URL.
- Session key.
- Optional project memory.
- Optional context path/lang for raw browser code snippets.

## Troubleshooting

If the page shows **IndexQube: Failed to fetch** or **Gateway unavailable**:

1. Confirm the gateway is running:

   ```bash
   curl http://localhost:8080/healthz
   ```

2. If it is not running:

   ```bash
   cd gateway
   go run ./cmd/gateway
   ```

3. Reload the extension in `chrome://extensions`.
4. Refresh the Claude/ChatGPT tab.
