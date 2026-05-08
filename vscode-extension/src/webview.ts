export function getHtml(nonce: string): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'nonce-${nonce}'; script-src 'nonce-${nonce}';">
  <title>IndexQube</title>
  <style nonce="${nonce}">
    :root { color-scheme: light dark; }
    body {
      margin: 0; padding: 0;
      font: 12px/1.45 var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-sideBar-background);
    }
    button, select, textarea { font: inherit; }
    .shell { display: grid; grid-template-rows: auto auto 1fr auto; min-height: 100vh; }
    .topbar {
      padding: 10px 12px 8px;
      border-bottom: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-sideBar-background);
    }
    .brand-row, .status-row, .action-row, .composer-actions { display: flex; align-items: center; gap: 8px; }
    .brand-row { justify-content: space-between; margin-bottom: 8px; }
    .brand { font-weight: 700; font-size: 13px; }
    .pill {
      border: 1px solid var(--vscode-badge-background);
      color: var(--vscode-badge-foreground);
      background: var(--vscode-badge-background);
      border-radius: 8px; padding: 2px 6px; font-size: 11px; white-space: nowrap;
    }
    .status-row { flex-wrap: wrap; color: var(--vscode-descriptionForeground); min-height: 20px; }
    .status-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--vscode-descriptionForeground); }
    .status-dot.online { background: #22c55e; }
    .status-dot.degraded { background: #f59e0b; }
    .status-dot.offline { background: #ef4444; }
    .toolbar {
      padding: 10px 12px;
      border-bottom: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
    }
    .field { display: grid; gap: 4px; margin-bottom: 8px; }
    label {
      color: var(--vscode-descriptionForeground); font-size: 11px;
      font-weight: 600; text-transform: uppercase;
    }
    select, textarea {
      width: 100%; box-sizing: border-box;
      border: 1px solid var(--vscode-input-border, transparent);
      border-radius: 6px;
      color: var(--vscode-input-foreground);
      background: var(--vscode-input-background);
    }
    select { padding: 5px 7px; }
    .action-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(74px, 1fr)); }
    .context-panel { margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border)); }
    .context-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; }
    .context-actions { display: flex; align-items: center; gap: 6px; }
    .context-list { display: grid; gap: 4px; margin: 0; padding: 0; list-style: none; }
    .context-file { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 6px; align-items: baseline; color: var(--vscode-foreground); }
    .context-path { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .context-meta { color: var(--vscode-descriptionForeground); font-size: 11px; white-space: nowrap; }
    .context-warning { display: none; margin: 6px 0; color: var(--vscode-errorForeground); font-size: 11px; }
    .context-warning.visible { display: block; }
    .context-blocked { display: grid; gap: 3px; margin: 4px 0 0; padding: 0; list-style: none; color: var(--vscode-errorForeground); font-size: 11px; }
    .small-button { min-height: 24px; padding: 2px 7px; font-size: 11px; }
    button {
      min-height: 28px; padding: 4px 8px;
      border: 1px solid transparent; border-radius: 6px;
      color: var(--vscode-button-foreground);
      background: var(--vscode-button-background);
      cursor: pointer; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    button.secondary { color: var(--vscode-button-secondaryForeground); background: var(--vscode-button-secondaryBackground); }
    button:disabled { cursor: not-allowed; opacity: 0.55; }
    .transcript { min-height: 0; overflow-y: auto; padding: 12px; }
    .message {
      margin: 0 0 10px; padding: 8px; border-radius: 8px;
      white-space: pre-wrap; word-break: break-word;
      border: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-editor-background);
    }
    .message.user { background: var(--vscode-input-background); }
    .message.assistant { border-left: 3px solid var(--vscode-button-background); }
    .message.error { color: var(--vscode-errorForeground); border-color: var(--vscode-errorForeground); }
    .message.notice { color: var(--vscode-descriptionForeground); }
    .receipt {
      display: none; margin: 0 12px 10px; padding: 8px; border-radius: 8px;
      border: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-editor-background);
      color: var(--vscode-descriptionForeground);
    }
    .receipt.visible { display: block; }
    .receipt strong { display: block; margin-bottom: 4px; color: var(--vscode-foreground); font-size: 12px; }
    .receipt-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; }
    .receipt-mode {
      padding: 1px 6px; border-radius: 999px;
      color: var(--vscode-badge-foreground); background: var(--vscode-badge-background);
      font-size: 10px; white-space: nowrap;
    }
    .receipt-grid { display: grid; gap: 4px; }
    .receipt-row { display: grid; grid-template-columns: minmax(74px, 0.8fr) minmax(0, 1.7fr); gap: 8px; align-items: baseline; }
    .receipt-label { color: var(--vscode-descriptionForeground); font-size: 11px; }
    .receipt-value { color: var(--vscode-foreground); font-size: 11px; }
    .receipt-total {
      margin-top: 6px; padding-top: 6px;
      border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      color: var(--vscode-descriptionForeground); font-size: 11px;
    }
    .composer { padding: 10px 12px 12px; border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border)); background: var(--vscode-sideBar-background); }
    textarea { min-height: 88px; max-height: 220px; padding: 8px; resize: vertical; }
    .composer-actions { margin-top: 8px; display: grid; grid-template-columns: 1fr 78px; }
    .meta { color: var(--vscode-descriptionForeground); font-size: 11px; margin-top: 6px; }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand-row">
        <div class="brand">IndexQube</div>
        <span id="keyPill" class="pill">Key</span>
      </div>
      <div class="status-row">
        <span id="statusDot" class="status-dot"></span>
        <span id="gatewayText">Starting...</span>
      </div>
      <div id="modelText" class="meta"></div>
      <div id="totalsText" class="meta"></div>
    </header>

    <section class="toolbar">
      <div class="field">
        <label for="contextMode">Context</label>
        <select id="contextMode">
          <option value="selection">Selection</option>
          <option value="activeFile">Active file</option>
          <option value="openEditors">Open editors</option>
          <option value="workspace">Workspace sample</option>
        </select>
      </div>
      <div class="field">
        <label for="memoryMode">Memory</label>
        <select id="memoryMode">
          <option value="workspace">Workspace</option>
          <option value="isolated">Isolated</option>
        </select>
      </div>
      <div class="action-row">
        <button id="checkGateway" class="secondary" type="button">Check</button>
        <button id="setKey" class="secondary" type="button">Key</button>
        <button id="settings" class="secondary" type="button">Settings</button>
        <button id="resetSession" class="secondary" type="button">New Session</button>
      </div>
      <div class="context-panel">
        <div class="context-head">
          <div id="contextText" class="meta">No context attached yet.</div>
          <div class="context-actions">
            <button id="previewContext" class="secondary small-button" type="button">Preview</button>
            <button id="copyContext" class="secondary small-button" type="button" disabled>Copy Context</button>
          </div>
        </div>
        <div id="contextWhy" class="meta"></div>
        <div id="contextWarning" class="context-warning"></div>
        <ul id="contextFiles" class="context-list"></ul>
        <ul id="contextBlocked" class="context-blocked"></ul>
      </div>
    </section>

    <main id="transcript" class="transcript"></main>

    <footer>
      <div id="receipt" class="receipt"></div>
      <div class="composer">
        <textarea id="input" placeholder="Ask about this workspace..."></textarea>
        <div class="composer-actions">
          <button id="send" type="button">Ask</button>
          <button id="stop" class="secondary" type="button" disabled>Stop</button>
        </div>
      </div>
    </footer>
  </div>

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const dom = {
      contextMode: document.getElementById('contextMode'),
      memoryMode: document.getElementById('memoryMode'),
      contextText: document.getElementById('contextText'),
      contextWhy: document.getElementById('contextWhy'),
      contextWarning: document.getElementById('contextWarning'),
      contextFiles: document.getElementById('contextFiles'),
      contextBlocked: document.getElementById('contextBlocked'),
      previewContext: document.getElementById('previewContext'),
      copyContext: document.getElementById('copyContext'),
      gatewayText: document.getElementById('gatewayText'),
      statusDot: document.getElementById('statusDot'),
      modelText: document.getElementById('modelText'),
      totalsText: document.getElementById('totalsText'),
      keyPill: document.getElementById('keyPill'),
      checkGateway: document.getElementById('checkGateway'),
      setKey: document.getElementById('setKey'),
      settings: document.getElementById('settings'),
      resetSession: document.getElementById('resetSession'),
      transcript: document.getElementById('transcript'),
      receipt: document.getElementById('receipt'),
      input: document.getElementById('input'),
      send: document.getElementById('send'),
      stop: document.getElementById('stop')
    };
    let state = vscode.getState() || { contextMode: 'activeFile', messages: [] };
    let currentAssistant = null;

    function saveState() { vscode.setState(state); }

    function appendMessage(kind, text) {
      const el = document.createElement('div');
      el.className = 'message ' + kind;
      el.textContent = text || '';
      dom.transcript.appendChild(el);
      state.messages.push({ kind, text: text || '' });
      saveState();
      scrollToBottom();
      return el;
    }

    function scrollToBottom() {
      dom.transcript.scrollTop = dom.transcript.scrollHeight;
    }

    function restoreMessages() {
      const frag = document.createDocumentFragment();
      for (const message of state.messages || []) {
        const el = document.createElement('div');
        el.className = 'message ' + message.kind;
        el.textContent = message.text || '';
        frag.appendChild(el);
      }
      dom.transcript.replaceChildren(frag);
      scrollToBottom();
    }

    function setBusy(busy) {
      dom.send.disabled = busy;
      dom.stop.disabled = !busy;
      dom.input.disabled = busy;
    }

    function sendPrompt() {
      const text = dom.input.value.trim();
      if (!text) return;
      appendMessage('user', text);
      dom.input.value = '';
      setBusy(true);
      vscode.postMessage({ type: 'sendMessage', text, contextMode: dom.contextMode.value, memoryMode: dom.memoryMode.value });
    }

    function renderReceipt(payload, outputTokens) {
      if (!payload) return;
      const stats = payload.stats || {};
      const saved   = positiveNumber(stats.estimated_tokens_saved);
      const before  = positiveNumber(stats.estimated_tokens_before);
      const after   = positiveNumber(stats.estimated_tokens_after);
      const bytesBefore = positiveNumber(stats.bytes_before);
      const bytesAfter  = positiveNumber(stats.bytes_after);
      const bytesSaved  = positiveNumber(stats.bytes_saved);
      const skipped  = positiveNumber(stats.blocks_skipped);
      const reduction = reductionPercent(stats);
      const blocks = [
        positiveNumber(stats.blocks_seen) + ' seen',
        positiveNumber(stats.blocks_pruned) + ' pruned',
        skipped + ' skipped'
      ].join(', ');

      const head = document.createElement('div');
      head.className = 'receipt-head';
      const title = document.createElement('strong');
      title.textContent = 'Optimizer receipt';
      const mode = document.createElement('span');
      mode.className = 'receipt-mode';
      mode.textContent = optimizerModeLabel(payload.mode || payload.source || 'stream');
      head.append(title, mode);

      const grid = document.createElement('div');
      grid.className = 'receipt-grid';
      grid.append(
        receiptRow('Input',  formatNumber(before) + ' -> ' + formatNumber(after) + ' tokens'),
        receiptRow('Saved',  formatNumber(saved) + ' tokens (' + reduction + '%)'),
        receiptRow('Bytes',  formatBytes(bytesBefore) + ' -> ' + formatBytes(bytesAfter) + ' (' + formatBytes(bytesSaved) + ' saved)'),
        receiptRow('Blocks', blocks),
        receiptRow('Output', '~' + formatNumber(outputTokens || 0) + ' tokens')
      );
      const skipReasons = formatSkipReasons(stats.skip_reasons);
      if (skipped > 0 && skipReasons) {
        grid.append(receiptRow('Skipped', skipReasons));
      }

      const total = document.createElement('div');
      total.className = 'receipt-total';
      total.textContent = formatTotalsLine(state.lastTotals);

      dom.receipt.className = 'receipt visible';
      dom.receipt.replaceChildren(head, grid, total);
    }

    function receiptRow(label, value) {
      const row = document.createElement('div');
      row.className = 'receipt-row';
      const labelEl = document.createElement('span');
      labelEl.className = 'receipt-label';
      labelEl.textContent = label;
      const valueEl = document.createElement('span');
      valueEl.className = 'receipt-value';
      valueEl.textContent = value;
      row.append(labelEl, valueEl);
      return row;
    }

    function renderTotals(totals) {
      state.lastTotals = normalizeTotals(totals);
      saveState();
      dom.totalsText.textContent = formatTotalsLine(state.lastTotals);
    }

    function formatTotalsLine(totals) {
      const t = normalizeTotals(totals);
      if (!t.requests) return 'Workspace savings: 0 requests';
      return 'Workspace savings: ' + formatNumber(t.tokensSaved) + ' input tokens, ' + formatBytes(t.bytesSaved) + ', ~' + formatNumber(t.outputTokens) + ' output tokens across ' + formatNumber(t.requests) + ' request(s)';
    }

    function normalizeTotals(totals) {
      const t = totals || {};
      return {
        requests:      positiveNumber(t.requests),
        tokensBefore:  positiveNumber(t.tokensBefore),
        tokensAfter:   positiveNumber(t.tokensAfter),
        tokensSaved:   positiveNumber(t.tokensSaved),
        outputTokens:  positiveNumber(t.outputTokens),
        bytesBefore:   positiveNumber(t.bytesBefore),
        bytesAfter:    positiveNumber(t.bytesAfter),
        bytesSaved:    positiveNumber(t.bytesSaved),
        blocksSeen:    positiveNumber(t.blocksSeen),
        blocksPruned:  positiveNumber(t.blocksPruned),
        blocksSkipped: positiveNumber(t.blocksSkipped)
      };
    }

    function reductionPercent(stats) {
      const ratio = Number(stats.reduction_ratio || 0);
      if (Number.isFinite(ratio) && ratio > 0) return Math.round(ratio * 100);
      const before = positiveNumber(stats.estimated_tokens_before);
      const saved  = positiveNumber(stats.estimated_tokens_saved);
      if (!before || !saved) return 0;
      return Math.round((saved / before) * 100);
    }

    function formatSkipReasons(reasons) {
      if (!reasons || typeof reasons !== 'object') return '';
      return Object.keys(reasons)
        .filter((key) => Number(reasons[key] || 0) > 0)
        .sort()
        .map((key) => key + '=' + Number(reasons[key] || 0))
        .join(', ');
    }

    function optimizerModeLabel(mode) {
      switch (mode) {
        case 'diff':      return 'diff';
        case 'unchanged': return 'dedupe';
        case 'warmup':    return 'warmup';
        case 'skipped':   return 'skipped';
        case 'none':      return 'no context';
        default:          return String(mode || 'stream');
      }
    }

    function renderContextSummary(message) {
      const files        = Array.isArray(message.files) ? message.files : [];
      const blockedFiles = Array.isArray(message.blockedFiles) ? message.blockedFiles : [];
      const contextTokens = Number(message.tokens || 0);
      const userTokens    = Number(message.userTokens || 0);
      const tokenText  = contextTokens ? ', ~' + formatNumber(contextTokens) + ' context tokens' : '';
      const totalText  = userTokens    ? ', ~' + formatNumber(userTokens)    + ' total input tokens' : '';

      dom.contextText.textContent = message.mode + ': ' + files.length + ' file(s), ' + formatBytes(message.bytes) + tokenText + totalText + (message.truncated ? ', capped' : '');
      dom.contextWhy.textContent  = message.why || '';
      dom.contextWarning.textContent = message.warning || '';
      dom.contextWarning.className   = message.warning ? 'context-warning visible' : 'context-warning';

      const filesFrag = document.createDocumentFragment();
      for (const file of files.slice(0, 12)) {
        const item = document.createElement('li');
        item.className = 'context-file';
        const path = document.createElement('span');
        path.className = 'context-path';
        path.textContent = file.path || '(untitled)';
        path.title = file.path || '';
        const meta = document.createElement('span');
        meta.className = 'context-meta';
        meta.textContent = formatFileMeta(file);
        item.append(path, meta);
        filesFrag.appendChild(item);
      }
      if (files.length > 12) {
        const item = document.createElement('li');
        item.className = 'context-meta';
        item.textContent = '+' + (files.length - 12) + ' more file(s)';
        filesFrag.appendChild(item);
      }
      dom.contextFiles.replaceChildren(filesFrag);

      const blockedFrag = document.createDocumentFragment();
      for (const file of blockedFiles.slice(0, 8)) {
        const item = document.createElement('li');
        item.textContent = 'Blocked ' + (file.path || '(untitled)') + ': ' + (file.reason || 'sensitive context');
        blockedFrag.appendChild(item);
      }
      if (blockedFiles.length > 8) {
        const item = document.createElement('li');
        item.textContent = '+' + (blockedFiles.length - 8) + ' more blocked file(s)';
        blockedFrag.appendChild(item);
      }
      dom.contextBlocked.replaceChildren(blockedFrag);
      dom.copyContext.disabled = message.copyAvailable === false;
    }

    function formatFileMeta(file) {
      const parts = [];
      if (file.source) parts.push(formatSource(file.source));
      if (file.language) parts.push(file.language);
      parts.push(formatBytes(file.bytes));
      if (file.truncated) parts.push('truncated');
      if (file.redactedCount) parts.push(file.redactedCount + ' redacted');
      return parts.join(' · ');
    }

    function formatSource(source) {
      switch (source) {
        case 'active':    return 'active';
        case 'visible':   return 'visible';
        case 'selection': return 'selection';
        case 'workspace': return 'workspace';
        default: return String(source || 'context');
      }
    }

    function setGateway(status, text) {
      dom.statusDot.className = 'status-dot ' + (status || '');
      dom.gatewayText.textContent = text || 'Gateway status unavailable';
    }

    dom.send.addEventListener('click', sendPrompt);
    dom.stop.addEventListener('click', () => vscode.postMessage({ type: 'stop' }));
    dom.setKey.addEventListener('click', () => vscode.postMessage({ type: 'setProviderKey' }));
    dom.checkGateway.addEventListener('click', () => vscode.postMessage({ type: 'checkGateway' }));
    dom.settings.addEventListener('click', () => vscode.postMessage({ type: 'openSettings' }));
    dom.resetSession.addEventListener('click', () => vscode.postMessage({ type: 'resetSession' }));
    dom.previewContext.addEventListener('click', () => vscode.postMessage({ type: 'previewContext', contextMode: dom.contextMode.value }));
    dom.copyContext.addEventListener('click', () => vscode.postMessage({ type: 'copyContext' }));
    dom.contextMode.addEventListener('change', () => {
      state.contextMode = dom.contextMode.value;
      saveState();
      dom.copyContext.disabled = true;
      dom.contextWarning.textContent = '';
      dom.contextWarning.className = 'context-warning';
      dom.contextBlocked.replaceChildren();
      vscode.postMessage({ type: 'setContextMode', contextMode: dom.contextMode.value });
    });
    dom.memoryMode.addEventListener('change', () => {
      state.memoryMode = dom.memoryMode.value;
      saveState();
      vscode.postMessage({ type: 'setMemoryMode', memoryMode: dom.memoryMode.value });
    });
    dom.input.addEventListener('keydown', (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
        event.preventDefault();
        sendPrompt();
      }
    });

    window.addEventListener('message', (event) => {
      const message = event.data || {};
      switch (message.type) {
        case 'state':
          state.contextMode = message.settings.contextMode || state.contextMode || 'activeFile';
          state.memoryMode  = message.settings.memoryMode  || state.memoryMode  || 'workspace';
          dom.contextMode.value = state.contextMode;
          dom.memoryMode.value  = state.memoryMode;
          saveState();
          dom.modelText.textContent = message.settings.provider + ' / ' + message.settings.model + ' / memory ' + dom.memoryMode.value + ' / session ' + message.settings.session + (message.settings.privacyMode === 'localOnly' ? ' / local-only' : '');
          dom.keyPill.textContent = message.settings.hasKey ? 'Key saved' : 'No key';
          renderTotals(message.settings.totals);
          break;
        case 'gateway':
          setGateway(message.status, message.text);
          break;
        case 'context':
          renderContextSummary(message);
          break;
        case 'startResponse':
          currentAssistant = appendMessage('assistant', '');
          dom.receipt.className = 'receipt';
          dom.receipt.replaceChildren();
          state.lastOptimizer = null;
          saveState();
          break;
        case 'delta':
          if (!currentAssistant) currentAssistant = appendMessage('assistant', '');
          currentAssistant.textContent += message.text || '';
          const last = state.messages[state.messages.length - 1];
          if (last && last.kind === 'assistant') {
            last.text = currentAssistant.textContent;
            saveState();
          }
          scrollToBottom();
          break;
        case 'optimizer':
          state.lastOptimizer = message.payload;
          saveState();
          renderReceipt(message.payload, 0);
          break;
        case 'done':
          setBusy(false);
          renderTotals(message.totals);
          if (message.requestSucceeded !== false) {
            renderReceipt(state.lastOptimizer, message.estimatedOutputTokens || 0);
          }
          currentAssistant = null;
          break;
        case 'error':
          setBusy(false);
          appendMessage('error', message.text || 'IndexQube error');
          currentAssistant = null;
          break;
        case 'notice':
          appendMessage('notice', message.text || '');
          break;
      }
    });

    function formatBytes(value) {
      const bytes = Number(value || 0);
      if (bytes < 1000) return bytes + ' B';
      if (bytes < 1000 * 1000) return (bytes / 1000).toFixed(1) + ' KB';
      return (bytes / 1000000).toFixed(1) + ' MB';
    }
    function formatNumber(value) {
      return Number(value || 0).toLocaleString('en-US');
    }
    function positiveNumber(value) {
      const n = Math.floor(Number(value) || 0);
      return Number.isFinite(n) && n > 0 ? n : 0;
    }

    restoreMessages();
    dom.contextMode.value = state.contextMode || 'activeFile';
    dom.memoryMode.value  = state.memoryMode  || 'workspace';
    renderTotals(state.lastTotals);
    vscode.postMessage({ type: 'ready' });
  </script>
</body>
</html>`;
}
