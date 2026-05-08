import { randomBytes } from 'crypto';
import { TextDecoder } from 'util';
import * as vscode from 'vscode';

type Provider = 'anthropic' | 'openai' | 'azure' | 'bedrock';
type ContextMode = 'selection' | 'activeFile' | 'openEditors' | 'workspace';

interface Settings {
    gatewayUrl: string;
    provider: Provider;
    model: string;
    contextMode: ContextMode;
    maxContextBytes: number;
    maxWorkspaceFiles: number;
    maxFileBytes: number;
    projectMemory: string;
    azureEndpoint: string;
    awsRegion: string;
    maxTokens: number;
    temperature: number;
}

interface ContextFile {
    path: string;
    language: string;
    bytes: number;
    truncated: boolean;
}

interface ContextBundle {
    mode: ContextMode;
    files: ContextFile[];
    content: string;
    bytes: number;
    truncated: boolean;
}

interface OptimizerEvent {
    mode: string;
    stats: {
        blocks_seen?: number;
        blocks_pruned?: number;
        blocks_skipped?: number;
        bytes_before?: number;
        bytes_after?: number;
        bytes_saved?: number;
        estimated_tokens_before?: number;
        estimated_tokens_after?: number;
        estimated_tokens_saved?: number;
        reduction_ratio?: number;
        skip_reasons?: Record<string, number>;
    };
}

interface WebviewMessage {
    type: string;
    text?: string;
    contextMode?: ContextMode;
}

const secretPrefix = 'indexqube.providerKey.';
const sessionKeyName = 'indexqube.sessionKey';
const textDecoder = new TextDecoder('utf-8', { fatal: false });

export function activate(context: vscode.ExtensionContext) {
    const provider = new IndexQubeChatViewProvider(context);

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(IndexQubeChatViewProvider.viewType, provider, {
            webviewOptions: { retainContextWhenHidden: true }
        }),
        vscode.commands.registerCommand('indexqube.openChat', async () => {
            await revealChatView();
        }),
        vscode.commands.registerCommand('indexqube.openSettings', () => {
            void vscode.commands.executeCommand('workbench.action.openSettings', 'IndexQube');
        }),
        vscode.commands.registerCommand('indexqube.setProviderKey', async () => {
            const settings = readSettings();
            await promptAndStoreProviderKey(context.secrets, settings.provider);
            provider.postState();
        }),
        vscode.commands.registerCommand('indexqube.checkGateway', async () => {
            await provider.checkGateway();
        }),
        vscode.commands.registerCommand('indexqube.resetSession', async () => {
            await context.workspaceState.update(sessionKeyName, createSessionKey());
            vscode.window.showInformationMessage('IndexQube workspace session reset.');
            provider.postState();
        })
    );
}

async function revealChatView() {
    await vscode.commands.executeCommand('workbench.view.extension.indexqube-sidebar');
    await vscode.commands.executeCommand(`${IndexQubeChatViewProvider.viewType}.focus`);
}

class IndexQubeChatViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'indexqube.chatView';

    private view?: vscode.WebviewView;
    private activeController?: AbortController;
    private assistantChars = 0;

    constructor(private readonly context: vscode.ExtensionContext) {}

    public resolveWebviewView(webviewView: vscode.WebviewView) {
        this.view = webviewView;
        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this.context.extensionUri]
        };

        webviewView.webview.html = this.getHtml(webviewView.webview);
        webviewView.webview.onDidReceiveMessage((data: WebviewMessage) => {
            void this.handleMessage(data, webviewView.webview);
        });
        webviewView.onDidDispose(() => this.abortActive('View closed.'));
    }

    public postState() {
        if (!this.view) {
            return;
        }
        void this.sendState(this.view.webview);
    }

    public async checkGateway() {
        if (!this.view) {
            return;
        }
        await this.checkGatewayForWebview(this.view.webview);
    }

    private async handleMessage(data: WebviewMessage, webview: vscode.Webview) {
        switch (data.type) {
            case 'ready':
                await this.sendState(webview);
                await this.checkGatewayForWebview(webview);
                break;
            case 'sendMessage':
                await this.handleChat(String(data.text || ''), normalizeContextMode(data.contextMode), webview);
                break;
            case 'stop':
                this.abortActive('Stopped by user.');
                break;
            case 'setProviderKey':
                await this.setProviderKey(webview);
                break;
            case 'checkGateway':
                await this.checkGatewayForWebview(webview);
                break;
            case 'openSettings':
                await vscode.commands.executeCommand('workbench.action.openSettings', 'IndexQube');
                break;
            case 'resetSession':
                await this.context.workspaceState.update(sessionKeyName, createSessionKey());
                await this.sendState(webview);
                webview.postMessage({ type: 'notice', text: 'Workspace session reset.' });
                break;
            case 'setContextMode':
                if (data.contextMode) {
                    await vscode.workspace.getConfiguration('indexqube').update('contextMode', data.contextMode, vscode.ConfigurationTarget.Workspace);
                    await this.sendState(webview);
                }
                break;
            default:
                break;
        }
    }

    private async setProviderKey(webview: vscode.Webview) {
        const settings = readSettings();
        const saved = await promptAndStoreProviderKey(this.context.secrets, settings.provider);
        if (saved) {
            webview.postMessage({ type: 'notice', text: `${settings.provider} key saved in VS Code Secret Storage.` });
            await this.sendState(webview);
        }
    }

    private async sendState(webview: vscode.Webview) {
        const settings = readSettings();
        const hasKey = Boolean(await this.context.secrets.get(secretName(settings.provider)));
        const sessionKey = await this.getSessionKey();
        webview.postMessage({
            type: 'state',
            settings: {
                gatewayUrl: settings.gatewayUrl,
                provider: settings.provider,
                model: settings.model,
                contextMode: settings.contextMode,
                hasKey,
                session: shortKey(sessionKey)
            }
        });
    }

    private async checkGatewayForWebview(webview: vscode.Webview) {
        const settings = readSettings();
        const started = Date.now();
        webview.postMessage({ type: 'gateway', status: 'checking', text: 'Checking gateway...' });
        try {
            const health = await fetchWithTimeout(`${settings.gatewayUrl}/healthz`, 2500);
            const ready = await fetchWithTimeout(`${settings.gatewayUrl}/readyz`, 2500);
            const elapsed = Math.max(1, Date.now() - started);
            webview.postMessage({
                type: 'gateway',
                status: health.ok && ready.ok ? 'online' : 'degraded',
                text: `${settings.gatewayUrl} (${elapsed} ms)`
            });
        } catch (err) {
            webview.postMessage({
                type: 'gateway',
                status: 'offline',
                text: err instanceof Error ? err.message : 'Gateway unavailable'
            });
        }
    }

    private async handleChat(text: string, mode: ContextMode, webview: vscode.Webview) {
        const prompt = text.trim();
        if (!prompt) {
            return;
        }
        if (this.activeController) {
            webview.postMessage({ type: 'error', text: 'A request is already running. Stop it before starting another.' });
            return;
        }

        const settings = readSettings(mode);
        const apiKey = await this.context.secrets.get(secretName(settings.provider));
        if (!apiKey) {
            webview.postMessage({ type: 'error', text: `No ${settings.provider} key saved. Click Key or run "IndexQube: Set Provider Key".` });
            return;
        }

        const sessionKey = await this.getSessionKey();
        const context = await collectContext(settings);
        const userContent = buildUserContent(prompt, context);
        const controller = new AbortController();
        this.activeController = controller;
        this.assistantChars = 0;

        webview.postMessage({
            type: 'context',
            mode: context.mode,
            files: context.files,
            bytes: context.bytes,
            truncated: context.truncated
        });
        webview.postMessage({ type: 'startResponse' });

        try {
            const response = await fetch(`${settings.gatewayUrl}/v1/chat/completions`, {
                method: 'POST',
                headers: buildHeaders(settings, apiKey, sessionKey),
                body: JSON.stringify({
                    model: settings.model,
                    messages: [{ role: 'user', content: userContent }],
                    stream: true,
                    max_tokens: settings.maxTokens,
                    temperature: settings.temperature
                }),
                signal: controller.signal
            });

            if (!response.ok) {
                const body = await response.text();
                webview.postMessage({ type: 'error', text: `Gateway ${response.status}: ${body || response.statusText}` });
                return;
            }
            if (!response.body) {
                webview.postMessage({ type: 'error', text: 'Gateway returned an empty stream.' });
                return;
            }

            await readSSE(response.body.getReader(), {
                onEvent: (event, data) => this.handleSSEEvent(webview, event, data),
                onText: (chunk) => {
                    this.assistantChars += chunk.length;
                    webview.postMessage({ type: 'delta', text: chunk });
                }
            });
        } catch (err) {
            if (controller.signal.aborted) {
                webview.postMessage({ type: 'notice', text: 'Request stopped.' });
                return;
            }
            webview.postMessage({ type: 'error', text: err instanceof Error ? err.message : 'IndexQube request failed.' });
        } finally {
            this.activeController = undefined;
            webview.postMessage({
                type: 'done',
                estimatedOutputTokens: estimateTokensFromChars(this.assistantChars)
            });
        }
    }

    private handleSSEEvent(webview: vscode.Webview, event: string, data: string) {
        if (event === 'error') {
            webview.postMessage({ type: 'error', text: data });
            return;
        }
        if (event !== 'iq_optimizer') {
            return;
        }
        try {
            const parsed = JSON.parse(data) as OptimizerEvent;
            webview.postMessage({ type: 'optimizer', payload: parsed });
        } catch {
            webview.postMessage({ type: 'notice', text: 'Received optimizer stats, but could not parse them.' });
        }
    }

    private abortActive(reason: string) {
        if (!this.activeController) {
            return;
        }
        this.activeController.abort(reason);
        this.activeController = undefined;
    }

    private async getSessionKey(): Promise<string> {
        const existing = this.context.workspaceState.get<string>(sessionKeyName);
        if (existing) {
            return existing;
        }
        const created = createSessionKey();
        await this.context.workspaceState.update(sessionKeyName, created);
        return created;
    }

    private getHtml(_webview: vscode.Webview): string {
        const nonce = getNonce();
        return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <title>IndexQube</title>
  <style>
    :root {
      color-scheme: light dark;
    }
    body {
      margin: 0;
      padding: 0;
      font: 12px/1.45 var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-sideBar-background);
    }
    button, select, textarea {
      font: inherit;
    }
    .shell {
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      min-height: 100vh;
    }
    .topbar {
      padding: 10px 12px 8px;
      border-bottom: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-sideBar-background);
    }
    .brand-row, .status-row, .action-row, .composer-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-row {
      justify-content: space-between;
      margin-bottom: 8px;
    }
    .brand {
      font-weight: 700;
      font-size: 13px;
      letter-spacing: 0;
    }
    .pill {
      border: 1px solid var(--vscode-badge-background);
      color: var(--vscode-badge-foreground);
      background: var(--vscode-badge-background);
      border-radius: 8px;
      padding: 2px 6px;
      font-size: 11px;
      white-space: nowrap;
    }
    .status-row {
      flex-wrap: wrap;
      color: var(--vscode-descriptionForeground);
      min-height: 20px;
    }
    .status-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--vscode-descriptionForeground);
    }
    .status-dot.online { background: #22c55e; }
    .status-dot.degraded { background: #f59e0b; }
    .status-dot.offline { background: #ef4444; }
    .toolbar {
      padding: 10px 12px;
      border-bottom: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
    }
    .field {
      display: grid;
      gap: 4px;
      margin-bottom: 8px;
    }
    label {
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    select, textarea {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid var(--vscode-input-border, transparent);
      border-radius: 6px;
      color: var(--vscode-input-foreground);
      background: var(--vscode-input-background);
    }
    select {
      padding: 5px 7px;
    }
    .action-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    button {
      min-height: 28px;
      padding: 4px 8px;
      border: 1px solid transparent;
      border-radius: 6px;
      color: var(--vscode-button-foreground);
      background: var(--vscode-button-background);
      cursor: pointer;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    button.secondary {
      color: var(--vscode-button-secondaryForeground);
      background: var(--vscode-button-secondaryBackground);
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.55;
    }
    .transcript {
      min-height: 0;
      overflow-y: auto;
      padding: 12px;
    }
    .message {
      margin: 0 0 10px;
      padding: 8px;
      border-radius: 8px;
      white-space: pre-wrap;
      word-break: break-word;
      border: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-editor-background);
    }
    .message.user {
      background: var(--vscode-input-background);
    }
    .message.assistant {
      border-left: 3px solid var(--vscode-button-background);
    }
    .message.error {
      color: var(--vscode-errorForeground);
      border-color: var(--vscode-errorForeground);
    }
    .message.notice {
      color: var(--vscode-descriptionForeground);
    }
    .receipt {
      display: none;
      margin: 0 12px 10px;
      padding: 8px;
      border-radius: 8px;
      border: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-editor-background);
      color: var(--vscode-descriptionForeground);
    }
    .receipt.visible {
      display: block;
    }
    .receipt strong {
      display: block;
      margin-bottom: 4px;
      color: var(--vscode-foreground);
      font-size: 12px;
    }
    .composer {
      padding: 10px 12px 12px;
      border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      background: var(--vscode-sideBar-background);
    }
    textarea {
      min-height: 88px;
      max-height: 220px;
      padding: 8px;
      resize: vertical;
    }
    .composer-actions {
      margin-top: 8px;
      display: grid;
      grid-template-columns: 1fr 78px;
    }
    .meta {
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
      margin-top: 6px;
    }
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
      <div class="action-row">
        <button id="checkGateway" class="secondary" type="button">Check</button>
        <button id="setKey" class="secondary" type="button">Key</button>
        <button id="settings" class="secondary" type="button">Settings</button>
        <button id="resetSession" class="secondary" type="button">Session</button>
      </div>
      <div id="contextText" class="meta">No context attached yet.</div>
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
      contextText: document.getElementById('contextText'),
      gatewayText: document.getElementById('gatewayText'),
      statusDot: document.getElementById('statusDot'),
      modelText: document.getElementById('modelText'),
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

    function saveState() {
      vscode.setState(state);
    }
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
      dom.transcript.replaceChildren();
      for (const message of state.messages || []) {
        const el = document.createElement('div');
        el.className = 'message ' + message.kind;
        el.textContent = message.text || '';
        dom.transcript.appendChild(el);
      }
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
      vscode.postMessage({ type: 'sendMessage', text, contextMode: dom.contextMode.value });
    }
    function renderReceipt(payload, outputTokens) {
      if (!payload) return;
      const stats = payload.stats || {};
      const saved = Number(stats.estimated_tokens_saved || 0);
      const before = Number(stats.estimated_tokens_before || 0);
      const after = Number(stats.estimated_tokens_after || 0);
      const blocks = [
        Number(stats.blocks_seen || 0) + ' seen',
        Number(stats.blocks_pruned || 0) + ' pruned',
        Number(stats.blocks_skipped || 0) + ' skipped'
      ].join(', ');
      dom.receipt.className = 'receipt visible';
      dom.receipt.replaceChildren();
      const title = document.createElement('strong');
      title.textContent = 'Optimizer receipt';
      const body = document.createElement('div');
      body.textContent = 'Mode: ' + payload.mode + ' | Input tokens: ' + before + ' -> ' + after + ' | Saved: ' + saved + ' | Output est: ' + (outputTokens || 0) + ' | Blocks: ' + blocks;
      dom.receipt.append(title, body);
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
    dom.contextMode.addEventListener('change', () => {
      state.contextMode = dom.contextMode.value;
      saveState();
      vscode.postMessage({ type: 'setContextMode', contextMode: dom.contextMode.value });
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
          dom.contextMode.value = state.contextMode || message.settings.contextMode || 'activeFile';
          dom.modelText.textContent = message.settings.provider + ' / ' + message.settings.model + ' / session ' + message.settings.session;
          dom.keyPill.textContent = message.settings.hasKey ? 'Key saved' : 'No key';
          break;
        case 'gateway':
          setGateway(message.status, message.text);
          break;
        case 'context':
          dom.contextText.textContent = message.mode + ': ' + message.files.length + ' file(s), ' + formatBytes(message.bytes) + (message.truncated ? ', capped' : '');
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
          renderReceipt(state.lastOptimizer, message.estimatedOutputTokens || 0);
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

    restoreMessages();
    dom.contextMode.value = state.contextMode || 'activeFile';
    vscode.postMessage({ type: 'ready' });
  </script>
</body>
</html>`;
    }
}

function readSettings(overrideMode?: ContextMode): Settings {
    const config = vscode.workspace.getConfiguration('indexqube');
    return {
        gatewayUrl: normalizeGatewayUrl(config.get<string>('gatewayUrl', 'http://localhost:8080')),
        provider: normalizeProvider(config.get<string>('provider', 'anthropic')),
        model: config.get<string>('model', 'claude-3-5-sonnet'),
        contextMode: overrideMode || normalizeContextMode(config.get<string>('contextMode', 'activeFile')),
        maxContextBytes: positiveNumber(config.get<number>('maxContextBytes', 120000), 120000),
        maxWorkspaceFiles: positiveNumber(config.get<number>('maxWorkspaceFiles', 30), 30),
        maxFileBytes: positiveNumber(config.get<number>('maxFileBytes', 60000), 60000),
        projectMemory: config.get<string>('projectMemory', ''),
        azureEndpoint: config.get<string>('azureEndpoint', ''),
        awsRegion: config.get<string>('awsRegion', 'us-east-1'),
        maxTokens: positiveNumber(config.get<number>('maxTokens', 4096), 4096),
        temperature: Number(config.get<number>('temperature', 0) || 0)
    };
}

async function collectContext(settings: Settings): Promise<ContextBundle> {
    switch (settings.contextMode) {
        case 'selection':
            return contextFromSelection(settings);
        case 'openEditors':
            return contextFromOpenEditors(settings);
        case 'workspace':
            return contextFromWorkspace(settings);
        case 'activeFile':
        default:
            return contextFromActiveFile(settings);
    }
}

async function contextFromSelection(settings: Settings): Promise<ContextBundle> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.selection.isEmpty) {
        return emptyContext('selection');
    }
    const doc = editor.document;
    return bundleFromDocuments(settings, 'selection', [{
        path: docPath(doc),
        language: doc.languageId,
        content: doc.getText(editor.selection)
    }]);
}

async function contextFromActiveFile(settings: Settings): Promise<ContextBundle> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        return emptyContext('activeFile');
    }
    const doc = editor.document;
    return bundleFromDocuments(settings, 'activeFile', [{
        path: docPath(doc),
        language: doc.languageId,
        content: doc.getText()
    }]);
}

async function contextFromOpenEditors(settings: Settings): Promise<ContextBundle> {
    const seen = new Set<string>();
    const docs = vscode.window.visibleTextEditors
        .map((editor) => editor.document)
        .filter((doc) => {
            const key = doc.uri.toString();
            if (seen.has(key)) {
                return false;
            }
            seen.add(key);
            return true;
        })
        .map((doc) => ({
            path: docPath(doc),
            language: doc.languageId,
            content: doc.getText()
        }));
    return bundleFromDocuments(settings, 'openEditors', docs);
}

async function contextFromWorkspace(settings: Settings): Promise<ContextBundle> {
    if (!vscode.workspace.workspaceFolders?.length) {
        return emptyContext('workspace');
    }
    const include = '**/*.{go,ts,tsx,js,jsx,py,rs,java,kt,swift,rb,cs,cpp,h,hpp,c,sql,yaml,yml,json,md,txt,mod,sum,html,css}';
    const exclude = '**/{.git,node_modules,dist,bin,out,build,vendor,.cache,coverage,tmp}/**';
    const uris = await vscode.workspace.findFiles(include, exclude, settings.maxWorkspaceFiles);
    const docs: Array<{ path: string; language: string; content: string }> = [];
    for (const uri of uris) {
        const stat = await vscode.workspace.fs.stat(uri);
        if (stat.size > settings.maxFileBytes) {
            continue;
        }
        const raw = await vscode.workspace.fs.readFile(uri);
        docs.push({
            path: vscode.workspace.asRelativePath(uri, false),
            language: languageFromPath(uri.fsPath),
            content: textDecoder.decode(raw)
        });
    }
    return bundleFromDocuments(settings, 'workspace', docs);
}

function bundleFromDocuments(
    settings: Settings,
    mode: ContextMode,
    docs: Array<{ path: string; language: string; content: string }>
): ContextBundle {
    const files: ContextFile[] = [];
    const parts: string[] = [];
    let total = 0;
    let truncated = false;

    for (const doc of docs) {
        if (total >= settings.maxContextBytes) {
            truncated = true;
            break;
        }
        const remaining = settings.maxContextBytes - total;
        const prepared = capText(doc.content, Math.min(settings.maxFileBytes, remaining));
        const bytes = byteLen(prepared.text);
        total += bytes;
        truncated = truncated || prepared.truncated;
        files.push({ path: doc.path, language: doc.language, bytes, truncated: prepared.truncated });
        parts.push(formatCodeFence(doc.path, doc.language, prepared.text));
    }

    return {
        mode,
        files,
        content: parts.join('\n\n'),
        bytes: total,
        truncated
    };
}

function buildUserContent(prompt: string, context: ContextBundle): string {
    if (!context.content) {
        return prompt;
    }
    const files = context.files.map((file) => `- ${file.path} (${file.language}, ${file.bytes} bytes)`).join('\n');
    return [
        'IndexQube IDE context:',
        `Mode: ${context.mode}`,
        files ? `Files:\n${files}` : 'Files: none',
        '',
        context.content,
        '',
        'User request:',
        prompt
    ].join('\n');
}

function formatCodeFence(path: string, language: string, content: string): string {
    return '```' + language + ' ' + path + '\n' + content.trimEnd() + '\n```';
}

function buildHeaders(settings: Settings, apiKey: string, sessionKey: string): Record<string, string> {
    const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'X-IQ-Provider': settings.provider,
        'X-IQ-Provider-Key': apiKey,
        'X-IQ-Session-Key': sessionKey,
        'X-IQ-Contract-Version': '2'
    };
    if (settings.provider === 'azure' && settings.azureEndpoint.trim()) {
        headers['X-IQ-Azure-Endpoint'] = settings.azureEndpoint.trim();
    }
    if (settings.provider === 'bedrock' && settings.awsRegion.trim()) {
        headers['X-IQ-AWS-Region'] = settings.awsRegion.trim();
    }
    if (settings.projectMemory.trim()) {
        headers['X-IQ-Project-Memory'] = settings.projectMemory.trim();
    }
    return headers;
}

async function readSSE(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    handlers: { onEvent: (event: string, data: string) => void; onText: (text: string) => void }
): Promise<void> {
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            break;
        }
        buffer += decoder.decode(value, { stream: true });
        const frames = buffer.split('\n\n');
        buffer = frames.pop() || '';
        for (const frame of frames) {
            if (handleSSEFrame(frame, handlers)) {
                return;
            }
        }
    }
    if (buffer.trim()) {
        handleSSEFrame(buffer, handlers);
    }
}

function handleSSEFrame(
    frame: string,
    handlers: { onEvent: (event: string, data: string) => void; onText: (text: string) => void }
): boolean {
    let event = 'message';
    const data: string[] = [];
    for (const line of frame.split('\n')) {
        if (line.startsWith('event:')) {
            event = line.slice('event:'.length).trim();
        } else if (line.startsWith('data:')) {
            data.push(line.slice('data:'.length).replace(/^ /, ''));
        }
    }
    const payload = data.join('\n');
    if (!payload) {
        return false;
    }
    if (payload === '[DONE]') {
        return true;
    }
    if (event !== 'message') {
        handlers.onEvent(event, payload);
        return false;
    }
    try {
        const parsed = JSON.parse(payload) as { choices?: Array<{ delta?: { content?: string } }> };
        const content = parsed.choices?.[0]?.delta?.content;
        if (content) {
            handlers.onText(content);
        }
    } catch {
        // Ignore malformed provider chunks.
    }
    return false;
}

async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { method: 'GET', signal: controller.signal });
    } finally {
        clearTimeout(timer);
    }
}


async function promptAndStoreProviderKey(secrets: vscode.SecretStorage, provider: Provider): Promise<boolean> {
    const key = await vscode.window.showInputBox({
        title: `IndexQube ${provider} provider key`,
        prompt: 'Stored locally in VS Code SecretStorage and sent only to your configured gateway.',
        password: true,
        ignoreFocusOut: true
    });
    if (!key?.trim()) {
        return false;
    }
    await secrets.store(secretName(provider), key.trim());
    return true;
}

function secretName(provider: Provider): string {
    return `${secretPrefix}${provider}`;
}

function emptyContext(mode: ContextMode): ContextBundle {
    return { mode, files: [], content: '', bytes: 0, truncated: false };
}

function capText(text: string, maxBytes: number): { text: string; truncated: boolean } {
    if (byteLen(text) <= maxBytes) {
        return { text, truncated: false };
    }
    let end = Math.max(0, Math.min(text.length, maxBytes));
    while (byteLen(text.slice(0, end)) > maxBytes && end > 0) {
        end--;
    }
    return { text: text.slice(0, end), truncated: true };
}

function docPath(doc: vscode.TextDocument): string {
    if (doc.uri.scheme === 'file') {
        return vscode.workspace.asRelativePath(doc.uri, false);
    }
    return doc.fileName || doc.uri.toString();
}

function languageFromPath(filePath: string): string {
    const lower = filePath.toLowerCase();
    const ext = lower.slice(lower.lastIndexOf('.') + 1);
    switch (ext) {
        case 'ts':
        case 'tsx':
        case 'js':
        case 'jsx':
        case 'go':
        case 'py':
        case 'rs':
        case 'java':
        case 'sql':
        case 'json':
        case 'md':
        case 'html':
        case 'css':
            return ext;
        case 'yml':
        case 'yaml':
            return 'yaml';
        default:
            return 'txt';
    }
}

function normalizeGatewayUrl(value: string): string {
    return String(value || 'http://localhost:8080').trim().replace(/\/+$/, '') || 'http://localhost:8080';
}

function normalizeProvider(value?: string): Provider {
    switch (value) {
        case 'openai':
        case 'azure':
        case 'bedrock':
        case 'anthropic':
            return value;
        default:
            return 'anthropic';
    }
}

function normalizeContextMode(value?: string): ContextMode {
    switch (value) {
        case 'selection':
        case 'openEditors':
        case 'workspace':
        case 'activeFile':
            return value;
        default:
            return 'activeFile';
    }
}

function positiveNumber(value: unknown, fallback: number): number {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

function byteLen(text: string): number {
    return Buffer.byteLength(text, 'utf8');
}

function estimateTokensFromChars(chars: number): number {
    return Math.max(0, Math.ceil(chars / 4));
}

function createSessionKey(): string {
    return randomBytes(16).toString('hex');
}

function shortKey(key: string): string {
    return key.length <= 12 ? key : `${key.slice(0, 4)}...${key.slice(-4)}`;
}

function getNonce(): string {
    return randomBytes(16).toString('base64');
}
