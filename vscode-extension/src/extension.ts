import { randomBytes } from 'crypto';
import { TextDecoder } from 'util';
import * as vscode from 'vscode';

type Provider = 'anthropic' | 'openai' | 'azure' | 'bedrock';
type ContextMode = 'selection' | 'activeFile' | 'openEditors' | 'workspace';
type ContextSource = 'selection' | 'active' | 'visible' | 'workspace';
type MemoryMode = 'workspace' | 'isolated';
type PrivacyMode = 'standard' | 'localOnly';

interface Settings {
    gatewayUrl: string;
    provider: Provider;
    model: string;
    contextMode: ContextMode;
    memoryMode: MemoryMode;
    privacyMode: PrivacyMode;
    contextExcludePatterns: string[];
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
    source: ContextSource;
}

interface ContextBundle {
    mode: ContextMode;
    files: ContextFile[];
    blockedFiles: ContextBlockedFile[];
    content: string;
    bytes: number;
    truncated: boolean;
    tokens: number;
    why: string;
    safe: boolean;
    warning: string;
}

interface ContextDocument {
    path: string;
    language: string;
    source: ContextSource;
    content: string;
}

interface ContextBlockedFile {
    path: string;
    source: ContextSource;
    reason: string;
}

interface OptimizerEvent {
    version?: string;
    source?: string;
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

interface UsageTotals {
    requests: number;
    tokensBefore: number;
    tokensAfter: number;
    tokensSaved: number;
    outputTokens: number;
    bytesBefore: number;
    bytesAfter: number;
    bytesSaved: number;
    blocksSeen: number;
    blocksPruned: number;
    blocksSkipped: number;
    lastUpdatedAt: number;
}

interface WebviewMessage {
    type: string;
    text?: string;
    contextMode?: ContextMode;
    memoryMode?: MemoryMode;
}

interface GatewaySecurityCheck {
    ok: boolean;
    message?: string;
}

const secretPrefix = 'indexqube.providerKey.';
const sessionKeyName = 'indexqube.sessionKey';
const usageTotalsKey = 'indexqube.usageTotals.v1';
const textDecoder = new TextDecoder('utf-8', { fatal: false });

const sensitiveFilePatterns = [
    '.env',
    '.env.local',
    '.env.development',
    '.env.production',
    '.npmrc',
    '.pypirc',
    '.netrc',
    'id_rsa',
    'id_dsa',
    'id_ecdsa',
    'id_ed25519'
];

const sensitiveExtensions = new Set([
    '.pem',
    '.key',
    '.crt',
    '.cer',
    '.p12',
    '.pfx',
    '.jks',
    '.keystore'
]);

const noisyLockFiles = new Set([
    'package-lock.json',
    'pnpm-lock.yaml',
    'yarn.lock',
    'bun.lockb',
    'poetry.lock',
    'Pipfile.lock',
    'Cargo.lock'
]);

const generatedPathSegments = new Set([
    '.git',
    'node_modules',
    'dist',
    'bin',
    'out',
    'build',
    'vendor',
    '.cache',
    'coverage',
    'tmp',
    'temp',
    '.next',
    '.nuxt',
    'target',
    '.turbo',
    '.venv',
    'venv',
    '__pycache__'
]);

const secretLikePatterns: Array<{ name: string; pattern: RegExp }> = [
    { name: 'private key', pattern: /-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----/ },
    { name: 'OpenAI-style API key', pattern: /\bsk-[A-Za-z0-9_-]{20,}\b/ },
    { name: 'GitHub token', pattern: /\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b/ },
    { name: 'GitHub fine-grained token', pattern: /\bgithub_pat_[A-Za-z0-9_]{30,}\b/ },
    { name: 'AWS access key', pattern: /\bAKIA[0-9A-Z]{16}\b/ },
    { name: 'Slack token', pattern: /\bxox[baprs]-[A-Za-z0-9-]{20,}\b/ },
    { name: 'JWT-like token', pattern: /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/ }
];

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
    private lastUserContent = '';
    private pendingOptimizer?: OptimizerEvent;

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
                await this.handleChat(String(data.text || ''), normalizeContextMode(data.contextMode), normalizeMemoryMode(data.memoryMode), webview);
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
                webview.postMessage({ type: 'notice', text: 'New workspace session created.' });
                break;
            case 'copyContext':
                await this.copyLastContext(webview);
                break;
            case 'previewContext':
                await this.previewContext(normalizeContextMode(data.contextMode), webview);
                break;
            case 'setContextMode':
                if (data.contextMode) {
                    await vscode.workspace.getConfiguration('indexqube').update('contextMode', data.contextMode, vscode.ConfigurationTarget.Workspace);
                    await this.sendState(webview);
                }
                break;
            case 'setMemoryMode':
                if (data.memoryMode) {
                    await vscode.workspace.getConfiguration('indexqube').update('memoryMode', normalizeMemoryMode(data.memoryMode), vscode.ConfigurationTarget.Workspace);
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
        const sessionKey = await this.getWorkspaceSessionKey();
        webview.postMessage({
            type: 'state',
            settings: {
                gatewayUrl: settings.gatewayUrl,
                provider: settings.provider,
                model: settings.model,
                contextMode: settings.contextMode,
                memoryMode: settings.memoryMode,
                privacyMode: settings.privacyMode,
                hasKey,
                session: sessionLabel(settings.memoryMode, sessionKey),
                totals: this.getUsageTotals()
            }
        });
    }

    private async checkGatewayForWebview(webview: vscode.Webview) {
        const settings = readSettings();
        const started = Date.now();
        webview.postMessage({ type: 'gateway', status: 'checking', text: 'Checking gateway...' });

        const gatewayCheck = validateGatewayUrlForSecrets(settings.gatewayUrl, settings.privacyMode);
        if (!gatewayCheck.ok) {
            webview.postMessage({
                type: 'gateway',
                status: 'offline',
                text: gatewayCheck.message || 'Unsafe gateway URL.'
            });
            return;
        }

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

    private async handleChat(text: string, mode: ContextMode, memoryMode: MemoryMode, webview: vscode.Webview) {
        const prompt = text.trim();
        if (!prompt) {
            return;
        }
        if (this.activeController) {
            webview.postMessage({ type: 'error', text: 'A request is already running. Stop it before starting another.' });
            return;
        }

        const settings = readSettings(mode, memoryMode);
        const gatewayCheck = validateGatewayUrlForSecrets(settings.gatewayUrl, settings.privacyMode);
        if (!gatewayCheck.ok) {
            webview.postMessage({ type: 'error', text: gatewayCheck.message || 'Unsafe gateway URL.' });
            return;
        }

        const context = await collectContext(settings);
        if (!context.safe) {
            this.lastUserContent = context.content;
            webview.postMessage({
                type: 'context',
                mode: context.mode,
                files: context.files,
                blockedFiles: context.blockedFiles,
                bytes: context.bytes,
                truncated: context.truncated,
                tokens: context.tokens,
                userTokens: context.tokens,
                why: context.why,
                warning: context.warning,
                copyAvailable: Boolean(context.content)
            });
            webview.postMessage({ type: 'error', text: context.warning || 'Context blocked before provider key use.' });
            return;
        }

        const sessionKey = await this.sessionKeyForRequest(settings);
        const apiKey = await this.context.secrets.get(secretName(settings.provider));
        if (!apiKey) {
            webview.postMessage({ type: 'error', text: `No ${settings.provider} key saved. Click Key or run "IndexQube: Set Provider Key".` });
            return;
        }

        const userContent = buildUserContent(prompt, context);
        this.lastUserContent = userContent;
        const controller = new AbortController();
        this.activeController = controller;
        this.assistantChars = 0;
        this.pendingOptimizer = undefined;
        let shouldRecordUsage = false;

        webview.postMessage({
            type: 'context',
            mode: context.mode,
            files: context.files,
            blockedFiles: context.blockedFiles,
            bytes: context.bytes,
            truncated: context.truncated,
            tokens: context.tokens,
            userTokens: estimateTokensFromChars(userContent.length),
            why: context.why,
            warning: context.warning,
            copyAvailable: true
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
                webview.postMessage({ type: 'error', text: safeGatewayErrorMessage(response.status, body, response.statusText) });
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
            shouldRecordUsage = true;
        } catch (err) {
            if (controller.signal.aborted) {
                webview.postMessage({ type: 'notice', text: 'Request stopped.' });
                return;
            }
            webview.postMessage({ type: 'error', text: safeErrorText(err instanceof Error ? err.message : 'IndexQube request failed.') });
        } finally {
            const outputTokens = estimateTokensFromChars(this.assistantChars);
            let totals: UsageTotals | undefined;
            if (shouldRecordUsage && this.pendingOptimizer) {
                totals = await this.recordUsageTotals(this.pendingOptimizer, outputTokens);
            }
            this.activeController = undefined;
            this.pendingOptimizer = undefined;
            webview.postMessage({
                type: 'done',
                estimatedOutputTokens: outputTokens,
                totals: totals || this.getUsageTotals()
            });
        }
    }

    private handleSSEEvent(webview: vscode.Webview, event: string, data: string) {
        if (event === 'error') {
            webview.postMessage({ type: 'error', text: parseSseErrorMessage(data) });
            return;
        }
        if (event !== 'iq_optimizer') {
            return;
        }
        try {
            const parsed = JSON.parse(data) as OptimizerEvent;
            this.pendingOptimizer = parsed;
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

    private async copyLastContext(webview: vscode.Webview) {
        if (!this.lastUserContent) {
            webview.postMessage({ type: 'notice', text: 'No assembled context available yet.' });
            return;
        }
        await vscode.env.clipboard.writeText(this.lastUserContent);
        webview.postMessage({ type: 'notice', text: 'Assembled context copied.' });
    }

    private async previewContext(mode: ContextMode, webview: vscode.Webview) {
        const settings = readSettings(mode);
        const context = await collectContext(settings);
        this.lastUserContent = context.content;
        webview.postMessage({
            type: 'context',
            mode: context.mode,
            files: context.files,
            blockedFiles: context.blockedFiles,
            bytes: context.bytes,
            truncated: context.truncated,
            tokens: context.tokens,
            userTokens: context.tokens,
            why: context.why,
            warning: context.warning,
            copyAvailable: Boolean(context.content)
        });
    }

    private getUsageTotals(): UsageTotals {
        return normalizeUsageTotals(this.context.workspaceState.get<Partial<UsageTotals>>(usageTotalsKey));
    }

    private async recordUsageTotals(event: OptimizerEvent, outputTokens: number): Promise<UsageTotals> {
        const stats = event.stats || {};
        const totals = this.getUsageTotals();
        const tokensBefore = positiveInt(stats.estimated_tokens_before);
        const tokensAfter = positiveInt(stats.estimated_tokens_after);
        const tokensSaved = positiveInt(stats.estimated_tokens_saved ?? tokensBefore - tokensAfter);
        const bytesBefore = positiveInt(stats.bytes_before);
        const bytesAfter = positiveInt(stats.bytes_after);
        const bytesSaved = positiveInt(stats.bytes_saved ?? bytesBefore - bytesAfter);

        totals.requests += 1;
        totals.tokensBefore += tokensBefore;
        totals.tokensAfter += tokensAfter;
        totals.tokensSaved += tokensSaved;
        totals.outputTokens += positiveInt(outputTokens);
        totals.bytesBefore += bytesBefore;
        totals.bytesAfter += bytesAfter;
        totals.bytesSaved += bytesSaved;
        totals.blocksSeen += positiveInt(stats.blocks_seen);
        totals.blocksPruned += positiveInt(stats.blocks_pruned);
        totals.blocksSkipped += positiveInt(stats.blocks_skipped);
        totals.lastUpdatedAt = Date.now();

        await this.context.workspaceState.update(usageTotalsKey, totals);
        return totals;
    }

    private async sessionKeyForRequest(settings: Settings): Promise<string> {
        if (settings.memoryMode === 'isolated') {
            return createSessionKey();
        }
        return this.getWorkspaceSessionKey();
    }

    private async getWorkspaceSessionKey(): Promise<string> {
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
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'nonce-${nonce}'; script-src 'nonce-${nonce}';">
  <title>IndexQube</title>
  <style nonce="${nonce}">
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
      grid-template-columns: repeat(auto-fit, minmax(74px, 1fr));
    }
    .context-panel {
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
    }
    .context-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .context-actions {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .context-list {
      display: grid;
      gap: 4px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .context-file {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      align-items: baseline;
      color: var(--vscode-foreground);
    }
    .context-path {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .context-meta {
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
      white-space: nowrap;
    }
    .context-warning {
      display: none;
      margin: 6px 0;
      color: var(--vscode-errorForeground);
      font-size: 11px;
    }
    .context-warning.visible {
      display: block;
    }
    .context-blocked {
      display: grid;
      gap: 3px;
      margin: 4px 0 0;
      padding: 0;
      list-style: none;
      color: var(--vscode-errorForeground);
      font-size: 11px;
    }
    .small-button {
      min-height: 24px;
      padding: 2px 7px;
      font-size: 11px;
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
    .receipt-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .receipt-mode {
      padding: 1px 6px;
      border-radius: 999px;
      color: var(--vscode-badge-foreground);
      background: var(--vscode-badge-background);
      font-size: 10px;
      white-space: nowrap;
    }
    .receipt-grid {
      display: grid;
      gap: 4px;
    }
    .receipt-row {
      display: grid;
      grid-template-columns: minmax(74px, 0.8fr) minmax(0, 1.7fr);
      gap: 8px;
      align-items: baseline;
    }
    .receipt-label {
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
    }
    .receipt-value {
      color: var(--vscode-foreground);
      font-size: 11px;
    }
    .receipt-total {
      margin-top: 6px;
      padding-top: 6px;
      border-top: 1px solid var(--vscode-sideBar-border, var(--vscode-panel-border));
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
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
      vscode.postMessage({ type: 'sendMessage', text, contextMode: dom.contextMode.value, memoryMode: dom.memoryMode.value });
    }
    function renderReceipt(payload, outputTokens) {
      if (!payload) return;
      const stats = payload.stats || {};
      const saved = positiveNumber(stats.estimated_tokens_saved);
      const before = positiveNumber(stats.estimated_tokens_before);
      const after = positiveNumber(stats.estimated_tokens_after);
      const bytesBefore = positiveNumber(stats.bytes_before);
      const bytesAfter = positiveNumber(stats.bytes_after);
      const bytesSaved = positiveNumber(stats.bytes_saved);
      const skipped = positiveNumber(stats.blocks_skipped);
      const reduction = reductionPercent(stats);
      const blocks = [
        positiveNumber(stats.blocks_seen) + ' seen',
        positiveNumber(stats.blocks_pruned) + ' pruned',
        skipped + ' skipped'
      ].join(', ');
      dom.receipt.className = 'receipt visible';
      dom.receipt.replaceChildren();
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
        receiptRow('Input', formatNumber(before) + ' -> ' + formatNumber(after) + ' tokens'),
        receiptRow('Saved', formatNumber(saved) + ' tokens (' + reduction + '%)'),
        receiptRow('Bytes', formatBytes(bytesBefore) + ' -> ' + formatBytes(bytesAfter) + ' (' + formatBytes(bytesSaved) + ' saved)'),
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
      dom.receipt.append(head, grid, total);
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
      if (!t.requests) {
        return 'Workspace savings: 0 requests';
      }
      return 'Workspace savings: ' + formatNumber(t.tokensSaved) + ' input tokens, ' + formatBytes(t.bytesSaved) + ', ~' + formatNumber(t.outputTokens) + ' output tokens across ' + formatNumber(t.requests) + ' request(s)';
    }
    function normalizeTotals(totals) {
      const t = totals || {};
      return {
        requests: positiveNumber(t.requests),
        tokensBefore: positiveNumber(t.tokensBefore),
        tokensAfter: positiveNumber(t.tokensAfter),
        tokensSaved: positiveNumber(t.tokensSaved),
        outputTokens: positiveNumber(t.outputTokens),
        bytesBefore: positiveNumber(t.bytesBefore),
        bytesAfter: positiveNumber(t.bytesAfter),
        bytesSaved: positiveNumber(t.bytesSaved),
        blocksSeen: positiveNumber(t.blocksSeen),
        blocksPruned: positiveNumber(t.blocksPruned),
        blocksSkipped: positiveNumber(t.blocksSkipped)
      };
    }
    function reductionPercent(stats) {
      const ratio = Number(stats.reduction_ratio || 0);
      if (Number.isFinite(ratio) && ratio > 0) {
        return Math.round(ratio * 100);
      }
      const before = positiveNumber(stats.estimated_tokens_before);
      const saved = positiveNumber(stats.estimated_tokens_saved);
      if (!before || !saved) {
        return 0;
      }
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
        case 'diff':
          return 'diff';
        case 'unchanged':
          return 'dedupe';
        case 'warmup':
          return 'warmup';
        case 'skipped':
          return 'skipped';
        case 'none':
          return 'no context';
        default:
          return String(mode || 'stream');
      }
    }
    function renderContextSummary(message) {
      const files = Array.isArray(message.files) ? message.files : [];
      const blockedFiles = Array.isArray(message.blockedFiles) ? message.blockedFiles : [];
      const contextTokens = Number(message.tokens || 0);
      const userTokens = Number(message.userTokens || 0);
      const tokenText = contextTokens ? ', ~' + formatNumber(contextTokens) + ' context tokens' : '';
      const totalText = userTokens ? ', ~' + formatNumber(userTokens) + ' total input tokens' : '';
      dom.contextText.textContent = message.mode + ': ' + files.length + ' file(s), ' + formatBytes(message.bytes) + tokenText + totalText + (message.truncated ? ', capped' : '');
      dom.contextWhy.textContent = message.why || '';
      dom.contextWarning.textContent = message.warning || '';
      dom.contextWarning.className = message.warning ? 'context-warning visible' : 'context-warning';
      dom.contextFiles.replaceChildren();
      dom.contextBlocked.replaceChildren();
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
        dom.contextFiles.appendChild(item);
      }
      if (files.length > 12) {
        const item = document.createElement('li');
        item.className = 'context-meta';
        item.textContent = '+' + (files.length - 12) + ' more file(s)';
        dom.contextFiles.appendChild(item);
      }
      for (const file of blockedFiles.slice(0, 8)) {
        const item = document.createElement('li');
        item.textContent = 'Blocked ' + (file.path || '(untitled)') + ': ' + (file.reason || 'sensitive context');
        dom.contextBlocked.appendChild(item);
      }
      if (blockedFiles.length > 8) {
        const item = document.createElement('li');
        item.textContent = '+' + (blockedFiles.length - 8) + ' more blocked file(s)';
        dom.contextBlocked.appendChild(item);
      }
      dom.copyContext.disabled = message.copyAvailable === false;
    }
    function formatFileMeta(file) {
      const parts = [];
      if (file.source) parts.push(formatSource(file.source));
      if (file.language) parts.push(file.language);
      parts.push(formatBytes(file.bytes));
      if (file.truncated) parts.push('truncated');
      return parts.join(' · ');
    }
    function formatSource(source) {
      switch (source) {
        case 'active':
          return 'active';
        case 'visible':
          return 'visible';
        case 'selection':
          return 'selection';
        case 'workspace':
          return 'workspace';
        default:
          return String(source || 'context');
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
          state.memoryMode = message.settings.memoryMode || state.memoryMode || 'workspace';
          dom.contextMode.value = state.contextMode;
          dom.memoryMode.value = state.memoryMode;
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
    function formatNumber(value) {
      return Number(value || 0).toLocaleString('en-US');
    }
    function positiveNumber(value) {
      const n = Math.floor(Number(value) || 0);
      return Number.isFinite(n) && n > 0 ? n : 0;
    }

    restoreMessages();
    dom.contextMode.value = state.contextMode || 'activeFile';
    dom.memoryMode.value = state.memoryMode || 'workspace';
    renderTotals(state.lastTotals);
    vscode.postMessage({ type: 'ready' });
  </script>
</body>
</html>`;
    }
}

function readSettings(overrideMode?: ContextMode, overrideMemoryMode?: MemoryMode): Settings {
    const config = vscode.workspace.getConfiguration('indexqube');
    return {
        gatewayUrl: normalizeGatewayUrl(config.get<string>('gatewayUrl', 'http://localhost:8080')),
        provider: normalizeProvider(config.get<string>('provider', 'anthropic')),
        model: config.get<string>('model', 'claude-3-5-sonnet'),
        contextMode: overrideMode || normalizeContextMode(config.get<string>('contextMode', 'activeFile')),
        memoryMode: overrideMemoryMode || normalizeMemoryMode(config.get<string>('memoryMode', 'workspace')),
        privacyMode: normalizePrivacyMode(config.get<string>('privacyMode', 'standard')),
        contextExcludePatterns: normalizeStringList(config.get<string[]>('contextExcludePatterns', [])),
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
        source: 'selection',
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
        source: 'active',
        content: doc.getText()
    }]);
}

async function contextFromOpenEditors(settings: Settings): Promise<ContextBundle> {
    const seen = new Set<string>();
    const docs = vscode.window.visibleTextEditors
        .map((editor) => editor.document)
        .filter((doc) => {
            if (!isFileUri(doc.uri)) {
                return false;
            }
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
            source: 'visible' as ContextSource,
            content: doc.getText()
        }));
    return bundleFromDocuments(settings, 'openEditors', docs);
}

async function contextFromWorkspace(settings: Settings): Promise<ContextBundle> {
    if (!vscode.workspace.workspaceFolders?.length) {
        return emptyContext('workspace');
    }

    const docs: ContextDocument[] = [];
    const seen = new Set<string>();

    const activeDoc = vscode.window.activeTextEditor?.document;
    if (activeDoc && isFileUri(activeDoc.uri)) {
        docs.push(documentToContextDoc(activeDoc, 'active'));
        seen.add(uriKey(activeDoc.uri));
    }

    for (const editor of vscode.window.visibleTextEditors) {
        const doc = editor.document;
        const key = uriKey(doc.uri);
        if (seen.has(key) || !isFileUri(doc.uri)) {
            continue;
        }
        docs.push(documentToContextDoc(doc, 'visible'));
        seen.add(key);
    }

    const include = '**/*.{go,ts,tsx,js,jsx,py,rs,java,kt,swift,rb,cs,cpp,h,hpp,c,sql,yaml,yml,json,md,txt,mod,sum,html,css,xml,toml,ini}';
    const exclude = '**/{.git,node_modules,dist,bin,out,build,vendor,.cache,coverage,tmp,temp,.next,.nuxt,target,.turbo,.venv,venv,__pycache__}/**';
    const remainingSlots = Math.max(0, settings.maxWorkspaceFiles - docs.length);
    const uris = remainingSlots > 0 ? await vscode.workspace.findFiles(include, exclude, remainingSlots) : [];

    for (const uri of uris) {
        const key = uriKey(uri);
        if (seen.has(key) || shouldSkipContextUri(uri, settings)) {
            continue;
        }
        const stat = await vscode.workspace.fs.stat(uri);
        if (stat.size > settings.maxFileBytes) {
            continue;
        }
        const raw = await vscode.workspace.fs.readFile(uri);
        docs.push({
            path: vscode.workspace.asRelativePath(uri, false),
            language: languageFromPath(uri.fsPath),
            source: 'workspace',
            content: textDecoder.decode(raw)
        });
        seen.add(key);
    }

    return bundleFromDocuments(settings, 'workspace', docs);
}

function bundleFromDocuments(
    settings: Settings,
    mode: ContextMode,
    docs: ContextDocument[]
): ContextBundle {
    const files: ContextFile[] = [];
    const blockedFiles: ContextBlockedFile[] = [];
    const parts: string[] = [];
    let total = 0;
    let truncated = false;

    for (const doc of docs) {
        const pathBlockReason = contextPathBlockReason(doc.path, settings);
        if (pathBlockReason) {
            blockedFiles.push({ path: doc.path, source: doc.source, reason: pathBlockReason });
            continue;
        }
        if (total >= settings.maxContextBytes) {
            truncated = true;
            break;
        }
        const remaining = settings.maxContextBytes - total;
        const prepared = capText(doc.content, Math.min(settings.maxFileBytes, remaining));
        const secretReasons = secretLikeContentReasons(prepared.text);
        if (secretReasons.length > 0) {
            blockedFiles.push({ path: doc.path, source: doc.source, reason: `secret-like content: ${secretReasons.join(', ')}` });
            continue;
        }
        const bytes = byteLen(prepared.text);
        total += bytes;
        truncated = truncated || prepared.truncated;
        files.push({ path: doc.path, language: doc.language, bytes, truncated: prepared.truncated, source: doc.source });
        parts.push(formatCodeFence(doc.path, doc.language, prepared.text));
    }

    const content = parts.join('\n\n');

    return {
        mode,
        files,
        blockedFiles,
        content,
        bytes: total,
        truncated,
        tokens: estimateTokensFromChars(content.length),
        why: describeContextChoice(mode, files, blockedFiles),
        safe: blockedFiles.length === 0,
        warning: contextWarning(blockedFiles)
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

function describeContextChoice(mode: ContextMode, files: ContextFile[], blockedFiles: ContextBlockedFile[]): string {
    if (files.length === 0 && blockedFiles.length === 0) {
        return 'No files attached.';
    }
    const counts = new Map<ContextSource, number>();
    for (const file of files) {
        counts.set(file.source, (counts.get(file.source) || 0) + 1);
    }
    const parts: string[] = [];
    appendContextPart(parts, counts, 'selection', 'selection');
    appendContextPart(parts, counts, 'active', 'active file');
    appendContextPart(parts, counts, 'visible', 'visible editor');
    appendContextPart(parts, counts, 'workspace', 'workspace file');
    const summary = parts.length > 0 ? parts.join(' + ') : 'no attached files';
    const blocked = blockedFiles.length > 0 ? ` ${blockedFiles.length} blocked.` : '';
    return `${contextModeLabel(mode)}: ${summary}.${blocked}`;
}

function appendContextPart(parts: string[], counts: Map<ContextSource, number>, source: ContextSource, label: string) {
    const count = counts.get(source) || 0;
    if (count === 0) {
        return;
    }
    parts.push(`${count} ${label}${count === 1 ? '' : 's'}`);
}

function contextModeLabel(mode: ContextMode): string {
    switch (mode) {
        case 'selection':
            return 'Selection context';
        case 'activeFile':
            return 'Active file context';
        case 'openEditors':
            return 'Open editor context';
        case 'workspace':
            return 'Workspace context';
        default:
            return 'Context';
    }
}

function contextWarning(blockedFiles: ContextBlockedFile[]): string {
    if (blockedFiles.length === 0) {
        return '';
    }
    return `${blockedFiles.length} context file(s) were blocked before sending. Remove or redact sensitive content, then preview again.`;
}

function shouldSkipContextUri(uri: vscode.Uri, settings: Settings): boolean {
    if (!isFileUri(uri)) {
        return true;
    }
    return Boolean(contextPathBlockReason(vscode.workspace.asRelativePath(uri, false), settings));
}

function isFileUri(uri: vscode.Uri): boolean {
    return uri.scheme === 'file';
}

function contextPathBlockReason(filePath: string, settings: Settings): string {
    const normalized = filePath.replace(/\\/g, '/');
    const lower = normalized.toLowerCase();
    const baseName = lower.split('/').pop() || lower;

    if (sensitiveFilePatterns.some((pattern) => baseName === pattern || lower.endsWith('/' + pattern))) {
        return 'sensitive file name';
    }

    if (hasGeneratedPathSegment(lower)) {
        return 'generated or noisy folder';
    }

    if (noisyLockFiles.has(baseName)) {
        return 'lockfile or noisy dependency metadata';
    }

    const dotIndex = baseName.lastIndexOf('.');
    const extension = dotIndex >= 0 ? baseName.slice(dotIndex) : '';
    if (sensitiveExtensions.has(extension)) {
        return 'sensitive file extension';
    }

    if (matchesUserExcludePattern(normalized, settings.contextExcludePatterns)) {
        return 'user exclude pattern';
    }

    return '';
}

function hasGeneratedPathSegment(path: string): boolean {
    return path.split('/').some((segment) => generatedPathSegments.has(segment));
}

function matchesUserExcludePattern(filePath: string, patterns: string[]): boolean {
    const normalizedPath = filePath.toLowerCase();
    return patterns.some((pattern) => {
        const normalizedPattern = pattern.replace(/\\/g, '/').toLowerCase();
        if (!normalizedPattern) {
            return false;
        }
        if (!normalizedPattern.includes('*')) {
            return normalizedPath.includes(normalizedPattern);
        }
        return wildcardToRegExp(normalizedPattern).test(normalizedPath);
    });
}

function wildcardToRegExp(pattern: string): RegExp {
    const escaped = pattern.replace(/[|\\{}()[\]^$+?.]/g, '\\$&').replace(/\*/g, '.*');
    return new RegExp(`(^|/)${escaped}($|/)`);
}

function secretLikeContentReasons(text: string): string[] {
    const reasons = new Set<string>();
    for (const rule of secretLikePatterns) {
        if (rule.pattern.test(text)) {
            reasons.add(rule.name);
        }
    }
    return Array.from(reasons);
}

function documentToContextDoc(doc: vscode.TextDocument, source: ContextSource): ContextDocument {
    return {
        path: docPath(doc),
        language: doc.languageId || languageFromPath(doc.fileName),
        source,
        content: doc.getText()
    };
}

function uriKey(uri: vscode.Uri): string {
    return uri.toString(true);
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

function normalizeUsageTotals(value?: Partial<UsageTotals>): UsageTotals {
    return {
        requests: positiveInt(value?.requests),
        tokensBefore: positiveInt(value?.tokensBefore),
        tokensAfter: positiveInt(value?.tokensAfter),
        tokensSaved: positiveInt(value?.tokensSaved),
        outputTokens: positiveInt(value?.outputTokens),
        bytesBefore: positiveInt(value?.bytesBefore),
        bytesAfter: positiveInt(value?.bytesAfter),
        bytesSaved: positiveInt(value?.bytesSaved),
        blocksSeen: positiveInt(value?.blocksSeen),
        blocksPruned: positiveInt(value?.blocksPruned),
        blocksSkipped: positiveInt(value?.blocksSkipped),
        lastUpdatedAt: positiveInt(value?.lastUpdatedAt)
    };
}

function emptyContext(mode: ContextMode): ContextBundle {
    return {
        mode,
        files: [],
        blockedFiles: [],
        content: '',
        bytes: 0,
        truncated: false,
        tokens: 0,
        why: 'No editor context matched this mode.',
        safe: true,
        warning: ''
    };
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

function normalizeStringList(value: unknown): string[] {
    if (!Array.isArray(value)) {
        return [];
    }
    return value
        .map((item) => String(item || '').trim())
        .filter(Boolean);
}

function validateGatewayUrlForSecrets(value: string, privacyMode: PrivacyMode = 'standard'): GatewaySecurityCheck {
    let parsed: URL;
    try {
        parsed = new URL(value);
    } catch {
        return { ok: false, message: 'Invalid IndexQube gateway URL.' };
    }

    if (parsed.username || parsed.password) {
        return { ok: false, message: 'Gateway URL must not contain credentials.' };
    }

    if (parsed.protocol === 'http:' && isLocalhost(parsed.hostname)) {
        return { ok: true };
    }

    if (privacyMode === 'localOnly') {
        return {
            ok: false,
            message: 'Privacy mode is localOnly. Only localhost gateways are allowed. Use http://localhost:8080.'
        };
    }

    if (parsed.protocol === 'https:') {
        return { ok: true };
    }

    return {
        ok: false,
        message: 'IndexQube blocks provider keys over insecure remote HTTP. Use HTTPS, localhost, or 127.0.0.1.'
    };
}

function isLocalhost(hostname: string): boolean {
    const normalized = hostname.toLowerCase();
    return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '::1' || normalized === '[::1]';
}

function safeGatewayErrorMessage(status: number, body: string, statusText: string): string {
    const parsed = parseGatewayErrorMessage(body);
    const message = parsed || statusText || 'Request failed.';
    return `Gateway ${status}: ${safeErrorText(message)}`;
}

function parseGatewayErrorMessage(body: string): string {
    const raw = String(body || '').trim();
    if (!raw) {
        return '';
    }
    try {
        const payload = JSON.parse(raw) as {
            error?: string | { message?: string; code?: string; type?: string };
            message?: string;
            detail?: string;
        };
        if (typeof payload.error === 'string') {
            return payload.error;
        }
        if (payload.error?.message) {
            return payload.error.message;
        }
        return payload.message || payload.detail || '';
    } catch {
        return 'Request failed. Check the gateway logs for details.';
    }
}

function parseSseErrorMessage(data: string): string {
    try {
        const parsed = JSON.parse(data) as { error?: { message?: string; code?: string } };
        if (parsed.error?.message) {
            return safeErrorText(parsed.error.message);
        }
    } catch {
        // fall through to raw text
    }
    return safeErrorText(data);
}

function safeErrorText(value: string): string {
    const redacted = redactSecretLikeText(String(value || '').replace(/\s+/g, ' ').trim());
    return truncateForUi(redacted || 'Request failed.');
}

function redactSecretLikeText(value: string): string {
    return value
        .replace(/(Authorization:\s*Bearer\s+)[A-Za-z0-9._-]+/gi, '$1[redacted]')
        .replace(/((?:X-IQ-Provider-Key|x-api-key|api-key):\s*)\S+/gi, '$1[redacted]')
        .replace(/\bsk-[A-Za-z0-9_-]{8,}\b/g, '[redacted-openai-key]')
        .replace(/\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{8,}\b/g, '[redacted-github-token]')
        .replace(/\bgithub_pat_[A-Za-z0-9_]{8,}\b/g, '[redacted-github-token]')
        .replace(/\bAKIA[0-9A-Z]{16}\b/g, '[redacted-aws-key]')
        .replace(/\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, '[redacted-jwt]');
}

function truncateForUi(value: string, maxLength = 700): string {
    if (value.length <= maxLength) {
        return value;
    }
    return `${value.slice(0, maxLength - 1)}...`;
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

function normalizeMemoryMode(value?: string): MemoryMode {
    switch (value) {
        case 'isolated':
        case 'workspace':
            return value;
        default:
            return 'workspace';
    }
}

function normalizePrivacyMode(value?: string): PrivacyMode {
    return value === 'localOnly' ? 'localOnly' : 'standard';
}

function positiveNumber(value: unknown, fallback: number): number {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

function positiveInt(value: unknown): number {
    const n = Math.floor(Number(value) || 0);
    return Number.isFinite(n) && n > 0 ? n : 0;
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

function sessionLabel(mode: MemoryMode, workspaceSessionKey: string): string {
    if (mode === 'isolated') {
        return 'fresh/request';
    }
    return shortKey(workspaceSessionKey);
}

function getNonce(): string {
    return randomBytes(16).toString('base64');
}
