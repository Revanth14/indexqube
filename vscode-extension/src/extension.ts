import * as vscode from 'vscode';
import { sessionKeyName, usageTotalsKey } from './constants';
import { collectContext } from './context';
import { pickAndStoreProviderKey, secretName } from './commands';
import { pickModel } from './models';
import { applyProviderSelection, readSettings } from './settings';
import { validateGatewayUrlForSecrets, parseSseErrorMessage, safeErrorText, safeGatewayErrorMessage } from './security';
import { readSSE } from './sse';
import { OptimizerEvent, UsageTotals, WebviewMessage } from './types';
import { buildHeaders, buildUserContent, createSessionKey, estimateTokensFromChars, fetchWithTimeout, getNonce, normalizeUsageTotals, positiveInt, sessionLabel } from './utils';
import { getHtml } from './webview';
import { normalizeContextMode, normalizeMemoryMode } from './settings';

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
            const savedProvider = await pickAndStoreProviderKey(context.secrets, settings.provider);
            if (savedProvider) {
                await applyProviderSelection(settings.provider, savedProvider);
                provider.postState();
            }
        }),
        vscode.commands.registerCommand('indexqube.pickModel', async () => {
            const settings = readSettings();
            const picked = await pickModel(settings.provider, settings.gatewayUrl);
            if (picked) {
                await vscode.workspace.getConfiguration('indexqube').update('model', picked, vscode.ConfigurationTarget.Global);
                vscode.window.showInformationMessage(`IndexQube model set to ${picked}.`);
                provider.postState();
            }
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
        webviewView.webview.html = getHtml(getNonce());
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
                await this.handleChat(
                    String(data.text || ''),
                    normalizeContextMode(data.contextMode),
                    normalizeMemoryMode(data.memoryMode),
                    webview
                );
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
        const savedProvider = await pickAndStoreProviderKey(this.context.secrets, settings.provider);
        if (savedProvider) {
            await applyProviderSelection(settings.provider, savedProvider);
            webview.postMessage({ type: 'notice', text: `${savedProvider} key saved in VS Code Secret Storage.` });
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
            webview.postMessage({ type: 'gateway', status: 'offline', text: gatewayCheck.message || 'Unsafe gateway URL.' });
            return;
        }

        try {
            const health = await fetchWithTimeout(`${settings.gatewayUrl}/healthz`, 2500);
            const ready  = await fetchWithTimeout(`${settings.gatewayUrl}/readyz`, 2500);
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

    private async handleChat(text: string, mode: import('./types').ContextMode, memoryMode: import('./types').MemoryMode, webview: vscode.Webview) {
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

        const ctx = await collectContext(settings);
        if (!ctx.safe) {
            this.lastUserContent = ctx.content;
            webview.postMessage({
                type: 'context',
                mode: ctx.mode,
                files: ctx.files,
                blockedFiles: ctx.blockedFiles,
                bytes: ctx.bytes,
                truncated: ctx.truncated,
                tokens: ctx.tokens,
                userTokens: ctx.tokens,
                why: ctx.why,
                warning: ctx.warning,
                copyAvailable: Boolean(ctx.content)
            });
            webview.postMessage({ type: 'error', text: ctx.warning || 'Context blocked before provider key use.' });
            return;
        }

        const sessionKey = await this.sessionKeyForRequest(settings);
        const apiKey = await this.context.secrets.get(secretName(settings.provider));
        if (!apiKey) {
            webview.postMessage({ type: 'error', text: `No ${settings.provider} key saved. Click Key or run "IndexQube: Set Provider Key".` });
            return;
        }

        const userContent = buildUserContent(prompt, ctx.content, ctx.files, ctx.mode);
        this.lastUserContent = userContent;
        const controller = new AbortController();
        this.activeController = controller;
        this.assistantChars = 0;
        this.pendingOptimizer = undefined;
        let shouldRecordUsage = false;
        let streamHadError = false;

        webview.postMessage({
            type: 'context',
            mode: ctx.mode,
            files: ctx.files,
            blockedFiles: ctx.blockedFiles,
            bytes: ctx.bytes,
            truncated: ctx.truncated,
            tokens: ctx.tokens,
            userTokens: estimateTokensFromChars(userContent.length),
            why: ctx.why,
            warning: ctx.warning,
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
                onEvent: (event, data) => {
                    if (event === 'error') {
                        streamHadError = true;
                    }
                    this.handleSSEEvent(webview, event, data);
                },
                onText: (chunk) => {
                    this.assistantChars += chunk.length;
                    webview.postMessage({ type: 'delta', text: chunk });
                }
            });
            shouldRecordUsage = !streamHadError;
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
                totals: totals || this.getUsageTotals(),
                requestSucceeded: shouldRecordUsage
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

    private async previewContext(mode: import('./types').ContextMode, webview: vscode.Webview) {
        const settings = readSettings(mode);
        const ctx = await collectContext(settings);
        this.lastUserContent = ctx.content;
        webview.postMessage({
            type: 'context',
            mode: ctx.mode,
            files: ctx.files,
            blockedFiles: ctx.blockedFiles,
            bytes: ctx.bytes,
            truncated: ctx.truncated,
            tokens: ctx.tokens,
            userTokens: ctx.tokens,
            why: ctx.why,
            warning: ctx.warning,
            copyAvailable: Boolean(ctx.content)
        });
    }

    private getUsageTotals(): UsageTotals {
        return normalizeUsageTotals(this.context.workspaceState.get<Partial<UsageTotals>>(usageTotalsKey));
    }

    private async recordUsageTotals(event: OptimizerEvent, outputTokens: number): Promise<UsageTotals> {
        const stats = event.stats || {};
        const totals = this.getUsageTotals();
        const tokensBefore = positiveInt(stats.estimated_tokens_before);
        const tokensAfter  = positiveInt(stats.estimated_tokens_after);
        const tokensSaved  = positiveInt(stats.estimated_tokens_saved ?? tokensBefore - tokensAfter);
        const bytesBefore  = positiveInt(stats.bytes_before);
        const bytesAfter   = positiveInt(stats.bytes_after);
        const bytesSaved   = positiveInt(stats.bytes_saved ?? bytesBefore - bytesAfter);

        totals.requests      += 1;
        totals.tokensBefore  += tokensBefore;
        totals.tokensAfter   += tokensAfter;
        totals.tokensSaved   += tokensSaved;
        totals.outputTokens  += positiveInt(outputTokens);
        totals.bytesBefore   += bytesBefore;
        totals.bytesAfter    += bytesAfter;
        totals.bytesSaved    += bytesSaved;
        totals.blocksSeen    += positiveInt(stats.blocks_seen);
        totals.blocksPruned  += positiveInt(stats.blocks_pruned);
        totals.blocksSkipped += positiveInt(stats.blocks_skipped);
        totals.lastUpdatedAt  = Date.now();

        await this.context.workspaceState.update(usageTotalsKey, totals);
        return totals;
    }

    private async sessionKeyForRequest(settings: import('./types').Settings): Promise<string> {
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
}
