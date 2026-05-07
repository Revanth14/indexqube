import * as vscode from 'vscode';

export function activate(context: vscode.ExtensionContext) {
    console.log('IndexQube extension is now active');

    const provider = new IndexQubeChatViewProvider(context.extensionUri, context.secrets);

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(
            IndexQubeChatViewProvider.viewType,
            provider
        )
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('indexqube.openSettings', () => {
            vscode.commands.executeCommand('workbench.action.openSettings', 'IndexQube');
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('indexqube.setProviderKey', async () => {
            const providerName = vscode.workspace.getConfiguration('indexqube').get<string>('provider', 'anthropic');
            await promptAndStoreProviderKey(context.secrets, providerName);
        })
    );
}

const secretPrefix = 'indexqube.providerKey.';

class IndexQubeChatViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'indexqube.chatView';

    constructor(
        private readonly _extensionUri: vscode.Uri,
        private readonly _secrets: vscode.SecretStorage
    ) {}

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken
    ) {
        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri]
        };

        webviewView.webview.html = this._getHtmlForWebview(webviewView.webview);

        webviewView.webview.onDidReceiveMessage(data => {
            switch (data.type) {
                case 'sendMessage':
                    void this._handleChat(data.text, webviewView.webview);
                    break;
                case 'setProviderKey':
                    void this._setProviderKey(webviewView.webview);
                    break;
            }
        });
    }

    private async _setProviderKey(webview: vscode.Webview) {
        const provider = vscode.workspace.getConfiguration('indexqube').get<string>('provider', 'anthropic');
        const saved = await promptAndStoreProviderKey(this._secrets, provider);
        if (saved) {
            webview.postMessage({ type: 'notice', text: `${provider} key saved in VS Code Secret Storage.` });
        }
    }

    private async _handleChat(text: string, webview: vscode.Webview) {
        const config = vscode.workspace.getConfiguration('indexqube');
        const gatewayUrl = config.get<string>('gatewayUrl', 'http://localhost:8080');
        const provider = config.get<string>('provider', 'anthropic');
        const model = config.get<string>('model', 'claude-3-5-sonnet');
        const apiKey = await this._secrets.get(secretName(provider));
        if (!apiKey) {
            webview.postMessage({
                type: 'error',
                text: `No ${provider} key saved. Run "IndexQube: Set Provider Key" or click Set Key.`
            });
            return;
        }

        // Capture active editor context if any
        const editor = vscode.window.activeTextEditor;
        let contextMessage = '';
        if (editor) {
            const document = editor.document;
            const selection = editor.selection;
            const content = selection.isEmpty ? document.getText() : document.getText(selection);
            contextMessage = `File: ${document.fileName}\n\n\`\`\`${document.languageId}\n${content}\n\`\`\``;
        }

        const messages = [];
        if (contextMessage) {
            messages.push({ role: 'user', content: contextMessage });
        }
        messages.push({ role: 'user', content: text });

        webview.postMessage({ type: 'startResponse' });

        try {
            const response = await fetch(`${gatewayUrl}/v1/chat/completions`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-IQ-Provider': provider,
                    'X-IQ-Provider-Key': apiKey
                },
                body: JSON.stringify({
                    model: model,
                    messages: messages,
                    stream: true
                })
            });

            if (!response.ok) {
                const err = await response.text();
                webview.postMessage({ type: 'error', text: `Gateway error: ${err}` });
                return;
            }

            const reader = response.body?.getReader();
            if (!reader) return;

            const decoder = new TextDecoder();
            let buffer = '';
            let finished = false;
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const frames = buffer.split('\n\n');
                buffer = frames.pop() || '';
                for (const frame of frames) {
                    const line = frame.split('\n').find((part) => part.startsWith('data: '));
                    if (!line) {
                        continue;
                    }
                    const data = line.slice(6);
                    if (data === '[DONE]') {
                        finished = true;
                        break;
                    }
                    try {
                        const json = JSON.parse(data);
                        const content = json.choices[0]?.delta?.content;
                        if (content) {
                            webview.postMessage({ type: 'delta', text: content });
                        }
                    } catch (e) {
                        // Ignore partial JSON
                    }
                }
                if (finished) {
                    break;
                }
            }
        } catch (err: any) {
            webview.postMessage({ type: 'error', text: `Failed to connect to IndexQube: ${err.message}` });
        }
    }

    private _getHtmlForWebview(_webview: vscode.Webview) {
        return `<!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>IndexQube Chat</title>
                <style>
                    body { font-family: var(--vscode-font-family); padding: 10px; color: var(--vscode-foreground); }
                    textarea { width: 100%; height: 100px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 8px; border-radius: 4px; resize: vertical; }
                    .actions { display: flex; gap: 8px; margin-top: 10px; }
                    button { flex: 1; padding: 8px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; border-radius: 4px; cursor: pointer; }
                    button:hover { background: var(--vscode-button-hoverBackground); }
                    #chat { margin-top: 20px; font-size: 0.9em; }
                    .message { margin-bottom: 10px; padding: 8px; border-radius: 4px; }
                    .user { background: var(--vscode-textBlockQuote-background); }
                    .assistant { border-left: 2px solid var(--vscode-button-background); }
                    .error { color: var(--vscode-errorForeground); }
                    .notice { color: var(--vscode-descriptionForeground); }
                </style>
            </head>
            <body>
                <textarea id="input" placeholder="Ask IndexQube..."></textarea>
                <div class="actions">
                    <button id="send" type="button">Send</button>
                    <button id="setKey" type="button">Set Key</button>
                </div>
                <div id="chat"></div>
                <script>
                    const vscode = acquireVsCodeApi();
                    const input = document.getElementById('input');
                    const send = document.getElementById('send');
                    const setKey = document.getElementById('setKey');
                    const chat = document.getElementById('chat');

                    function appendMessage(className, text) {
                        const div = document.createElement('div');
                        div.className = 'message ' + className;
                        div.textContent = text;
                        chat.appendChild(div);
                        window.scrollTo(0, document.body.scrollHeight);
                        return div;
                    }

                    send.addEventListener('click', () => {
                        const text = input.value;
                        if (!text) return;
                        appendMessage('user', text);
                        vscode.postMessage({ type: 'sendMessage', text: text });
                        input.value = '';
                    });

                    setKey.addEventListener('click', () => {
                        vscode.postMessage({ type: 'setProviderKey' });
                    });

                    window.addEventListener('message', event => {
                        const message = event.data;
                        switch (message.type) {
                            case 'startResponse':
                                const responseDiv = document.createElement('div');
                                responseDiv.className = 'message assistant';
                                chat.appendChild(responseDiv);
                                break;
                            case 'delta':
                                const lastAssistant = chat.querySelector('.assistant:last-child');
                                if (lastAssistant) {
                                    lastAssistant.textContent += message.text;
                                    window.scrollTo(0, document.body.scrollHeight);
                                }
                                break;
                            case 'error':
                                appendMessage('error', message.text);
                                break;
                            case 'notice':
                                appendMessage('notice', message.text);
                                break;
                        }
                    });
                </script>
            </body>
            </html>`;
    }
}

function secretName(provider: string): string {
    return `${secretPrefix}${provider}`;
}

async function promptAndStoreProviderKey(secrets: vscode.SecretStorage, provider: string): Promise<boolean> {
    const key = await vscode.window.showInputBox({
        title: `IndexQube ${provider} provider key`,
        prompt: 'Stored locally in VS Code Secret Storage and sent only to your configured gateway.',
        password: true,
        ignoreFocusOut: true
    });
    if (!key) {
        return false;
    }
    await secrets.store(secretName(provider), key.trim());
    return true;
}
