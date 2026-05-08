import * as vscode from 'vscode';
import { Provider } from './types';
import { secretPrefix } from './constants';

type ProviderPick = { label: string; description: string; value: Provider };

export function secretName(provider: Provider): string {
    return `${secretPrefix}${provider}`;
}

export async function pickAndStoreProviderKey(
    secrets: vscode.SecretStorage,
    defaultProvider: Provider
): Promise<Provider | undefined> {
    const providers: ProviderPick[] = [
        { label: 'Anthropic', description: 'Claude models',   value: 'anthropic' },
        { label: 'OpenAI',    description: 'GPT models',      value: 'openai' },
        { label: 'Azure',     description: 'Azure OpenAI',    value: 'azure' },
        { label: 'Bedrock',   description: 'AWS Bedrock',     value: 'bedrock' }
    ];

    const picked = await vscode.window.showQuickPick(
        orderCurrentProviderFirst(providers, defaultProvider),
        {
            title: 'IndexQube: Set Provider Key',
            placeHolder: 'Select a provider',
            matchOnDescription: true
        }
    );
    if (!picked) {
        return undefined;
    }
    if (!await promptAndStoreProviderKey(secrets, picked.value)) {
        return undefined;
    }
    return picked.value;
}

function orderCurrentProviderFirst(items: ProviderPick[], current: Provider): ProviderPick[] {
    return [...items]
        .map((item) => ({
            ...item,
            description: item.value === current ? `${item.description} (current)` : item.description
        }))
        .sort((a, b) => Number(b.value === current) - Number(a.value === current));
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
