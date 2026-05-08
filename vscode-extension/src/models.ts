import * as vscode from 'vscode';
import { Provider } from './types';
import { fetchWithTimeout } from './utils';

type ModelItem = { label: string; description: string };

const providerModels: Record<Provider, ModelItem[]> = {
    anthropic: [
        { label: 'claude-opus-4-7',               description: 'Most capable' },
        { label: 'claude-sonnet-4-6',              description: 'Balanced (recommended)' },
        { label: 'claude-haiku-4-5',               description: 'Fast and affordable' },
        { label: 'claude-3-5-sonnet-20241022',     description: 'Claude 3.5 Sonnet' },
        { label: 'claude-3-5-haiku-20241022',      description: 'Claude 3.5 Haiku' },
        { label: 'claude-3-opus-20240229',         description: 'Claude 3 Opus' }
    ],
    openai: [
        { label: 'gpt-4o',        description: 'Fastest GPT-4 class (recommended)' },
        { label: 'gpt-4o-mini',   description: 'Affordable GPT-4 class' },
        { label: 'gpt-4-turbo',   description: 'GPT-4 Turbo' },
        { label: 'gpt-4',         description: 'GPT-4' },
        { label: 'o1',            description: 'OpenAI o1 reasoning' },
        { label: 'o1-mini',       description: 'OpenAI o1 mini' },
        { label: 'o3-mini',       description: 'OpenAI o3 mini' },
        { label: 'gpt-3.5-turbo', description: 'GPT-3.5 Turbo' }
    ],
    azure: [
        { label: 'gpt-4o',       description: 'Azure GPT-4o deployment' },
        { label: 'gpt-4-turbo',  description: 'Azure GPT-4 Turbo deployment' },
        { label: 'gpt-4',        description: 'Azure GPT-4 deployment' },
        { label: 'gpt-35-turbo', description: 'Azure GPT-3.5 Turbo deployment' }
    ],
    bedrock: [
        { label: 'anthropic.claude-opus-4-7',                       description: 'Claude Opus 4.7 on Bedrock' },
        { label: 'anthropic.claude-sonnet-4-6',                     description: 'Claude Sonnet 4.6 on Bedrock' },
        { label: 'anthropic.claude-haiku-4-5-20251001-v1:0',        description: 'Claude Haiku 4.5 on Bedrock' },
        { label: 'anthropic.claude-3-5-sonnet-20241022-v2:0',       description: 'Claude 3.5 Sonnet on Bedrock' },
        { label: 'anthropic.claude-3-5-haiku-20241022-v1:0',        description: 'Claude 3.5 Haiku on Bedrock' },
        { label: 'anthropic.claude-3-opus-20240229-v1:0',           description: 'Claude 3 Opus on Bedrock' },
        { label: 'anthropic.claude-3-sonnet-20240229-v1:0',         description: 'Claude 3 Sonnet on Bedrock' }
    ]
};

async function fetchModelsFromGateway(gatewayUrl: string, provider: Provider): Promise<ModelItem[] | null> {
    try {
        const resp = await fetchWithTimeout(`${gatewayUrl}/v1/models?provider=${provider}`, 2500);
        if (!resp.ok) {
            return null;
        }
        const data = await resp.json() as { data?: Array<{ id: string; description?: string }> };
        const items = (data.data || [])
            .filter((m) => typeof m.id === 'string' && m.id)
            .map((m) => ({ label: m.id, description: m.description || '' }));
        return items.length > 0 ? items : null;
    } catch {
        return null;
    }
}

export async function pickModel(provider: Provider, gatewayUrl?: string): Promise<string | undefined> {
    let models: ModelItem[] = providerModels[provider] ?? [];

    if (gatewayUrl) {
        const remote = await fetchModelsFromGateway(gatewayUrl, provider);
        if (remote) {
            models = remote;
        }
    }

    const items = [
        ...models,
        { label: 'Other', description: 'Enter a custom model ID' }
    ];

    const picked = await vscode.window.showQuickPick(items, {
        title: `IndexQube: Pick Model (${provider})`,
        placeHolder: 'Select a model',
        matchOnDescription: true
    });

    if (!picked) {
        return undefined;
    }
    if (picked.label === 'Other') {
        return vscode.window.showInputBox({
            title: 'IndexQube: Custom Model ID',
            prompt: 'Enter the exact model ID string',
            ignoreFocusOut: true
        });
    }
    return picked.label;
}
