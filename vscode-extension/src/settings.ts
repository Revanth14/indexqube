import * as vscode from 'vscode';
import { ContextMode, MemoryMode, PrivacyMode, Provider, Settings } from './types';
import { positiveNumber } from './utils';

export function readSettings(overrideMode?: ContextMode, overrideMemoryMode?: MemoryMode): Settings {
    const config = vscode.workspace.getConfiguration('indexqube');
    const provider = normalizeProvider(config.get<string>('provider', 'anthropic'));
    return {
        gatewayUrl: normalizeGatewayUrl(config.get<string>('gatewayUrl', 'http://localhost:8080')),
        provider,
        model: normalizeModelForProvider(config.get<string>('model', defaultModelForProvider(provider)), provider),
        contextMode: overrideMode || normalizeContextMode(config.get<string>('contextMode', 'activeFile')),
        memoryMode: overrideMemoryMode || normalizeMemoryMode(config.get<string>('memoryMode', 'workspace')),
        privacyMode: normalizePrivacyMode(config.get<string>('privacyMode', 'standard')),
        contextExcludePatterns: normalizeStringList(config.get<string[]>('contextExcludePatterns', [])),
        secretPatterns: normalizeStringList(config.get<string[]>('secretPatterns', [])),
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

export async function applyProviderSelection(previousProvider: Provider, nextProvider: Provider): Promise<void> {
    const config = vscode.workspace.getConfiguration('indexqube');
    await config.update('provider', nextProvider, vscode.ConfigurationTarget.Global);
    const currentModel = normalizeModelForProvider(config.get<string>('model', ''), previousProvider);
    if (previousProvider !== nextProvider && currentModel === defaultModelForProvider(previousProvider)) {
        await config.update('model', defaultModelForProvider(nextProvider), vscode.ConfigurationTarget.Global);
    }
}

export function defaultModelForProvider(provider: Provider): string {
    switch (provider) {
        case 'openai':
            return 'gpt-4o-mini';
        case 'azure':
            return 'gpt-4o';
        case 'bedrock':
            return 'anthropic.claude-sonnet-4-6';
        case 'anthropic':
        default:
            return 'claude-sonnet-4-6';
    }
}

export function normalizeModelForProvider(value: string | undefined, provider: Provider): string {
    const model = String(value || '').trim();
    if (!model || isLegacyDefaultModel(model, provider)) {
        return defaultModelForProvider(provider);
    }
    return model;
}

export function isLegacyDefaultModel(model: string, provider: Provider): boolean {
    if (provider === 'anthropic') {
        return model === 'claude-3-5-sonnet';
    }
    return model === 'claude-3-5-sonnet' || model === defaultModelForProvider('anthropic');
}

export function normalizeProvider(value?: string): Provider {
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

export function normalizeContextMode(value?: string): ContextMode {
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

export function normalizeMemoryMode(value?: string): MemoryMode {
    switch (value) {
        case 'isolated':
        case 'workspace':
            return value;
        default:
            return 'workspace';
    }
}

export function normalizePrivacyMode(value?: string): PrivacyMode {
    return value === 'localOnly' ? 'localOnly' : 'standard';
}

export function normalizeGatewayUrl(value: string): string {
    return String(value || 'http://localhost:8080').trim().replace(/\/+$/, '') || 'http://localhost:8080';
}

export function normalizeStringList(value: unknown): string[] {
    if (!Array.isArray(value)) {
        return [];
    }
    return value
        .map((item) => String(item || '').trim())
        .filter(Boolean);
}
