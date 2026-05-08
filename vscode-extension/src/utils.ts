import { randomBytes } from 'crypto';
import * as vscode from 'vscode';
import { ContextSource, MemoryMode, Settings, UsageTotals } from './types';

export function positiveNumber(value: unknown, fallback: number): number {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

export function positiveInt(value: unknown): number {
    const n = Math.floor(Number(value) || 0);
    return Number.isFinite(n) && n > 0 ? n : 0;
}

export function byteLen(text: string): number {
    return Buffer.byteLength(text, 'utf8');
}

export function estimateTokensFromChars(chars: number): number {
    return Math.max(0, Math.ceil(chars / 4));
}

export function createSessionKey(): string {
    return randomBytes(16).toString('hex');
}

export function getNonce(): string {
    return randomBytes(16).toString('base64');
}

export function shortKey(key: string): string {
    return key.length <= 12 ? key : `${key.slice(0, 4)}...${key.slice(-4)}`;
}

export function sessionLabel(mode: MemoryMode, workspaceSessionKey: string): string {
    if (mode === 'isolated') {
        return 'fresh/request';
    }
    return shortKey(workspaceSessionKey);
}

export function normalizeUsageTotals(value?: Partial<UsageTotals>): UsageTotals {
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

export function capText(text: string, maxBytes: number): { text: string; truncated: boolean } {
    if (byteLen(text) <= maxBytes) {
        return { text, truncated: false };
    }
    let end = Math.max(0, Math.min(text.length, maxBytes));
    while (byteLen(text.slice(0, end)) > maxBytes && end > 0) {
        end--;
    }
    return { text: text.slice(0, end), truncated: true };
}

export function formatCodeFence(path: string, language: string, content: string): string {
    return '```' + language + ' ' + path + '\n' + content.trimEnd() + '\n```';
}

export function docPath(doc: vscode.TextDocument): string {
    if (doc.uri.scheme === 'file') {
        return vscode.workspace.asRelativePath(doc.uri, false);
    }
    return doc.fileName || doc.uri.toString();
}

export function uriKey(uri: vscode.Uri): string {
    return uri.toString(true);
}

export function languageFromPath(filePath: string): string {
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

export function buildHeaders(settings: Settings, apiKey: string, sessionKey: string): Record<string, string> {
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

export function buildUserContent(prompt: string, contextContent: string, contextFiles: Array<{ path: string; language: string; bytes: number }>, mode: string): string {
    if (!contextContent) {
        return prompt;
    }
    const files = contextFiles.map((f) => `- ${f.path} (${f.language}, ${f.bytes} bytes)`).join('\n');
    return [
        'IndexQube IDE context:',
        `Mode: ${mode}`,
        files ? `Files:\n${files}` : 'Files: none',
        '',
        contextContent,
        '',
        'User request:',
        prompt
    ].join('\n');
}

export async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { method: 'GET', signal: controller.signal });
    } finally {
        clearTimeout(timer);
    }
}

export function formatSource(source: ContextSource): string {
    switch (source) {
        case 'active': return 'active';
        case 'visible': return 'visible';
        case 'selection': return 'selection';
        case 'workspace': return 'workspace';
        default: return String(source || 'context');
    }
}
