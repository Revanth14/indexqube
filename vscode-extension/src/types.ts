export type Provider = 'anthropic' | 'openai' | 'azure' | 'bedrock';
export type ContextMode = 'selection' | 'activeFile' | 'openEditors' | 'workspace';
export type ContextSource = 'selection' | 'active' | 'visible' | 'workspace';
export type MemoryMode = 'workspace' | 'isolated';
export type PrivacyMode = 'standard' | 'localOnly';

export interface Settings {
    gatewayUrl: string;
    provider: Provider;
    model: string;
    contextMode: ContextMode;
    memoryMode: MemoryMode;
    privacyMode: PrivacyMode;
    contextExcludePatterns: string[];
    secretPatterns: string[];
    maxContextBytes: number;
    maxWorkspaceFiles: number;
    maxFileBytes: number;
    projectMemory: string;
    azureEndpoint: string;
    awsRegion: string;
    maxTokens: number;
    temperature: number;
}

export interface ContextFile {
    path: string;
    language: string;
    bytes: number;
    truncated: boolean;
    source: ContextSource;
    redactedCount?: number;
}

export interface ContextBundle {
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

export interface ContextDocument {
    path: string;
    language: string;
    source: ContextSource;
    content: string;
}

export interface ContextBlockedFile {
    path: string;
    source: ContextSource;
    reason: string;
}

export interface OptimizerEvent {
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

export interface UsageTotals {
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

export interface WebviewMessage {
    type: string;
    text?: string;
    contextMode?: ContextMode;
    memoryMode?: MemoryMode;
}

export interface GatewaySecurityCheck {
    ok: boolean;
    message?: string;
}
