import * as vscode from 'vscode';
import { GatewaySecurityCheck, PrivacyMode, Settings } from './types';
import {
    builtinSecretPatterns,
    generatedPathSegments,
    noisyLockFiles,
    sensitiveExtensions,
    sensitiveFilePatterns
} from './constants';

export function validateGatewayUrlForSecrets(value: string, privacyMode: PrivacyMode = 'standard'): GatewaySecurityCheck {
    let parsed: URL;
    try {
        parsed = new URL(value);
    } catch {
        return { ok: false, message: 'Invalid IndexQube gateway URL.' };
    }

    if (parsed.username || parsed.password) {
        return { ok: false, message: 'Gateway URL must not contain credentials.' };
    }

    if ((parsed.protocol === 'http:' || parsed.protocol === 'https:') && isLocalhost(parsed.hostname)) {
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

export function isLocalhost(hostname: string): boolean {
    const normalized = hostname.toLowerCase();
    return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '::1' || normalized === '[::1]';
}

export function contextPathBlockReason(filePath: string, settings: Settings): string {
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

export function shouldSkipContextUri(uri: vscode.Uri, settings: Settings): boolean {
    if (!isFileUri(uri)) {
        return true;
    }
    return Boolean(contextPathBlockReason(vscode.workspace.asRelativePath(uri, false), settings));
}

export function isFileUri(uri: vscode.Uri): boolean {
    return uri.scheme === 'file';
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

export function parseSecretPatterns(patterns: string[]): RegExp[] {
    return patterns.flatMap((p) => {
        try {
            return [new RegExp(p)];
        } catch {
            return [];
        }
    });
}

export function redactSecretContent(text: string, customPatterns: RegExp[]): { text: string; redactedCount: number } {
    let result = text;
    let redactedCount = 0;

    for (const rule of builtinSecretPatterns) {
        const global = withGlobalFlag(rule.pattern);
        const matches = result.match(global);
        if (matches) {
            redactedCount += matches.length;
            result = result.replace(global, '[redacted]');
        }
    }

    for (const pattern of customPatterns) {
        const global = withGlobalFlag(pattern);
        const matches = result.match(global);
        if (matches) {
            redactedCount += matches.length;
            result = result.replace(global, '[redacted]');
        }
    }

    return { text: result, redactedCount };
}

function withGlobalFlag(pattern: RegExp): RegExp {
    return new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : pattern.flags + 'g');
}

export function looksLikeBinary(text: string): boolean {
    const sample = text.slice(0, 8000);
    const nullCount = (sample.match(/\0/g) || []).length;
    return nullCount > sample.length * 0.01;
}

export function redactSecretLikeText(value: string): string {
    return value
        .replace(/(Authorization:\s*Bearer\s+)[A-Za-z0-9._-]+/gi, '$1[redacted]')
        .replace(/((?:X-IQ-Provider-Key|x-api-key|api-key):\s*)\S+/gi, '$1[redacted]')
        .replace(/\bsk-[A-Za-z0-9_-]{8,}\b/g, '[redacted-openai-key]')
        .replace(/\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{8,}\b/g, '[redacted-github-token]')
        .replace(/\bgithub_pat_[A-Za-z0-9_]{8,}\b/g, '[redacted-github-token]')
        .replace(/\bAKIA[0-9A-Z]{16}\b/g, '[redacted-aws-key]')
        .replace(/\bxox[baprs]-[A-Za-z0-9-]{20,}\b/g, '[redacted-slack-token]')
        .replace(/\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, '[redacted-jwt]')
        .replace(/-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----[\s\S]*?-----END (?:[A-Z ]+ )?PRIVATE KEY-----/g, '[redacted-private-key]');
}

export function truncateForUi(value: string, maxLength = 700): string {
    if (value.length <= maxLength) {
        return value;
    }
    return `${value.slice(0, maxLength - 1)}...`;
}

export function safeErrorText(value: string): string {
    const redacted = redactSecretLikeText(String(value || '').replace(/\s+/g, ' ').trim());
    return truncateForUi(redacted || 'Request failed.');
}

export function safeGatewayErrorMessage(status: number, body: string, statusText: string): string {
    const parsed = parseGatewayErrorMessage(body);
    const message = parsed || statusText || 'Request failed.';
    return `Gateway ${status}: ${safeErrorText(message)}`;
}

export function parseGatewayErrorMessage(body: string): string {
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
        if (payload.error && typeof payload.error === 'object' && payload.error.message) {
            return payload.error.message;
        }
        return payload.message || payload.detail || '';
    } catch {
        return 'Request failed. Check the gateway logs for details.';
    }
}

export function parseSseErrorMessage(data: string): string {
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
