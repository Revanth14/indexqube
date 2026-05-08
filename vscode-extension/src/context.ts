import * as vscode from 'vscode';
import {
    ContextBundle,
    ContextBlockedFile,
    ContextDocument,
    ContextFile,
    ContextMode,
    ContextSource,
    Settings
} from './types';
import { contextPathBlockReason, isFileUri, looksLikeBinary, parseSecretPatterns, redactSecretContent, shouldSkipContextUri } from './security';
import { byteLen, capText, docPath, estimateTokensFromChars, formatCodeFence, languageFromPath, uriKey } from './utils';

const fileDecoder = new TextDecoder('utf-8', { fatal: false });

export async function collectContext(settings: Settings): Promise<ContextBundle> {
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
    const candidateUris = remainingSlots > 0
        ? await vscode.workspace.findFiles(include, exclude, remainingSlots)
        : [];

    const toRead = candidateUris.filter(
        (uri) => !seen.has(uriKey(uri)) && !shouldSkipContextUri(uri, settings)
    );

    const readResults = await readFilesParallel(toRead, settings);
    for (const result of readResults) {
        if (result) {
            docs.push(result);
        }
    }

    return bundleFromDocuments(settings, 'workspace', docs);
}

async function readFilesParallel(
    uris: vscode.Uri[],
    settings: Settings,
    concurrency = 8
): Promise<(ContextDocument | null)[]> {
    if (uris.length === 0) {
        return [];
    }

    const results: (ContextDocument | null)[] = new Array(uris.length).fill(null);
    let idx = 0;

    async function worker(): Promise<void> {
        while (true) {
            const i = idx++;
            if (i >= uris.length) {
                return;
            }
            const uri = uris[i];
            try {
                const stat = await vscode.workspace.fs.stat(uri);
                if (stat.size > settings.maxFileBytes) {
                    continue;
                }
                const raw = await vscode.workspace.fs.readFile(uri);
                const content = fileDecoder.decode(raw);
                if (looksLikeBinary(content)) {
                    continue;
                }
                results[i] = {
                    path: vscode.workspace.asRelativePath(uri, false),
                    language: languageFromPath(uri.fsPath),
                    source: 'workspace',
                    content
                };
            } catch {
                // Skip unreadable files.
            }
        }
    }

    await Promise.all(Array.from({ length: Math.min(concurrency, uris.length) }, worker));
    return results;
}

export function bundleFromDocuments(
    settings: Settings,
    mode: ContextMode,
    docs: ContextDocument[]
): ContextBundle {
    const files: ContextFile[] = [];
    const blockedFiles: ContextBlockedFile[] = [];
    const parts: string[] = [];
    let total = 0;
    let truncated = false;
    const customPatterns = parseSecretPatterns(settings.secretPatterns);

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
        const capped = capText(doc.content, Math.min(settings.maxFileBytes, remaining));
        const { text: redacted, redactedCount } = redactSecretContent(capped.text, customPatterns);
        const bytes = byteLen(redacted);
        total += bytes;
        truncated = truncated || capped.truncated;
        files.push({
            path: doc.path,
            language: doc.language,
            bytes,
            truncated: capped.truncated,
            source: doc.source,
            redactedCount: redactedCount > 0 ? redactedCount : undefined
        });
        parts.push(formatCodeFence(doc.path, doc.language, redacted));
    }

    const content = parts.join('\n\n');
    const totalRedacted = files.reduce((sum, f) => sum + (f.redactedCount ?? 0), 0);

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
        warning: contextWarning(blockedFiles, totalRedacted)
    };
}

function documentToContextDoc(doc: vscode.TextDocument, source: ContextSource): ContextDocument {
    return {
        path: docPath(doc),
        language: doc.languageId || languageFromPath(doc.fileName),
        source,
        content: doc.getText()
    };
}

export function emptyContext(mode: ContextMode): ContextBundle {
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
        case 'selection': return 'Selection context';
        case 'activeFile': return 'Active file context';
        case 'openEditors': return 'Open editor context';
        case 'workspace': return 'Workspace context';
        default: return 'Context';
    }
}

function contextWarning(blockedFiles: ContextBlockedFile[], totalRedacted: number): string {
    const parts: string[] = [];
    if (blockedFiles.length > 0) {
        parts.push(`${blockedFiles.length} file(s) blocked (sensitive path).`);
    }
    if (totalRedacted > 0) {
        parts.push(`${totalRedacted} secret(s) redacted inline.`);
    }
    return parts.join(' ');
}
