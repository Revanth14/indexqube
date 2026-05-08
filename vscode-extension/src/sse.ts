export interface SSEHandlers {
    onEvent: (event: string, data: string) => void;
    onText: (text: string) => void;
}

export async function readSSE(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    handlers: SSEHandlers
): Promise<void> {
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            // Flush any bytes still held in the decoder's internal state.
            const tail = decoder.decode();
            if (tail) {
                buffer += normalizeLineEndings(tail);
            }
            break;
        }
        buffer += normalizeLineEndings(decoder.decode(value, { stream: true }));
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

function normalizeLineEndings(text: string): string {
    return text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
}

function handleSSEFrame(
    frame: string,
    handlers: SSEHandlers
): boolean {
    let event = 'message';
    const dataParts: string[] = [];

    for (const line of frame.split('\n')) {
        if (line.startsWith('event:')) {
            event = line.slice('event:'.length).trim();
        } else if (line.startsWith('data:')) {
            dataParts.push(line.slice('data:'.length).replace(/^ /, ''));
        }
        // Ignore comments (':' lines) and id/retry fields.
    }

    const payload = dataParts.join('\n');
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
