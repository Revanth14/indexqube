"use strict";

const USAGE_KEY = "iqUsage";
const DEFAULT_USAGE = {
  requestsTotal: 0,
  optimizedRequests: 0,
  errorsTotal: 0,
  blocksSeenTotal: 0,
  blocksPrunedTotal: 0,
  blocksSkippedTotal: 0,
  bytesBeforeTotal: 0,
  bytesAfterTotal: 0,
  bytesSavedTotal: 0,
  tokensBeforeTotal: 0,
  tokensAfterTotal: 0,
  tokensSavedTotal: 0,
  lastBytesBefore: 0,
  lastBytesAfter: 0,
  lastBytesSaved: 0,
  lastTokensBefore: 0,
  lastTokensAfter: 0,
  lastTokensSaved: 0,
  lastBlocksSeen: 0,
  lastBlocksPruned: 0,
  lastBlocksSkipped: 0,
  lastReductionRatio: 0,
  lastSkipReasons: "",
  lastOutcome: "idle",
  lastStatusCode: 0,
  lastError: "",
  lastUpdatedAt: ""
};

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || message.type !== "IQ_OPTIMIZE") {
    return false;
  }
  optimizeText(message.text, message.settings)
    .then((result) => sendResponse({ ok: true, result }))
    .catch((err) => sendResponse({ ok: false, error: err.message || "IndexQube optimize failed" }));
  return true;
});

async function optimizeText(text, settings) {
  const gatewayUrl = normalizeGatewayUrl(settings?.gatewayUrl);
  const headers = {
    "Accept": "application/json",
    "Content-Type": "text/plain; charset=utf-8",
    "X-IQ-Session-Key": settings?.sessionKey || ""
  };
  const projectMemory = cleanOptional(settings?.projectMemory);
  if (projectMemory) {
    headers["X-IQ-Project-Memory"] = projectMemory;
  }
  const contextPath = cleanOptional(settings?.contextPath);
  const contextLang = cleanOptional(settings?.contextLang);
  const legacyDefaultContext = contextPath === "browser-prompt.txt" && (!contextLang || contextLang === "txt");
  if (!legacyDefaultContext) {
    if (contextPath) {
      headers["X-IQ-Context-Path"] = contextPath;
    }
    if (contextLang) {
      headers["X-IQ-Context-Lang"] = contextLang;
    }
  }

  let response;
  try {
    response = await fetch(`${gatewayUrl}/v1/optimize`, {
      method: "POST",
      headers,
      body: text
    });
  } catch (err) {
    const detail = err && err.message ? err.message : String(err);
    const message = `Gateway unavailable at ${gatewayUrl}: ${detail}`;
    await safeRecordUsageError({ message });
    throw new Error(message);
  }

  const body = await response.text();
  if (!response.ok) {
    const message = body || `IndexQube returned ${response.status}`;
    await safeRecordUsageError({ message, statusCode: response.status });
    throw new Error(message);
  }
  const result = parseOptimizeResult(body, response);
  await safeRecordUsage(result);
  return result;
}

function cleanOptional(value) {
  return String(value || "").trim();
}

function normalizeGatewayUrl(value) {
  const raw = String(value || "http://localhost:8080").trim().replace(/\/+$/, "");

  try {
    const url = new URL(raw);
    if (!["http:", "https:"].includes(url.protocol)) {
      throw new Error("Gateway URL must start with http:// or https://");
    }
    return url.toString().replace(/\/+$/, "");
  } catch (_err) {
    throw new Error("Invalid gateway URL. Use a full URL like http://localhost:8080");
  }
}

function storageGet(defaults) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.get(defaults, (items) => {
      const error = chrome.runtime.lastError;
      if (error) {
        reject(new Error(error.message));
        return;
      }
      resolve(items);
    });
  });
}

function storageSet(values) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.set(values, () => {
      const error = chrome.runtime.lastError;
      if (error) {
        reject(new Error(error.message));
        return;
      }
      resolve();
    });
  });
}

async function safeRecordUsage(result) {
  try {
    await recordUsage(result);
  } catch (err) {
    console.warn("IndexQube usage recording failed:", err);
  }
}

async function safeRecordUsageError(detail = {}) {
  try {
    await recordUsageError(detail);
  } catch (err) {
    console.warn("IndexQube usage error recording failed:", err);
  }
}

async function recordUsage(result) {
  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  const usage = normalizeUsage(stored[USAGE_KEY]);
  const bytesSaved = Number(result.bytesSaved || 0);
  const tokensSaved = Number(result.tokensSaved || 0);
  usage.requestsTotal += 1;
  usage.optimizedRequests += result.blocksPruned > 0 ? 1 : 0;
  usage.blocksSeenTotal += result.blocksSeen;
  usage.blocksPrunedTotal += result.blocksPruned;
  usage.blocksSkippedTotal += result.blocksSkipped;
  usage.bytesBeforeTotal += result.bytesBefore;
  usage.bytesAfterTotal += result.bytesAfter;
  usage.bytesSavedTotal += bytesSaved;
  usage.tokensBeforeTotal += result.tokensBefore;
  usage.tokensAfterTotal += result.tokensAfter;
  usage.tokensSavedTotal += tokensSaved;
  usage.lastBytesBefore = result.bytesBefore;
  usage.lastBytesAfter = result.bytesAfter;
  usage.lastBytesSaved = bytesSaved;
  usage.lastTokensBefore = result.tokensBefore;
  usage.lastTokensAfter = result.tokensAfter;
  usage.lastTokensSaved = tokensSaved;
  usage.lastBlocksSeen = result.blocksSeen;
  usage.lastBlocksPruned = result.blocksPruned;
  usage.lastBlocksSkipped = result.blocksSkipped;
  usage.lastReductionRatio = result.ratio;
  usage.lastSkipReasons = result.skipReasons;
  usage.lastOutcome = classifyOutcome(result);
  usage.lastStatusCode = result.statusCode;
  usage.lastError = "";
  usage.lastUpdatedAt = new Date().toISOString();
  await storageSet({ [USAGE_KEY]: usage });
}

async function recordUsageError(detail = {}) {
  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  const usage = normalizeUsage(stored[USAGE_KEY]);
  usage.errorsTotal += 1;
  usage.lastOutcome = "error";
  usage.lastError = detail.message || "Gateway error";
  usage.lastStatusCode = detail.statusCode || 0;
  usage.lastUpdatedAt = new Date().toISOString();
  await storageSet({ [USAGE_KEY]: usage });
}

function normalizeUsage(raw) {
  const usage = Object.assign({}, DEFAULT_USAGE, raw || {});

  for (const key of Object.keys(DEFAULT_USAGE)) {
    if (typeof DEFAULT_USAGE[key] === "number") {
      usage[key] = numberValue(usage[key]);
    }
  }

  return usage;
}

function classifyOutcome(result) {
  switch (result.mode) {
    case "diff":
    case "unchanged":
      return result.mode;
    case "skipped":
    case "stateless":
    case "warmup":
      return result.mode;
    default:
      break;
  }
  if (result.blocksPruned > 0) {
    return "optimized";
  }
  if (result.blocksSkipped > 0) {
    return "skipped";
  }
  if (result.blocksSeen > 0) {
    return "checked";
  }
  return "no_code";
}

function parseOptimizeResult(body, response) {
  const headers = response.headers;
  let payload = null;
  try {
    payload = JSON.parse(body);
  } catch (_err) {
    payload = null;
  }
  if (!payload || typeof payload !== "object") {
    return resultFromHeaders(body, headers, response.status);
  }

  const stats = payload.stats || {};
  const text = String(payload.text !== undefined && payload.text !== null ? payload.text : messagesToText(payload.messages) || "");
  const bytesBefore = numberValue(stats.bytes_before, headers.get("X-IQ-Bytes-Before"));
  const bytesAfter = numberValue(stats.bytes_after, headers.get("X-IQ-Bytes-After"));
  const tokensBefore = numberValue(stats.estimated_tokens_before, headers.get("X-IQ-Tokens-Before"));
  const tokensAfter = numberValue(stats.estimated_tokens_after, headers.get("X-IQ-Tokens-After"));
  return {
    version: String(payload.version || headers.get("X-IQ-Contract-Version") || ""),
    mode: String(payload.mode || headers.get("X-IQ-Mode") || ""),
    text,
    statusCode: response.status,
    blocksSeen: numberValue(stats.blocks_seen, headers.get("X-IQ-Blocks-Seen")),
    blocksPruned: numberValue(stats.blocks_pruned, headers.get("X-IQ-Blocks-Pruned")),
    blocksSkipped: numberValue(stats.blocks_skipped, headers.get("X-IQ-Blocks-Skipped")),
    skipReasons: formatSkipReasonMap(stats.skip_reasons) || headers.get("X-IQ-Skip-Reasons") || "",
    bytesBefore,
    bytesAfter,
    bytesSaved: numberValue(payload.bytes_saved, stats.bytes_saved, headers.get("X-IQ-Bytes-Saved"), Math.max(0, bytesBefore - bytesAfter)),
    tokensBefore,
    tokensAfter,
    tokensSaved: numberValue(payload.estimated_tokens_saved, stats.estimated_tokens_saved, headers.get("X-IQ-Tokens-Saved"), Math.max(0, tokensBefore - tokensAfter)),
    ratio: numberValue(stats.reduction_ratio, headers.get("X-IQ-Reduction-Ratio"))
  };
}

function resultFromHeaders(body, headers, statusCode) {
  const bytesBefore = numberValue(headers.get("X-IQ-Bytes-Before"));
  const bytesAfter = numberValue(headers.get("X-IQ-Bytes-After"));
  const tokensBefore = numberValue(headers.get("X-IQ-Tokens-Before"));
  const tokensAfter = numberValue(headers.get("X-IQ-Tokens-After"));
  return {
    version: headers.get("X-IQ-Contract-Version") || "",
    mode: headers.get("X-IQ-Mode") || "",
    text: body,
    statusCode,
    blocksSeen: numberValue(headers.get("X-IQ-Blocks-Seen")),
    blocksPruned: numberValue(headers.get("X-IQ-Blocks-Pruned")),
    blocksSkipped: numberValue(headers.get("X-IQ-Blocks-Skipped")),
    skipReasons: headers.get("X-IQ-Skip-Reasons") || "",
    bytesBefore,
    bytesAfter,
    bytesSaved: numberValue(headers.get("X-IQ-Bytes-Saved"), Math.max(0, bytesBefore - bytesAfter)),
    tokensBefore,
    tokensAfter,
    tokensSaved: numberValue(headers.get("X-IQ-Tokens-Saved"), Math.max(0, tokensBefore - tokensAfter)),
    ratio: numberValue(headers.get("X-IQ-Reduction-Ratio"))
  };
}

function numberValue(...values) {
  for (const value of values) {
    if (value === undefined || value === null) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    const n = Number(value);
    if (Number.isFinite(n)) {
      return n;
    }
  }
  return 0;
}

function messagesToText(messages) {
  if (!Array.isArray(messages)) {
    return "";
  }
  return messages
    .map((message) => String(message?.content || "").trim())
    .filter(Boolean)
    .join("\n\n");
}

function formatSkipReasonMap(reasons) {
  if (!reasons || typeof reasons !== "object" || Array.isArray(reasons)) {
    return "";
  }
  return Object.keys(reasons)
    .filter((reason) => reason && Number(reasons[reason]) > 0)
    .sort()
    .map((reason) => `${reason}=${Number(reasons[reason])}`)
    .join(",");
}
