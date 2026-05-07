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
  const gatewayUrl = String(settings?.gatewayUrl || "http://localhost:8080").replace(/\/+$/, "");
  const headers = {
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
    const message = `Gateway unavailable at ${gatewayUrl}: ${err.message}`;
    await recordUsageError({ message });
    throw new Error(message);
  }

  const body = await response.text();
  if (!response.ok) {
    await recordUsageError({ message: body || `IndexQube returned ${response.status}`, statusCode: response.status });
    throw new Error(body || `IndexQube returned ${response.status}`);
  }
  const result = {
    text: body,
    statusCode: response.status,
    blocksSeen: Number(response.headers.get("X-IQ-Blocks-Seen") || "0"),
    blocksPruned: Number(response.headers.get("X-IQ-Blocks-Pruned") || "0"),
    blocksSkipped: Number(response.headers.get("X-IQ-Blocks-Skipped") || "0"),
    skipReasons: response.headers.get("X-IQ-Skip-Reasons") || "",
    bytesBefore: Number(response.headers.get("X-IQ-Bytes-Before") || "0"),
    bytesAfter: Number(response.headers.get("X-IQ-Bytes-After") || "0"),
    tokensBefore: Number(response.headers.get("X-IQ-Tokens-Before") || "0"),
    tokensAfter: Number(response.headers.get("X-IQ-Tokens-After") || "0"),
    ratio: Number(response.headers.get("X-IQ-Reduction-Ratio") || "0")
  };
  await recordUsage(result);
  return result;
}

function cleanOptional(value) {
  return String(value || "").trim();
}

function storageGet(defaults) {
  return new Promise((resolve) => chrome.storage.local.get(defaults, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

async function recordUsage(result) {
  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  const usage = normalizeUsage(stored[USAGE_KEY]);
  const bytesSaved = Math.max(0, result.bytesBefore - result.bytesAfter);
  const tokensSaved = Math.max(0, result.tokensBefore - result.tokensAfter);
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
  return Object.assign({}, DEFAULT_USAGE, raw || {});
}

function classifyOutcome(result) {
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
