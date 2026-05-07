"use strict";

const DEFAULT_SETTINGS = {
  enabled: true,
  gatewayUrl: "http://localhost:8080",
  sessionKey: "",
  sessionKeys: {},
  pendingSessionKey: "",
  pendingSessionHost: "",
  pendingSessionCreatedAt: 0,
  projectMemory: "",
  contextPath: "",
  contextLang: ""
};

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

const fields = {
  enabled: document.getElementById("enabled"),
  gatewayUrl: document.getElementById("gatewayUrl"),
  gatewaySummary: document.getElementById("gatewaySummary"),
  checkGateway: document.getElementById("checkGateway"),
  sessionKey: document.getElementById("sessionKey"),
  sessionSummary: document.getElementById("sessionSummary"),
  resetSession: document.getElementById("resetSession"),
  resetAllSessions: document.getElementById("resetAllSessions"),
  resetUsage: document.getElementById("resetUsage"),
  projectMemory: document.getElementById("projectMemory"),
  contextPath: document.getElementById("contextPath"),
  contextLang: document.getElementById("contextLang"),
  save: document.getElementById("save"),
  status: document.getElementById("status"),
  usageRequests: document.getElementById("usageRequests"),
  usageOptimized: document.getElementById("usageOptimized"),
  usageTokensSaved: document.getElementById("usageTokensSaved"),
  usageBytesSaved: document.getElementById("usageBytesSaved"),
  usageBlocksPruned: document.getElementById("usageBlocksPruned"),
  usageErrors: document.getElementById("usageErrors"),
  usageLast: document.getElementById("usageLast"),
  lastState: document.getElementById("lastState"),
  lastBlocksSeen: document.getElementById("lastBlocksSeen"),
  lastBlocksPruned: document.getElementById("lastBlocksPruned"),
  lastBlocksSkipped: document.getElementById("lastBlocksSkipped"),
  lastTokens: document.getElementById("lastTokens"),
  lastSkipReasons: document.getElementById("lastSkipReasons")
};

function storageGet(defaults) {
  return new Promise((resolve) => chrome.storage.local.get(defaults, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

function createSessionKey() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

function normalizeGatewayURL(value) {
  return String(value || DEFAULT_SETTINGS.gatewayUrl).trim().replace(/\/+$/, "") || DEFAULT_SETTINGS.gatewayUrl;
}

async function checkGatewayHealth() {
  const gatewayUrl = normalizeGatewayURL(fields.gatewayUrl.value);
  renderGatewayState("checking", "Checking gateway...");
  const startedAt = performance.now();
  try {
    const response = await fetch(`${gatewayUrl}/healthz`, { method: "GET", cache: "no-store" });
    const elapsed = Math.max(1, Math.round(performance.now() - startedAt));
    if (!response.ok) {
      renderGatewayState("offline", `Gateway ${response.status} at ${gatewayUrl}`);
      return;
    }
    renderGatewayState("online", `Online at ${gatewayUrl} (${elapsed} ms)`);
  } catch (_err) {
    renderGatewayState("offline", `Offline at ${gatewayUrl}`);
  }
}

function renderGatewayState(state, text) {
  fields.gatewaySummary.dataset.state = state;
  fields.gatewaySummary.textContent = text;
}

async function load() {
  const settings = await storageGet(DEFAULT_SETTINGS);
  if (settings.contextPath === "browser-prompt.txt" && (!settings.contextLang || settings.contextLang === "txt")) {
    settings.contextPath = "";
    settings.contextLang = "";
    await storageSet({ contextPath: "", contextLang: "" });
  }
  const sessionKey = settings.sessionKey || createSessionKey();
  if (!settings.sessionKey) {
    await storageSet({ sessionKey });
  }

  fields.enabled.checked = Boolean(settings.enabled);
  fields.gatewayUrl.value = settings.gatewayUrl || DEFAULT_SETTINGS.gatewayUrl;
  fields.sessionKey.value = sessionKey;
  fields.projectMemory.value = settings.projectMemory || "";
  fields.contextPath.value = settings.contextPath || "";
  fields.contextLang.value = settings.contextLang || "";
  renderSessionSummary(sessionKey, settings.sessionKeys);

  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  renderUsage(stored[USAGE_KEY]);
  void checkGatewayHealth();
}

async function save() {
  const sessionKey = fields.sessionKey.value.trim() || createSessionKey();
  await storageSet({
    enabled: fields.enabled.checked,
    gatewayUrl: fields.gatewayUrl.value.trim() || DEFAULT_SETTINGS.gatewayUrl,
    sessionKey,
    projectMemory: fields.projectMemory.value,
    contextPath: fields.contextPath.value.trim(),
    contextLang: fields.contextLang.value.trim()
  });
  fields.sessionKey.value = sessionKey;
  const stored = await storageGet({ sessionKeys: {} });
  renderSessionSummary(sessionKey, stored.sessionKeys || {});
  showStatus("Saved");
  void checkGatewayHealth();
}

function renderSessionSummary(sessionKey, sessionKeys) {
  const scopeCount = Object.keys(sessionKeys || {}).length;
  const short = sessionKey ? shortKey(sessionKey) : "none";
  fields.sessionSummary.textContent = scopeCount > 0
    ? `Global: ${short} · ${scopeCount} conversation scope${scopeCount === 1 ? "" : "s"}`
    : `Global: ${short}`;
}

function shortKey(key) {
  return key.length <= 12 ? key : `${key.slice(0, 4)}...${key.slice(-4)}`;
}

function showStatus(text) {
  fields.status.textContent = text;
  window.clearTimeout(showStatus.timer);
  showStatus.timer = window.setTimeout(() => {
    fields.status.textContent = "";
  }, 1800);
}

function renderUsage(raw) {
  const usage = Object.assign({}, DEFAULT_USAGE, raw || {});
  fields.usageRequests.textContent = formatNumber(usage.requestsTotal);
  fields.usageOptimized.textContent = formatNumber(usage.optimizedRequests);
  fields.usageTokensSaved.textContent = formatNumber(usage.tokensSavedTotal);
  fields.usageBytesSaved.textContent = formatBytes(usage.bytesSavedTotal);
  fields.usageBlocksPruned.textContent = formatNumber(usage.blocksPrunedTotal);
  fields.usageErrors.textContent = formatNumber(usage.errorsTotal);
  fields.lastBlocksSeen.textContent = formatNumber(usage.lastBlocksSeen);
  fields.lastBlocksPruned.textContent = formatNumber(usage.lastBlocksPruned);
  fields.lastBlocksSkipped.textContent = formatNumber(usage.lastBlocksSkipped);
  fields.lastTokens.textContent = `${formatNumber(usage.lastTokensBefore)} -> ${formatNumber(usage.lastTokensAfter)}`;
  renderLastState(usage);

  if (!usage.lastUpdatedAt) {
    fields.usageLast.textContent = "No optimized requests yet";
    fields.lastSkipReasons.textContent = "";
    return;
  }
  const savedPct = Math.round(usage.lastReductionRatio * 100);
  const gateway = usage.lastStatusCode ? `Gateway ${usage.lastStatusCode}` : "Gateway unavailable";
  if (usage.lastOutcome === "error") {
    fields.usageLast.textContent = `${gateway}: ${usage.lastError || "request failed"}`;
  } else {
    fields.usageLast.textContent = `${stateLabel(usage.lastOutcome, "")} · saved ${formatNumber(usage.lastTokensSaved)} estimated tokens / ${formatBytes(usage.lastBytesSaved)} · ${savedPct}% · ${gateway}`;
  }
  fields.lastSkipReasons.textContent = usage.lastSkipReasons
    ? `Skipped: ${formatSkipReasons(usage.lastSkipReasons)}`
    : "";
}

function renderLastState(usage) {
  const label = stateLabel(usage.lastOutcome, usage.lastSkipReasons);
  fields.lastState.textContent = label;
  fields.lastState.dataset.state = usage.lastOutcome || "idle";
}

function stateLabel(outcome, skipReasons) {
  switch (outcome) {
    case "diff":       return "Diff";
    case "unchanged":  return "Unchanged";
    case "warmup":     return "Warmup";
    case "stateless":  return "Stateless";
    case "optimized":  return "Optimized";
    case "skipped":    return `Skipped${skipReasons ? ": " + firstSkipReason(skipReasons) : ""}`;
    case "checked":    return "Checked";
    case "no_code":    return "No code";
    case "error":      return "Gateway error";
    default:           return "Idle";
  }
}

function firstSkipReason(skipReasons) {
  return String(skipReasons).split(",")[0]?.split("=")[0] || "unknown";
}

function formatSkipReasons(skipReasons) {
  return String(skipReasons)
    .split(",")
    .filter(Boolean)
    .map((part) => {
      const [reason, count] = part.split("=");
      return count ? `${reason} (${count})` : reason;
    })
    .join(", ");
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes < 1000) return `${bytes} B`;
  if (bytes < 1000 * 1000) return `${(bytes / 1000).toFixed(1)} KB`;
  return `${(bytes / (1000 * 1000)).toFixed(1)} MB`;
}

// Event listeners

fields.save.addEventListener("click", () => void save());
fields.checkGateway.addEventListener("click", () => void checkGatewayHealth());
fields.gatewayUrl.addEventListener("change", () => void checkGatewayHealth());
fields.enabled.addEventListener("change", () => void save());

fields.resetSession.addEventListener("click", async () => {
  const sessionKey = createSessionKey();
  fields.sessionKey.value = sessionKey;
  await save();
  showStatus("Session reset");
});

fields.resetAllSessions.addEventListener("click", async () => {
  const sessionKey = createSessionKey();
  await storageSet({
    sessionKey,
    sessionKeys: {},
    pendingSessionKey: "",
    pendingSessionHost: "",
    pendingSessionCreatedAt: 0,
    currentSessionScope: "global",
    currentSessionLabel: "Global fallback",
    currentSessionScoped: false,
    currentSessionKey: sessionKey
  });
  fields.sessionKey.value = sessionKey;
  renderSessionSummary(sessionKey, {});
  showStatus("Local sessions forgotten");
});

fields.resetUsage.addEventListener("click", async () => {
  const usage = Object.assign({}, DEFAULT_USAGE);
  await storageSet({ [USAGE_KEY]: usage });
  renderUsage(usage);
  showStatus("Usage reset");
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") return;
  if (changes[USAGE_KEY]) {
    renderUsage(changes[USAGE_KEY].newValue);
  }
});

void load();
