"use strict";

const DEFAULT_SETTINGS = {
  enabled: true,
  gatewayUrl: "http://localhost:8080",
  sessionKey: "",
  sessionKeys: {},
  currentSessionScope: "",
  currentSessionLabel: "",
  currentSessionScoped: false,
  currentSessionKey: "",
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
  projectMemory: document.getElementById("projectMemory"),
  contextPath: document.getElementById("contextPath"),
  contextLang: document.getElementById("contextLang"),
  save: document.getElementById("save"),
  resetSession: document.getElementById("resetSession"),
  resetBoth: document.getElementById("resetBoth"),
  sessionSummary: document.getElementById("sessionSummary"),
  resetUsage: document.getElementById("resetUsage"),
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
  lastSkipReasons: document.getElementById("lastSkipReasons"),
  copyReceipt: document.getElementById("copyReceipt"),
  status: document.getElementById("status")
};

let lastRenderedUsage = null;

let activeSessionScope = globalSessionScope();

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

async function getActiveSessionScope(settings) {
  const tab = await queryActiveTab();
  if (tab?.url) {
    return deriveSessionScope(tab.url);
  }
  if (settings.currentSessionScope) {
    const key = String(settings.currentSessionScope);
    return {
      key,
      label: settings.currentSessionLabel || key,
      scoped: Boolean(settings.currentSessionScoped),
      pending: key.endsWith("/pending"),
      host: key.endsWith("/pending") ? key.slice(0, -"/pending".length) : "global"
    };
  }
  return globalSessionScope();
}

function queryActiveTab() {
  return new Promise((resolve) => {
    if (!chrome.tabs?.query) {
      resolve(null);
      return;
    }
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
      const err = chrome.runtime.lastError;
      if (err) {
        resolve(null);
        return;
      }
      resolve(tabs?.[0] || null);
    });
  });
}

async function ensureSessionForScope(scope, settings) {
  if (scope.pending) {
    const sessionKey = reusablePendingSession(settings, scope.host) || createSessionKey();
    await storageSet({
      pendingSessionKey: sessionKey,
      pendingSessionHost: scope.host,
      pendingSessionCreatedAt: settings.pendingSessionKey === sessionKey
        ? settings.pendingSessionCreatedAt
        : Date.now(),
      ...currentSessionValues(scope, sessionKey)
    });
    return sessionKey;
  }

  if (!scope.scoped) {
    const sessionKey = settings.sessionKey || createSessionKey();
    if (!settings.sessionKey) {
      await storageSet({ sessionKey });
    }
    await storageSet(currentSessionValues(scope, sessionKey));
    return sessionKey;
  }
  const sessionKeys = normalizeSessionKeys(settings.sessionKeys);
  if (!sessionKeys[scope.key]) {
    const pendingKey = reusablePendingSession(settings, scope.host);
    sessionKeys[scope.key] = pendingKey || createSessionKey();
    const values = { sessionKeys };
    if (pendingKey || settings.pendingSessionHost === scope.host) {
      values.pendingSessionKey = "";
      values.pendingSessionHost = "";
      values.pendingSessionCreatedAt = 0;
    }
    await storageSet(values);
  }
  await storageSet(currentSessionValues(scope, sessionKeys[scope.key]));
  return sessionKeys[scope.key];
}

function currentSessionValues(scope, sessionKey) {
  return {
    currentSessionScope: scope.key,
    currentSessionLabel: scope.label,
    currentSessionScoped: scope.scoped,
    currentSessionKey: sessionKey
  };
}

function normalizeSessionKeys(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function reusablePendingSession(settings, host) {
  const createdAt = Number(settings.pendingSessionCreatedAt || 0);
  const ageMs = Date.now() - createdAt;
  if (
    settings.pendingSessionKey &&
    settings.pendingSessionHost === host &&
    ageMs >= 0 &&
    ageMs < 10 * 60 * 1000
  ) {
    return settings.pendingSessionKey;
  }
  return "";
}

function deriveSessionScope(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch (_err) {
    return globalSessionScope();
  }
  const host = url.hostname.replace(/^www\./, "");
  const parts = url.pathname.split("/").filter(Boolean);
  if (host === "chatgpt.com" || host === "chat.openai.com") {
    const id = segmentAfter(parts, "c");
    if (id) {
      return {
        key: `chatgpt.com/c/${id}`,
        label: `ChatGPT ${shortScopeID(id)}`,
        scoped: true,
        pending: false,
        host
      };
    }
    return pendingSessionScope(host, "New ChatGPT chat");
  }
  if (host === "claude.ai") {
    const id = segmentAfter(parts, "chat");
    if (id) {
      return {
        key: `claude.ai/chat/${id}`,
        label: `Claude ${shortScopeID(id)}`,
        scoped: true,
        pending: false,
        host
      };
    }
    return pendingSessionScope(host, "New Claude chat");
  }
  return globalSessionScope(host);
}

function segmentAfter(parts, marker) {
  const idx = parts.indexOf(marker);
  if (idx < 0 || idx + 1 >= parts.length) {
    return "";
  }
  return parts[idx + 1] || "";
}

function shortScopeID(id) {
  return id.length <= 10 ? id : `${id.slice(0, 4)}...${id.slice(-4)}`;
}

function pendingSessionScope(host, label) {
  return { key: `${host}/pending`, label, scoped: false, pending: true, host };
}

function globalSessionScope(host = "global") {
  return { key: "global", label: "Global fallback", scoped: false, pending: false, host };
}

async function load() {
  const settings = await storageGet(DEFAULT_SETTINGS);
  if (!settings.sessionKey) {
    settings.sessionKey = createSessionKey();
    await storageSet({ sessionKey: settings.sessionKey });
  }
  if (settings.contextPath === "browser-prompt.txt" && (!settings.contextLang || settings.contextLang === "txt")) {
    settings.contextPath = "";
    settings.contextLang = "";
    await storageSet({ contextPath: "", contextLang: "" });
  }
  activeSessionScope = await getActiveSessionScope(settings);
  const sessionKey = await ensureSessionForScope(activeSessionScope, settings);
  fields.enabled.checked = Boolean(settings.enabled);
  fields.gatewayUrl.value = settings.gatewayUrl || DEFAULT_SETTINGS.gatewayUrl;
  fields.sessionKey.value = sessionKey;
  fields.projectMemory.value = settings.projectMemory || "";
  fields.contextPath.value = settings.contextPath || "";
  fields.contextLang.value = settings.contextLang || "";
  renderSessionSummary(sessionKey);

  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  renderUsage(stored[USAGE_KEY]);
  void checkGatewayHealth();
}

async function save() {
  const sessionKey = fields.sessionKey.value.trim() || createSessionKey();
  const values = {
    enabled: fields.enabled.checked,
    gatewayUrl: fields.gatewayUrl.value.trim() || DEFAULT_SETTINGS.gatewayUrl,
    projectMemory: fields.projectMemory.value,
    contextPath: fields.contextPath.value.trim(),
    contextLang: fields.contextLang.value.trim()
  };
  if (activeSessionScope.scoped) {
    const stored = await storageGet({ sessionKeys: {} });
    const sessionKeys = normalizeSessionKeys(stored.sessionKeys);
    sessionKeys[activeSessionScope.key] = sessionKey;
    values.sessionKeys = sessionKeys;
  } else if (activeSessionScope.pending) {
    values.pendingSessionKey = sessionKey;
    values.pendingSessionHost = activeSessionScope.host;
    values.pendingSessionCreatedAt = Date.now();
  } else {
    values.sessionKey = sessionKey;
  }
  values.currentSessionScope = activeSessionScope.key;
  values.currentSessionLabel = activeSessionScope.label;
  values.currentSessionScoped = activeSessionScope.scoped;
  values.currentSessionKey = sessionKey;
  await storageSet(values);
  fields.sessionKey.value = sessionKey;
  renderSessionSummary(sessionKey);
  showStatus("Saved");
  void checkGatewayHealth();
}

function showStatus(text) {
  fields.status.textContent = text;
  window.clearTimeout(showStatus.timer);
  showStatus.timer = window.setTimeout(() => {
    fields.status.textContent = "";
  }, 1600);
}

function renderUsage(raw) {
  const usage = normalizeUsage(raw);
  lastRenderedUsage = usage;
  fields.copyReceipt.disabled = !usage.lastUpdatedAt || usage.lastOutcome === "idle";
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

async function checkGatewayHealth() {
  const gatewayUrl = normalizeGatewayURL(fields.gatewayUrl.value || DEFAULT_SETTINGS.gatewayUrl);
  renderGatewayState("checking", "Checking gateway...");
  const startedAt = performance.now();
  try {
    const response = await fetch(`${gatewayUrl}/healthz`, {
      method: "GET",
      cache: "no-store"
    });
    const elapsed = Math.max(1, Math.round(performance.now() - startedAt));
    if (!response.ok) {
      renderGatewayState("offline", `Gateway ${response.status} at ${gatewayUrl}`);
      return;
    }
    renderGatewayState("online", `Online at ${gatewayUrl} (${elapsed} ms)`);
  } catch (err) {
    renderGatewayState("offline", `Offline at ${gatewayUrl}`);
  }
}

function renderGatewayState(state, text) {
  fields.gatewaySummary.dataset.state = state;
  fields.gatewaySummary.textContent = text;
}

function normalizeGatewayURL(value) {
  return String(value || DEFAULT_SETTINGS.gatewayUrl).trim().replace(/\/+$/, "") || DEFAULT_SETTINGS.gatewayUrl;
}

function normalizeUsage(raw) {
  return Object.assign({}, DEFAULT_USAGE, raw || {});
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes < 1000) {
    return `${bytes} B`;
  }
  if (bytes < 1000 * 1000) {
    return `${(bytes / 1000).toFixed(1)} KB`;
  }
  return `${(bytes / (1000 * 1000)).toFixed(1)} MB`;
}

function renderSessionSummary(sessionKey) {
  if (!sessionKey) {
    fields.sessionSummary.textContent = "Session inactive";
    return;
  }
  fields.sessionSummary.textContent = `${activeSessionScope.label} · ${shortSessionKey(sessionKey)}`;
}

function shortSessionKey(sessionKey) {
  if (sessionKey.length <= 12) {
    return sessionKey;
  }
  return `${sessionKey.slice(0, 4)}...${sessionKey.slice(-4)}`;
}

function renderLastState(usage) {
  const label = stateLabel(usage.lastOutcome, usage.lastSkipReasons);
  fields.lastState.textContent = label;
  fields.lastState.dataset.state = usage.lastOutcome || "idle";
}

function stateLabel(outcome, skipReasons) {
  switch (outcome) {
    case "diff":
      return "Diff";
    case "unchanged":
      return "Unchanged";
    case "warmup":
      return "Warmup";
    case "stateless":
      return "Stateless";
    case "optimized":
      return "Optimized";
    case "skipped":
      return `Skipped${skipReasons ? ": " + firstSkipReason(skipReasons) : ""}`;
    case "checked":
      return "Checked";
    case "no_code":
      return "No code";
    case "error":
      return "Gateway error";
    default:
      return "Idle";
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

function freshUsage() {
  return Object.assign({}, DEFAULT_USAGE);
}

fields.save.addEventListener("click", () => void save());
fields.checkGateway.addEventListener("click", () => void checkGatewayHealth());
fields.gatewayUrl.addEventListener("change", () => void checkGatewayHealth());
fields.resetSession.addEventListener("click", () => {
  fields.sessionKey.value = createSessionKey();
  void save().then(() => showStatus("Session reset"));
});
fields.enabled.addEventListener("change", () => void save());
fields.resetUsage.addEventListener("click", async () => {
  const usage = freshUsage();
  await storageSet({ [USAGE_KEY]: usage });
  renderUsage(usage);
  showStatus("Usage reset");
});
fields.resetBoth.addEventListener("click", async () => {
  const sessionKey = createSessionKey();
  const usage = freshUsage();
  const values = {
    [USAGE_KEY]: usage,
    ...currentSessionValues(activeSessionScope, sessionKey)
  };
  if (activeSessionScope.scoped) {
    values.sessionKeys = { [activeSessionScope.key]: sessionKey };
    values.pendingSessionKey = "";
    values.pendingSessionHost = "";
    values.pendingSessionCreatedAt = 0;
  } else if (activeSessionScope.pending) {
    values.pendingSessionKey = sessionKey;
    values.pendingSessionHost = activeSessionScope.host;
    values.pendingSessionCreatedAt = Date.now();
  } else {
    values.sessionKeys = {};
    values.sessionKey = sessionKey;
    values.pendingSessionKey = "";
    values.pendingSessionHost = "";
    values.pendingSessionCreatedAt = 0;
  }
  fields.sessionKey.value = sessionKey;
  await storageSet(values);
  renderSessionSummary(sessionKey);
  renderUsage(usage);
  showStatus("Session and usage reset");
});

function buildReceipt(usage) {
  const mode = usage.lastOutcome || "unknown";
  const tokensSaved = Number(usage.lastTokensSaved || 0);
  const bytesBefore = Number(usage.lastBytesBefore || 0);
  const bytesAfter = Number(usage.lastBytesAfter || 0);
  const seen = Number(usage.lastBlocksSeen || 0);
  const pruned = Number(usage.lastBlocksPruned || 0);
  const skipped = Number(usage.lastBlocksSkipped || 0);
  return [
    `IndexQube saved ${new Intl.NumberFormat().format(tokensSaved)} estimated input tokens.`,
    `Mode: ${mode}`,
    `Bytes: ${new Intl.NumberFormat().format(bytesBefore)} -> ${new Intl.NumberFormat().format(bytesAfter)}`,
    `Blocks: ${seen} seen, ${pruned} pruned, ${skipped} skipped`,
  ].join("\n");
}

fields.copyReceipt.addEventListener("click", async () => {
  if (!lastRenderedUsage || !lastRenderedUsage.lastUpdatedAt) return;
  const text = buildReceipt(lastRenderedUsage);
  try {
    await navigator.clipboard.writeText(text);
    showStatus("Copied");
  } catch (_err) {
    showStatus("Copy failed");
  }
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") {
    return;
  }
  if (changes[USAGE_KEY]) {
    renderUsage(changes[USAGE_KEY].newValue);
  }
  if (changes.sessionKey) {
    if (!activeSessionScope.scoped && !activeSessionScope.pending) {
      fields.sessionKey.value = changes.sessionKey.newValue || "";
      renderSessionSummary(fields.sessionKey.value);
    }
  }
  if (changes.sessionKeys && activeSessionScope.scoped) {
    const sessionKeys = normalizeSessionKeys(changes.sessionKeys.newValue);
    fields.sessionKey.value = sessionKeys[activeSessionScope.key] || "";
    renderSessionSummary(fields.sessionKey.value);
  }
  if (changes.pendingSessionKey && activeSessionScope.pending) {
    fields.sessionKey.value = changes.pendingSessionKey.newValue || "";
    renderSessionSummary(fields.sessionKey.value);
  }
});

void load();
