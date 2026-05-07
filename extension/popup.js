"use strict";

const DEFAULT_SETTINGS = {
  enabled: true,
  gatewayUrl: "http://localhost:8080",
  sessionKey: "",
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
  lastTokensBefore: 0,
  lastTokensAfter: 0,
  lastReductionRatio: 0,
  lastUpdatedAt: ""
};

const fields = {
  enabled: document.getElementById("enabled"),
  gatewayUrl: document.getElementById("gatewayUrl"),
  sessionKey: document.getElementById("sessionKey"),
  projectMemory: document.getElementById("projectMemory"),
  contextPath: document.getElementById("contextPath"),
  contextLang: document.getElementById("contextLang"),
  save: document.getElementById("save"),
  resetSession: document.getElementById("resetSession"),
  resetUsage: document.getElementById("resetUsage"),
  usageRequests: document.getElementById("usageRequests"),
  usageOptimized: document.getElementById("usageOptimized"),
  usageTokensSaved: document.getElementById("usageTokensSaved"),
  usageBytesSaved: document.getElementById("usageBytesSaved"),
  usageBlocksPruned: document.getElementById("usageBlocksPruned"),
  usageErrors: document.getElementById("usageErrors"),
  usageLast: document.getElementById("usageLast"),
  status: document.getElementById("status")
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
  fields.enabled.checked = Boolean(settings.enabled);
  fields.gatewayUrl.value = settings.gatewayUrl || DEFAULT_SETTINGS.gatewayUrl;
  fields.sessionKey.value = settings.sessionKey;
  fields.projectMemory.value = settings.projectMemory || "";
  fields.contextPath.value = settings.contextPath || "";
  fields.contextLang.value = settings.contextLang || "";

  const stored = await storageGet({ [USAGE_KEY]: DEFAULT_USAGE });
  renderUsage(stored[USAGE_KEY]);
}

async function save() {
  await storageSet({
    enabled: fields.enabled.checked,
    gatewayUrl: fields.gatewayUrl.value.trim() || DEFAULT_SETTINGS.gatewayUrl,
    sessionKey: fields.sessionKey.value.trim() || createSessionKey(),
    projectMemory: fields.projectMemory.value,
    contextPath: fields.contextPath.value.trim(),
    contextLang: fields.contextLang.value.trim()
  });
  showStatus("Saved");
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
  fields.usageRequests.textContent = formatNumber(usage.requestsTotal);
  fields.usageOptimized.textContent = formatNumber(usage.optimizedRequests);
  fields.usageTokensSaved.textContent = formatNumber(usage.tokensSavedTotal);
  fields.usageBytesSaved.textContent = formatBytes(usage.bytesSavedTotal);
  fields.usageBlocksPruned.textContent = formatNumber(usage.blocksPrunedTotal);
  fields.usageErrors.textContent = formatNumber(usage.errorsTotal);

  if (!usage.lastUpdatedAt) {
    fields.usageLast.textContent = "No optimized requests yet";
    return;
  }
  const savedPct = Math.round(usage.lastReductionRatio * 100);
  fields.usageLast.textContent = `Last: ${formatNumber(usage.lastTokensBefore)} -> ${formatNumber(usage.lastTokensAfter)} estimated tokens, ${savedPct}% reduction`;
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

fields.save.addEventListener("click", () => void save());
fields.resetSession.addEventListener("click", () => {
  fields.sessionKey.value = createSessionKey();
  void save();
});
fields.enabled.addEventListener("change", () => void save());
fields.resetUsage.addEventListener("click", async () => {
  await storageSet({ [USAGE_KEY]: DEFAULT_USAGE });
  renderUsage(DEFAULT_USAGE);
  showStatus("Usage reset");
});

void load();
