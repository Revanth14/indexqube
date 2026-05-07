"use strict";

const DEFAULT_SETTINGS = {
  enabled: true,
  gatewayUrl: "http://localhost:8080",
  sessionKey: "",
  projectMemory: "",
  contextPath: "",
  contextLang: ""
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

fields.save.addEventListener("click", () => void save());
fields.resetSession.addEventListener("click", () => {
  fields.sessionKey.value = createSessionKey();
  void save();
});
fields.enabled.addEventListener("change", () => void save());

void load();
