"use strict";

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

  const response = await fetch(`${gatewayUrl}/v1/optimize`, {
    method: "POST",
    headers,
    body: text
  }).catch((err) => {
    throw new Error(`Gateway unavailable at ${gatewayUrl}: ${err.message}`);
  });

  const body = await response.text();
  if (!response.ok) {
    throw new Error(body || `IndexQube returned ${response.status}`);
  }
  return {
    text: body,
    blocksPruned: Number(response.headers.get("X-IQ-Blocks-Pruned") || "0"),
    bytesBefore: Number(response.headers.get("X-IQ-Bytes-Before") || "0"),
    bytesAfter: Number(response.headers.get("X-IQ-Bytes-After") || "0"),
    ratio: Number(response.headers.get("X-IQ-Reduction-Ratio") || "0")
  };
}

function cleanOptional(value) {
  return String(value || "").trim();
}
