(function () {
  "use strict";

  const DEFAULT_SETTINGS = {
    enabled: true,
    gatewayUrl: "http://localhost:8080",
    sessionKey: "",
    projectMemory: "",
    contextPath: "",
    contextLang: ""
  };

  let submitBypass = false;
  let optimizeInFlight = false;

  function storageGet(defaults) {
    return new Promise((resolve) => {
      chrome.storage.local.get(defaults, resolve);
    });
  }

  function storageSet(values) {
    return new Promise((resolve) => {
      chrome.storage.local.set(values, resolve);
    });
  }

  async function getSettings() {
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
    settings.gatewayUrl = String(settings.gatewayUrl || DEFAULT_SETTINGS.gatewayUrl).replace(/\/+$/, "");
    return settings;
  }

  function createSessionKey() {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  }

  function ensureStatus() {
    let el = document.querySelector(".iq-status");
    if (el) {
      return el;
    }
    el = document.createElement("div");
    el.className = "iq-status";
    el.setAttribute("role", "status");
    el.setAttribute("aria-live", "polite");
    document.documentElement.appendChild(el);
    return el;
  }

  function showStatus(kind, title, message, ttl = 2600) {
    const el = ensureStatus();
    el.dataset.visible = "true";
    el.dataset.kind = kind;
    el.innerHTML = "";
    const strong = document.createElement("strong");
    strong.textContent = title;
    const body = document.createElement("div");
    body.textContent = message;
    el.append(strong, body);
    window.clearTimeout(showStatus.timer);
    showStatus.timer = window.setTimeout(() => {
      el.dataset.visible = "false";
    }, ttl);
  }

  function hideStatus() {
    window.clearTimeout(showStatus.timer);
    const el = document.querySelector(".iq-status");
    if (el) {
      el.dataset.visible = "false";
    }
  }

  function isEditable(el) {
    return el instanceof HTMLTextAreaElement ||
      el instanceof HTMLInputElement ||
      Boolean(el && el.isContentEditable);
  }

  function findComposer(start) {
    if (isEditable(start)) {
      return start;
    }
    const active = document.activeElement;
    if (isEditable(active)) {
      return active;
    }
    const selectors = [
      "textarea",
      "[contenteditable='true']",
      "[contenteditable=true]",
      ".ProseMirror",
      "[role='textbox']"
    ];
    const candidates = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
    return candidates
      .filter(isEditable)
      .filter((el) => !isHidden(el) && !isDisabled(el))
      .sort((a, b) => area(b) - area(a))[0] || null;
  }

  function isHidden(el) {
    const rect = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    return rect.width === 0 || rect.height === 0 || style.visibility === "hidden" || style.display === "none";
  }

  function isDisabled(el) {
    return Boolean(el.disabled || el.getAttribute("aria-disabled") === "true");
  }

  function area(el) {
    const rect = el.getBoundingClientRect();
    return rect.width * rect.height;
  }

  function getComposerText(el) {
    if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) {
      return el.value;
    }
    return el.innerText || el.textContent || "";
  }

  function setComposerText(el, text) {
    if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) {
      const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
      const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
      if (setter) {
        setter.call(el, text);
      } else {
        el.value = text;
      }
      el.dispatchEvent(new Event("input", { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      return;
    }
    el.focus();
    document.execCommand("selectAll", false, null);
    document.execCommand("insertText", false, text);
    el.dispatchEvent(new InputEvent("input", {
      bubbles: true,
      inputType: "insertText",
      data: text
    }));
  }

  function findSendButton(from) {
    const scope = from?.closest("form") || document;
    const buttons = Array.from(scope.querySelectorAll("button")).filter((button) => !isHidden(button) && !isDisabled(button));
    return buttons.find(isLikelySendButton) || null;
  }

  function isLikelySendButton(button) {
    const text = [
      button.getAttribute("aria-label"),
      button.getAttribute("title"),
      button.getAttribute("data-testid"),
      button.textContent
    ].filter(Boolean).join(" ").toLowerCase();
    return text.includes("send") ||
      text.includes("submit") ||
      text.includes("composer-submit") ||
      text.includes("arrow-up");
  }

  function shouldOptimizePrompt(text, settings) {
    if (hasExplicitOptimizationSettings(settings)) {
      return true;
    }
    return containsFence(text) || looksLikeCodeText(text);
  }

  function hasExplicitOptimizationSettings(settings) {
    return Boolean(
      cleanOptional(settings.projectMemory) ||
      cleanOptional(settings.contextPath) ||
      cleanOptional(settings.contextLang)
    );
  }

  function cleanOptional(value) {
    return String(value || "").trim();
  }

  function containsFence(text) {
    return text.includes("```");
  }

  function looksLikeCodeText(text) {
    const s = text.trim();
    if (!s) {
      return false;
    }
    if (looksLikeGo(s)) {
      return true;
    }
    const signals = ["function ", "const ", "let ", "var ", "class ", "import ", "export ", "return ", "{", "}", "=>", ":=", "def ", "SELECT ", "select "];
    const hits = signals.reduce((count, signal) => count + (s.includes(signal) ? 1 : 0), 0);
    return hits >= 2 || (s.includes("\n") && hits >= 1);
  }

  function looksLikeGo(text) {
    return text.includes("func ") ||
      text.includes("package ") ||
      text.includes(" := ") ||
      text.includes("interface {") ||
      text.includes("struct {");
  }

  async function optimizeText(text, settings) {
    return sendRuntimeMessage({ type: "IQ_OPTIMIZE", text, settings }).then((reply) => {
      if (!reply || !reply.ok) {
        throw new Error(reply?.error || "IndexQube optimize failed");
      }
      return reply.result;
    });
  }

  function sendRuntimeMessage(message) {
    return new Promise((resolve, reject) => {
      chrome.runtime.sendMessage(message, (reply) => {
        const err = chrome.runtime.lastError;
        if (err) {
          reject(new Error(err.message));
          return;
        }
        resolve(reply);
      });
    });
  }

  async function optimizeComposer(composer) {
    if (optimizeInFlight) {
      return false;
    }
    const settings = await getSettings();
    if (!settings.enabled) {
      return true;
    }
    const original = getComposerText(composer);
    if (!original.trim()) {
      return true;
    }
    if (!shouldOptimizePrompt(original, settings)) {
      return true;
    }

    optimizeInFlight = true;
    showStatus("info", "IndexQube", "Optimizing prompt...");
    try {
      const result = await optimizeText(original, settings);
      if (result.text !== original) {
        setComposerText(composer, result.text);
      }
      if (result.blocksPruned > 0) {
        const savedPct = Math.round(result.ratio * 100);
        showStatus("success", "IndexQube", `Reduced ${savedPct}% (${result.bytesBefore} -> ${result.bytesAfter} bytes).`);
      } else {
        hideStatus();
      }
      return true;
    } catch (err) {
      console.warn("[IndexQube] optimize failed", err);
      showStatus("error", "IndexQube", err.message || "Optimize failed; sending original prompt.", 4200);
      return true;
    } finally {
      optimizeInFlight = false;
    }
  }

  async function interceptAndSubmit(event, submitter) {
    if (submitBypass) {
      return;
    }
    const composer = findComposer(submitter || event.target);
    if (!composer) {
      return;
    }
    event.preventDefault();
    event.stopImmediatePropagation();

    const shouldContinue = await optimizeComposer(composer);
    if (!shouldContinue) {
      return;
    }
    submitBypass = true;
    try {
      if (submitter && typeof submitter.click === "function") {
        submitter.click();
      } else {
        const button = findSendButton(composer);
        if (button) {
          button.click();
        } else {
          const form = composer.closest("form");
          if (form && typeof form.requestSubmit === "function") {
            form.requestSubmit();
          }
        }
      }
    } finally {
      window.setTimeout(() => {
        submitBypass = false;
      }, 0);
    }
  }

  document.addEventListener("click", (event) => {
    const button = event.target?.closest?.("button");
    if (!button || !isLikelySendButton(button)) {
      return;
    }
    void interceptAndSubmit(event, button);
  }, true);

  document.addEventListener("keydown", (event) => {
    if (event.defaultPrevented || event.shiftKey || event.altKey || event.ctrlKey || event.metaKey) {
      return;
    }
    if (event.key !== "Enter") {
      return;
    }
    const composer = findComposer(event.target);
    if (!composer || composer instanceof HTMLTextAreaElement && composer.rows > 1) {
      return;
    }
    void interceptAndSubmit(event, null);
  }, true);
})();
