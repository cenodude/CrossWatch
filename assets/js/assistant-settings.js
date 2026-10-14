/* assets/js/assistant-settings.js */
/* CrossWatch - Assistant provider settings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  if (w.cwAssistantSettings) return;

  const API = "/api/assistant";
  const PRESETS = {
    openai: "https://api.openai.com/v1",
    gemini: "https://generativelanguage.googleapis.com/v1beta/openai",
    openrouter: "https://openrouter.ai/api/v1",
    ollama: "http://localhost:11434/v1",
    lmstudio: "http://localhost:1234/v1",
  };
  const LOCAL = new Set(["ollama", "lmstudio"]);
  const LOG_QUESTIONS = {
    sync: "Analyze the latest sync run in the sync log. Tell me what happened and whether anything went wrong.",
    watcher: "Analyze the recent watcher log. Is scrobbling working, and did anything go wrong?",
    debug: "Analyze the recent debug log and point out any errors or warnings that matter.",
  };
  const $ = (id) => d.getElementById(id);
  const txt = (v) => String(v ?? "").trim();
  const clean = (url) => txt(url).replace(/\/+$/, "");
  let current = null;
  let preset = "openai";

  function toast(message, ok = false) {
    try { w._cwShowToast?.(String(message || ""), !!ok); } catch {}
  }

  async function api(path, body) {
    const opts = { cache: "no-store", credentials: "same-origin" };
    if (body !== undefined) {
      opts.method = "POST";
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(API + path, opts);
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function presetFor(url) {
    const value = clean(url);
    if (!value) return "openai";
    const exact = Object.keys(PRESETS).find((key) => PRESETS[key] === value);
    if (exact) return exact;
    if (/:11434(\/|$)/.test(value)) return "ollama";
    if (/:1234(\/|$)/.test(value)) return "lmstudio";
    return "custom";
  }

  function keyKept() {
    return !!current?.has_key && clean($("assistant_base_url")?.value) === clean(current?.base_url);
  }

  function setStatus(text, tone = "") {
    const el = $("assistant_state");
    if (!el) return;
    el.textContent = text || "";
    el.dataset.tone = tone;
  }

  function hint() {
    if (LOCAL.has(preset)) return "Enter the address of your model, choose a model and save.";
    if (preset === "custom") return "Enter the address and key of your provider, choose a model and save.";
    return "Add your key, choose a model and save.";
  }

  function renderPreset() {
    for (const btn of d.querySelectorAll("[data-assistant-preset]")) {
      btn.setAttribute("aria-checked", btn.dataset.assistantPreset === preset ? "true" : "false");
    }
    $("assistant_url_field")?.classList.toggle("hidden", !(LOCAL.has(preset) || preset === "custom"));
    $("assistant_key_field")?.classList.toggle("hidden", LOCAL.has(preset));
    const key = $("assistant_api_key");
    if (key) key.placeholder = keyKept() ? "Saved. Leave empty to keep it" : "Paste your key";
  }

  const ASK_ON = "Ask CrossWatch";
  const ASK_OFF = "Ask CrossWatch: set up the assistant under Settings, UI and Security, Security first";

  function askButton() {
    let btn = $("cw-ask-button");
    if (btn) return btn;
    const account = $("cw-nav-profile-menu");
    if (!account) return null;
    btn = d.createElement("button");
    btn.id = "cw-ask-button";
    btn.type = "button";
    btn.disabled = true;
    btn.title = ASK_OFF;
    btn.setAttribute("aria-label", ASK_OFF);
    btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">auto_awesome</span>';
    ($("cw-notifications-button") || account).before(btn);
    return btn;
  }

  function showAnalyze(on) {
    d.documentElement.dataset.cwAssistant = on ? "on" : "off";
    const btn = askButton();
    if (!btn) return;
    btn.disabled = !on;
    btn.title = on ? ASK_ON : ASK_OFF;
    btn.setAttribute("aria-label", on ? ASK_ON : ASK_OFF);
  }

  function render(conf) {
    current = conf;
    showAnalyze(!!conf.ready);
    if (!$("assistant_base_url")) return;
    preset = presetFor(conf.base_url);
    $("assistant_base_url").value = conf.base_url || PRESETS[preset] || "";
    $("assistant_model").value = conf.model || "";
    $("assistant_api_key").value = "";
    renderPreset();
    const pill = $("assistant_pill");
    if (pill) {
      pill.textContent = conf.ready ? `On, ${conf.model}` : conf.base_url && conf.model ? "Turned off" : "Off";
      pill.dataset.state = conf.ready ? "on" : "off";
    }
    if (conf.ready) setStatus("");
    else if (conf.base_url && conf.model) setStatus("Set up, but turned off.");
    else setStatus(hint());
    $("btn-assistant-open").disabled = !conf.ready;
    $("btn-assistant-off").classList.toggle("hidden", !conf.enabled);
    $("btn-assistant-save").textContent = conf.enabled ? "Save" : "Save and turn on";
  }

  async function refresh() {
    if (!$("assistant_settings")) return;
    try {
      render(await api("/settings"));
    } catch (err) {
      setStatus(err?.message || "Could not load the assistant settings.", "error");
    }
  }

  function choose(next) {
    if (!next || next === preset) return;
    preset = next;
    const url = $("assistant_base_url");
    if (PRESETS[next] && presetFor(url.value) !== next) url.value = PRESETS[next];
    if (next === "custom" && Object.values(PRESETS).includes(clean(url.value))) url.value = "";
    renderPreset();
    if (!current?.ready && !(current?.base_url && current?.model)) setStatus(hint());
    (LOCAL.has(next) || next === "custom" ? url : $("assistant_api_key"))?.focus();
  }

  function fields() {
    const body = { base_url: clean($("assistant_base_url")?.value), model: txt($("assistant_model")?.value) };
    const key = LOCAL.has(preset) ? "" : txt($("assistant_api_key")?.value);
    if (key) body.api_key = key;
    return body;
  }

  async function save(enabled) {
    const body = { ...fields(), enabled };
    if (enabled && !body.base_url) return setStatus("Enter the address of your model first.", "error");
    if (enabled && !body.model) return setStatus("Choose a model first. Load models lists what your provider offers.", "error");
    try {
      render(await api("/settings", body));
      toast(enabled ? "Assistant is on" : "Assistant is off", true);
    } catch (err) {
      setStatus(err?.message || "Could not save", "error");
    }
  }

  async function loadModels() {
    const btn = $("btn-assistant-models");
    if (btn) btn.disabled = true;
    setStatus("Asking your provider which models it has...");
    try {
      const { base_url, api_key } = fields();
      const data = await api("/models", { base_url, api_key });
      const list = $("assistant_models");
      list.textContent = "";
      for (const name of data.models || []) {
        const option = d.createElement("option");
        option.value = name;
        list.appendChild(option);
      }
      const count = data.models?.length || 0;
      setStatus(count ? `${count} models found. Click the Model box to pick one.` : "Your provider listed no models. Type the model name instead.", count ? "good" : "");
      if (count) $("assistant_model")?.focus();
    } catch (err) {
      setStatus(err?.message || "Could not load the models", "error");
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  d.addEventListener("input", (event) => {
    if (event.target?.id === "assistant_base_url") renderPreset();
  });

  d.addEventListener("click", (event) => {
    const target = event.target;
    const choice = target?.closest?.("[data-assistant-preset]");
    if (choice) choose(choice.dataset.assistantPreset);
    if (target?.closest?.("#btn-assistant-save")) save(true);
    if (target?.closest?.("#btn-assistant-off")) save(false);
    if (target?.closest?.("#btn-assistant-models")) loadModels();
    if (target?.closest?.("#btn-assistant-open")) w.openAssistant?.();
    if (target?.closest?.("#cw-ask-button:not(:disabled)")) w.openAssistant?.();
    const analyze = target?.closest?.("[data-ai-analyze]");
    if (analyze) {
      const question = analyze.id === "det-ai" ? LOG_QUESTIONS[w._detailsTab] || LOG_QUESTIONS.sync : txt(analyze.dataset.aiQuestion);
      if (question) w.openAssistant?.({ question });
    }
  });

  w.cwAssistantSettings = { refresh, presets: PRESETS, presetFor };
  d.addEventListener("DOMContentLoaded", () => {
    askButton();
    refresh();
  });
  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (txt(event?.detail?.pane).toLowerCase() === "security") refresh();
  });
})(window, document);
