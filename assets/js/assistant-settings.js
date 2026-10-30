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
  let models = [];
  let revision = 0;

  function modelInfo(id) {
    const name = id.replace(/^models\//, "");
    const specialist = /(?:^|[-_/])(?:embedding\w*|imagen\w*|image|tts|whisper\w*|transcri\w*|audio|robotics|rerank\w*|moderation\w*|veo\w*|dall-e)(?:$|[-_/])/i.test(name);
    const preview = /(?:^|[-_/])(?:preview|experimental|exp)(?:$|[-_/])/i.test(name);
    const latest = /(?:^|[-_/:])latest(?:$|[-_/:])/i.test(name);
    const rank = preview ? 2 : latest ? 0 : 1;
    return {id, name, specialist, rank, label: [name, preview ? "Preview" : "", specialist ? "Likely specialist" : ""].filter(Boolean).join(" · ")};
  }

  function renderModels() {
    const list = $("assistant_models");
    if (!list) return;
    const all = $("assistant_models_all")?.value === "all";
    const shown = models.map(modelInfo).filter(m => all || !m.specialist)
      .sort((a, b) => a.rank - b.rank || a.name.localeCompare(b.name, undefined, {numeric: true, sensitivity: "base"}));
    list.textContent = "";
    for (const model of shown) {
      const option = d.createElement("option");
      option.value = model.id;
      option.textContent = model.label;
      list.appendChild(option);
    }
  }

  function invalidateModels(clear = false) {
    revision++;
    if ($("assistant_model_test")) $("assistant_model_test").textContent = "Optional check. Uses two short requests; provider charges may apply.";
    if (clear) {
      models = [];
      renderModels();
    }
  }

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
  const ASK_WIKI = "Ask CrossWatch (wiki only)";
  const ASK_OFF = "Ask CrossWatch: set up the assistant under Settings, UI and Security, Security first";

  function modeOf(conf) {
    if (conf?.mode === "full" || conf?.mode === "wiki" || conf?.mode === "off") return conf.mode;
    return conf?.ready ? "full" : "off";
  }

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

  function showAnalyze(mode) {
    d.documentElement.dataset.cwAssistant = mode === "full" ? "on" : "off";
    const btn = askButton();
    if (!btn) return;
    const label = mode === "full" ? ASK_ON : mode === "wiki" ? ASK_WIKI : ASK_OFF;
    btn.disabled = mode === "off";
    btn.title = label;
    btn.setAttribute("aria-label", label);
  }

  function render(conf) {
    invalidateModels(true);
    current = conf;
    const mode = modeOf(conf);
    showAnalyze(mode);
    if (!$("assistant_base_url")) return;
    const wiki = $("assistant_wiki_help");
    if (wiki) {
      wiki.value = conf.wiki_help === false ? "off" : "on";
      try { w.CW?.IconSelect?.enhance?.(wiki, wiki.__cwIconSelectCfg || { className: "cw-plain-select" }); } catch {}
    }
    preset = presetFor(conf.base_url);
    $("assistant_base_url").value = conf.base_url || PRESETS[preset] || "";
    $("assistant_model").value = conf.model || "";
    $("assistant_api_key").value = "";
    renderPreset();
    const pill = $("assistant_pill");
    if (pill) {
      pill.textContent = conf.ready ? `On, ${conf.model}` : conf.base_url && conf.model ? "Turned off" : mode === "wiki" ? "Wiki only" : "Off";
      pill.dataset.state = conf.ready ? "on" : "off";
    }
    if (conf.ready) setStatus("");
    else if (conf.base_url && conf.model) setStatus("Set up, but turned off.");
    else setStatus(hint());
    $("btn-assistant-open").disabled = mode === "off";
    $("btn-assistant-off").classList.toggle("hidden", !conf.enabled);
    $("btn-assistant-save").textContent = conf.enabled ? "Save" : "Save and turn on";
  }

  async function refresh() {
    if (!$("assistant_settings")) return;
    try { await w.__cwAuthBootstrapPromise; } catch {}
    try {
      render(await api("/settings"));
    } catch (err) {
      setStatus(err?.message || "Could not load the assistant settings.", "error");
    }
  }

  function choose(next) {
    if (!next || next === preset) return;
    invalidateModels(true);
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

  async function saveWikiHelp(on) {
    try {
      render(await api("/settings", { wiki_help: on }));
      toast(on ? "Wiki answers are on" : "Wiki answers are off", true);
    } catch (err) {
      setStatus(err?.message || "Could not save", "error");
    }
  }

  async function loadModels() {
    const btn = $("btn-assistant-models");
    if (btn) btn.disabled = true;
    setStatus("Asking your provider which models it has...");
    const started = revision;
    try {
      const { base_url, api_key } = fields();
      const data = await api("/models", { base_url, api_key });
      if (started !== revision) return;
      models = (data.models || []).filter(name => typeof name === "string");
      renderModels();
      setStatus(models.length ? "Models loaded. Type in the model field to search." : "Your provider listed no models. Type the model ID instead.", models.length ? "good" : "");
      $("assistant_model")?.focus();
    } catch (err) {
      if (started === revision) setStatus(err?.message || "Could not load the models", "error");
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function testModel() {
    const body = fields();
    const status = $("assistant_model_test");
    if (!body.model || !body.base_url) { status.textContent = "Enter a provider address and choose a model first."; return; }
    const button = $("btn-assistant-test");
    if (button.disabled) return;
    button.disabled = true;
    const started = revision;
    status.textContent = "Testing a tool call and text reply…";
    try {
      const data = await api("/test-model", body);
      if (started !== revision) return;
      status.textContent = data.compatible ? "Passed: this model can use assistant tools."
        : !data.tool_call ? "The model did not make the required tool call. Try another model."
        : "The tool call worked, but the model did not return a text answer. Try another model.";
    } catch (err) {
      if (started === revision) status.textContent = `Could not verify compatibility: ${err?.message || "Request failed"}`;
    } finally { button.disabled = false; }
  }

  d.addEventListener("input", (event) => {
    if (event.target?.id === "assistant_base_url") renderPreset();
    if (["assistant_base_url", "assistant_api_key", "assistant_model"].includes(event.target?.id)) invalidateModels(event.target.id !== "assistant_model");
  });

  d.addEventListener("change", (event) => {
    if (event.target?.id === "assistant_models_all") renderModels();
    if (event.target?.id === "assistant_wiki_help") saveWikiHelp(event.target.value !== "off");

  });

  d.addEventListener("click", (event) => {
    const target = event.target;
    const choice = target?.closest?.("[data-assistant-preset]");
    if (choice) choose(choice.dataset.assistantPreset);
    if (target?.closest?.("#btn-assistant-save")) save(true);
    if (target?.closest?.("#btn-assistant-off")) save(false);
    if (target?.closest?.("#btn-assistant-models")) loadModels();
    if (target?.closest?.("#btn-assistant-test")) testModel();
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
