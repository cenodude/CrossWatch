/* assets/js/assistant-settings.js */
/* CrossWatch - Assistant provider settings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
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
  const DEFAULT_PRESET = "gemini";
  const DEFAULT_MODELS = { gemini: "gemini-flash-lite-latest" };
  const KEY_NOTE = T("assistant.settings.keyNote");
  const GEMINI_KEY_NOTE = T("assistant.settings.geminiNote");
  const LOG_QUESTIONS = {
    sync: "Analyze the latest sync run in the sync log. Tell me what happened and whether anything went wrong.",
    watcher: "Analyze the recent watcher log. Is scrobbling working, and did anything go wrong?",
    debug: "Analyze the recent debug log and point out any errors or warnings that matter.",
  };
  const $ = (id) => d.getElementById(id);
  const txt = (v) => String(v ?? "").trim();
  const clean = (url) => txt(url).replace(/\/+$/, "");
  let current = null;
  let preset = DEFAULT_PRESET;
  let models = [];
  let revision = 0;

  function modelInfo(id) {
    const name = id.replace(/^models\//, "");
    const specialist = /(?:^|[-_/])(?:embedding\w*|imagen\w*|image|tts|whisper\w*|transcri\w*|audio|robotics|rerank\w*|moderation\w*|veo\w*|dall-e)(?:$|[-_/])/i.test(name);
    const preview = /(?:^|[-_/])(?:preview|experimental|exp)(?:$|[-_/])/i.test(name);
    const latest = /(?:^|[-_/:])latest(?:$|[-_/:])/i.test(name);
    const rank = preview ? 2 : latest ? 0 : 1;
    return {id, name, specialist, rank, label: [name, preview ? T("assistant.settings.preview") : "", specialist ? T("assistant.settings.specialist") : ""].filter(Boolean).join(" · ")};
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
    if ($("assistant_model_test")) $("assistant_model_test").textContent = T("assistant.settings.optionalCheck");
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
    if (!value) return DEFAULT_PRESET;
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
    if (LOCAL.has(preset)) return T("assistant.settings.local");
    if (preset === "custom") return T("assistant.settings.custom");
    if (DEFAULT_MODELS[preset]) return T("assistant.settings.paste");
    return T("assistant.settings.addKey");
  }

  function renderPreset() {
    for (const btn of d.querySelectorAll("[data-assistant-preset]")) {
      btn.setAttribute("aria-checked", btn.dataset.assistantPreset === preset ? "true" : "false");
    }
    $("assistant_url_field")?.classList.toggle("hidden", !(LOCAL.has(preset) || preset === "custom"));
    $("assistant_key_field")?.classList.toggle("hidden", LOCAL.has(preset));
    const key = $("assistant_api_key");
    if (key) key.placeholder = keyKept() ? T("assistant.settings.kept") : T("assistant.settings.pasteKey");
    const note = $("assistant_key_note");
    if (note) note.textContent = preset === "gemini" ? GEMINI_KEY_NOTE : KEY_NOTE;
  }

  const askLabel = key => T(`assistant.ask.${key}`);

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
    btn.title = askLabel("off");
    btn.setAttribute("aria-label", askLabel("off"));
    btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">auto_awesome</span>';
    ($("cw-notifications-button") || account).before(btn);
    return btn;
  }

  function showAnalyze(mode) {
    d.documentElement.dataset.cwAssistant = mode === "full" ? "on" : "off";
    const btn = askButton();
    if (!btn) return;
    const label = askLabel(mode === "full" ? "on" : mode === "wiki" ? "wiki" : "off");
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
    $("assistant_model").value = conf.model || DEFAULT_MODELS[preset] || "";
    $("assistant_api_key").value = "";
    renderPreset();
    const pill = $("assistant_pill");
    if (pill) {
      pill.textContent = conf.ready ? T("assistant.settings.onModel", {model: conf.model}) : conf.base_url && conf.model ? T("assistant.settings.turnedOff") : mode === "wiki" ? T("assistant.settings.wikiOnly") : T("assistant.settings.off");
      pill.dataset.state = conf.ready ? "on" : "off";
    }
    if (conf.ready) setStatus("");
    else if (conf.base_url && conf.model) setStatus(T("assistant.settings.setUpOff"));
    else setStatus(hint());
    $("btn-assistant-open").disabled = mode === "off";
    $("btn-assistant-off").classList.toggle("hidden", !conf.enabled);
    $("btn-assistant-save").textContent = conf.enabled ? T("assistant.settings.save") : T("assistant.settings.saveOn");
  }

  async function refresh() {
    if (!$("assistant_settings") && !$("cw-nav-profile-menu")) return;
    try { await w.__cwAuthBootstrapPromise; } catch {}
    try {
      render(await api("/settings"));
    } catch (err) {
      setStatus(err?.message || T("assistant.settings.loadFailed"), "error");
    }
  }

  function choose(next) {
    if (!next || next === preset) return;
    invalidateModels(true);
    preset = next;
    const url = $("assistant_base_url");
    if (PRESETS[next] && presetFor(url.value) !== next) url.value = PRESETS[next];
    if (next === "custom" && Object.values(PRESETS).includes(clean(url.value))) url.value = "";
    const model = $("assistant_model");
    const value = txt(model?.value);
    const savedPreset = current?.base_url ? presetFor(current.base_url) : "";
    const saved = savedPreset === next ? txt(current?.model) : "";
    const stale = !value || Object.values(DEFAULT_MODELS).includes(value) || (value === txt(current?.model) && savedPreset !== next);
    if (model && stale) model.value = saved || DEFAULT_MODELS[next] || "";
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
    if (enabled && !body.base_url) return setStatus(T("assistant.settings.addressFirst"), "error");
    if (enabled && !body.model) return setStatus(T("assistant.settings.modelFirst"), "error");
    try {
      render(await api("/settings", body));
      toast(enabled ? T("assistant.settings.isOn") : T("assistant.settings.isOff"), true);
    } catch (err) {
      setStatus(err?.message || T("assistant.settings.couldNotSave"), "error");
    }
  }

  async function saveWikiHelp(on) {
    try {
      render(await api("/settings", { wiki_help: on }));
      toast(on ? T("assistant.settings.wikiOn") : T("assistant.settings.wikiOff"), true);
    } catch (err) {
      setStatus(err?.message || T("assistant.settings.couldNotSave"), "error");
    }
  }

  async function loadModels() {
    const btn = $("btn-assistant-models");
    if (btn) btn.disabled = true;
    setStatus(T("assistant.settings.asking"));
    const started = revision;
    try {
      const { base_url, api_key } = fields();
      const data = await api("/models", { base_url, api_key });
      if (started !== revision) return;
      models = (data.models || []).filter(name => typeof name === "string");
      renderModels();
      setStatus(models.length ? T("assistant.settings.modelsLoaded") : T("assistant.settings.noModels"), models.length ? "good" : "");
      $("assistant_model")?.focus();
    } catch (err) {
      if (started === revision) setStatus(err?.message || T("assistant.settings.modelsFailed"), "error");
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function testModel() {
    const body = fields();
    const status = $("assistant_model_test");
    if (!body.model || !body.base_url) { status.textContent = T("assistant.settings.needBoth"); return; }
    const button = $("btn-assistant-test");
    if (button.disabled) return;
    button.disabled = true;
    const started = revision;
    status.textContent = T("assistant.settings.testing");
    try {
      const data = await api("/test-model", body);
      if (started !== revision) return;
      status.textContent = data.compatible ? T("assistant.settings.passed")
        : !data.tool_call ? T("assistant.settings.noTool")
        : T("assistant.settings.noText");
    } catch (err) {
      if (started === revision) status.textContent = T("assistant.settings.verifyFailed", {error: err?.message || T("assistant.settings.requestFailed")});
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
