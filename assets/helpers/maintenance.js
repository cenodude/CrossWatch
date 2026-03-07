/* assets/helpers/maintenance.js */
/* Refactored and expanded maintenance helper with toolbar messages and action handling */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  function toolbarMessage(text, delay = 1800){
    const m = document.getElementById("tb_msg");
    if (!m) return;
    m.classList.remove("hidden");
    m.textContent = text;
    clearTimeout(toolbarMessage._t);
    toolbarMessage._t = setTimeout(() => m.classList.add("hidden"), delay);
  }

  async function runAction({ url, method = "POST", body, successText, failText, delay = 1800, onSuccess }){
    try {
      const init = { method, cache: "no-store" };
      if (body !== undefined) {
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify(body);
      }
      const r = await fetch(url, init);
      const j = await r.json().catch(() => ({}));
      const ok = !!(r.ok && j?.ok !== false);
      const error = j?.error ? ` (${j.error})` : "";
      toolbarMessage(ok ? successText : `${failText}${error}`, delay);
      if (ok) await onSuccess?.(j);
      return ok;
    } catch {
      toolbarMessage(`${failText} (network)`, delay);
      return false;
    }
  }

  async function clearState(){
    return runAction({
      url: "/api/maintenance/reset-state",
      body: { mode: "clear_both" },
      successText: "Clear State – started ✓",
      failText: "Clear State – failed"
    });
  }

  async function clearCache(){
    return runAction({
      url: "/api/maintenance/clear-cache",
      successText: "Clear Cache – done ✓",
      failText: "Clear Cache – failed"
    });
  }

  async function resetStats(){
    return runAction({
      url: "/api/maintenance/reset-stats",
      successText: "Reset Statistics – done ✓",
      failText: "Reset Statistics – failed",
      delay: 2200,
      onSuccess: async () => { try { await window.refreshStats?.(true); } catch {} }
    });
  }

  async function resetCurrentlyPlaying(){
    return runAction({
      url: "/api/maintenance/reset-currently-watching",
      successText: "Reset Currently Playing – done ✓",
      failText: "Reset Currently Playing – failed",
      delay: 2200
    });
  }

  async function restartCrossWatch(){
    if (typeof window.cwRestartCrossWatchWithOverlay === "function") return window.cwRestartCrossWatchWithOverlay();
    try { await fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }); } catch {}
    try { window.location.reload(); } catch {}
  }

  Object.assign(window, { clearState, clearCache, resetStats, resetCurrentlyPlaying, restartCrossWatch });
  (window.CW ||= {});
  window.CW.Maintenance = { runAction, clearState, clearCache, resetStats, resetCurrentlyPlaying, restartCrossWatch };
})();
