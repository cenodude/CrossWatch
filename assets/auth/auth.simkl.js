// auth.simkl.js — SIMKL auth only
(function (w, d) {
  // --- tiny utils
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;
  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function"
      ? w.computeRedirectURI()
      : (location.origin + "/callback"));

  // ---------- success banner
  function setSimklSuccess(on) { $("simkl_msg")?.classList.toggle("hidden", !on); }

  // ---------- form state
  function updateSimklButtonState() {
    try {
      const cid = ($("simkl_client_id")?.value || "").trim();
      const sec = ($("simkl_client_secret")?.value || "").trim();
      const btn = $("simkl_start_btn"); const hint = $("simkl_hint"); const rid = $("redirect_uri_preview");
      if (rid) rid.textContent = computeRedirect();
      const ok = cid.length > 0 && sec.length > 0;
      if (btn) btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) { console.warn("updateSimklButtonState failed", e); }
  }
  const updateSimklHint = updateSimklButtonState;

  async function copyRedirect() { try { await navigator.clipboard.writeText(computeRedirect()); notify("Redirect URI copied ✓"); } catch {} }

  // ---------- OAuth start + poll for token
  let simklPoll = null;
  async function startSimkl() {
    try { setSimklSuccess(false); } catch {}
    try { await w.saveSettings?.(); } catch {}
    const origin = location.origin;
    const j = await fetch("/api/simkl/authorize", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ origin }), cache: "no-store"
    }).then(r => r.json()).catch(() => null);
    if (!j?.ok || !j.authorize_url) return;
    w.open(j.authorize_url, "_blank");
    if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }
    const deadline = Date.now() + 120000;
    const back = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    const poll = async () => {
      if (Date.now() >= deadline) { simklPoll = null; return; }
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }
      let cfg = null; try { cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json()); } catch {}
      const tok = (cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token || "").trim();
      if (tok) { try { const el = $("simkl_access_token"); if (el) el.value = tok; } catch {} try { setSimklSuccess(true); } catch {} simklPoll = null; return; }
      simklPoll = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };
    simklPoll = setTimeout(poll, 1000);
  }

  // ---------- lifecycle
  d.addEventListener("DOMContentLoaded", () => {
    $("simkl_client_id")?.addEventListener("input", updateSimklButtonState);
    $("simkl_client_secret")?.addEventListener("input", updateSimklButtonState);
    const rid = $("redirect_uri_preview"); if (rid) rid.textContent = computeRedirect();
    updateSimklButtonState();
  });

  // ---------- exports
  Object.assign(w, {
    setSimklSuccess, updateSimklButtonState, updateSimklHint,
    startSimkl, copyRedirect
  });
})(window, document);
