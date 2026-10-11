/* assets/js/analyzer/movie-matching.js */
/* CrossWatch - Matching review for a movie that does not match between two providers */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

export function describeCandidate(candidate) {
  const ids = Object.entries(candidate.ids || {}).map(([name, value]) => `${name.toUpperCase()} ${value}`).join(" · ");
  return [[candidate.title, candidate.year].filter(Boolean).join(" · "), ids].filter(Boolean);
}

export async function openMovieMatching(trigger, {reference, title = "", onSaved, onManual} = {}) {
  const dialog = document.createElement("dialog");
  dialog.className = "eg-dialog cw-page-panel";
  dialog.setAttribute("aria-labelledby", "movie-matching-title");
  dialog.innerHTML = `<div class="eg-head"><div><h2 id="movie-matching-title">Fix matching</h2><p>${esc(title)}</p></div><button type="button" class="eg-close" data-close aria-label="Close matching review"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div>
    <div class="eg-body"><p class="eg-status" role="status">Looking this film up…</p><p class="eg-error" role="alert"></p><div data-review></div></div>
    <footer class="eg-footer"><div><button type="button" data-manual>Search manually</button><button type="button" class="eg-primary" data-save disabled>Use this match</button></div></footer>`;
  document.body.append(dialog);
  const $ = selector => dialog.querySelector(selector);
  const controller = new AbortController();
  let version = "", result = null, busy = false, outcome = "";
  const request = async (action, extra = {}) => {
    const response = await fetch("/api/analyzer/mapping", {method:"POST", credentials:"same-origin", cache:"no-store",
      signal:controller.signal, headers:{"Content-Type":"application/json"}, body:JSON.stringify({...reference, version, action, ...extra})});
    const data = await response.json();
    if (!response.ok || data.ok === false) throw new Error(typeof data.detail === "string" ? data.detail : "The matching review could not be completed.");
    return data;
  };
  const render = () => {
    const found = result.candidate ? `<section class="eg-saved"><div class="eg-saved-head"><h3>Proposed match on ${esc(result.destination)}</h3></div><article class="eg-saved-group"><div class="eg-saved-heading"><strong>${esc(describeCandidate(result.candidate)[0])}</strong></div><div class="eg-saved-mapping">${esc(describeCandidate(result.candidate)[1] || "")}</div></article></section>` : "";
    const several = result.candidates.length ? `<ul>${result.candidates.map(item => `<li>${esc(describeCandidate(item).join(" — "))}</li>`).join("")}</ul>` : "";
    $("[data-review]").innerHTML = `<p class="eg-hint">Checked ${esc(result.checked.join(" · "))}</p><p class="eg-empty">${esc(result.message)}</p>${found}${several}`;
    $("[data-save]").disabled = busy || !result.candidate;
  };
  $("[data-save]").onclick = async () => {
    if (busy || !result?.candidate) return;
    busy = true; render(); $(".eg-error").textContent = "";
    try {
      await request("save", {item:{type:"movie", title:result.candidate.title, year:result.candidate.year, ids:result.candidate.ids}});
      outcome = "saved"; busy = false; dialog.close();
    } catch (error) {
      busy = false;
      if (!controller.signal.aborted) { $(".eg-error").textContent = error.message; render(); }
    }
  };
  $("[data-manual]").onclick = () => { if (!busy) { outcome = "manual"; dialog.close(); } };
  $("[data-close]").onclick = () => { if (!busy) dialog.close(); };
  dialog.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
  dialog.addEventListener("close", () => {
    controller.abort(); dialog.remove(); trigger?.focus();
    if (outcome === "saved") onSaved?.();
    if (outcome === "manual") onManual?.();
  }, {once:true});
  dialog.showModal();
  try {
    version = (await request("context")).version;
    result = await request("diagnose");
    $(".eg-status").textContent = "";
    render();
  } catch (error) {
    if (!controller.signal.aborted) { $(".eg-status").textContent = ""; $(".eg-error").textContent = error.message; }
  }
}
