// assets/auth/auth.crosswatch.js
// CrossWatch - Local Tracker authentication UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function (w, d) {
  const Shared = w.CW && w.CW.AuthShared;
  if (!Shared) return;

  const $ = (id) => d.getElementById(id);
  const q = (sel, root = d) => root.querySelector(sel);
  const txt = (v) => String(v ?? "").trim();
  const truth = (v, fallback = true) => {
    if (v == null || v === "") return !!fallback;
    if (typeof v === "boolean") return v;
    return ["1", "true", "yes", "on", "enabled"].includes(txt(v).toLowerCase());
  };
  const intOr = (v, fallback) => {
    const n = parseInt(txt(v), 10);
    return Number.isFinite(n) ? Math.max(0, n) : fallback;
  };

  const profile = Shared.createProfileAdapter({
    provider: "crosswatch",
    configKey: "crosswatch",
    label: "CrossWatch",
    sectionId: "sec-crosswatch",
    selectId: "crosswatch_instance",
    storageKey: "cw.ui.crosswatch.auth.instance.v1",
    instanceProvider: "crosswatch",
    title: "Select which CrossWatch Local Tracker profile this config applies to.",
  });

  function selectedInstance() {
    return profile?.getInstance?.() || "default";
  }

  function baseRoot(cfg) {
    return txt(cfg?.crosswatch?.root_dir || cfg?.CrossWatch?.root_dir || "/config/.cw_provider") || "/config/.cw_provider";
  }

  function derivedRoot(cfg, inst) {
    if (!inst || inst === "default") return baseRoot(cfg);
    return `${baseRoot(cfg).replace(/[\\/]+$/, "")}/profiles/${inst}`;
  }

  function selectedBlock(cfg) {
    const root = cfg?.crosswatch && typeof cfg.crosswatch === "object" ? cfg.crosswatch : {};
    const inst = selectedInstance();
    if (inst === "default") return { base: root, block: root, merged: { ...root } };
    const insts = root.instances && typeof root.instances === "object" ? root.instances : {};
    const block = insts[inst] && typeof insts[inst] === "object" ? insts[inst] : {};
    return { base: root, block, merged: { ...root, instances: undefined, ...block } };
  }

  function setVal(id, value) {
    const el = $(id);
    if (!el) return;
    el.value = value == null ? "" : String(value);
    try { el.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
    el.dataset.loaded = "1";
    el.dataset.touched = "";
  }

  function setSelect(id, value) {
    const el = $(id);
    if (!el) return;
    el.value = String(value);
    try { w.CW?.IconSelect?.refresh?.(el); } catch {}
    el.dataset.loaded = "1";
    el.dataset.touched = "";
  }

  function snapshotLabel(name) {
    try {
      if (typeof w.formatCwSnapshotLabel === "function") return w.formatCwSnapshotLabel(name);
    } catch {}
    return name || "";
  }

  async function loadSnapshotOptions(cfg) {
    const root = derivedRoot(cfg || {}, selectedInstance());
    const wanted = {
      watchlist: txt($("cw_tracker_restore_watchlist")?.dataset?.wanted) || "latest",
      history: txt($("cw_tracker_restore_history")?.dataset?.wanted) || "latest",
      ratings: txt($("cw_tracker_restore_ratings")?.dataset?.wanted) || "latest",
      progress: txt($("cw_tracker_restore_progress")?.dataset?.wanted) || "latest",
    };
    const groups = { watchlist: [], history: [], ratings: [], progress: [] };
    try {
      const res = await fetch(`/api/files?path=${encodeURIComponent(`${root.replace(/[\\/]+$/, "")}/snapshots`)}`, { cache: "no-store" });
      const files = res.ok ? await res.json().catch(() => []) : [];
      (Array.isArray(files) ? files : []).forEach((f) => {
        const name = txt(f?.name);
        if (!name.endsWith(".json")) return;
        Object.keys(groups).forEach((feature) => {
          if (name.endsWith(`-${feature}.json`)) groups[feature].push(name);
        });
      });
    } catch {}
    Object.keys(groups).forEach((feature) => groups[feature].sort());
    const ids = {
      watchlist: "cw_tracker_restore_watchlist",
      history: "cw_tracker_restore_history",
      ratings: "cw_tracker_restore_ratings",
      progress: "cw_tracker_restore_progress",
    };
    Object.entries(ids).forEach(([feature, id]) => {
      const sel = $(id);
      if (!sel) return;
      sel.innerHTML = "";
      const latest = d.createElement("option");
      latest.value = "latest";
      latest.textContent = "Latest (default)";
      sel.appendChild(latest);
      groups[feature].forEach((name) => {
        const opt = d.createElement("option");
        opt.value = name;
        opt.textContent = snapshotLabel(name);
        sel.appendChild(opt);
      });
      sel.value = groups[feature].includes(wanted[feature]) ? wanted[feature] : "latest";
      sel.dataset.loaded = "1";
      sel.dataset.touched = "";
    });
  }

  function setAuthStatus(text = "Connected", ok = true) {
    const msg = $("cw_tracker_auth_msg");
    if (!msg) return;
    msg.textContent = text;
    msg.classList.toggle("hidden", !text);
    msg.classList.toggle("warn", !ok);
    msg.classList.toggle("ok", ok);
    msg.removeAttribute("aria-hidden");
  }

  async function hydrate() {
    const cfg = await fetch(`/api/config?ts=${Date.now()}`, { cache: "no-store" }).then((r) => r.json()).catch(() => ({}));
    const inst = selectedInstance();
    const { block, merged } = selectedBlock(cfg || {});
    setVal("cw_tracker_label", txt(block.label).slice(0, 12));
    setVal("cw_tracker_retention_days", intOr(merged.retention_days, 30));
    setSelect("cw_tracker_auto_snapshot", truth(merged.auto_snapshot, true) ? "true" : "false");
    setVal("cw_tracker_max_snapshots", intOr(merged.max_snapshots, 64));
    ["watchlist", "history", "ratings", "progress"].forEach((feature) => {
      const sel = $(`cw_tracker_restore_${feature}`);
      if (sel) sel.dataset.wanted = txt(merged[`restore_${feature}`]) || "latest";
    });
    if ((merged || {}).connected === true) setAuthStatus("Connected", true);
    else setAuthStatus("", true);
    await loadSnapshotOptions(cfg || {});
    try { w.CW?.ProvidersUI?.refreshAuthPresentation?.(false); } catch {}
  }

  function wireFields() {
    [
      "cw_tracker_retention_days", "cw_tracker_auto_snapshot", "cw_tracker_max_snapshots", "cw_tracker_label",
      "cw_tracker_restore_watchlist", "cw_tracker_restore_history", "cw_tracker_restore_ratings", "cw_tracker_restore_progress"
    ].forEach((id) => {
      const el = $(id);
      if (!el || el.__cwTrackerWired) return;
      el.__cwTrackerWired = true;
      el.addEventListener("input", () => {
        el.dataset.touched = "1";
        if (id === "cw_tracker_label" && el.value.length > 12) el.value = el.value.slice(0, 12);
      });
      el.addEventListener("change", () => { el.dataset.touched = "1"; });
    });
    const connect = $("cw_crosswatch_connect");
    if (connect && !connect.__cwTrackerWired) {
      connect.__cwTrackerWired = true;
      connect.addEventListener("click", async () => {
        const inst = selectedInstance();
        connect.disabled = true;
        connect.setAttribute("aria-busy", "true");
        try {
          const res = await fetch(`/api/crosswatch/connect?instance=${encodeURIComponent(inst)}`, { method: "POST", cache: "no-store" });
          const data = await res.json().catch(() => ({}));
          if (!res.ok || data?.ok === false) {
            setAuthStatus("Connection failed", false);
            Shared.showConnectionWarning?.(data?.error || "Could not connect CrossWatch Local Tracker.");
            return;
          }
          setAuthStatus("Connected", true);
          try { w.invalidateConfigCache?.(); } catch {}
          try { d.dispatchEvent(new CustomEvent("config-saved", { bubbles: true })); } catch {}
          try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
          await hydrate().catch(() => {});
        } catch {
          setAuthStatus("Connection failed", false);
          Shared.showConnectionWarning?.("Could not connect CrossWatch Local Tracker.");
        } finally {
          connect.disabled = false;
          connect.removeAttribute("aria-busy");
        }
      });
    }
    const del = $("cw_crosswatch_disconnect");
    if (del && !del.__cwTrackerWired) {
      del.__cwTrackerWired = true;
      del.addEventListener("click", async () => {
        if (selectedInstance() !== "default") {
          Shared.showConnectionWarning?.("Use the profile trash button to delete CrossWatch profiles.");
          return;
        }
        try {
          const res = await fetch("/api/crosswatch/disconnect", { method: "POST", cache: "no-store" });
          const data = await res.json().catch(() => ({}));
          if (!res.ok || data?.ok === false) {
            const msg = data?.error === "profiles_exist"
              ? "Remove the additional profiles before deleting the main connection."
              : data?.message || data?.error || "Could not delete CrossWatch connection.";
            Shared.showConnectionWarning?.(msg);
            return;
          }
          try { w.invalidateConfigCache?.(); } catch {}
          try { d.dispatchEvent(new CustomEvent("config-saved", { bubbles: true })); } catch {}
          try { w.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
          setTimeout(() => w.CW?.ProvidersUI?.refreshAuthPresentation?.(true), 250);
        } catch (e) {
          Shared.showConnectionWarning?.("Could not delete CrossWatch connection.");
        }
      });
    }
  }

  function ensureUI() {
    profile?.ensureUI?.(hydrate);
    wireFields();
  }

  function init() {
    ensureUI();
    hydrate().catch(() => {});
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", init, { once: true });
  else init();

  d.addEventListener("cw-auth-profile-created", (ev) => {
    if (String(ev?.detail?.provider || "").toLowerCase() !== "crosswatch") return;
    hydrate().catch(() => {});
  }, true);

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.crosswatch = w.cwAuth.crosswatch || {};
  w.cwAuth.crosswatch.init = init;
  w.cwAuth.crosswatch.rehydrate = hydrate;
  w.cwAuth.crosswatch.profile = profile;
})(window, document);
