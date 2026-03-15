(function (w, d) {
  w.CW ||= {};

  function create(deps) {
    const {
      STATE,
      el,
      on,
      $,
      j,
      API,
      provider,
      activeProviderInstance,
      asArray,
      read,
      write,
      setNote,
      chip,
      removeUserWatch,
      removeUserWebhook,
      onSelectWatchUser,
    } = deps;

    const USER_PICK = { mode: "watch", anchor: null, users: [], all: [], prov: "plex" };

    function closeUserPicker() {
      const pop = d.getElementById("sc_user_pop");
      if (pop) pop.classList.add("hidden");
    }

    function placeUserPickerPop() {
      const pop = d.getElementById("sc_user_pop");
      const anchor = USER_PICK.anchor;
      if (!pop || !anchor) return;
      const r = anchor.getBoundingClientRect();
      const wPop = pop.offsetWidth || 360;
      const left = Math.max(12, Math.min(w.innerWidth - wPop - 12, r.left));
      const hPop = pop.offsetHeight || 320;
      const preferAbove = r.top - hPop - 8;
      const minTop = 12;
      const below = r.bottom + 8;
      let top = preferAbove >= minTop ? preferAbove : below;
      top = Math.max(minTop, Math.min(w.innerHeight - hPop - 12, top));
      pop.style.left = left + "px";
      pop.style.top = top + "px";
    }

    function userNameFromObj(u) {
      return String(u?.username || u?.title || u?.Name || u?.name || u?.user?.Name || "").trim();
    }

    function userIdFromObj(u) {
      return String(u?.id || u?.Id || u?.user_id || u?.user?.Id || "").trim();
    }

    function userTagFromObj(u, prov) {
      if (prov === "plex") return String(u?.type || "").trim();
      const isAdmin =
        u?.IsAdministrator === true ||
        u?.Policy?.IsAdministrator === true ||
        u?.is_admin === true ||
        u?.admin === true;
      const tags = [];
      if (isAdmin) tags.push("admin");
      if (u?.IsHidden === true) tags.push("hidden");
      if (u?.IsDisabled === true) tags.push("disabled");
      return tags.join(" ");
    }

    function addToWhitelist(hostSel, path, name, removeFn, onClick) {
      const clean = String(name || "").trim();
      if (!clean) return false;
      const cur = asArray(read(path, []));
      if (cur.includes(clean)) return false;
      const next = [...cur, clean];
      write(path, next);
      const host = $(hostSel, STATE.mount);
      if (host) host.append(chip(clean, removeFn, onClick));
      return true;
    }

    function applyPickedUser(u) {
      const prov = provider();
      const name = String(u?.name || "").trim();
      const uid = String(u?.id || "").trim();

      if (USER_PICK.mode === "webhook") {
        const added = addToWhitelist("#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", name, removeUserWebhook);
        setNote("sc-users-note-webhook", added ? `Picked ${name}` : `${name} already added`);
        closeUserPicker();
        return;
      }

      const added = addToWhitelist(
        "#sc-whitelist",
        "scrobble.watch.filters.username_whitelist",
        name,
        removeUserWatch,
        prov === "emby" || prov === "jellyfin" ? onSelectWatchUser : undefined
      );

      if ((prov === "emby" || prov === "jellyfin") && uid) {
        const inp = $("#sc-server-uuid", STATE.mount);
        if (inp) inp.value = uid;
        write("scrobble.watch.filters.server_uuid", uid);
        write("scrobble.watch.filters.user_id", uid);
        setNote("sc-uuid-note", "User ID set");
      }

      setNote("sc-users-note", added ? `Picked ${name}` : `${name} already added`);
      closeUserPicker();
    }

    function renderUserPickerList() {
      const listEl = d.getElementById("sc_user_list");
      const q = String(d.getElementById("sc_user_filter")?.value || "").toLowerCase().trim();
      if (!listEl) return;

      listEl.innerHTML = "";
      const items = (USER_PICK.users || []).filter((u) => !q || String(u.name || "").toLowerCase().includes(q));
      if (!items.length) {
        listEl.appendChild(el("div", { className: "sub", textContent: "No users found." }));
        return;
      }

      for (const u of items) {
        const btn = el("button", { type: "button", className: "userrow" });
        const row = el("div", { className: "row1" });
        const name = el("strong", { textContent: u.name || "" });
        row.appendChild(name);
        if (u.tag) row.appendChild(el("span", { className: "tag", textContent: u.tag }));
        btn.appendChild(row);
        on(btn, "click", (e) => {
          e.preventDefault();
          applyPickedUser(u);
        });
        listEl.appendChild(btn);
      }
    }

    function ensureUserPickerPop() {
      if (d.getElementById("sc_user_pop")) return;
      const pop = el("div", { id: "sc_user_pop", className: "sc-user-pop hidden" });
      const head = el("div", { className: "head" });
      const title = el("div", { className: "title", id: "sc_user_title", textContent: "Pick user" });
      const closeBtn = el("button", { type: "button", id: "sc_user_close", className: "btn small", textContent: "Close" });
      head.append(title, closeBtn);

      const body = el("div", { className: "body" });
      const filter = el("input", { id: "sc_user_filter", className: "input", placeholder: "Filter users..." });
      const list = el("div", { id: "sc_user_list", className: "list" });
      body.append(filter, list);
      pop.append(head, body);
      d.body.appendChild(pop);

      on(closeBtn, "click", (e) => {
        e.preventDefault();
        closeUserPicker();
      });
      on(filter, "input", () => renderUserPickerList());

      if (!STATE.__scUserAwayBound) {
        STATE.__scUserAwayBound = true;
        d.addEventListener("click", (e) => {
          const p = d.getElementById("sc_user_pop");
          if (!p || p.classList.contains("hidden")) return;
          if (p.contains(e.target)) return;
          const a = USER_PICK.anchor;
          if (a && (a === e.target || a.contains(e.target))) return;
          closeUserPicker();
        });
        d.addEventListener("keydown", (e) => {
          if (e.key === "Escape") closeUserPicker();
        });
      }

      if (!STATE.__scUserPosBound) {
        STATE.__scUserPosBound = true;
        let raf = null;
        const safe = () => {
          const p = d.getElementById("sc_user_pop");
          if (!p || p.classList.contains("hidden")) return;
          if (raf) return;
          raf = requestAnimationFrame(() => {
            raf = null;
            try {
              placeUserPickerPop();
            } catch {}
          });
        };
        w.addEventListener("resize", safe, { passive: true });
        w.addEventListener("scroll", safe, { passive: true, capture: true });
        d.addEventListener("scroll", safe, { passive: true, capture: true });
      }
    }

    async function fetchUsersForPicker(mode) {
      if (mode === "webhook") {
        const x = await j(`/api/plex/users?instance=${encodeURIComponent("default")}`);
        const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
        return Array.isArray(a) ? a : [];
      }
      return API.users(activeProviderInstance());
    }

    async function openUserPicker(mode, anchorEl) {
      USER_PICK.mode = mode === "webhook" ? "webhook" : "watch";
      USER_PICK.anchor = anchorEl || null;
      USER_PICK.prov = USER_PICK.mode === "webhook" ? "plex" : provider();

      if (USER_PICK.mode === "watch" && (USER_PICK.prov === "emby" || USER_PICK.prov === "jellyfin") && w.cwMediaUserPicker && typeof w.cwMediaUserPicker.open === "function") {
        w.cwMediaUserPicker.open({
          provider: USER_PICK.prov,
          instance: activeProviderInstance(),
          anchorEl: USER_PICK.anchor,
          title: USER_PICK.prov === "emby" ? "Pick Emby user" : "Pick Jellyfin user",
          onPick: (u) => applyPickedUser({ name: u?.name, id: u?.id }),
        });
        return;
      }

      ensureUserPickerPop();

      const pop = d.getElementById("sc_user_pop");
      const title = d.getElementById("sc_user_title");
      const filter = d.getElementById("sc_user_filter");
      const listEl = d.getElementById("sc_user_list");
      if (!pop || !title || !filter || !listEl) return;

      const provLabel = USER_PICK.prov === "plex" ? "Plex" : USER_PICK.prov === "emby" ? "Emby" : "Jellyfin";
      title.textContent = USER_PICK.mode === "webhook" ? "Pick Plex user" : `Pick ${provLabel} user`;
      filter.value = "";
      listEl.innerHTML = "";
      listEl.appendChild(el("div", { className: "sub", textContent: "Loading..." }));

      pop.classList.remove("hidden");
      try {
        placeUserPickerPop();
      } catch {}

      let list = [];
      try {
        list = await fetchUsersForPicker(USER_PICK.mode);
      } catch (e) {
        console.warn("[scrobbler] users fetch failed:", e);
        listEl.innerHTML = "";
        listEl.appendChild(el("div", { className: "sub", textContent: "Couldn't load users. Check Authentication + logs." }));
        return;
      }

      const prov = USER_PICK.prov;
      const all = Array.isArray(list) ? list : [];
      const prio = (x) => {
        if (prov === "plex") {
          const t = String(x?.type || "").toLowerCase().trim();
          if (t === "owner" || x?.owned === true) return 0;
          if (t === "managed" || x?.isHomeUser === true) return 1;
          return 2;
        }
        const isAdmin =
          x?.IsAdministrator === true ||
          x?.Policy?.IsAdministrator === true ||
          x?.is_admin === true ||
          x?.admin === true;
        return isAdmin ? 0 : 1;
      };

      const mapped = all
        .map((raw) => ({
          raw,
          name: userNameFromObj(raw),
          id: userIdFromObj(raw),
          tag: userTagFromObj(raw, prov),
          prio: prio(raw),
          hidden: raw?.IsHidden === true ? 1 : 0,
          disabled: raw?.IsDisabled === true ? 1 : 0,
        }))
        .filter((x) => x.name)
        .sort(
          (a, b) =>
            a.prio - b.prio ||
            a.disabled - b.disabled ||
            a.hidden - b.hidden ||
            a.name.localeCompare(b.name, undefined, { sensitivity: "base" })
        );

      USER_PICK.all = all;
      USER_PICK.users = mapped;
      STATE.users = USER_PICK.all;

      renderUserPickerList();
      try {
        placeUserPickerPop();
        filter.focus();
      } catch {}
    }

    return { closeUserPicker, openUserPicker };
  }

  w.CW.ScrobblerUserPicker = { create };
})(window, document);
