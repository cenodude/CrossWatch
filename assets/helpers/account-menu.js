/* assets/helpers/account-menu.js */
/* CrossWatch - header account menu */
(function () {
  if (window.__cwAccountMenuInstalled) return;
  window.__cwAccountMenuInstalled = true;

  const $ = (id) => document.getElementById(id);
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));

  function menu() {
    return $("cw-profile-menu");
  }

  function button() {
    return $("cw-nav-profile-link");
  }

  function initMobileNavigation() {
    const nav = document.querySelector('header > nav.tabs');
    if (!nav || document.documentElement.classList.contains('cw-compact') || $('cw-mobile-nav-toggle')) return;
    const header = nav.parentElement;
    const panel = document.createElement('div');
    panel.id = 'cw-mobile-navigation';
    panel.className = 'cw-mobile-navigation';
    const destinations = nav.querySelectorAll(':scope > .tab, :scope > .cw-tabmenu');
    nav.insertBefore(panel, nav.firstChild);
    destinations.forEach(destination => panel.appendChild(destination));
    const toggle = document.createElement('button');
    toggle.id = 'cw-mobile-nav-toggle';
    toggle.className = 'cw-mobile-nav-toggle';
    toggle.type = 'button';
    toggle.setAttribute('aria-label', 'Menu');
    toggle.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">menu</span><span>Menu</span>';
    nav.id ||= 'cw-primary-navigation';
    toggle.setAttribute('aria-controls', panel.id);
    toggle.setAttribute('aria-expanded', 'false');
    header.insertBefore(toggle, nav);
    const close = () => {
      if (toggle.getAttribute('aria-expanded') !== 'true') return;
      if (panel.contains(document.activeElement)) toggle.focus();
      header.classList.remove('cw-nav-open');
      toggle.setAttribute('aria-expanded', 'false');
      window.cwCloseSettingsMenu?.();
      window.cwCloseAboutMenu?.();
    };
    toggle.addEventListener('click', () => {
      if (toggle.getAttribute('aria-expanded') === 'true') return close();
      header.classList.add('cw-nav-open');
      toggle.setAttribute('aria-expanded', 'true');
    });
    nav.addEventListener('click', event => {
      if (event.target.closest('.tab:not([aria-haspopup]), [role="menuitem"], a[href], #cw-nav-profile-link, #cw-notifications-button')) close();
    });
    document.addEventListener('click', event => { if (!header.contains(event.target)) close(); });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && toggle.getAttribute('aria-expanded') === 'true') {
        close();
        toggle.focus();
      }
    });
    window.addEventListener('hashchange', close);
    window.matchMedia('(max-width: 760px)').addEventListener('change', close);
  }

  function normalizeAvatarUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (!raw.startsWith("/api/profile/avatar/")) return raw;
    const q = raw.indexOf("?");
    return `/api/profile/avatar${q >= 0 ? raw.slice(q) : ""}`;
  }

  function setAvatarNode(node, value) {
    if (!node) return;
    const url = normalizeAvatarUrl(value);
    const existing = node.querySelector("img")?.getAttribute("src") || "";
    const isNavAvatar = node.id === "cw-nav-profile-avatar";
    if (url && existing === url) {
      node.dataset.cwAvatarUrl = url;
      if (isNavAvatar) node.classList.remove("material-symbols-rounded");
      return;
    }
    if (url) {
      node.innerHTML = `<img src="${esc(url)}" alt="">`;
      node.dataset.cwAvatarUrl = url;
      if (isNavAvatar) node.classList.remove("material-symbols-rounded");
      node.querySelector("img")?.addEventListener("error", () => setAvatarNode(node, ""), { once: true });
      return;
    }
    node.dataset.cwAvatarUrl = "";
    if (isNavAvatar) {
      node.innerHTML = "person";
      node.classList.add("material-symbols-rounded");
    } else {
      node.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">person</span>';
    }
  }

  function setOpen(open) {
    const m = menu();
    const b = button();
    if (!m || !b) return;
    m.classList.toggle("hidden", !open);
    b.setAttribute("aria-expanded", open ? "true" : "false");
    if (open) {
      const first = m.querySelector(".cw-menu-item");
      setTimeout(() => first?.focus?.(), 0);
    }
  }

  function close() {
    setOpen(false);
  }

  function toggle(event) {
    event?.preventDefault?.();
    event?.stopPropagation?.();
    const m = menu();
    if (!m) return;
    setOpen(m.classList.contains("hidden"));
  }

  function openProfileTab(tab) {
    if (window.location?.pathname === "/profile") {
      if (window.location.hash !== `#${tab}`) window.location.hash = tab;
      else {
        try { window.dispatchEvent(new HashChangeEvent("hashchange")); }
        catch { window.dispatchEvent(new Event("hashchange")); }
      }
      return;
    }
    window.location.href = `/profile#${tab}`;
  }

  async function logout() {
    try {
      await fetch("/api/app-auth/logout", {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
      });
    } catch {}
    window.location.href = "/login";
  }

  document.addEventListener("click", (event) => {
    const toggleButton = event.target?.closest?.("#cw-nav-profile-link");
    if (toggleButton) {
      toggle(event);
      return;
    }
    const action = event.target?.closest?.("[data-cw-profile-menu-action]");
    if (action) {
      event.preventDefault();
      event.stopPropagation();
      close();
      const value = String(action.dataset.cwProfileMenuAction || "");
      if (value === "profile") window.location.href = "/profile";
      else if (value === "watchlist") openProfileTab("watchlist");
      else if (value === "history") openProfileTab("history");
      else if (value === "ratings") openProfileTab("ratings");
      else if (value === "collections") openProfileTab("collection");
      else if (value === "playback") openProfileTab("playback");
      else if (value === "logout") void logout();
      return;
    }
    if (!event.target?.closest?.("#cw-nav-profile-menu")) close();
  }, true);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") close();
  }, true);

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", initMobileNavigation, {once: true});
  else initMobileNavigation();

  window.CW = window.CW || {};
  window.CW.AccountMenu = {
    normalizeAvatarUrl,
    setAvatarNode,
    setAvatarUrl(value) {
      setAvatarNode($("cw-nav-profile-avatar"), value);
      setAvatarNode($("profile-avatar-button"), value);
    },
  };
})();
