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
    const focusToggle = () => {
      const more = $('cw-mobile-more');
      (more?.getClientRects().length ? more : toggle).focus();
    };
    const close = () => {
      if (toggle.getAttribute('aria-expanded') !== 'true') return;
      if (panel.contains(document.activeElement)) focusToggle();
      header.classList.remove('cw-nav-open');
      toggle.setAttribute('aria-expanded', 'false');
      $('cw-mobile-more')?.setAttribute('aria-expanded', 'false');
      window.cwCloseSettingsMenu?.();
      window.cwCloseAboutMenu?.();
    };
    toggle.addEventListener('click', () => {
      if (toggle.getAttribute('aria-expanded') === 'true') return close();
      header.classList.add('cw-nav-open');
      toggle.setAttribute('aria-expanded', 'true');
      $('cw-mobile-more')?.setAttribute('aria-expanded', 'true');
    });
    nav.addEventListener('click', event => {
      if (event.target.closest('.tab:not([aria-haspopup]), [role="menuitem"], a[href], #cw-nav-profile-link, #cw-notifications-button')) close();
    });
    document.addEventListener('click', event => { if (!header.contains(event.target) && !$('cw-mobile-bottom-nav')?.contains(event.target)) close(); });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && toggle.getAttribute('aria-expanded') === 'true') {
        close();
        focusToggle();
      }
    });
    window.addEventListener('hashchange', close);
    window.matchMedia('(max-width: 760px)').addEventListener('change', close);
  }

  function mobileNavigationFeedback() {
    try {
      if (localStorage.getItem('cw.mobile.haptics') !== '0' && typeof navigator.vibrate === 'function') navigator.vibrate(30);
    } catch {}
  }

  function initMobileBottomNavigation() {
    if (!$('cw-mobile-nav-toggle') || $('cw-mobile-bottom-nav')) return;
    const nav = document.createElement('nav');
    nav.id = 'cw-mobile-bottom-nav';
    nav.setAttribute('aria-label', 'Mobile navigation');
    const items = [['main', 'home', 'Home']];
    if ($('tab-settings') && document.documentElement.dataset.cwRole !== 'user') {
      items.push(['settings/providers', 'device_hub', 'Connections'], ['settings/sync', 'sync_alt', 'Sync pairs'], ['settings/scrobbler', 'sensors', 'Scrobbler']);
    }
    const routeBase = window.location.pathname === '/' ? '' : '/?ui=full';
    nav.innerHTML = items.map(([route, icon, label]) => `<a href="${routeBase}#${route}" data-mobile-route="${route}"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${label}</span></a>`).join('') +
      '<button id="cw-mobile-more" type="button" aria-controls="cw-mobile-navigation" aria-expanded="false"><span class="material-symbols-rounded" aria-hidden="true">menu</span><span>More</span></button>';
    document.body.appendChild(nav);
    document.body.classList.add('cw-has-bottom-nav');
    const icons = {'tab-main':'home','tab-snapshots':'photo_library','tab-playlists':'queue_music','tab-editor':'edit_note','tab-settings':'settings','tab-about':'info'};
    document.querySelectorAll('#cw-mobile-navigation .tab').forEach(tab => {
      const icon = document.createElement('span');
      icon.className = 'material-symbols-rounded cw-mobile-destination-icon';
      icon.setAttribute('aria-hidden', 'true');
      icon.textContent = icons[tab.id] || 'info';
      tab.prepend(icon);
    });
    document.dispatchEvent(new Event('cw-mobile-navigation-ready'));

    $('cw-mobile-more').addEventListener('click', () => {
      $('cw-mobile-nav-toggle').click();
      if ($('cw-mobile-more').getAttribute('aria-expanded') === 'true') {
        $('cw-mobile-navigation').querySelector('button, a[href]')?.focus();
      }
    });
    nav.addEventListener('click', event => {
      if (event.target.closest('a, button')) mobileNavigationFeedback();
      if (event.target.closest('a') && $('cw-mobile-more').getAttribute('aria-expanded') === 'true') $('cw-mobile-nav-toggle').click();
    });
    const update = () => {
      const route = window.location.pathname === '/' ? window.location.hash.slice(1).split('?')[0] || 'main' : '';
      let matched = false;
      nav.querySelectorAll('[data-mobile-route]').forEach(link => {
        const active = link.dataset.mobileRoute === route;
        if (active) link.setAttribute('aria-current', 'page');
        else link.removeAttribute('aria-current');
        matched ||= active;
      });
      $('cw-mobile-more').classList.toggle('is-current', !matched);
    };
    const scheduleUpdate = () => window.requestAnimationFrame(update);
    window.addEventListener('hashchange', update);
    document.addEventListener('tab-changed', scheduleUpdate);
    document.addEventListener('cw-settings-pane-changed', scheduleUpdate);
    update();
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

  function initNavigation() {
    $('cw-profile-compact-switch')?.addEventListener('click', () => {
      try { localStorage.setItem('cw_ui_mode', 'compact'); } catch {}
      window.location.assign('/?ui=compact#main');
    });
    initMobileNavigation();
    initMobileBottomNavigation();
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", initNavigation, {once: true});
  else initNavigation();

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
