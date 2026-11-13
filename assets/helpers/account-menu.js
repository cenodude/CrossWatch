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
    const mobileLayout = window.matchMedia('(max-width: 760px)');
    const syncLayout = () => {
      panel.classList.toggle('cw-page-mobile-navigation', mobileLayout.matches);
      close();
    };
    mobileLayout.addEventListener('change', syncLayout);
    syncLayout();
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
    const fixed = [['main', 'home', 'Home']];
    const choices = [];
    if ($('tab-tracker')) choices.push(['tracker', 'inventory_2', 'Tracker']);
    if ($('tab-settings') && document.documentElement.dataset.cwRole !== 'user') {
      choices.push(['settings/providers', 'device_hub', 'Connections'], ['settings/sync', 'sync_alt', 'Sync pairs'], ['settings/scrobbler', 'sensors', 'Scrobbler']);
    }
    for (const item of [['snapshots', 'photo_library', 'Captures'], ['playlists', 'queue_music', 'Playlists'], ['editor', 'edit_note', 'Editor']]) {
      if ($(`tab-${item[0]}`)) choices.push(item);
    }
    if (document.querySelector('[data-cw-profile-menu-action="profile"]')) choices.push(['profile/overview', 'bar_chart', 'My Media Hub']);
    const storageKey = 'cw.mobile.shortcuts.v1';
    const defaults = [...new Set(['tracker', 'profile/overview', ...choices.map(item => item[0])])].filter(route => choices.some(item => item[0] === route)).slice(0, 2);
    const readShortcuts = () => {
      let saved;
      try { saved = JSON.parse(localStorage.getItem(storageKey)); } catch {}
      const valid = Array.isArray(saved) ? [...new Set(saved.filter(route => choices.some(item => item[0] === route)))].slice(0, 2) : [];
      return [...valid, ...defaults.filter(route => !valid.includes(route))].slice(0, 2);
    };
    const shortcuts = readShortcuts();
    const routeBase = window.location.pathname === '/' ? '' : document.documentElement.dataset.cwRole === 'user' ? '/?main=1&ui=full' : '/?ui=full';
    const link = ([route, icon, label], extra = '') => `<a href="${route === 'profile/overview' ? '/profile#overview' : `${routeBase}#${route}`}" data-mobile-route="${route}" ${extra}><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${label}</span></a>`;
    nav.innerHTML =
      '<button id="cw-mobile-more" type="button" aria-controls="cw-mobile-navigation" aria-expanded="false"><span class="material-symbols-rounded" aria-hidden="true">menu</span><span>More</span></button>';
    document.body.appendChild(nav);
    document.body.classList.add('cw-has-bottom-nav');
    const icons = {'tab-main':'home','tab-snapshots':'photo_library','tab-playlists':'queue_music','tab-tracker':'inventory_2','tab-editor':'edit_note','tab-settings':'settings','tab-about':'info'};
    document.querySelectorAll('#cw-mobile-navigation .tab').forEach(tab => {
      const icon = document.createElement('span');
      icon.className = 'material-symbols-rounded cw-mobile-destination-icon';
      icon.setAttribute('aria-hidden', 'true');
      icon.textContent = icons[tab.id] || 'info';
      tab.prepend(icon);
    });
    const extra = document.createElement('div');
    extra.className = 'cw-mobile-extra-destinations';
    $('cw-mobile-navigation').prepend(extra);
    const openMediaHub = event => {
      if (window.location.pathname !== '/profile' || !event.target.closest('[data-mobile-route="profile/overview"]')) return;
      event.preventDefault();
      openProfileTab('overview');
    };
    extra.addEventListener('click', openMediaHub);
    nav.addEventListener('click', openMediaHub);
    const renderShortcuts = () => {
      const items = [...fixed, ...shortcuts.map(route => choices.find(item => item[0] === route)).filter(Boolean)];
      nav.querySelectorAll('a').forEach(node => node.remove());
      $('cw-mobile-more').insertAdjacentHTML('beforebegin', items.map(item => link(item)).join(''));
      extra.innerHTML = choices.filter(item => !$(`tab-${item[0]}`) && !shortcuts.includes(item[0])).map(item => link(item, 'class="tab"')).join('');
      for (const route of ['main', 'tracker', 'snapshots', 'playlists', 'editor']) {
        $(`tab-${route}`)?.classList.toggle('cw-mobile-pinned', items.some(item => item[0] === route));
      }
      document.querySelectorAll('#cw-mobile-navigation [data-settings-pane]').forEach(item => {
        item.classList.toggle('cw-mobile-pinned', shortcuts.includes(`settings/${item.dataset.settingsPane}`));
      });
      update();
    };
    const installShortcuts = () => {
      const picker = $('cw-mobile-widgets');
      if (!picker || picker.querySelector('.cw-mobile-shortcuts') || choices.length < 2) return;
      const controls = document.createElement('div');
      controls.className = 'cw-mobile-shortcuts';
      controls.innerHTML = `<h3>Navigation shortcuts</h3><p>${fixed.map(item => item[2]).join(', ')} and More stay fixed. Choose two shortcuts for this browser.</p>` + [0, 1].map(index => `<label for="cw-mobile-shortcut-${index}">Shortcut ${index + 1}</label><select id="cw-mobile-shortcut-${index}" data-cw-native-select="true">${choices.map(([route, , label]) => `<option value="${route}">${label}</option>`).join('')}</select>`).join('');
      picker.querySelector('summary').after(controls);
      const selects = [...controls.querySelectorAll('select')];
      const sync = () => selects.forEach((select, index) => {
        select.value = shortcuts[index];
        [...select.options].forEach(option => { option.disabled = option.value === shortcuts[1 - index]; });
      });
      controls.addEventListener('change', event => {
        const index = selects.indexOf(event.target);
        if (index < 0) return;
        if (shortcuts[1 - index] === event.target.value || !choices.some(item => item[0] === event.target.value)) return sync();
        shortcuts[index] = event.target.value;
        try { localStorage.setItem(storageKey, JSON.stringify(shortcuts)); } catch {}
        sync();
        renderShortcuts();
      });
      sync();
    };
    document.addEventListener('cw-mobile-settings-ready', installShortcuts);
    document.dispatchEvent(new Event('cw-mobile-navigation-ready'));

    $('cw-mobile-more').addEventListener('click', () => {
      $('cw-mobile-nav-toggle').click();
      if ($('cw-mobile-more').getAttribute('aria-expanded') === 'true') {
        [...$('cw-mobile-navigation').querySelectorAll('button, a[href], summary')].find(node => node.getClientRects().length)?.focus();
      }
    });
    nav.addEventListener('click', event => {
      if (event.target.closest('a, button')) mobileNavigationFeedback();
      if (event.target.closest('[data-mobile-route="tracker"]') && window.location.pathname === '/' && window.location.hash === '#tracker') void window.TrackerPage?.resume?.();
      if (event.target.closest('a') && $('cw-mobile-more').getAttribute('aria-expanded') === 'true') $('cw-mobile-nav-toggle').click();
    });
    const update = () => {
      const profileTab = document.querySelector('[data-profile-tab].active')?.dataset.profileTab || window.location.hash.slice(1).split('?')[0] || 'overview';
      const route = window.location.pathname === '/' ? window.location.hash.slice(1).split('?')[0] || 'main' : window.location.pathname === '/profile' && profileTab === 'overview' ? 'profile/overview' : '';
      let matched = false;
      document.querySelectorAll('[data-mobile-route]').forEach(link => {
        const active = link.dataset.mobileRoute === route;
        if (active) link.setAttribute('aria-current', 'page');
        else link.removeAttribute('aria-current');
        link.classList.toggle('active', active);
        matched ||= active && nav.contains(link);
      });
      $('cw-mobile-more').classList.toggle('is-current', !matched);
    };
    const scheduleUpdate = () => window.requestAnimationFrame(update);
    window.addEventListener('hashchange', update);
    document.addEventListener('tab-changed', scheduleUpdate);
    document.addEventListener('cw-settings-pane-changed', scheduleUpdate);
    const profileTabs = document.querySelector('.cw-profile-tabs');
    if (profileTabs) new MutationObserver(scheduleUpdate).observe(profileTabs, {subtree:true, attributes:true, attributeFilter:['class']});
    renderShortcuts();
    installShortcuts();
    initMobileSwipeNavigation(nav);
  }

  function initMobileSwipeNavigation(nav) {
    const mobile = window.matchMedia('(max-width: 760px)');
    const excluded = 'header, nav, input, textarea, select, summary, [contenteditable]:not([contenteditable="false"]), [role="slider"], [role="scrollbar"], [draggable="true"], [aria-haspopup], [data-no-swipe], .wr-page, video, audio, canvas';
    let gesture = null;
    const blocked = () => !mobile.matches || !nav.getClientRects().length || (window.visualViewport?.scale || 1) > 1.05 ||
      document.querySelector('.cw-nav-open, .cw-card-sheet-open, .cw-ia-sheet-open, .cw-mm-open, .cx-modal-open') ||
      [...document.querySelectorAll('dialog[open], [aria-modal="true"], [role="dialog"], [popover]:popover-open')].some(node => node.getClientRects().length);
    const currentRoute = () => window.location.pathname === '/profile' ? 'profile/overview' :
      window.location.pathname === '/' ? window.location.hash.slice(1).split('?')[0] || 'main' : '';
    const destination = dx => {
      const links = [...nav.querySelectorAll('a[data-mobile-route]')];
      const index = links.findIndex(link => link.dataset.mobileRoute === currentRoute());
      return index < 0 ? null : links[index + (dx > 0 ? 1 : -1)];
    };
    const reset = () => { gesture = null; };
    document.addEventListener('touchstart', event => {
      reset();
      if (event.touches.length !== 1 || blocked() || !(event.target instanceof Element) || event.target.closest(excluded)) return;
      const touch = event.touches[0];
      if (touch.clientX < 24 || touch.clientX > window.innerWidth - 24) return;
      for (let node = event.target; node && node !== document.body; node = node.parentElement) {
        const style = getComputedStyle(node);
        if ((node.scrollWidth > node.clientWidth + 2 && /auto|scroll/.test(style.overflowX)) || /none|pan-x/.test(style.touchAction)) return;
      }
      gesture = {id:touch.identifier, x:touch.clientX, y:touch.clientY, started:performance.now(), route:currentRoute(), horizontal:false};
    }, {passive:true});
    document.addEventListener('touchmove', event => {
      if (!gesture) return;
      if (event.touches.length !== 1 || blocked() || currentRoute() !== gesture.route) return reset();
      const touch = event.touches[0];
      const dx = touch.clientX - gesture.x, dy = touch.clientY - gesture.y;
      if (!gesture.horizontal && Math.abs(dy) > 12 && Math.abs(dy) >= Math.abs(dx)) return reset();
      if (!gesture.horizontal && Math.abs(dx) > 18 && Math.abs(dx) > Math.abs(dy) * 1.7) {
        if (!destination(dx) || !event.cancelable) return reset();
        gesture.horizontal = true;
      }
      if (gesture.horizontal && event.cancelable) event.preventDefault();
    }, {passive:false});
    document.addEventListener('touchend', event => {
      const start = gesture;
      reset();
      if (!start?.horizontal) return;
      const touch = [...event.changedTouches].find(item => item.identifier === start.id);
      if (!touch) return;
      if (event.cancelable) event.preventDefault();
      const dx = touch.clientX - start.x, dy = touch.clientY - start.y;
      if (event.touches.length || blocked() || currentRoute() !== start.route || performance.now() - start.started > 900 ||
          Math.abs(dx) < Math.max(70, window.innerWidth * .18) || Math.abs(dx) < Math.abs(dy) * 1.7) return;
      destination(dx)?.click();
    }, {passive:false});
    document.addEventListener('touchcancel', reset, {passive:true});
    window.addEventListener('hashchange', reset);
    mobile.addEventListener('change', reset);
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

  function menuItems() {
    return [...(menu()?.querySelectorAll('[role="menuitem"]') || [])].filter(item => !item.closest('[hidden]') && !item.disabled);
  }

  function focusMenuItem(item) {
    if (!item) return;
    item.focus({preventScroll:true});
    const m = menu();
    const bounds = m.getBoundingClientRect();
    const row = item.getBoundingClientRect();
    if (row.top < bounds.top + 12) m.scrollTop += row.top - bounds.top - 12;
    else if (row.bottom > bounds.bottom - 12) m.scrollTop += row.bottom - bounds.bottom + 12;
  }

  function sizeMenu() {
    const m = menu();
    if (!m || m.classList.contains('hidden')) return;
    const viewport = window.visualViewport;
    const nav = $('cw-mobile-bottom-nav')?.getBoundingClientRect();
    const bottom = Math.min(viewport ? viewport.height + viewport.offsetTop : window.innerHeight, nav?.height ? nav.top : Infinity);
    m.style.maxHeight = `${Math.max(44, bottom - m.getBoundingClientRect().top - 12)}px`;
  }

  function markCurrentPage() {
    const onProfile = window.location.pathname === '/profile';
    const tab = document.querySelector('[data-profile-tab].active')?.dataset.profileTab || window.location.hash.slice(1).split('?')[0] || 'overview';
    const profileActions = {overview:'profile', collection:'collections', account:'account', security:'account', preferences:'account', playback_progress:'playback'};
    const current = onProfile ? profileActions[tab] || tab : window.location.hash.startsWith('#tracker') ? 'tracker' : '';
    menu()?.querySelectorAll('[data-cw-profile-menu-action]').forEach(item => {
      if (item.dataset.cwProfileMenuAction === current) item.setAttribute('aria-current', 'page');
      else item.removeAttribute('aria-current');
    });
  }

  function setOpen(open) {
    const m = menu();
    const b = button();
    if (!m || !b) return;
    m.classList.toggle("hidden", !open);
    b.setAttribute("aria-expanded", open ? "true" : "false");
    if (open) {
      markCurrentPage();
      sizeMenu();
      focusMenuItem(menuItems()[0]);
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
    const group = event.target?.closest?.('#cw-profile-menu .cw-account-group-toggle');
    if (group) {
      event.preventDefault();
      event.stopPropagation();
      const expanded = group.getAttribute('aria-expanded') !== 'true';
      group.setAttribute('aria-expanded', String(expanded));
      $(group.getAttribute('aria-controls')).hidden = !expanded;
      focusMenuItem(group);
      return;
    }
    const action = event.target?.closest?.("[data-cw-profile-menu-action]");
    if (action) {
      event.preventDefault();
      event.stopPropagation();
      close();
      const value = String(action.dataset.cwProfileMenuAction || "");
      if (value === "profile") window.location.href = "/profile";
      else if (value === "account") window.location.href = "/profile#account";
      else if (value === "tracker") window.location.href = `${document.documentElement.dataset.cwRole === 'user' ? '/?main=1' : '/'}#tracker?view=overview`;
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
    const m = menu();
    if (!m) return;
    if (event.target === button() && ['ArrowDown', 'ArrowUp'].includes(event.key)) {
      event.preventDefault();
      setOpen(true);
      if (event.key === 'ArrowUp') focusMenuItem(menuItems().at(-1));
      return;
    }
    if (m.classList.contains('hidden')) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      close();
      button()?.focus();
      return;
    }
    if (!m.contains(event.target)) return;
    const items = menuItems();
    const index = items.indexOf(event.target);
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
      event.preventDefault();
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : (index + (event.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
      focusMenuItem(items[next]);
    }
  }, true);
  document.addEventListener('focusin', event => {
    if (!event.target?.closest?.('#cw-nav-profile-menu')) close();
  });
  window.addEventListener('resize', sizeMenu);
  window.visualViewport?.addEventListener('resize', sizeMenu);

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
