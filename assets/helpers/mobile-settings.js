/* assets/helpers/mobile-settings.js */
/* CrossWatch - Shared mobile preferences across all pages */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const $ = selector => document.querySelector(selector);
  let config = {};
  function ensure(options = {}) {
    if (options.config) config = options.config;
    const menu = $("#cw-mobile-navigation");
    if (!menu) return null;
    let picker = $("#cw-mobile-widgets");
    if (!picker) {
      picker = document.createElement("details");
      picker.id = "cw-mobile-widgets";
      picker.className = "cw-mobile-widgets";
      picker.innerHTML = `<summary><span class="material-symbols-rounded" aria-hidden="true">tune</span><span>${TH("common.mobileSettings.title")}</span><span class="material-symbols-rounded cw-mobile-settings-chevron" aria-hidden="true">chevron_right</span></summary>
        <div class="cw-mobile-widget-options">
          <h3>${TH("common.mobileSettings.visibility")}</h3>
          <label><input type="checkbox" data-mobile-playing-card><span class="material-symbols-rounded cw-mobile-widget-icon" aria-hidden="true">play_circle</span><span>${TH("common.mobileSettings.playingCard")}</span><small>${TH("common.mobileSettings.showOnDevice")}</small></label>
          <h3>${TH("common.mobileSettings.notifications")}</h3>
          <label><input type="checkbox" data-mobile-notification-alerts><span class="material-symbols-rounded cw-mobile-widget-icon" aria-hidden="true">notifications_active</span><span>${TH("common.mobileSettings.alerts")}</span><small>${TH("common.mobileSettings.alertsSub")}</small></label>
          <label><input type="checkbox" data-mobile-push><span class="material-symbols-rounded cw-mobile-widget-icon" aria-hidden="true">smartphone</span><span>${TH("common.mobileSettings.push")}</span><small>${TH("common.mobileSettings.pushSub")}</small></label>
          <h3>${TH("common.mobileSettings.touch")}</h3>
          <label><input type="checkbox" data-mobile-haptics><span class="material-symbols-rounded cw-mobile-widget-icon" aria-hidden="true">vibration</span><span>${TH("common.mobileSettings.haptics")}</span><small>${TH("common.mobileSettings.hapticsSub")}</small></label>
        </div>`;
      menu.appendChild(picker);
      const playingCard = picker.querySelector('[data-mobile-playing-card]');
      playingCard.addEventListener('change', () => {
        try { localStorage.setItem('cw.mobile.playingCard', playingCard.checked ? '1' : '0'); } catch {}
        window.dispatchEvent(new CustomEvent('cw:mobile-playing-card-changed'));
      });
      const notificationAlerts = picker.querySelector('[data-mobile-notification-alerts]');
      const syncAlerts = () => {
        try { notificationAlerts.checked = localStorage.getItem('cw.mobile.notificationAlerts') === '1'; } catch {}
      };
      syncAlerts();
      window.addEventListener('cw:mobile-notification-alerts-changed', syncAlerts);
      window.addEventListener('storage', event => { if (event.key === 'cw.mobile.notificationAlerts' || event.key === null) syncAlerts(); });
      notificationAlerts.addEventListener('change', () => {
        try { localStorage.setItem('cw.mobile.notificationAlerts', notificationAlerts.checked ? '1' : '0'); } catch {}
        window.dispatchEvent(new CustomEvent('cw:mobile-notification-alerts-changed'));
      });
      const push = picker.querySelector('[data-mobile-push]');
      const pushNote = push.parentElement.querySelector('small');
      let pushBusy = false;
      let pushRequest = 0;
      push.disabled = true;
      const showPush = status => {
        push.disabled = !status.available;
        push.checked = !!status.on;
        pushNote.textContent = status.note;
      };
      const refreshPush = async () => {
        const api = window.CW?.Notifications?.push;
        push.parentElement.hidden = !api;
        if (!api || pushBusy) return;
        const request = ++pushRequest;
        push.disabled = true;
        const status = await api.status();
        if (request === pushRequest) showPush(status);
      };
      push.addEventListener('change', async () => {
        const api = window.CW?.Notifications?.push;
        if (!api || pushBusy) return;
        pushBusy = true;
        ++pushRequest;
        push.disabled = true;
        try { showPush(await api.set(push.checked)); }
        finally { pushBusy = false; }
      });
      window.addEventListener('cw:notification-push-changed', () => { if (picker.open) void refreshPush(); });
      picker.addEventListener('toggle', () => { if (picker.open) void refreshPush(); });
      const haptics = picker.querySelector('[data-mobile-haptics]');
      const supportsHaptics = typeof navigator.vibrate === 'function';
      haptics.disabled = !supportsHaptics;
      try { haptics.checked = supportsHaptics && localStorage.getItem('cw.mobile.haptics') !== '0'; } catch {}
      if (!supportsHaptics) haptics.parentElement.querySelector('small').textContent = T("common.mobileSettings.unsupported");
      haptics.addEventListener('change', () => {
        try { localStorage.setItem('cw.mobile.haptics', haptics.checked ? '1' : '0'); } catch {}
        if (haptics.checked && supportsHaptics) {
          try { navigator.vibrate(30); } catch {}
        }
      });
    }
    const playingCard = picker.querySelector('[data-mobile-playing-card]');
    const cfg = window._cfgCache || config;
    const auth = window.CW?.AuthState?.read?.();
    const prefs = auth?.preferences || auth?.user?.preferences || {};
    const reason = cfg.ui?.show_playingcard === false ? T("common.mobileSettings.disabledMain") : prefs.playing_card === false ? T("common.mobileSettings.disabledProfile") : '';
    let showPlayingCard = true;
    try { showPlayingCard = localStorage.getItem('cw.mobile.playingCard') !== '0'; } catch {}
    playingCard.checked = !reason && showPlayingCard;
    playingCard.disabled = !!reason;
    playingCard.parentElement.querySelector('small').textContent = reason || T("common.mobileSettings.showOnDevice");
    document.dispatchEvent(new Event('cw-mobile-settings-ready'));
    return picker;
  }
  window.CW = window.CW || {};
  window.CW.MobileSettings = {ensure};
  document.addEventListener('cw-mobile-navigation-ready', ensure);
  window.addEventListener('cw:auth-state-changed', ensure);
  window.addEventListener('storage', event => { if (event.key === 'cw.mobile.playingCard' || event.key === null) ensure(); });
  document.addEventListener('config-saved', ensure);
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', ensure, {once:true});
  else ensure();
})();
