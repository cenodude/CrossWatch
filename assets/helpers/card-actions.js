/* assets/helpers/card-actions.js */
/* CrossWatch - Shared media card action menus */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const esc = value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");

  let activeCardMenu = null;
  const touchLayout = matchMedia('(max-width:760px), (pointer:coarse) and (max-height:600px)');
  let press = null, held = null, openingFromHold = false;
  const cancelPress = () => { clearTimeout(press?.timer); press = null; };
  const coverAction = target => {
    if (!(target instanceof Element) || !target.closest('#page-tracker,.cw-profile-shell')) return null;
    if (target.closest('input,select,textarea,label,.cw-card-actions,[data-provider-overflow]')) return null;
    const cover = target.closest('.trk-grid-art,.trk-overview-art,.cw-hist-art,.cw-collection-poster');
    const card = cover?.closest('.trk-grid-card,.trk-overview-card,.cw-hist-card,.cw-collection-card');
    const button = card?.querySelector('button.cw-card-actions');
    return button && !button.disabled && button.checkVisibility() ? {card,button} : null;
  };
  document.addEventListener('pointerdown', event => {
    cancelPress();
    held = null;
    if (!touchLayout.matches || !event.isPrimary || !['touch','pen'].includes(event.pointerType) || event.button !== 0) return;
    const action = coverAction(event.target);
    if (!action) return;
    const gesture = {...action, id:event.pointerId, x:event.clientX, y:event.clientY};
    press = gesture;
    gesture.timer = setTimeout(() => {
      cancelPress();
      if (!gesture.card.isConnected || gesture.button.disabled || !gesture.button.checkVisibility() || document.hidden) return;
      held = {...gesture, until:performance.now()+10000};
      openingFromHold = true;
      try { gesture.button.click(); } finally { openingFromHold = false; }
      window.CW?.MobileUI?.feedback?.();
    }, 400);
  }, {capture:true,passive:true});
  document.addEventListener('pointermove', event => {
    if (press && event.pointerId === press.id && Math.hypot(event.clientX-press.x,event.clientY-press.y)>10) cancelPress();
  }, {capture:true,passive:true});
  document.addEventListener('pointerup', event => {
    if (event.pointerId === press?.id) cancelPress();
    if (event.pointerId === held?.id) {
      held.until = performance.now()+800;
      event.preventDefault();
    }
  }, {capture:true,passive:false});
  document.addEventListener('pointercancel', event => {
    if (event.pointerId === press?.id) cancelPress();
    if (event.pointerId === held?.id) held.until = performance.now()+800;
  }, {capture:true,passive:true});
  document.addEventListener('click', event => {
    if (!held || !event.detail || performance.now()>held.until) return;
    if (event.pointerId === held.id || held.card.contains(event.target) || Math.hypot(event.clientX-held.x,event.clientY-held.y)<16) {
      event.preventDefault();
      event.stopImmediatePropagation();
      held = null;
    }
  }, true);
  document.addEventListener('contextmenu', event => {
    if (press || held && performance.now()<held.until) event.preventDefault();
  }, true);
  document.addEventListener('scroll', cancelPress, {capture:true,passive:true});
  document.addEventListener('visibilitychange', cancelPress);
  window.addEventListener('blur', cancelPress);
  window.addEventListener('resize', cancelPress);

  function open(anchor, entries, options = {}) {
    activeCardMenu?.hidePopover();
    const menu = document.createElement("div");
    const mobile = matchMedia("(max-width:760px), (pointer:coarse) and (max-height:600px)").matches;
    const card = anchor.closest(".trk-grid-card,.trk-overview-card,[data-playback-key],[data-timeline-key],[data-collection-key]");
    const title = options.title || card?.querySelector("strong,h3,.cw-collection-name")?.textContent || "Actions";
    menu.className = `cw-card-action-menu${mobile ? " is-sheet" : ""}`;
    const holdMenu = openingFromHold;
    menu.setAttribute("popover", holdMenu ? "manual" : "auto");
    menu.setAttribute("role", mobile ? "dialog" : "menu");
    menu.tabIndex = -1;
    if (mobile) menu.setAttribute("aria-modal", "true");
    menu.setAttribute("aria-label", anchor.getAttribute("aria-label") || "Actions");
    const heading = mobile ? `<header class="cw-card-action-heading"><span class="cw-card-action-grip" aria-hidden="true"></span><h2>${esc(title)}</h2>${options.subtitle ? `<p>${esc(options.subtitle)}</p>` : ""}</header>` : "";
    menu.innerHTML = heading + `<div role="${mobile ? "menu" : "presentation"}" class="cw-card-action-items">` + entries.map((entry, index) => `<button type="button" role="menuitem" data-card-command="${index}"${entry.attrs || ""}${entry.disabled ? " disabled" : ""}${entry.danger ? ' class="is-danger"' : ""}><span class="material-symbols-rounded" aria-hidden="true">${esc(entry.icon || "chevron_right")}</span><span>${esc(entry.label)}</span></button>`).join("") + "</div>";
    let pendingAction = null, closed = false, historyPending = false;
    const marker = `card-actions-${Date.now()}-${Math.random()}`;
    const inert = [];
    const locked = document.documentElement.classList.contains("cw-card-sheet-open");
    const finish = () => {
      anchor.focus({preventScroll:true});
      const run = pendingAction;
      pendingAction = null;
      run?.();
    };
    const back = () => {
      if (history.state?.cwCardActions === marker) return;
      window.removeEventListener("popstate", back);
      if (menu.matches(":popover-open")) menu.hidePopover();
      else if (historyPending) { historyPending = false; finish(); }
    };
    document.body.append(menu);
    if (mobile) {
      for (const node of document.body.children) {
        if (node !== menu && node instanceof HTMLElement && !node.inert) { inert.push(node); node.inert = true; }
      }
      document.documentElement.classList.add("cw-card-sheet-open");
      history.pushState({...history.state,cwCardActions:marker}, "", location.href);
      window.addEventListener("popstate", back);
      menu.setAttribute("aria-label", title);
    }
    activeCardMenu = menu;
    anchor.setAttribute("aria-expanded", "true");
    const dismiss = (event) => {
      if (event.target instanceof Node && menu.contains(event.target)) return;
      if (holdMenu && event.type === 'pointerdown') {
        event.preventDefault();
        held = {id:event.pointerId,card:menu,x:event.clientX,y:event.clientY,until:performance.now()+10000};
      }
      menu.hidePopover();
    };
    window.addEventListener("resize", dismiss);
    if (holdMenu) document.addEventListener('pointerdown', dismiss);
    menu.addEventListener("beforetoggle", (event) => {
      if (event.newState !== "closed" || closed) return;
      closed = true;
      anchor.setAttribute("aria-expanded", "false");
      for (const node of inert) node.inert = false;
      anchor.focus({preventScroll:true});
      if (mobile && !locked) document.documentElement.classList.remove("cw-card-sheet-open");
      if (mobile && history.state?.cwCardActions === marker) {
        historyPending = true;
        history.back();
      } else {
        window.removeEventListener("popstate", back);
        finish();
      }
    });
    menu.addEventListener("toggle", () => {
      if (menu.matches(":popover-open")) return;
      window.removeEventListener("resize", dismiss);
      document.removeEventListener('pointerdown', dismiss);
      anchor.setAttribute("aria-expanded", "false");
      if (activeCardMenu === menu) activeCardMenu = null;
      menu.remove();
    });
    menu.addEventListener("click", (event) => {
      const button = event.target.closest("[data-card-command]");
      if (!button || button.disabled) return;
      pendingAction = entries[Number(button.dataset.cardCommand)]?.run;
      menu.hidePopover();
    });
    menu.addEventListener("keydown", (event) => {
      const buttons = [...menu.querySelectorAll("button:not(:disabled)")];
      const index = buttons.indexOf(document.activeElement);
      if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
        event.preventDefault();
        const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : (index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
        buttons[next]?.focus();
      }
      if (event.key === "Escape") {
        event.preventDefault();
        menu.hidePopover();

      }
      if (event.key === "Tab") {
        if (mobile) {
          event.preventDefault();
          (buttons[(index + (event.shiftKey ? -1 : 1) + buttons.length) % buttons.length] || menu).focus();
        } else menu.hidePopover();
      }
    });
    if (mobile) {
      const heading = menu.querySelector("header");
      let start = null;
      heading.addEventListener("pointerdown", event => {
        if (event.pointerType === "mouse") return;
        start = {x:event.clientX,y:event.clientY};
        heading.setPointerCapture(event.pointerId);
      });
      heading.addEventListener("pointerup", event => {
        if (start && event.clientY - start.y > 60 && Math.abs(event.clientX - start.x) < 80) menu.hidePopover();
        start = null;
      });
      heading.addEventListener("pointercancel", () => { start = null; });
    }
    menu.showPopover();
    if (!mobile) {
      const rect = anchor.getBoundingClientRect();
      const bounds = menu.getBoundingClientRect();
      menu.style.left = `${Math.max(8, Math.min(rect.right - bounds.width, innerWidth - bounds.width - 8))}px`;
      menu.style.top = `${Math.max(8, rect.bottom + bounds.height + 8 <= innerHeight ? rect.bottom + 4 : rect.top - bounds.height - 4)}px`;
    }
    (menu.querySelector("button:not(:disabled)") || menu).focus({ preventScroll: true });
    return menu;
  }

  (window.CW ||= {}).CardActions = {open};
})();
