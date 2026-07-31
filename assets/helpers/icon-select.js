(function () {
  const d = document;
  const KEY = "__CW_ICON_SELECT__";


  let OPEN = null;

  function closeAll(except) {
    if (OPEN && OPEN !== except) {
      OPEN.menu.classList.add("hidden");
      OPEN.wrap.classList.remove("is-open");
      OPEN.btn.setAttribute("aria-expanded", "false");
      OPEN = null;
    }
  }

  function bindAway() {
    if (window[KEY]?.awayBound) return;
    window[KEY] = window[KEY] || {};
    window[KEY].awayBound = true;
    d.addEventListener("click", (ev) => {
      const cur = OPEN;
      if (!cur) return;
      if (cur.wrap.contains(ev.target)) return;
      if (cur.menu.contains(ev.target)) return;
      closeAll(null);
    });
    d.addEventListener("keydown", (ev) => {
      if (ev.key === "Escape") closeAll(null);
    });
  }

  function normalizeIcon(icon) {
    if (!icon) return null;
    if (typeof icon === "string") return { src: icon, alt: "" };
    return {
      src: String(icon.src || "").trim(),
      alt: String(icon.alt || "").trim(),
      text: String(icon.text || "").trim(),
      symbol: String(icon.symbol || "").trim(),
    };
  }

  function iconNode(icon) {
    const meta = normalizeIcon(icon);
    if (!meta) return null;
    if (meta.src) {
      const img = d.createElement("img");
      img.className = "cw-icon-select-icon";
      img.src = meta.src;
      img.alt = meta.alt || "";
      return img;
    }
    if (meta.text) {
      const span = d.createElement("span");
      span.className = "cw-icon-select-icon empty";
      span.textContent = meta.text;
      return span;
    }
    if (meta.symbol) {
      const span = d.createElement("span");
      span.className = "cw-icon-select-icon material-symbols-rounded";
      span.setAttribute("aria-hidden", "true");
      span.textContent = meta.symbol;
      return span;
    }
    return null;
  }

  function rowMain(data) {
    const main = d.createElement("span");
    main.className = "cw-icon-select-main";

    if (Array.isArray(data.leadingBadges) && data.leadingBadges.length) {
      const leadingBadges = d.createElement("span");
      leadingBadges.className = "cw-icon-select-leading-badges";
      data.leadingBadges.forEach((badgeText) => {
        const badge = String(badgeText || "").trim();
        if (!badge) return;
        const badgeEl = d.createElement("span");
        badgeEl.className = "cw-icon-select-badge";
        badgeEl.textContent = badge;
        leadingBadges.appendChild(badgeEl);
      });
      if (leadingBadges.childNodes.length) main.appendChild(leadingBadges);
    }

    const segments = Array.isArray(data.segments) ? data.segments.filter(Boolean) : [];
    if (segments.length) {
      const iconWrap = d.createElement("span");
      iconWrap.className = "cw-icon-select-icons";
      segments.forEach((segment, idx) => {
        const seg = d.createElement("span");
        seg.className = "cw-icon-select-segment";
        const node = iconNode(segment.icon || segment);
        if (idx && data.separator === "arrow") {
          const sep = d.createElement("span");
          sep.className = "cw-icon-select-sep";
          sep.textContent = "→";
          iconWrap.appendChild(sep);
        }
        if (node) seg.appendChild(node);
        const badge = String(segment.badge || "").trim();
        if (badge) {
          const badgeEl = d.createElement("span");
          badgeEl.className = "cw-icon-select-badge";
          badgeEl.textContent = badge;
          seg.appendChild(badgeEl);
        }
        if (!seg.childNodes.length) return;
        iconWrap.appendChild(seg);
      });
      if (iconWrap.childNodes.length) main.appendChild(iconWrap);
    } else {
      const icons = Array.isArray(data.icons) ? data.icons.filter(Boolean) : [];
      if (icons.length) {
        const iconWrap = d.createElement("span");
        iconWrap.className = "cw-icon-select-icons";
        icons.forEach((icon, idx) => {
          const node = iconNode(icon);
          if (!node) return;
          if (idx && data.separator === "arrow") {
            const sep = d.createElement("span");
            sep.className = "cw-icon-select-sep";
            sep.textContent = "→";
            iconWrap.appendChild(sep);
          }
          iconWrap.appendChild(node);
        });
        if (iconWrap.childNodes.length) main.appendChild(iconWrap);
      }
    }

    const text = d.createElement("span");
    text.className = "cw-icon-select-text";
    if (data.label) {
      const label = d.createElement("span");
      label.className = "cw-icon-select-label";
      label.textContent = data.label;
      text.appendChild(label);
    }
    if (Array.isArray(data.badges) && data.badges.length) {
      const badges = d.createElement("span");
      badges.className = "cw-icon-select-badges";
      data.badges.forEach((badgeText) => {
        const badge = String(badgeText || "").trim();
        if (!badge) return;
        const badgeEl = d.createElement("span");
        badgeEl.className = "cw-icon-select-badge";
        badgeEl.textContent = badge;
        badges.appendChild(badgeEl);
      });
      if (badges.childNodes.length) text.appendChild(badges);
    }
    if (data.note && data.showNote !== false) {
      const note = d.createElement("span");
      note.className = "cw-icon-select-note";
      note.textContent = data.note;
      text.appendChild(note);
    }
    main.appendChild(text);
    return main;
  }

  function dataForOption(select, option, cfg) {
    const base = {
      value: String(option?.value || ""),
      label: String(option?.textContent || "").trim() || "-",
      note: "",
      icons: [],
      separator: "",
      disabled: !!option?.disabled,
    };
    const extra = typeof cfg?.getOptionData === "function"
      ? cfg.getOptionData(base.value, option, select) || {}
      : {};
    return { ...base, ...extra };
  }

  function sync(select, wrap, cfg) {
    const btn = wrap.querySelector(".cw-icon-select-btn");
    const labelHost = wrap.querySelector(".cw-icon-select-main");
    if (!btn || !labelHost) return;
    btn.disabled = !!select.disabled;
    wrap.classList.toggle("is-disabled", !!select.disabled);
    const option = select.options && select.selectedIndex >= 0 ? select.options[select.selectedIndex] : null;
    const data = dataForOption(select, option, cfg);
    const selectedData = Object.prototype.hasOwnProperty.call(data, "selectedLabel")
      ? { ...data, label: String(data.selectedLabel || "") }
      : data;
    labelHost.replaceWith(rowMain(selectedData));
    btn.insertBefore(btn.querySelector(".cw-icon-select-main"), btn.querySelector(".cw-icon-select-caret"));

    const menu = wrap.__cwMenu;
    if (!menu) return;
    [...menu.querySelectorAll(".cw-icon-select-item")].forEach((item) => {
      item.setAttribute("aria-selected", item.dataset.value === data.value ? "true" : "false");
    });
  }

  function buildMenu(select, wrap, cfg) {
    const menu = wrap.__cwMenu;
    if (!menu) return;
    menu.innerHTML = "";
    [...(select.options || [])].forEach((option) => {
      const data = dataForOption(select, option, cfg);
      const item = d.createElement("button");
      item.type = "button";
      item.className = "cw-icon-select-item";
      item.dataset.value = data.value;
      item.disabled = !!data.disabled;
      item.setAttribute("aria-selected", "false");
      item.appendChild(rowMain(data));
      item.addEventListener("click", (ev) => {
        ev.preventDefault();
        if (item.disabled) return;
        if (select.value !== data.value) {
          select.value = data.value;
          select.dispatchEvent(new Event("change", { bubbles: true }));
          select.dispatchEvent(new Event("input", { bubbles: true }));
        } else {
          sync(select, wrap, cfg);
        }
        closeAll(null);
      });
      menu.appendChild(item);
    });
  }

  function positionMenu(wrap, btn, menu) {
    if (!wrap || !btn || !menu || menu.classList.contains("hidden")) return;
    const rect = btn.getBoundingClientRect();
    const viewportWidth = window.innerWidth || d.documentElement.clientWidth || 0;
    const viewportHeight = window.innerHeight || d.documentElement.clientHeight || 0;
    const margin = 12;
    const gap = 8;
    const cfg = wrap.__cwNativeSelect?.__cwIconSelectCfg || {};
    const minWidth = Number(cfg.menuMinWidth || 0);
    const width = Math.max(0, Math.round(rect.width), Number.isFinite(minWidth) ? minWidth : 0);
    const left = Math.max(margin, Math.min(Math.round(rect.left), Math.max(margin, viewportWidth - width - margin)));
    const spaceBelow = Math.max(0, viewportHeight - rect.bottom - gap - margin);
    const spaceAbove = Math.max(0, rect.top - gap - margin);

    menu.style.width = `${width}px`;
    menu.style.left = `${left}px`;
    menu.style.top = "0px";
    menu.style.maxHeight = "320px";

    const naturalHeight = Math.max(menu.scrollHeight || 0, menu.offsetHeight || 0);
    const preferredHeight = Math.min(320, naturalHeight || 320);
    const openAbove = spaceAbove > spaceBelow && (spaceBelow < 180 || preferredHeight > spaceBelow);
    const availableSpace = openAbove ? spaceAbove : spaceBelow;
    const maxHeight = Math.max(120, Math.min(320, availableSpace || 320));
    const renderedHeight = Math.min(preferredHeight, maxHeight);
    const top = openAbove
      ? Math.max(margin, Math.round(rect.top - gap - renderedHeight))
      : Math.min(Math.max(margin, Math.round(rect.bottom + gap)), Math.max(margin, viewportHeight - renderedHeight - margin));

    menu.style.top = `${top}px`;
    menu.style.maxHeight = `${maxHeight}px`;
  }

  function enhance(select, cfg = {}) {
    if (!select) return select;
    bindAway();
    select.__cwIconSelectCfg = cfg;

    let wrap = select.nextElementSibling;
    const wrapMatchesSelect = wrap && wrap.classList && wrap.classList.contains("cw-icon-select") && wrap.__cwNativeSelect === select;
    if (!wrapMatchesSelect) {
      wrap = d.createElement("div");
      wrap.className = `cw-icon-select ${String(cfg.className || "").trim()}`.trim();
      wrap.innerHTML = `<button type="button" class="cw-icon-select-btn" aria-haspopup="listbox" aria-expanded="false"><span class="cw-icon-select-main"></span><span class="cw-icon-select-caret" aria-hidden="true"></span></button>`;
      const menu = d.createElement("div");
      menu.className = "cw-icon-select-menu hidden";
      menu.setAttribute("role", "listbox");
      d.body.appendChild(menu);
      wrap.__cwMenu = menu;
      select.classList.add("cw-icon-select-native");
      select.insertAdjacentElement("afterend", wrap);
    } else if (!wrap.__cwMenu) {
      const menu = d.createElement("div");
      menu.className = "cw-icon-select-menu hidden";
      menu.setAttribute("role", "listbox");
      d.body.appendChild(menu);
      wrap.__cwMenu = menu;
    }
    wrap.__cwNativeSelect = select;
    wrap.className = `cw-icon-select ${String(select.__cwIconSelectCfg?.className || "").trim()}`.trim();
    if (wrap.__cwMenu) {
      const hidden = wrap.__cwMenu.classList.contains("hidden");
      wrap.__cwMenu.className = `cw-icon-select-menu${hidden ? " hidden" : ""} ${String(select.__cwIconSelectCfg?.menuClassName || "").trim()}`.trim();
    }

    const legacyChev = wrap.nextElementSibling;
    if (legacyChev?.classList?.contains("chev")) legacyChev.style.display = "none";

    buildMenu(select, wrap, select.__cwIconSelectCfg);
    sync(select, wrap, select.__cwIconSelectCfg);

    const btn = wrap.querySelector(".cw-icon-select-btn");
    const menu = wrap.__cwMenu;
    if (btn && menu && btn.dataset.cwBound !== "1") {
      btn.dataset.cwBound = "1";
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const open = menu.classList.contains("hidden");
        closeAll(wrap);
        menu.classList.toggle("hidden", !open);
        wrap.classList.toggle("is-open", open);
        btn.setAttribute("aria-expanded", String(open));
        if (open) positionMenu(wrap, btn, menu);
        OPEN = open ? { wrap, btn, menu } : null;
      });
      menu.addEventListener("mousedown", (ev) => ev.stopPropagation());
      menu.addEventListener("click", (ev) => ev.stopPropagation());
    }

    if (!select.__cwSyncBound) {
      select.__cwSyncBound = true;
      select.addEventListener("change", () => sync(select, wrap, select.__cwIconSelectCfg));
    }
    if (!select.__cwOptionsObserver && typeof MutationObserver === "function") {
      const obs = new MutationObserver(() => {
        buildMenu(select, wrap, select.__cwIconSelectCfg);
        sync(select, wrap, select.__cwIconSelectCfg);
        if (OPEN?.wrap === wrap) positionMenu(wrap, btn, menu);
      });
      obs.observe(select, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["disabled", "label", "value", "selected"],
      });
      select.__cwOptionsObserver = obs;
    }
    if (!wrap.dataset.cwPosBound) {
      wrap.dataset.cwPosBound = "1";
      window.addEventListener("resize", () => {
        if (OPEN?.wrap === wrap) positionMenu(wrap, btn, menu);
      });
      window.addEventListener("scroll", () => {
        if (OPEN?.wrap === wrap) positionMenu(wrap, btn, menu);
      }, true);
    }
    return wrap;
  }

  function isPlainEligible(select) {
    if (!select || select.tagName !== "SELECT") return false;
    if (select.multiple) return false;
    if ((select.size | 0) > 1) return false;
    if (select.hidden) return false;
    if (select.style?.display === "none") return false;
    if (select.classList.contains("hidden")) return false;
    if (select.classList.contains("cw-icon-select-native")) return false;
    if (select.dataset.cwNativeSelect === "true") return false;
    if (select.dataset.cwIconSelect === "off") return false;
    if (select.classList.contains("lm-hidden")) return false;
    if (select.closest(".cx-ico-select")) return false;
    return true;
  }

  function enhancePlain(root = d) {
    const scope = root?.querySelectorAll ? root : d;
    scope.querySelectorAll('select').forEach((select) => {
      if (!isPlainEligible(select)) return;
      enhance(select, { className: "cw-plain-select" });
    });
  }

  function bindPlainAuto() {
    if (window[KEY]?.plainBound) return;
    window[KEY] = window[KEY] || {};
    window[KEY].plainBound = true;

    const run = (root) => enhancePlain(root);
    const boot = () => {
      run(d);

      if (typeof MutationObserver === "function") {
        const obs = new MutationObserver((mutations) => {
          for (const mutation of mutations) {
            if (mutation.type !== "childList") continue;
            mutation.addedNodes.forEach((node) => {
              if (!node || node.nodeType !== 1) return;
              run(node);
            });
          }
        });
        obs.observe(d.body || d.documentElement, { childList: true, subtree: true });
      }
    };

    if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", boot, { once: true });
    else boot();

    d.addEventListener("tab-changed", () => run(d));
  }

  window.CW = window.CW || {};
  bindPlainAuto();
  window.CW.IconSelect = { enhance, enhancePlain, closeAll };
})();
