(function () {
  const d = document;
  const KEY = "__CW_ICON_SELECT__";

  function injectCss() {
    if (d.getElementById("cw-icon-select-css")) return;
    const s = d.createElement("style");
    s.id = "cw-icon-select-css";
    s.textContent = `
.cw-icon-select-native{display:none!important}
.cw-icon-select{position:relative;display:block;min-width:0}
.cw-icon-select-btn{width:100%;display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:42px;padding:0 12px;border:1px solid rgba(255,255,255,.09);border-radius:14px;background:linear-gradient(180deg,rgba(8,10,18,.82),rgba(7,8,15,.92));color:#eef3ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);cursor:pointer}
.cw-icon-select-btn:focus-visible{outline:none;box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.03)}
.cw-icon-select.is-open{z-index:520}
.cw-icon-select-main{display:flex;align-items:center;gap:10px;min-width:0;flex:1 1 auto}
.cw-icon-select-text{display:grid;gap:2px;min-width:0;flex:1 1 auto}
.cw-icon-select-text:empty{display:none}
.cw-icon-select-label{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:left}
.cw-icon-select-note{font-size:11px;color:rgba(197,206,224,.68);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cw-icon-select-badges{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.cw-icon-select-badge{display:inline-flex;align-items:center;justify-content:center;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:#eef3ff;font-size:11px;font-weight:800;letter-spacing:.05em;text-transform:uppercase}
.cw-icon-select-badges .cw-icon-select-badge:first-child{border-color:rgba(92,96,182,.24);background:rgba(92,96,182,.14);color:#eef1ff}
.cw-icon-select-badges .cw-icon-select-badge:nth-child(2){border-color:rgba(91,160,255,.22);background:rgba(91,160,255,.12);color:#eef7ff}
.cw-icon-select-caret{position:relative;display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;flex:0 0 14px;opacity:.72;transition:transform .16s ease,opacity .16s ease}
.cw-icon-select-caret::before{content:"";display:block;width:7px;height:7px;border-right:2px solid rgba(238,243,255,.92);border-bottom:2px solid rgba(238,243,255,.92);transform:translateY(-1px) rotate(45deg);border-radius:1px}
.cw-icon-select:hover .cw-icon-select-caret{opacity:.9}
.cw-icon-select.is-open .cw-icon-select-caret{transform:rotate(180deg)}
.cw-icon-select-icons{display:inline-flex;align-items:center;gap:6px;flex:0 0 auto}
.cw-icon-select-sep{display:inline-flex;align-items:center;justify-content:center;min-width:16px;color:rgba(214,222,242,.68);font-size:15px;line-height:1;transform:translateY(-1px)}
.cw-icon-select-icon{width:18px;height:18px;object-fit:contain;display:block;flex:0 0 18px}
.cw-icon-select-icon.empty{display:inline-flex;align-items:center;justify-content:center;border-radius:999px;background:rgba(255,255,255,.05);color:rgba(236,241,255,.7);font-size:10px;font-weight:900}
.cw-icon-select-menu{position:fixed;left:0;top:0;z-index:640;display:grid;gap:6px;padding:6px;border:1px solid rgba(255,255,255,.10);border-radius:16px;background:#171a29;box-shadow:0 18px 44px rgba(0,0,0,.52);backdrop-filter:blur(14px) saturate(115%);-webkit-backdrop-filter:blur(14px) saturate(115%);max-height:320px;overflow:auto;pointer-events:auto}
.cw-icon-select-menu.hidden{display:none}
.cw-icon-select-item{width:100%;display:flex;align-items:center;gap:10px;padding:10px 11px;border:1px solid transparent;border-radius:12px;background:transparent;color:#eef3ff;text-align:left;cursor:pointer}
.cw-icon-select-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10)}
.cw-icon-select-item[aria-selected="true"]{background:rgba(92,96,182,.14);border-color:rgba(92,96,182,.18)}
.cw-icon-select-item:disabled{opacity:.45;cursor:not-allowed}
`;
    d.head.appendChild(s);
  }

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
    return null;
  }

  function rowMain(data) {
    const main = d.createElement("span");
    main.className = "cw-icon-select-main";

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
    const option = select.options && select.selectedIndex >= 0 ? select.options[select.selectedIndex] : null;
    const data = dataForOption(select, option, cfg);
    labelHost.replaceWith(rowMain(data));
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
    menu.style.left = `${Math.round(rect.left)}px`;
    menu.style.top = `${Math.round(rect.bottom + 8)}px`;
    menu.style.width = `${Math.round(rect.width)}px`;
  }

  function enhance(select, cfg = {}) {
    if (!select) return select;
    injectCss();
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
