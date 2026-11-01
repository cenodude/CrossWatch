/* assets/helpers/dialog-focus.js */
/* CrossWatch - Keyboard focus containment for standalone dialogs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const bound = new WeakSet();
  const active = [];
  const NS = (window.CW ||= {});
  const prune = () => {
    for (let i = active.length - 1; i >= 0; i--) {
      if (!active[i].root.isConnected) active.splice(i, 1);
    }
  };
  document.addEventListener("keydown", event => {
    if (event.defaultPrevented || !["Escape", "Tab"].includes(event.key)) return;
    prune();
    const current = active.at(-1);
    if (!current || !current.root.isConnected || !current.root.getClientRects().length) return;
    if (document.querySelector('dialog[open], #cw-trailer.show')) return;
    const focusedDialog = document.activeElement?.closest('[role="dialog"], dialog');
    if (focusedDialog && !current.root.contains(focusedDialog) && !focusedDialog.contains(current.root)) return;
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopImmediatePropagation();
      current.close();
    } else if (!current.root.contains(document.activeElement)) {
      event.preventDefault();
      current.root.focus({preventScroll:true});
    }
  });
  NS.DialogFocus = {
    activate(root, close) {
      prune();
      this.deactivate(root);
      root.tabIndex = -1;
      this.trap(root);
      active.push({root, close});
    },
    deactivate(root) {
      const index = active.findIndex(entry => entry.root === root);
      if (index >= 0) active.splice(index, 1);
    },
    trap(root) {
      if (!root || bound.has(root)) return;
      bound.add(root);
      root.addEventListener("keydown", event => {
        if (event.key !== "Tab" || event.defaultPrevented) return;
        const items = [...root.querySelectorAll('a[href],button,input,select,textarea,summary,iframe,[contenteditable],audio[controls],video[controls],[tabindex]')]
          .filter(node => !node.disabled && (node.tabIndex >= 0 || (!node.hasAttribute('tabindex') && (node.isContentEditable || node.matches('audio[controls],video[controls]')))) && node.getClientRects().length && !node.closest('[hidden],[inert]'));
        const first = items[0], last = items[items.length - 1];
        const focused = document.activeElement;
        const entering = focused === root || !root.contains(focused);
        if (!first) {
          event.preventDefault();
          root.tabIndex = -1;
          root.focus({preventScroll:true});
        } else if (event.shiftKey && (focused === first || entering)) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && (focused === last || entering)) {
          event.preventDefault();
          first.focus();
        }
      });
    }
  };
})();
