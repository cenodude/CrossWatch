/* docs/assets/js/showcase.js */
/* CrossWatch - Feature and provider showcase content */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const showcasePages = Object.fromEntries(
  [...document.querySelectorAll(".showcase-panel")].map((panel) => [
    panel.dataset.key,
    { panel, route: panel.dataset.route },
  ]),
);
const showcaseRoutes = new Map();
for (const [key, { panel, route }] of Object.entries(showcasePages)) {
  for (const path of [route, ...JSON.parse(panel.dataset.aliases)])
    showcaseRoutes.set(path, key);
}
const showcaseDialog = document.querySelector("#showcase-dialog");
const showcaseStack = [];
let showcaseReturnFocus;
function resolveShowcase(link) {
  if (link.dataset.showcase) return link.dataset.showcase;
  const url = new URL(link.href, location.href);
  if (url.origin !== wiki) return null;
  const path = url.pathname.replace(/\.md$/, "").replace(/\/$/, "");
  return showcaseRoutes.get(path + url.hash) || showcaseRoutes.get(path);
}
function renderShowcase(key) {
  const { panel, route } = showcasePages[key];
  for (const page of Object.values(showcasePages))
    page.panel.hidden = page.panel !== panel;
  const title = panel.querySelector(".showcase-title");
  showcaseDialog.dataset.kind = panel.dataset.kind;
  showcaseDialog.setAttribute("aria-labelledby", title.id);
  showcaseDialog.setAttribute(
    "aria-describedby",
    panel.querySelector(".showcase-description").id,
  );
  document.querySelector("#showcase-docs").href = wiki + route;
  document.querySelector("#showcase-back").hidden = showcaseStack.length < 2;
  showcaseDialog.scrollTop = 0;
  title.focus({ preventScroll: true });
}
showcaseDialog.addEventListener("click", (event) => {
  const button = event.target.closest("[data-related]");
  if (button) openShowcase(button.dataset.related);
});
function openShowcase(key, trigger) {
  if (!showcasePages[key]) return;
  if (!showcaseDialog.open) {
    showcaseReturnFocus = trigger?.closest("#search-dialog")
      ? document.querySelector(".search-trigger")
      : trigger || document.activeElement;
    document
      .querySelectorAll("dialog[open]")
      .forEach((dialog) => dialog.close());
    showcaseStack.length = 0;
    showcaseStack.push(key);
    renderShowcase(key);
    showcaseDialog.showModal();
    showcasePages[key].panel
      .querySelector(".showcase-title")
      .focus({ preventScroll: true });
  } else {
    showcaseStack.push(key);
    renderShowcase(key);
  }
}
document.addEventListener("click", (event) => {
  const link = event.target.closest("a");
  if (
    !link ||
    link.closest("footer,#showcase-dialog") ||
    event.ctrlKey ||
    event.metaKey ||
    event.shiftKey ||
    event.altKey ||
    event.button !== 0
  )
    return;
  const key = resolveShowcase(link);
  if (!key || !showcasePages[key]) return;
  event.preventDefault();
  openShowcase(key, link);
});
document.querySelectorAll("a").forEach((link) => {
  if (!link.closest("footer,#showcase-dialog") && resolveShowcase(link))
    link.setAttribute("aria-haspopup", "dialog");
});
document.querySelector("#showcase-back").addEventListener("click", () => {
  if (showcaseStack.length < 2) return;
  showcaseStack.pop();
  renderShowcase(showcaseStack.at(-1));
});
showcaseDialog.addEventListener("close", () => {
  if (
    showcaseReturnFocus?.isConnected &&
    showcaseReturnFocus.getClientRects().length
  )
    showcaseReturnFocus.focus({ preventScroll: true });
});
document.querySelector("#showcase-start").addEventListener("click", () => {
  showcaseReturnFocus = document.querySelector("#copy-compose");
  showcaseDialog.close();
  document.querySelector("#start").scrollIntoView({
    behavior: matchMedia("(prefers-reduced-motion: reduce)").matches
      ? "instant"
      : "smooth",
  });
  document.querySelector("#copy-compose").focus({ preventScroll: true });
});
