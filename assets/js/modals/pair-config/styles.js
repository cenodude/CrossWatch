/* assets/js/modals/pair-config/styles.js */
/* Stylesheet loader for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

let pairConfigStylesReady = null;

export function ensurePairConfigStyles(){
  const id = "cx-pair-config-css";
  const existing = document.getElementById(id);
  if(existing?.tagName === "LINK") return pairConfigStylesReady || Promise.resolve();
  existing?.remove();

  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if(version) cssUrl.searchParams.set("v", version);

  const link = document.createElement("link");
  link.id = id;
  link.rel = "stylesheet";
  link.href = cssUrl.href;

  pairConfigStylesReady = new Promise(resolve => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
  });
  document.head.appendChild(link);
  return pairConfigStylesReady;
}
