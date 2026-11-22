/* docs/assets/js/site.js */
/* CrossWatch - Public showcase interactions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const imageBase = document.body.dataset.imageBase;
const wiki = "https://wiki.crosswatch.app";
const connectionPath = "/crosswatch/settings/connections/";
const providers = [...document.querySelectorAll(".integration-card")].map(
  (card) => [
    card.querySelector("strong").textContent,
    card.querySelector("img").getAttribute("src").split("/").at(-1),
    card.dataset.providerGroup,
    card.querySelector("small").textContent,
    new URL(card.href).pathname.slice(connectionPath.length),
  ],
);

let activeFilter = "all";
const providerSearch = document.querySelector("#provider-search");
const providerGrid = document.querySelector("#integration-grid");

function renderProviders() {
  const query = providerSearch.value.trim().toLowerCase();
  const filtered = providers.filter(
    (provider) =>
      (activeFilter === "all" ||
        provider[2].split(" ").includes(activeFilter)) &&
      `${provider[0]} ${provider[3]}`.toLowerCase().includes(query),
  );
  providerGrid.replaceChildren();
  for (const [name, image, , category, path] of filtered) {
    const link = document.createElement("a");
    link.className = "integration-card";
    link.href = wiki + connectionPath + path;
    link.setAttribute("aria-label", `Explore ${name}`);
    link.setAttribute("aria-haspopup", "dialog");
    const logo = document.createElement("img");
    logo.src = `${imageBase}/providers/${image}`;
    logo.alt = "";
    logo.decoding = "async";
    logo.loading = "lazy";
    logo.width = 36;
    logo.height = 36;
    const text = document.createElement("span");
    const title = document.createElement("strong");
    title.textContent = name;
    const description = document.createElement("small");
    description.textContent = category;
    text.append(title, description);
    link.append(logo, text);
    providerGrid.append(link);
  }
  if (!filtered.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No matching services. Try another name or category.";
    providerGrid.append(empty);
  }
  document.querySelector("#integration-count").textContent =
    `${filtered.length} of ${providers.length} integrations`;
}

document.querySelectorAll("[data-filter]").forEach((button) =>
  button.addEventListener("click", () => {
    activeFilter = button.dataset.filter;
    document.querySelectorAll("[data-filter]").forEach((item) => {
      item.classList.toggle("active", item === button);
      item.setAttribute("aria-pressed", String(item === button));
    });
    renderProviders();
  }),
);
providerSearch.addEventListener("input", renderProviders);
renderProviders();

const shots = {
  sync: [
    `${imageBase}/screenshots/sync-hub.webp`,
    "CrossWatch Sync Hub showing sync status, feature controls, provider statistics, scheduling and the synced watchlist.",
    "Your sync engine at a glance: features, providers, schedules and recent results.",
    2226,
    1177,
  ],
  tracker: [
    `${imageBase}/screenshots/tracker.webp`,
    "CrossWatch local Tracker with continue watching, playback progress, upcoming episodes and watch history navigation.",
    "Your local tracker brings watched history, ratings, playback progress and upcoming episodes together.",
    2235,
    1236,
  ],
  hub: [
    `${imageBase}/screenshots/hub.webp`,
    "CrossWatch Hub showing playback progress across providers, recently added watchlist titles and recent sync activity.",
    "One view across your connected services, with shared progress, watchlists and recent sync activity.",
    2242,
    1231,
  ],
  editor: [
    `${imageBase}/screenshots/editor.webp`,
    "CrossWatch Editor with editable history records, title IDs, provider selection, mapping and block rules.",
    "Inspect the records behind a sync. Edit history, correct mappings and manage what CrossWatch syncs.",
    2214,
    1237,
  ],
};
const screenshot = document.querySelector("#product-screenshot");
const screenshotButton = document.querySelector(".screenshot-open");
const shotTabs = [...document.querySelectorAll("[data-shot]")];

function selectShot(button) {
  const [src, alt, caption, width, height] = shots[button.dataset.shot];
  screenshot.src = src;
  screenshot.alt = alt;
  screenshot.width = width;
  screenshot.height = height;
  document.querySelector("#shot-caption").textContent = caption;
  document.querySelector("#shot-count").textContent =
    `0${shotTabs.indexOf(button) + 1} / 04`;
  screenshotButton.setAttribute(
    "aria-label",
    `Enlarge ${button.textContent.trim()} screenshot`,
  );
  document
    .querySelector("#screenshot-panel")
    .setAttribute("aria-labelledby", button.id);
  shotTabs.forEach((tab) => {
    tab.setAttribute("aria-selected", String(tab === button));
    tab.tabIndex = tab === button ? 0 : -1;
  });
}
shotTabs.forEach((button, index) => {
  button.addEventListener("click", () => selectShot(button));
  button.addEventListener("keydown", (event) => {
    let next;
    if (event.key === "ArrowRight") next = (index + 1) % shotTabs.length;
    if (event.key === "ArrowLeft")
      next = (index - 1 + shotTabs.length) % shotTabs.length;
    if (event.key === "Home") next = 0;
    if (event.key === "End") next = shotTabs.length - 1;
    if (next === undefined) return;
    event.preventDefault();
    shotTabs[next].focus();
    selectShot(shotTabs[next]);
  });
});

const imageDialog = document.querySelector("#image-dialog");
document.querySelectorAll("[data-gallery]").forEach((link) => {
  link.addEventListener("click", () => {
    const tab = shotTabs.find(
      (item) => item.dataset.shot === link.dataset.gallery,
    );
    if (tab) selectShot(tab);
  });
});
const imageViewport = document.querySelector(".screenshot-viewport");
const imageZoom = document.querySelector("#image-zoom");
function updateImageDialog() {
  imageZoom.setAttribute("aria-pressed", "false");
  imageZoom.textContent = "Actual size";
  imageViewport.classList.remove("is-zoomed");
  imageViewport.scrollTo(0, 0);
  const largeImage = document.querySelector("#large-screenshot");
  largeImage.src = screenshot.src.replace(/\.webp$/, ".png");
  largeImage.alt = screenshot.alt;
  largeImage.width = screenshot.width;
  largeImage.height = screenshot.height;
  const active = shotTabs.find(
    (tab) => tab.getAttribute("aria-selected") === "true",
  );
  document.querySelector("#image-title").textContent =
    `CrossWatch — ${active.textContent.trim()}`;
  document.querySelector("#image-count").textContent =
    `${shotTabs.indexOf(active) + 1} of ${shotTabs.length}`;
}
screenshotButton.addEventListener("click", () => {
  updateImageDialog();
  imageDialog.showModal();
});
imageZoom.addEventListener("click", () => {
  const zoomed = imageZoom.getAttribute("aria-pressed") !== "true";
  imageZoom.setAttribute("aria-pressed", String(zoomed));
  imageZoom.textContent = zoomed ? "Fit to screen" : "Actual size";
  imageViewport.classList.toggle("is-zoomed", zoomed);
  imageViewport.scrollTo(0, 0);
});
function stepScreenshot(direction) {
  const index = shotTabs.findIndex(
    (tab) => tab.getAttribute("aria-selected") === "true",
  );
  selectShot(shotTabs[(index + direction + shotTabs.length) % shotTabs.length]);
  updateImageDialog();
}
document
  .querySelector("#previous-shot")
  .addEventListener("click", () => stepScreenshot(-1));
document
  .querySelector("#next-shot")
  .addEventListener("click", () => stepScreenshot(1));
imageDialog.addEventListener("keydown", (event) => {
  if (imageViewport.classList.contains("is-zoomed")) return;
  if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
  event.preventDefault();
  stepScreenshot(event.key === "ArrowRight" ? 1 : -1);
});

document
  .querySelector("#copy-compose")
  .addEventListener("click", async (event) => {
    const button = event.currentTarget;
    const content = document.querySelector("#compose-code").textContent;
    const status = document.querySelector("#copy-status");
    try {
      await navigator.clipboard.writeText(content);
      button.querySelector("span").textContent = "Copied!";
      status.textContent = "Docker Compose configuration copied.";
      window.setTimeout(() => {
        button.querySelector("span").textContent = "Copy";
      }, 2500);
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(document.querySelector("#compose-code"));
      selection.removeAllRanges();
      selection.addRange(range);
      button.querySelector("span").textContent = "Select & copy";
      status.textContent =
        "Automatic copying is unavailable. The configuration is selected; press Control C or Command C to copy.";
    }
  });

const menuButton = document.querySelector(".menu-toggle");
const navigation = document.querySelector("#navigation");
function closeMenu() {
  menuButton.setAttribute("aria-expanded", "false");
  menuButton.setAttribute("aria-label", "Open navigation");
  navigation.classList.remove("is-open");
}
menuButton.addEventListener("click", () => {
  const open = menuButton.getAttribute("aria-expanded") !== "true";
  menuButton.setAttribute("aria-expanded", String(open));
  menuButton.setAttribute(
    "aria-label",
    open ? "Close navigation" : "Open navigation",
  );
  navigation.classList.toggle("is-open", open);
});
navigation
  .querySelectorAll("a")
  .forEach((link) => link.addEventListener("click", closeMenu));
document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  closeMenu();
  document.querySelectorAll("dialog[open]").forEach((dialog) => {
    event.preventDefault();
    dialog.close();
  });
});

const guides = [
  [
    "First-time setup",
    "Installation, accounts and your first dry run",
    "/getting-started/first-time-setup",
  ],
  [
    "Docker installation",
    "Docker Compose, ports and persistent storage",
    "/getting-started/installation/docker-setup",
  ],
  [
    "Support matrix",
    "Compare history, ratings, watchlists and progress support",
    "/getting-started/support-matrix",
  ],
  [
    "Interactive Sync",
    "Review additions, updates and removals before applying",
    "/crosswatch/main-dashboard/interactive-sync",
  ],
  [
    "Configure pairs",
    "Sync directions and features for each connection",
    "/crosswatch/settings/configure-pairs",
  ],
  [
    "Watcher",
    "Playback tracking and scrobbling routes",
    "/crosswatch/settings/scrobbler/watcher",
  ],
  [
    "Profiles",
    "Separate accounts, household sync pairs and schedules",
    "/crosswatch/settings/connections/profiles",
  ],
  [
    "Managed users",
    "Configure the people using your installation",
    "/crosswatch/settings/connections/managed-users",
  ],
  [
    "Scheduling",
    "Set up automatic sync runs",
    "/crosswatch/settings/scheduling",
  ],
  [
    "Captures",
    "Snapshots of watchlists, ratings and history",
    "/crosswatch/main-dashboard/navigation/captures",
  ],
  [
    "Backup & restore",
    "Protect configuration, mappings and tracker data",
    "/crosswatch/main-dashboard/tools/backup-and-restore",
  ],
  [
    "Mappings & episode groups",
    "Fix incorrect matches and episode numbering",
    "/crosswatch/main-dashboard/navigation/editor/mappings-and-episode-groups",
  ],
  [
    "Anime ID mapping",
    "Connect anime identifiers between providers",
    "/crosswatch/settings/connections/metadata/anime-id-mapping",
  ],
  [
    "Published lists",
    "Lists for Kometa, Radarr and Sonarr",
    "/crosswatch/main-dashboard/navigation/playlists/published-lists",
  ],
  [
    "Events & activity",
    "Explore titles, sync runs and raw events",
    "/crosswatch/main-dashboard/tools/events",
  ],
  [
    "Diagnostic logs",
    "Read, download and share logs for troubleshooting",
    "/crosswatch/main-dashboard/tools/logs",
  ],
  [
    "Analyzer",
    "Compare snapshots and investigate differences",
    "/crosswatch/main-dashboard/tools/analyzer",
  ],
  [
    "Topology advisor",
    "Understand sync conflicts and loops",
    "/crosswatch/main-dashboard/tools/sync-topology-advisor",
  ],
  [
    "Quick Add",
    "Add media activity outside your server flow",
    "/crosswatch/main-dashboard/navigation/quick-add",
  ],
  [
    "Local tracker",
    "Keep your own watch history in CrossWatch",
    "/crosswatch/main-dashboard/navigation/cw-tracker",
  ],
  ...providers.map(([name, , , category, path]) => [
    `${name} setup`,
    `${category} connection guide`,
    connectionPath + path,
  ]),
];
const searchDialog = document.querySelector("#search-dialog");
const docsInput = document.querySelector("#docs-search");
function renderGuides() {
  const query = docsInput.value.trim().toLowerCase();
  const results = guides.filter((guide) =>
    `${guide[0]} ${guide[1]}`.toLowerCase().includes(query),
  );
  const list = document.querySelector("#search-results");
  list.replaceChildren();
  for (const [title, description, path] of results) {
    const link = document.createElement("a");
    link.href = wiki + path;
    link.setAttribute("aria-haspopup", "dialog");
    const label = document.createElement("strong");
    label.textContent = title;
    const detail = document.createElement("small");
    detail.textContent = description;
    link.append(label, detail);
    list.append(link);
  }
  if (!results.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No matches. Try a service name or a feature.";
    list.append(empty);
  }
  document.querySelector("#search-count").textContent =
    `${results.length} results`;
}
function openSearch() {
  closeMenu();
  document.querySelectorAll("dialog[open]").forEach((dialog) => dialog.close());
  docsInput.value = "";
  renderGuides();
  searchDialog.showModal();
  docsInput.focus();
}
document
  .querySelectorAll(".search-trigger")
  .forEach((button) => button.addEventListener("click", openSearch));
docsInput.addEventListener("input", renderGuides);
document.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    if (imageDialog.open) imageDialog.close();
    if (searchDialog.open) searchDialog.close();
    else openSearch();
  }
});
document.querySelectorAll("dialog").forEach((dialog) => {
  dialog
    .querySelector(".dialog-close")
    .addEventListener("click", () => dialog.close());
  dialog.addEventListener("click", (event) => {
    const bounds = dialog.getBoundingClientRect();
    if (
      event.target === dialog &&
      (event.clientX < bounds.left ||
        event.clientX > bounds.right ||
        event.clientY < bounds.top ||
        event.clientY > bounds.bottom)
    )
      dialog.close();
  });
});
