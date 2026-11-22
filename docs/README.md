<!-- docs/README.md -->
<!-- CrossWatch - Public showcase content -->
<!-- Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) -->
<header class="site-header">
  <div class="nav-wrap">
    <a class="brand" href="#" aria-label="CrossWatch home"
      ><img
        src="{{ '/images/providers/CROSSWATCH.svg' | relative_url }}"
        alt=""
        width="34"
        height="34"
      />CrossWatch<span class="brand-dot">.</span></a
    >
    <nav id="navigation" aria-label="Main navigation">
      <a href="#product">Product</a><a href="#integrations">Integrations</a
      ><a href="#docs">Documentation</a
      ><a href="https://github.com/cenodude/CrossWatch"
        >GitHub
        <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
          <path
            d="M9 18H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v3M14 14h6v6h-6z"
          /></svg
      ></a>
      <a href="https://discord.gg/c9cjzHGve">Discord</a>
    </nav>
    <div class="nav-actions">
      <button class="search-trigger" aria-label="Explore CrossWatch">
        <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="10.5" cy="10.5" r="6.5" />
          <path d="m16 16 5 5" /></svg
        ><span>Explore</span><kbd>Ctrl K</kbd></button
      ><a class="button button-small button-light" href="#start"
        >Get started</a
      ><button
        class="menu-toggle"
        aria-label="Open navigation"
        aria-expanded="false"
        aria-controls="navigation"
      >
        <span></span><span></span>
      </button>
    </div>
  </div>
</header>
<main id="main">
  <section class="hero wrap">
    <div class="hero-copy">
      <a
        class="eyebrow release-link"
        href="https://github.com/cenodude/CrossWatch/releases/latest"
        ><span class="status-dot"></span> OPEN SOURCE. SELF-HOSTED.
        YOURS.</a
      >
      <h1>Your media.<br /><span class="gradient-text">One home.</span></h1>
      <p>
        Connect your providers, track what you watch, keep everything in
        sync and protect your data. All in one self-hosted hub.
      </p>
      <div class="hero-actions">
        <a class="button button-primary" href="#start"
          >Get CrossWatch<svg
            class="icon"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path d="M12 3v11m-4-4 4 4 4-4M5 15v5h14v-5" /></svg></a
        ><a class="button button-ghost" href="#product"
          ><svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
            <path d="m9 5 11 7-11 7z" /></svg
          >Take a closer look</a
        >
      </div>
      <div class="hero-assurances">
        <span>Runs on your server</span
        ><span>Built for your household</span>
      </div>
    </div>
    <div class="product-roles" aria-label="What CrossWatch brings together">
      <p class="roles-intro">ONE CROSSWATCH. YOUR WHOLE SETUP.</p>
      <a class="product-role" href="#product" data-showcase="hub"
        ><svg viewBox="0 0 24 24" aria-hidden="true">
          <rect x="8" y="8" width="8" height="8" rx="2" />
          <path
            d="M12 3v5m0 8v5M3 12h5m8 0h5M5 5l3 3m8 8 3 3M5 19l3-3m8-8 3-3"
          />
        </svg>
        <div>
          <h2>Central hub</h2>
          <p>
            Your providers, watchlists and activity. Together in one place.
          </p>
        </div></a
      >
      <a
        class="product-role"
        href="#sync-features"
        data-showcase="automatic"
        ><svg viewBox="0 0 24 24" aria-hidden="true">
          <path
            d="M4 9a8 8 0 0 1 14-4l2 3M20 15a8 8 0 0 1-14 4l-2-3M20 3v5h-5M4 21v-5h5"
          />
        </svg>
        <div>
          <h2>Sync engine</h2>
          <p>Keep your services in step, with control over what changes.</p>
        </div></a
      >
      <a class="product-role" href="#product" data-showcase="tracker"
        ><svg viewBox="0 0 24 24" aria-hidden="true">
          <rect x="3" y="3" width="18" height="18" rx="3" />
          <path d="m7 12 3 3 7-7" />
        </svg>
        <div>
          <h2>Your own tracker</h2>
          <p>Keep your history, ratings and progress on your own server.</p>
        </div></a
      >
      <a class="product-role" href="#recovery" data-showcase="backup"
        ><svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3 4 6v6c0 5 8 9 8 9s8-4 8-9V6zM8 12l3 3 5-6" />
        </svg>
        <div>
          <h2>Backup &amp; recovery</h2>
          <p>
            Capture your data, protect your setup and restore when needed.
          </p>
        </div></a
      >
    </div>
  </section>
  <div class="capabilities-band">
    <div class="wrap">
      <span>WATCH HISTORY</span><span>RATINGS</span><span>WATCHLISTS</span
      ><span>PLAYBACK PROGRESS</span><span>COLLECTIONS</span
      ><span>PLAYLISTS</span>
    </div>
  </div>
  <section class="section wrap product-section" id="product">
    <div class="section-heading">
      <div>
        <p class="eyebrow">ONE HOME FOR YOUR MEDIA LIFE</p>
        <h2>
          Less keeping track.<br /><span class="muted-heading"
            >More pressing play.</span
          >
        </h2>
      </div>
      <p>
        Manage your connected providers, explore your own tracker and see
        your media activity together. This is CrossWatch in use.
      </p>
    </div>
    <div class="product-window">
      <div class="window-bar">
        <span class="window-title"
          ><img
            src="{{ '/images/providers/CROSSWATCH.svg' | relative_url }}"
            alt=""
            width="20"
            height="20"
          />
          A look inside CrossWatch</span
        >
        <div
          class="screenshot-tabs"
          role="tablist"
          aria-label="Product screenshots"
        >
          <button
            role="tab"
            id="tab-sync"
            aria-controls="screenshot-panel"
            aria-selected="true"
            data-shot="sync"
          >
            Sync Hub
          </button>
          <button
            role="tab"
            id="tab-tracker"
            aria-controls="screenshot-panel"
            aria-selected="false"
            tabindex="-1"
            data-shot="tracker"
          >
            Tracker
          </button>
          <button
            role="tab"
            id="tab-hub"
            aria-controls="screenshot-panel"
            aria-selected="false"
            tabindex="-1"
            data-shot="hub"
          >
            Hub
          </button>
          <button
            role="tab"
            id="tab-editor"
            aria-controls="screenshot-panel"
            aria-selected="false"
            tabindex="-1"
            data-shot="editor"
          >
            Editor
          </button>
        </div>
      </div>
      <div id="screenshot-panel" role="tabpanel" aria-labelledby="tab-sync">
        <button
          class="screenshot-open"
          aria-label="Enlarge Sync Hub screenshot"
        >
          <img
            id="product-screenshot"
            loading="lazy"
            decoding="async"
            src="{{ '/images/screenshots/sync-hub.webp' | relative_url }}"
            alt="CrossWatch Sync Hub showing sync status, feature controls, provider statistics, scheduling and the synced watchlist."
            width="2226"
            height="1177"
          /><span class="enlarge-label"
            ><svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
              <path d="M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5" />
            </svg>
            View full resolution</span
          >
        </button>
        <div class="screenshot-caption">
          <p id="shot-caption" aria-live="polite">
            Your sync engine at a glance: features, providers, schedules and
            recent results.
          </p>
          <span id="shot-count">01 / 04</span>
        </div>
      </div>
    </div>
    <div class="product-footnotes">
      <span><span class="small-mark">◇</span> Web UI & CLI</span
      ><span><span class="small-mark">◇</span> Desktop & mobile</span
      ><span
        ><span class="small-mark">◇</span> Manual or scheduled sync</span
      >
    </div>
  </section>
  <section class="section wrap feature-section" id="sync-features">
    <div class="section-heading">
      <div>
        <p class="eyebrow">AUTOMATION, ON YOUR TERMS</p>
        <h2>
          It does the busywork.<br /><span class="muted-heading"
            >You call the shots.</span
          >
        </h2>
      </div>
      <p>
        Run automatically, review changes yourself, or build a workflow
        around your setup.
      </p>
    </div>
    <div class="sync-options">
      <article class="sync-option">
        <span class="feature-kicker blue">AUTOMATIC SYNC</span>
        <h3>Set it up.<br />Let it sync.</h3>
        <p>
          Choose your pairs and features, then let scheduled runs keep your
          services in sync using your configured rules.
        </p>
        <a
          class="text-link"
          href="https://wiki.crosswatch.app/crosswatch/settings/scheduling#standard-scheduling"
          >Explore Automatic Sync</a
        >
      </article>
      <article class="sync-option">
        <span class="feature-kicker violet">INTERACTIVE SYNC</span>
        <h3>See the changes.<br />Then make the call.</h3>
        <p>
          Preview additions, updates and removals. Resolve matches and apply
          only the changes you choose.
        </p>
        <a
          class="text-link"
          href="https://wiki.crosswatch.app/crosswatch/main-dashboard/interactive-sync"
          >Explore Interactive Sync</a
        >
      </article>
      <article class="sync-option">
        <span class="feature-kicker mint">AUTOMATION</span>
        <h3>Connect the steps.<br />Let them run.</h3>
        <p>
          Build workflows with ordered sync steps, scheduled captures and
          triggers from Watcher or webhook activity.
        </p>
        <a
          class="text-link"
          href="https://wiki.crosswatch.app/crosswatch/settings/scheduling#advanced-scheduling"
          >Explore Automation</a
        >
      </article>
    </div>
    <div class="supporting-features">
      <div>
        <h3>Playback tracking with Watcher</h3>
        <p>
          Send playback activity to your trackers automatically. No Plex
          Pass or Emby Premiere required for Watcher.
        </p>
        <a
          class="text-link"
          href="https://wiki.crosswatch.app/crosswatch/settings/scrobbler/watcher"
          >Meet Watcher</a
        >
      </div>
      <div>
        <h3>Built for your household</h3>
        <p>
          Keep accounts, sync pairs and schedules separate with profiles and
          managed users.
        </p>
        <a
          class="text-link"
          href="https://wiki.crosswatch.app/crosswatch/settings/connections/profiles"
          >Explore profiles</a
        >
      </div>
    </div>
  </section>
  <section class="integrations-section section" id="integrations">
    <div class="wrap">
      <div class="section-heading">
        <div>
          <p class="eyebrow">A GOOD FIT FOR YOUR SETUP</p>
          <h2>
            Your favorites.<br /><span class="gradient-text"
              >Already invited.</span
            >
          </h2>
        </div>
        <div>
          <p>
            Media servers, players, trackers and anime services. Find your
            connection and discover what it brings to CrossWatch.
          </p>
        </div>
      </div>

      <div class="integration-toolbar">
        <div
          class="filter-list"
          role="group"
          aria-label="Filter integrations"
        >
          <button class="active" data-filter="all" aria-pressed="true">
            All integrations</button
          ><button data-filter="server" aria-pressed="false">Servers</button
          ><button data-filter="tracker" aria-pressed="false">
            Trackers</button
          ><button data-filter="anime" aria-pressed="false">Anime</button
          ><button data-filter="player" aria-pressed="false">Players</button
          ><button data-filter="other" aria-pressed="false">
            Companions
          </button>
        </div>
        <label class="provider-search"
          ><svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
            <circle cx="10.5" cy="10.5" r="6.5" />
            <path d="m16 16 5 5" /></svg
          ><input
            id="provider-search"
            type="search"
            aria-label="Find an integration"
            placeholder="Find your service"
        /></label>
      </div>
      <div class="integration-grid" id="integration-grid">{% include provider-cards.html %}</div>
      <p
        id="integration-count"
        class="integration-count"
        aria-live="polite"
      ></p>
      <p class="integration-note">
        Explore the integrations, then make the setup your own.
      </p>
    </div>
  </section>
  <section class="section wrap tools-section">
    <div class="section-heading">
      <div>
        <p class="eyebrow">GO BEYOND THE SYNC</p>
        <h2>
          A little more order.<br /><span class="muted-heading"
            >A lot more possibility.</span
          >
        </h2>
      </div>
      <p>
        The tools you need when your media life gets a little more
        interesting.
      </p>
    </div>
    <div class="tools-grid">
      <a
        class="tool-card"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/navigation/captures"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <path d="M4 7h4l2-3h4l2 3h4v13H4z" />
          <circle cx="12" cy="13" r="4" />
        </svg>
        <h3>Keep a way back.</h3>
        <p>
          Capture watchlists, ratings and history. Compare snapshots and
          restore when you need to.
        </p>
        <span>Captures & snapshots</span></a
      >
      <a
        class="tool-card"
        id="recovery"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/tools/backup-and-restore"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <path d="M12 3 4 6v6c0 5 8 9 8 9s8-4 8-9V6zM8 12l3 3 5-6" />
        </svg>
        <h3>Protect your setup.</h3>
        <p>
          Back up settings, mappings and tracker data. Schedule backups and
          keep an optional encrypted remote copy.
        </p>
        <span>Backup & restore</span></a
      >
      <a
        class="tool-card"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/navigation/editor/mappings-and-episode-groups"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <path d="M4 6h6l4 12h6M4 18h6l4-12h6" />
          <circle cx="3" cy="6" r="1" />
          <circle cx="21" cy="18" r="1" />
        </svg>
        <h3>Make the right match.</h3>
        <p>
          Fix mismatched titles, map anime IDs, and handle split episodes
          and different numbering systems.
        </p>
        <span>Mappings & episode groups</span></a
      >
      <a
        class="tool-card"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/navigation/playlists/published-lists"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <path d="M4 5h16M4 11h10M4 17h8m5-3 5 4-5 4z" />
        </svg>
        <h3>Give your lists a life.</h3>
        <p>
          Build playlists and publish lists for tools like Kometa, Radarr
          and Sonarr.
        </p>
        <span>Playlists & published lists</span></a
      >
      <a
        class="tool-card"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/tools/events"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <path d="M3 12h4l3-8 4 16 3-8h4" />
        </svg>
        <h3>See the whole story.</h3>
        <p>
          Follow activity from title to sync run. Open diagnostic logs when
          it’s time to investigate.
        </p>
        <span>Activity & diagnostics</span></a
      >
      <a
        class="tool-card"
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/tools/analyzer"
        ><svg viewBox="0 0 24 24" class="tool-icon" aria-hidden="true">
          <circle cx="10" cy="10" r="7" />
          <path d="m15 15 6 6M7 12V9m3 3V6m3 6V8" />
        </svg>
        <h3>Find what’s missing.</h3>
        <p>
          Compare snapshots, inspect differences and understand what needs
          attention with Analyzer.
        </p>
        <span>Analyzer & troubleshooting</span></a
      >
    </div>
    <div class="extra-features">
      <span>AND THERE’S MORE TO EXPLORE</span
      ><a
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/navigation/cw-tracker"
        >Your own local tracker</a
      ><a
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/navigation/quick-add"
        >Quick Add</a
      ><a
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/tools/sync-topology-advisor"
        >Topology advisor</a
      ><a
        href="https://wiki.crosswatch.app/crosswatch/settings/ui-settings/ai-help"
        >Optional AI assistance</a
      >
    </div>
  </section>
  <section class="start-section section" id="start">
    <div class="wrap start-grid">
      <div>
        <p class="eyebrow mint">MAKE YOURSELF AT HOME</p>
        <h2>
          Your server.<br />Your media.<br /><span class="gradient-text"
            >Your CrossWatch.</span
          >
        </h2>
        <p class="start-intro">
          Start with Docker. Connect your services.<br />Make your first
          sync a small one.
        </p>
        <ol class="setup-steps">
          <li>
            <span>1</span>
            <div>
              <strong>Bring CrossWatch online</strong>
              <p>Save the Compose file and start your container.</p>
            </div>
          </li>
          <li>
            <span>2</span>
            <div>
              <strong>Connect your first pair</strong>
              <p>Add your accounts and choose what to sync.</p>
            </div>
          </li>
          <li>
            <span>3</span>
            <div>
              <strong>Review, then let it run</strong>
              <p>Try a dry run before setting your schedule.</p>
            </div>
          </li>
        </ol>
        <div class="setup-links">
          <a
            class="text-link"
            href="https://wiki.crosswatch.app/getting-started/first-time-setup"
            >First-time setup guide</a
          ><a
            class="text-link"
            href="https://wiki.crosswatch.app/getting-started/installation/unraid-apps"
            >Using Unraid?</a
          >
        </div>
      </div>
      <div class="install-panel">
        <div class="install-heading">
          <span
            ><span class="code-symbol">&lt;/&gt;</span>
            docker-compose.yml</span
          ><button id="copy-compose" class="copy-button">
            <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
              <rect x="8" y="8" width="12" height="13" rx="2" />
              <path d="M15 8V3H3v13h5" /></svg
            ><span>Copy</span>
          </button>
        </div>
        <pre><code id="compose-code"><span class="code-key">services:</span>
  <span class="code-key">crosswatch:</span>
    <span class="code-muted">image:</span> <span class="code-value">ghcr.io/cenodude/crosswatch:latest</span>
    <span class="code-muted">container_name:</span> crosswatch
    <span class="code-muted">ports:</span>
      - <span class="code-value">"8787:8787"</span>
    <span class="code-muted">environment:</span>
      <span class="code-muted">TZ:</span> Europe/Amsterdam
    <span class="code-muted">volumes:</span>
      - crosswatch_config:/config
    <span class="code-muted">restart:</span> unless-stopped

<span class="code-key">volumes:</span>
  <span class="code-key">crosswatch_config:</span></code></pre>
        <div class="terminal-command">
          <span>$</span><code>docker compose up -d</code>
        </div>
        <div class="install-note">
          Set <code>TZ</code> to your timezone. Then open
          <code>http://YOUR_SERVER_IP:8787</code>.
        </div>
        <a
          class="install-guide"
          href="https://wiki.crosswatch.app/getting-started/installation/docker-setup"
          >Explore Docker setup</a
        >
        <p id="copy-status" class="sr-only" aria-live="polite"></p>
      </div>
    </div>
  </section>
  <section class="section wrap docs-section" id="docs">
    <div class="section-heading">
      <div>
        <p class="eyebrow">TAKE A CLOSER LOOK</p>
        <h2>
          Get comfortable.<br /><span class="muted-heading"
            >Then get creative.</span
          >
        </h2>
      </div>
      <button class="button button-ghost search-trigger">
        <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="10.5" cy="10.5" r="6.5" />
          <path d="m16 16 5 5" /></svg
        >Explore the features
      </button>
    </div>
    <div class="docs-grid">
      <a href="https://wiki.crosswatch.app/getting-started/first-time-setup"
        ><span class="doc-category">START HERE</span>
        <h3>Your first successful sync</h3>
        <p>
          From installation to connected accounts and your first dry run.
        </p>
        <span class="doc-link">Getting started</span></a
      ><a
        href="https://wiki.crosswatch.app/crosswatch/settings/configure-pairs"
        ><span class="doc-category">BUILD YOUR SETUP</span>
        <h3>Make it work your way</h3>
        <p>
          Choose sync directions, profiles, schedules and supported
          features.
        </p>
        <span class="doc-link">Explore configuration</span></a
      ><a
        href="https://wiki.crosswatch.app/crosswatch/main-dashboard/tools/logs"
        ><span class="doc-category">GET UNSTUCK</span>
        <h3>Understand what happened</h3>
        <p>
          Inspect diagnostics and share the right logs when you need help.
        </p>
        <span class="doc-link">Logs & troubleshooting</span></a
      >
    </div>
    <div class="faq">
      <h3>A few good questions.</h3>
      <div>
        <details>
          <summary>
            Does every service support every feature?<span>+</span>
          </summary>
          <p>
            No. Supported features, sync directions and scrobbling vary by
            provider. Check the
            <a
              href="https://wiki.crosswatch.app/getting-started/support-matrix"
              >integration overview</a
            >
            before building your pairs.
          </p>
        </details>
        <details>
          <summary>
            Can I check changes before they happen?<span>+</span>
          </summary>
          <p>
            Yes. Interactive Sync lets you review proposed additions,
            updates and removals, resolve mappings and apply selected
            changes. Start with one pair and a dry run.
          </p>
        </details>
        <details>
          <summary>
            Can different people use the same installation?<span>+</span>
          </summary>
          <p>
            Yes. Profiles and managed users let you scope connections,
            pairs, Watcher routes and schedules to the people in your
            household.
          </p>
        </details>
        <details>
          <summary>
            Do I need Plex Pass or Emby Premiere?<span>+</span>
          </summary>
          <p>
            CrossWatch’s Watcher can track playback without Plex Pass or
            Emby Premiere. Other provider capabilities still depend on the
            service. Read the
            <a
              href="https://wiki.crosswatch.app/crosswatch/settings/scrobbler/watcher"
              >Watcher overview</a
            >
            to see how it works.
          </p>
        </details>
      </div>
    </div>
  </section>
  <section class="closing wrap">
    <img
      src="{{ '/images/providers/CROSSWATCH.svg' | relative_url }}"
      width="64"
      height="64"
      alt=""
    />
    <p class="eyebrow">BUILT FOR PEOPLE WHO LOVE THEIR MEDIA</p>
    <h2>Bring your media together.<br />Keep it yours.</h2>
    <div>
      <a class="button button-primary" href="#start">Make it yours</a
      ><a
        class="button button-ghost"
        href="https://github.com/cenodude/CrossWatch"
        >Explore on GitHub</a
      >
    </div>
  </section>
</main>
<footer class="wrap">
  <div class="footer-main">
    <div>
      <a class="brand" href="#"
        ><img
          src="{{ '/images/providers/CROSSWATCH.svg' | relative_url }}"
          width="30"
          height="30"
          alt=""
        />CrossWatch<span class="brand-dot">.</span></a
      >
      <p>Your media. Your setup. In sync.</p>
    </div>
    <div class="footer-links">
      <a href="https://wiki.crosswatch.app/">Wiki</a
      ><a href="https://github.com/cenodude/CrossWatch">GitHub</a
      ><a href="https://github.com/cenodude/CrossWatch/releases/latest"
        >Changelog</a
      ><a href="https://discord.gg/c9cjzHGve">Discord</a
      ><a href="https://www.reddit.com/r/CrossWatchApp/">Reddit</a>
    </div>
  </div>
  <div class="footer-bottom">
    <span>© 2026 CrossWatch / Cenodude</span
    ><span>Open source · Self-hosted · Community built</span
    ><a href="https://github.com/cenodude/CrossWatch/blob/main/LICENSE"
      >AGPL-3.0</a
    >
  </div>
</footer>
<dialog id="search-dialog" aria-labelledby="search-title">
  <div class="dialog-heading">
    <h2 id="search-title">Find your next step</h2>
    <button class="dialog-close" aria-label="Close search">×</button>
  </div>
  <label class="docs-search-input"
    ><svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="10.5" cy="10.5" r="6.5" />
      <path d="m16 16 5 5" /></svg
    ><input
      type="search"
      id="docs-search"
      placeholder="Try “Plex”, “backup” or “ratings”…"
      aria-label="Explore CrossWatch guides"
  /></label>
  <p class="search-hint">
    Explore features, integrations and ways to get started
  </p>
  <div id="search-results"></div>
  <p id="search-count" class="sr-only" aria-live="polite"></p>
</dialog>
<dialog id="image-dialog" aria-labelledby="image-title">
  <div class="dialog-heading">
    <span id="image-title">CrossWatch - Sync Hub</span>
    <div class="image-actions">
      <button id="image-zoom" aria-pressed="false">Actual size</button
      ><button class="dialog-close" aria-label="Close screenshot">
        &times;
      </button>
    </div>
  </div>
  <div
    class="screenshot-viewport"
    tabindex="0"
    role="region"
    aria-label="Enlarged screenshot. Scroll to inspect when at actual size."
  >
    <img
      id="large-screenshot"
      decoding="async"
      alt="CrossWatch Sync Hub"
      width="2226"
      height="1177"
    />
  </div>
  <div class="image-footer">
    <button id="previous-shot" aria-label="Previous screenshot">
      Previous</button
    ><span id="image-count" aria-live="polite">1 of 4</span
    ><button id="next-shot" aria-label="Next screenshot">Next</button>
  </div>
</dialog>
<dialog
  id="showcase-dialog"
  aria-labelledby="feature-title-hub"
  aria-describedby="feature-description-hub"
>
  <div class="showcase-toolbar">
    <div>
      <button id="showcase-back" hidden>Back</button
      ><span>CROSSWATCH / EXPLORE</span>
    </div>
    <button class="dialog-close" aria-label="Close feature overview">
      &times;
    </button>
  </div>
  <div class="showcase-content">{% include showcase-panels.html %}</div>
  <div class="showcase-footer">
    <button class="button button-primary" id="showcase-start">
      Get CrossWatch</button
    ><a
      id="showcase-docs"
      data-documentation
      href="https://wiki.crosswatch.app/"
      target="_blank"
      rel="noopener noreferrer"
      >Detailed setup in the wiki</a
    >
  </div>
</dialog>
