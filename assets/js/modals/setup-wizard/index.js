// assets/js/modals/setup-wizard/index.js

function _norm(v) {
  return String(v || '').replace(/^v/i, '').trim();
}

function _collapseByDefault() {
  const ids = ['sec-auth', 'sec-meta', 'sec-sync', 'sec-scrobbler', 'sc-sec-webhook', 'sc-sec-watch'];
  for (const id of ids) {
    try { document.getElementById(id)?.classList.remove('open'); } catch {}
  }
}

function _openSettings() {
  try { window.showTab?.('settings'); } catch {}
  try { _collapseByDefault(); } catch {}
}

export default {
  async mount(hostEl, props = {}) {
    if (!hostEl) return;

    const ver = _norm(props.current_version || window.__CW_VERSION__ || '0.0.0');

    _openSettings();

    hostEl.innerHTML = `
    <style>
      #setup-host{--w:900px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;
        border:1px solid rgba(255,255,255,.08);
        background:
          radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),
          radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),
          radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),
          rgba(7,8,11,.92);
        box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(135%) blur(10px)
      }
      #setup-host:before{content:"";position:absolute;inset:-120px;pointer-events:none;
        background:conic-gradient(from 180deg at 50% 50%, rgba(150,70,255,.0), rgba(150,70,255,.30), rgba(60,140,255,.24), rgba(60,255,215,.10), rgba(150,70,255,.0));
        filter:blur(90px);opacity:.35;transform:translate3d(0,0,0);
        animation:setupGlow 16s ease-in-out infinite alternate
      }
      @keyframes setupGlow{from{transform:translate(-16px,-10px) scale(1)}to{transform:translate(16px,12px) scale(1.03)}}
      @media (prefers-reduced-motion: reduce){#setup-host:before{animation:none}}

      #setup-host .head{position:relative;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))
      }
      #setup-host .logoWrap{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);
        box-shadow:0 12px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04)
      }
      #setup-host .logo{width:30px;height:30px;opacity:.95;filter:drop-shadow(0 10px 16px rgba(0,0,0,.45))}
      #setup-host .title{font-weight:950;letter-spacing:.2px;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
      #setup-host .sub{opacity:.72;font-size:12px;margin-top:2px}
      #setup-host .v{margin-left:auto;opacity:.85;font-weight:900;font-size:12px;padding:6px 10px;border-radius:999px;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)
      }

      #setup-host .body{position:relative;padding:16px 16px 8px 16px;max-height:72vh;overflow:auto}
      #setup-host .badge{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;
        background:rgba(0,0,0,.24);border:1px solid rgba(255,255,255,.08)
      }
      #setup-host .badge .dot{width:8px;height:8px;border-radius:999px;background:rgba(150,70,255,.90);box-shadow:0 0 0 4px rgba(150,70,255,.18)}
      #setup-host .headline{font-weight:950;font-size:20px;line-height:1.15;margin:10px 0 8px 0}
      #setup-host .lede{opacity:.84;max-width:72ch}

      #setup-host .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:14px 0 10px 0}
      @media (max-width:780px){#setup-host .grid{grid-template-columns:1fr}}
      #setup-host .card{display:flex;gap:10px;align-items:flex-start;padding:12px 12px;border-radius:14px;
        background:rgba(255,255,255,.03);
        border:1px solid rgba(255,255,255,.08);
        box-shadow:0 10px 30px rgba(0,0,0,.32)
      }
      #setup-host .ico{width:34px;height:34px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)
      }
      #setup-host .card b{font-weight:950}
      #setup-host .muted{opacity:.78;font-size:12.5px;margin-top:2px}

      #setup-host .tip{margin:10px 0 10px 0;font-size:12.5px;opacity:.84}
      #setup-host code{opacity:.95}

      #setup-host details.disc{margin-top:10px;border-radius:14px;border:1px solid rgba(255,255,255,.08);
        background:rgba(255,255,255,.02);box-shadow:inset 0 1px 0 rgba(255,255,255,.03)
      }
      #setup-host details.disc>summary{cursor:pointer;list-style:none;padding:10px 12px;font-weight:950;display:flex;align-items:center;gap:8px}
      #setup-host details.disc>summary::-webkit-details-marker{display:none}
      #setup-host .discBody{padding:0 12px 12px 12px;opacity:.82;font-size:12.5px;line-height:1.38}
      #setup-host .discBody p{margin:8px 0 0 0}

      #setup-host .foot{position:relative;display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 16px;
        border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))
      }
      #setup-host .mini{opacity:.68;font-size:12px}
      #setup-host .btns{display:flex;gap:10px;align-items:center}
      #setup-host .btn{appearance:none;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:10px 14px;font-weight:950;cursor:pointer;
        background:rgba(255,255,255,.04);color:#eaf0ff
      }
      #setup-host .btn:hover{filter:brightness(1.06)}
      #setup-host .btn.primary{border-color:rgba(150,70,255,.35);
        background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82));
        box-shadow:0 16px 50px rgba(0,0,0,.48)
      }
      #setup-host .btn.primary:active{transform:translateY(1px)}
    </style>

    <div id="setup-host" role="dialog" aria-label="CrossWatch first run">
      <div class="head">
        <div class="logoWrap" aria-hidden="true">
          <img class="logo" src="/assets/img/CROSSWATCH.svg" alt="" />
        </div>
        <div>
          <div class="title">Welcome to CrossWatch</div>
          <div class="sub">First run setup (no config.json yet)</div>
        </div>
        <div class="v">v${ver}</div>
      </div>

      <div class="body">
        <div class="badge"><span class="dot" aria-hidden="true"></span><span style="font-weight:900">Quick setup</span></div>
        <div class="headline">Configure it once. Then forget it exists <span style="opacity:.70">(hopefully)</span>.</div>
        <div class="lede">Configure what you need in <b>Settings</b>.</div>

        <div class="grid" role="list">
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">key</span></div>
            <div>
              <div><b>Authentication providers</b></div>
              <div class="muted">Required: Link one or more providers</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">database</span></div>
            <div>
              <div><b>Metadata provider</b></div>
              <div class="muted">Configure TMDb. One key, just do it.</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">sync_alt</span></div>
            <div>
              <div><b>Synchronization pairs</b></div>
              <div class="muted">Optional: Define what syncs where (and how).</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">podcasts</span></div>
            <div>
              <div><b>Scrobbler</b></div>
              <div class="muted">Optional: Configure scrobbling options.</div>
            </div>
          </div>
        </div>

        <div class="tip">When you’re happy, hit <b>Save</b> in Settings. That creates <code>/config/config.json</code>.</div>

        <details class="disc">
          <summary><span class="material-symbols-rounded" aria-hidden="true">gavel</span>Disclaimer</summary>
          <div class="discBody">
            <p>This is an independent, community-maintained project and is not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, SIMKL, Tautulli, AniList or MDBList. Use at your own risk.</p>
            <p>All product names, logos, and brands are property of their respective owners and used for identification only.</p>
            <p>Interacts with third-party services; you are responsible for complying with their Terms of Use and API rules.</p>
            <p>Provided “as is,” without warranties or guarantees.</p>
          </div>
        </details>
      </div>

      <div class="foot">
        <div class="mini">Tip: For more help, check the <a href="https://github.com/cenodude/CrossWatch/wiki" target="_blank" rel="noopener noreferrer">CrossWatch Wiki</a>.</div>
        <div class="btns">
          <button class="btn" type="button" data-x="close">Close</button>
          <button class="btn primary" type="button" data-x="settings">Open Settings</button>
        </div>
      </div>
    </div>
    `;

    const shell = hostEl.closest('.cx-modal-shell');
    if (shell) {
      shell.style.width = 'auto';
      shell.style.maxWidth = 'none';
      shell.style.height = 'auto';
      shell.style.maxHeight = 'none';
      shell.style.display = 'inline-block';
    }

    hostEl.querySelector('[data-x="close"]')?.addEventListener('click', () => {
      try { window.cxCloseModal?.(); } catch {}
    });
    hostEl.querySelector('[data-x="settings"]')?.addEventListener('click', () => {
      _openSettings();
      try { window.cxCloseModal?.(); } catch {}
    });
  },

  unmount() {

  }
};
