# providers/auth/_auth_CROSSWATCH.py
# CrossWatch - Local Tracker Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.provider_instances import get_provider_block, normalize_instance_id


class CrossWatchAuth(AuthProvider):
    name = "CROSSWATCH"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="CROSSWATCH",
            label="CrossWatch Local Tracker",
            flow="local",
            fields=[],
            actions={"start": False, "finish": False, "refresh": False, "disconnect": True},
            notes="Local tracker settings are stored in the CrossWatch config.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True, "progress": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        block = get_provider_block(cfg or {}, "crosswatch", inst)
        exists = isinstance((cfg or {}).get("crosswatch"), Mapping) or isinstance((cfg or {}).get("CrossWatch"), Mapping)
        ok = bool(exists) and block.get("connected") is True and block.get("enabled") is not False
        label = "CrossWatch Local Tracker" if inst == "default" else f"CrossWatch Local Tracker ({inst})"
        return AuthStatus(connected=ok, label=label)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        return self.get_status(cfg or {}, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg or {}, instance_id=instance_id)


PROVIDER = CrossWatchAuth()


def html() -> str:
    return r"""<div class="section" id="sec-crosswatch">
  <style>
    #sec-crosswatch .cw-tracker-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-crosswatch .cw-tracker-grid input,#sec-crosswatch .cw-tracker-grid select{width:100%}
    #sec-crosswatch .cw-tracker-settings-stack{display:grid;gap:18px}
    #sec-crosswatch .cw-tracker-section{display:grid;gap:14px;padding:18px;border:1px solid rgba(150,164,205,.14);border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.014))}
    #sec-crosswatch .cw-tracker-section-head{display:grid;grid-template-columns:38px minmax(0,1fr);gap:12px;align-items:center}
    #sec-crosswatch .cw-tracker-section-icon{display:grid;place-items:center;width:38px;height:38px;border-radius:14px;background:rgba(124,92,255,.16);color:#a78bfa}
    #sec-crosswatch .cw-tracker-section-icon .material-symbols-rounded{font-size:22px}
    #sec-crosswatch .cw-tracker-section-title{font-size:15px;font-weight:900;color:#f2f5ff}
    #sec-crosswatch .cw-tracker-section-sub{margin-top:2px;color:rgba(214,221,238,.68);font-size:13px;line-height:1.35}
    #sec-crosswatch .cw-tracker-status{display:inline-flex;align-items:center;gap:8px;width:max-content;margin-top:12px;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,224,132,.22);background:rgba(0,224,132,.08);color:#b9ffd7;font-weight:700}
    #sec-crosswatch .cw-tracker-status.warn{border-color:rgba(255,210,0,.2);background:rgba(255,210,0,.08);color:#ffe9a6}
  </style>

  <div class="head" data-toggle-section="sec-crosswatch">
    <span class="chev"></span><strong>CrossWatch Local Tracker</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="crosswatch">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">CrossWatch Local Tracker</div>
            <div class="muted">Local watchlist, ratings, history and progress storage.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="settings">Settings</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-tracker-auth-card">
              <div>
                <h4>Connect Local Tracker</h4>
                <p class="muted">Enable this local tracker profile as a normal CrossWatch connection.</p>
              </div>
              <div class="cw-tracker-auth-actions">
                <button id="cw_crosswatch_connect" class="btn primary" type="button">Connect local tracker</button>
                <span id="cw_tracker_auth_msg" class="cw-tracker-status hidden" aria-live="polite"></span>
              </div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="settings">
            <div class="cw-tracker-settings-stack">
              <section class="cw-tracker-section" aria-labelledby="cw_tracker_storage_title">
                <div class="cw-tracker-section-head">
                  <span class="cw-tracker-section-icon" aria-hidden="true"><span class="material-symbols-rounded">database</span></span>
                  <div>
                    <div class="cw-tracker-section-title" id="cw_tracker_storage_title">Local tracker storage</div>
                    <div class="cw-tracker-section-sub">Profile label, snapshot retention and automatic capture behavior.</div>
                  </div>
                </div>
                <div class="cw-tracker-grid">
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_label">Label</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Label: Short name for your own administration. Maximum 12 characters and only used inside CrossWatch." aria-label="Local tracker label setting help">help</button>
                    </div>
                    <input id="cw_tracker_label" name="cw_tracker_label" type="text" maxlength="12" placeholder="Personal" autocomplete="off" spellcheck="false" autocapitalize="off" />
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_retention_days">Retention (days)</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Retention days: Number of days CrossWatch keeps local tracker snapshots. Use 0 to keep snapshots forever." aria-label="Local tracker retention setting help">help</button>
                    </div>
                    <input id="cw_tracker_retention_days" name="cw_tracker_retention_days" type="number" min="0" step="1" placeholder="30" autocomplete="off" />
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_auto_snapshot">Auto snapshot</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Auto snapshot: Creates a snapshot before CrossWatch writes local tracker data, so changes can be restored." aria-label="Local tracker auto snapshot setting help">help</button>
                    </div>
                    <select id="cw_tracker_auto_snapshot" name="cw_tracker_auto_snapshot" autocomplete="off">
                      <option value="true">On (before writes)</option>
                      <option value="false">Off</option>
                    </select>
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_max_snapshots">Max snapshots per feature</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Max snapshots per feature: Maximum snapshots kept for watchlist, history, ratings and progress. Use 0 for unlimited." aria-label="Local tracker max snapshots setting help">help</button>
                    </div>
                    <input id="cw_tracker_max_snapshots" name="cw_tracker_max_snapshots" type="number" min="0" step="1" placeholder="64" autocomplete="off" />
                  </div>
                </div>
              </section>

              <section class="cw-tracker-section" aria-labelledby="cw_tracker_restore_title">
                <div class="cw-tracker-section-head">
                  <span class="cw-tracker-section-icon" aria-hidden="true"><span class="material-symbols-rounded">restore_page</span></span>
                  <div>
                    <div class="cw-tracker-section-title" id="cw_tracker_restore_title">Restore snapshots</div>
                    <div class="cw-tracker-section-sub">Choose latest, or pin a stored snapshot for this tracker profile.</div>
                  </div>
                </div>
                <div class="cw-tracker-grid" id="cw_tracker_restore_fields">
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_restore_watchlist">Watchlist snapshot</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Watchlist snapshot: Snapshot used when restoring or reading this local tracker profile's watchlist. Latest follows the newest snapshot." aria-label="Local tracker watchlist restore help">help</button>
                    </div>
                    <select id="cw_tracker_restore_watchlist" name="cw_tracker_restore_watchlist" autocomplete="off"></select>
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_restore_history">History snapshot</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="History snapshot: Snapshot used when restoring or reading this local tracker profile's history. Latest follows the newest snapshot." aria-label="Local tracker history restore help">help</button>
                    </div>
                    <select id="cw_tracker_restore_history" name="cw_tracker_restore_history" autocomplete="off"></select>
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_restore_ratings">Ratings snapshot</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Ratings snapshot: Snapshot used when restoring or reading this local tracker profile's ratings. Latest follows the newest snapshot." aria-label="Local tracker ratings restore help">help</button>
                    </div>
                    <select id="cw_tracker_restore_ratings" name="cw_tracker_restore_ratings" autocomplete="off"></select>
                  </div>
                  <div>
                    <div class="cw-field-label-row">
                      <label for="cw_tracker_restore_progress">Progress snapshot</label>
                      <button type="button" class="cw-field-help material-symbols-rounded" title="Progress snapshot: Snapshot used when restoring or reading this local tracker profile's playback progress. Latest follows the newest snapshot." aria-label="Local tracker progress restore help">help</button>
                    </div>
                    <select id="cw_tracker_restore_progress" name="cw_tracker_restore_progress" autocomplete="off"></select>
                  </div>
                </div>
              </section>
            </div>
            <button id="cw_crosswatch_disconnect" type="button" hidden aria-hidden="true" tabindex="-1"></button>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""
