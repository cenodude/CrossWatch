import React, { useEffect, useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { LinkIcon, RefreshCw, PlayCircle, Save, Settings } from 'lucide-react'

type Config = {
  plex?: { account_token?: string }
  simkl?: {
    client_id: string
    client_secret: string
    access_token?: string
    refresh_token?: string
    token_expires_at?: number
  }
  sync?: { mode?: 'mirror'|'two-way' }
  runtime?: { debug?: boolean }
  [k: string]: any
}

export default function App() {
  const [cfg, setCfg] = useState<Config>({
    plex: { account_token: '' },
    simkl: { client_id: '', client_secret: '' },
    sync: { mode: 'mirror' },
    runtime: { debug: false }
  })
  const [log, setLog] = useState('')
  const [busy, setBusy] = useState(false)

  useEffect(() => { (async () => {
    try {
      const current = await invoke<Config>('cmd_read_config')
      setCfg({
        plex: { account_token: current?.plex?.account_token || '' },
        simkl: {
          client_id: current?.simkl?.client_id || '',
          client_secret: current?.simkl?.client_secret || '',
          access_token: current?.simkl?.access_token || '',
          refresh_token: current?.simkl?.refresh_token || '',
          token_expires_at: current?.simkl?.token_expires_at || 0
        },
        sync: { mode: (current?.sync?.mode as any) || 'mirror' },
        runtime: { debug: !!current?.runtime?.debug },
        ...current
      })
    } catch {}
  })() }, [])

  function update<K extends keyof Config>(k: K, v: Config[K]) {
    setCfg(prev => ({ ...prev, [k]: v }))
  }

  async function saveConfig() {
    setBusy(true)
    try {
      await invoke('cmd_write_config', { cfg })
    } finally { setBusy(false) }
  }

  async function getPlexToken() {
    setBusy(true); setLog(l => l + "\n[Plex] Starting PIN flow...")
    try { await invoke('cmd_plex_pin_flow'); setLog(l => l + "\n[Plex] Token saved.") }
    catch (e:any) { setLog(l => l + "\n[Plex] Error: " + e?.toString()) }
    finally { setBusy(false) }
  }

  async function connectSimkl() {
    setBusy(true); setLog(l => l + "\n[SIMKL] Opening authorize page...")
    try { await invoke('cmd_simkl_oauth'); setLog(l => l + "\n[SIMKL] Tokens saved.") }
    catch (e:any) { setLog(l => l + "\n[SIMKL] Error: " + e?.toString()) }
    finally { setBusy(false) }
  }

  async function runSync() {
    setBusy(true); setLog(l => l + "\n[SYNC] Launching Python sidecar...")
    try { await invoke('cmd_run_sync'); setLog(l => l + "\n[SYNC] Completed.") }
    catch (e:any) { setLog(l => l + "\n[SYNC] Error: " + e?.toString()) }
    finally { setBusy(false) }
  }

  const simklReady = !!cfg.simkl?.client_id && !!cfg.simkl?.client_secret

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold tracking-tight">Plex ⇄ SIMKL Desktop</h1>
        <div className="text-sm text-zinc-400">Settings & Authentication</div>
      </header>

      <div className="card space-y-6">
        <div className="flex items-center gap-2">
          <Settings size={18}/><h2 className="font-semibold">Settings</h2>
        </div>

        <div className="grid md:grid-cols-3 gap-4">
          <div className="space-y-2">
            <label className="label">Sync mode</label>
            <select
              className="input"
              value={cfg.sync?.mode || 'mirror'}
              onChange={e => update('sync', { ...(cfg.sync||{}), mode: e.target.value as any })}
            >
              <option value="mirror">mirror</option>
              <option value="two-way">two-way</option>
            </select>
          </div>

          <div className="space-y-2">
            <label className="label">Debug</label>
            <select
              className="input"
              value={cfg.runtime?.debug ? 'true' : 'false'}
              onChange={e => update('runtime', { ...(cfg.runtime||{}), debug: e.target.value === 'true' })}
            >
              <option value="false">false</option>
              <option value="true">true</option>
            </select>
          </div>
        </div>

        <div className="divider"></div>

        <div className="grid md:grid-cols-2 gap-6">
          <div className="space-y-4">
            <div className="section-title">Authentication Provider — Plex</div>
            <div className="space-y-2">
              <label className="label">Account token (manual)</label>
              <input className="input" value={cfg.plex?.account_token || ''}
                onChange={e => update('plex', { account_token: e.target.value })} placeholder="Paste token..." />
            </div>
            <div className="flex gap-3">
              <button className="btn btn-primary" disabled={busy} onClick={getPlexToken}><LinkIcon size={16}/> Get via PIN</button>
              <button className="btn" disabled={busy} onClick={saveConfig}><Save size={16}/> Save</button>
            </div>
          </div>

          <div className="space-y-4">
            <div className="section-title">Authentication Provider — SIMKL</div>
            <div className="space-y-2">
              <label className="label">Client ID</label>
              <input className="input" value={cfg.simkl?.client_id || ''}
                onChange={e => update('simkl', { ...(cfg.simkl||{}), client_id: e.target.value })} placeholder="SIMKL client_id" />
            </div>
            <div className="space-y-2">
              <label className="label">Client Secret</label>
              <input className="input" value={cfg.simkl?.client_secret || ''}
                onChange={e => update('simkl', { ...(cfg.simkl||{}), client_secret: e.target.value })} placeholder="SIMKL client_secret" />
            </div>
            <div className="flex gap-3">
              <button className="btn btn-primary" disabled={busy || !simklReady} onClick={connectSimkl}><RefreshCw size={16}/> Connect SIMKL</button>
              <button className="btn" disabled={busy} onClick={saveConfig}><Save size={16}/> Save</button>
            </div>
          </div>
        </div>
      </div>

      <div className="card space-y-3">
        <div className="flex items-center gap-2">
          <h3 className="font-semibold">Run Sync</h3>
        </div>
        <p className="text-sm text-zinc-300">Runs your Python script as sidecar.</p>
        <button className="btn" disabled={busy} onClick={runSync}><PlayCircle size={16}/> Start Sync</button>
        <pre className="log">{log}</pre>
      </div>
    </div>
  )
}
