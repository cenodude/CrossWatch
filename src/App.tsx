import React, { useEffect, useMemo, useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { LinkIcon, RefreshCw, PlayCircle, Save, Settings, X } from 'lucide-react'

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

function Badge({ ok }: { ok: boolean }) {
  return (
    <span className={"px-2 py-1 rounded-md text-xs " + (ok ? "bg-emerald-600/20 text-emerald-300 ring-1 ring-emerald-700" : "bg-rose-600/20 text-rose-300 ring-1 ring-rose-700")}>
      {ok ? "Connected" : "Not connected"}
    </span>
  )
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
  const plexConnected = !!cfg.plex?.account_token
  const simklConnected = !!cfg.simkl?.access_token && (!!cfg.simkl?.token_expires_at ? cfg.simkl!.token_expires_at! > Math.floor(Date.now()/1000) : true)
  const needsSetup = !(plexConnected && simklConnected)
  const [showSettings, setShowSettings] = useState(false)

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

  useEffect(() => {
    if (needsSetup) setShowSettings(true)
  }, [needsSetup])

  function update<K extends keyof Config>(k: K, v: Config[K]) {
    setCfg(prev => ({ ...prev, [k]: v }))
  }

  async function saveConfig() {
    setBusy(true)
    try { await invoke('cmd_write_config', { cfg }) }
    finally { setBusy(false) }
  }

  // --- SYNC ---
  async function runSync() {
    setBusy(true); setLog(l => l + "\n[SYNC] Launching Python sidecar...")
    try { await invoke('cmd_run_sync'); setLog(l => l + "\n[SYNC] Completed.") }
    catch (e:any) { setLog(l => l + "\n[SYNC] Error: " + e?.toString()) }
    finally { setBusy(false) }
  }

  // --- PLEX PIN UX ---
  const [pinId, setPinId] = useState<number|undefined>(undefined)
  const [pinCode, setPinCode] = useState<string>('')
  const [pinBusy, setPinBusy] = useState(false)
  async function plexCreatePin() {
  setPinBusy(true)
  try {
    const res = await invoke<{id:number, code:string, expires_at:number}>('cmd_plex_create_pin')
    setPinId(res.id); setPinCode(res.code)
    setLog(l => l + `
[Plex] PIN created: ${res.code}. Opening plex.tv/link...`)
    await invoke('cmd_open_url', { url: "https://plex.tv/link" })
    // auto-poll until token received or expiry (~90s)
    const start = Date.now()
    const poll = async () => {
      try {
        const token = await invoke<string>('cmd_plex_poll_pin', { id: res.id })
        if (token) {
          setLog(l => l + "
[Plex] Token received.")
          setCfg(prev => ({ ...prev, plex: { account_token: token } }))
          await saveConfig()
          setPinBusy(false)
          return
        }
      } catch (e:any) {
        // keep polling unless error is terminal
      }
      if (Date.now() - start < 90000) {
        setTimeout(poll, 1500)
      } else {
        setPinBusy(false)
        setLog(l => l + "
[Plex] PIN expired; please create a new PIN.")
      }
    }
    setTimeout(poll, 1500)
  } catch (e:any) {
    setLog(l => l + "
[Plex] Error creating PIN: " + e?.toString())
    setPinBusy(false)
  }
}
  // --- SIMKL ---
  const simklReady = !!cfg.simkl?.client_id && !!cfg.simkl?.client_secret
  async function connectSimkl() {
    setBusy(true); setLog(l => l + "\n[SIMKL] Opening authorize page...")
    try { await invoke('cmd_simkl_oauth'); setLog(l => l + "\n[SIMKL] Tokens saved.") }
    catch (e:any) { setLog(l => l + "\n[SIMKL] Error: " + e?.toString()) }
    finally { setBusy(false) }
  }

  // --- UI ---
  return (
    <div className="max-w-5xl mx-auto p-6 space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold tracking-tight">CrossWatch</h1>
        <div className="flex items-center gap-3">
          <button className="btn" onClick={() => setShowSettings(true)}><Settings size={16}/> Settings</button>
        </div>
      </header>

      {/* Main Screen */}
      <div className="card space-y-5">
        <div className="flex items-center justify-between">
          <h2 className="font-semibold">Run Sync</h2>
          <div className="flex gap-3 items-center text-sm">
            <div className="flex items-center gap-2">Plex <Badge ok={plexConnected}/></div>
            <div className="flex items-center gap-2">SIMKL <Badge ok={simklConnected}/></div>
          </div>
        </div>
        <p className="text-sm text-zinc-300">Launches your Python sync script. Disabled until both providers are connected.</p>
        <button className="btn btn-primary" disabled={busy || !plexConnected || !simklConnected} onClick={runSync}>
          <PlayCircle size={16}/> Start Sync
        </button>
        {!plexConnected || !simklConnected ? <div className="text-xs text-zinc-400">Complete setup in <span className="kbd">Settings</span> first.</div> : null}
        <pre className="log">{log}</pre>
      </div>

      {/* Settings Modal */}
      {showSettings && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
          <div className="w-[900px] max-w-[95vw] card relative space-y-6">
            <button className="absolute right-4 top-4 btn" onClick={() => setShowSettings(false)}><X size={16}/></button>
            <div className="flex items-center gap-2">
              <Settings size={18}/><h2 className="font-semibold">Settings</h2>
            </div>

            {/* General */}
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

            {/* Providers */}
            <div className="grid md:grid-cols-2 gap-6">
              {/* Plex */}
              <div className="space-y-4">
                <div className="section-title">Authentication — Plex</div>
                <div className="space-y-2">
                  <label className="label">Account token (manual)</label>
                  <input className="input" value={cfg.plex?.account_token || ''}
                    onChange={e => update('plex', { account_token: e.target.value })} placeholder="Paste token..." />
                </div>
                <div className="space-y-2">
                  <label className="label">PIN code</label>
                  <div className="flex gap-2">
                    <input className="input" value={pinCode} readOnly placeholder="Press 'Create PIN'"/>
                    <button className="btn" onClick={() => invoke('cmd_open_url', { url: 'https://plex.tv/link' })}><LinkIcon size={16}/> Open link</button>
                  </div>
                </div>
                <div className="flex gap-3">
                  <button className="btn btn-primary" disabled={pinBusy} onClick={plexCreatePin}><LinkIcon size={16}/> Create PIN</button>
                  <button className="btn" disabled={pinBusy || !pinId} onClick={plexPollPin}><RefreshCw size={16}/> I entered the code</button>
                  <button className="btn" disabled={busy} onClick={saveConfig}><Save size={16}/> Save</button>
                </div>
              </div>

              {/* SIMKL */}
              <div className="space-y-4">
                <div className="section-title">Authentication — SIMKL</div>
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
                  <button className="btn btn-primary" disabled={busy || !simklReady} onClick={async () => { await saveConfig(); connectSimkl(); }}><RefreshCw size={16}/> Connect SIMKL</button>
                  <button className="btn" disabled={busy} onClick={saveConfig}><Save size={16}/> Save</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
