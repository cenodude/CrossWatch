import React, { useEffect, useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { LinkIcon, RefreshCw, PlayCircle, Save, Settings, X, ChevronDown, Check, XCircle } from 'lucide-react'

type PlexCfg = { account_token?: string | null }
type SimklCfg = {
  client_id: string
  client_secret: string
  access_token?: string | null
  refresh_token?: string | null
  token_expires_at?: number | null
}
type Config = {
  plex?: PlexCfg
  simkl?: SimklCfg
  sync?: { mode?: string }
  debug?: boolean
  [key: string]: any
}

const Badge = ({ ok }: { ok: boolean }) => (
  <span className={`px-3 py-1 text-xs rounded-lg ring-1 ${ok ? 'text-emerald-200 bg-emerald-900/40 ring-emerald-700/50' : 'text-rose-200 bg-rose-900/40 ring-rose-700/50'}`}>
    {ok ? 'Connected' : 'Not connected'}
  </span>
)

export default function App() {
  const [cfg, setCfg] = useState<Config>({})
  const [loading, setLoading] = useState(true)
  const [log, setLog] = useState('')
  const [showSettings, setShowSettings] = useState(false)

  // Collapsibles
  const [plexOpen, setPlexOpen] = useState(false)
  const [simklOpen, setSimklOpen] = useState(false)

  // Plex PIN state
  const [pinId, setPinId] = useState<number | null>(null)
  const [pinCode, setPinCode] = useState<string>('')
  const [pinCid, setPinCid] = useState<string>('')
  const [pinBusy, setPinBusy] = useState(false)

  // Toast
  const [toast, setToast] = useState<string>('')
  const [showToast, setShowToast] = useState(false)
  const showToastMsg = (msg: string) => { setToast(msg); setShowToast(true); setTimeout(()=>setShowToast(false), 2200) }

  const copyToClipboard = async (t: string) => {
    try { if ((navigator as any)?.clipboard?.writeText) { await (navigator as any).clipboard.writeText(t); return true } } catch {}
    try { const ta = document.createElement('textarea'); ta.value = t; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); return true } catch {}
    return false
  }

  const plexConnected = !!cfg.plex?.account_token
  const simklConnected = !!cfg.simkl?.access_token

  useEffect(() => {
    (async () => {
      try {
        const c = await invoke<Config>('cmd_read_config')
        setCfg(c || {})
      } catch (e:any) {
        setLog(l => l + `\n[Init] ${e?.toString()}`)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  const saveConfig = async () => {
    try {
      await invoke('cmd_write_config', { cfg })
      showToastMsg('Settings saved')
    } catch (e:any) {
      setLog(l => l + `\n[Save] ${e?.toString()}`)
    }
  }

  async function plexCreatePin() {
    try {
      setPinBusy(true)
      setLog(l => l + `\n[Plex] Requesting PIN...`)
      const res = await invoke<{ id: number, code: string, expires_at: number, client_id: string }>('cmd_plex_create_pin')
      setPinId(res.id); setPinCode(res.code); setPinCid(res.client_id)
      try { const ok = await copyToClipboard(res.code); if (ok) showToastMsg('PIN copied to clipboard') } catch {}
      await invoke('cmd_open_external_sized', { url: 'https://plex.tv/link', width: 520, height: 720 })

      const deadline = Date.now() + (res.expires_at * 1000 - Date.now())
      const poll = async () => {
        try {
          const token = await invoke<string>('cmd_plex_poll_pin', { id: res.id, clientId: res.client_id })
          if (token) {
            setLog(l => l + `\n[Plex] Token received.`)
            setCfg(prev => ({ ...prev, plex: { account_token: token } }))
            setPinBusy(false)
            return
          }
        } catch (e:any) {
          setLog(l => l + `\n[Plex] Poll error: ${e?.toString()}`)
        }
        if (Date.now() < deadline) setTimeout(poll, 1500)
        else { setPinBusy(false); setLog(l => l + `\n[Plex] PIN expired; request a new one.`) }
      }
      setTimeout(poll, 1200)
    } catch (e:any) {
      setLog(l => l + `\n[Plex] Error creating PIN: ${e?.toString()}`)
      setPinBusy(false)
    }
  }

  const runSync = async () => {
    try {
      setLog(l => l + `\n[Sync] Running...`)
      await invoke('cmd_run_sync')
      setLog(l => l + `\n[Sync] Done.`)
    } catch (e:any) {
      setLog(l => l + `\n[Sync] Error: ${e?.toString()}`)
    }
  }

  if (loading) {
    return <div className="min-h-screen bg-zinc-950 text-zinc-100 flex items-center justify-center">Loading…</div>
  }

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100">
      {/* Header */}
      <div className="px-6 py-4 flex items-center justify-between border-b border-zinc-800/80">
        <div className="flex items-center gap-3">
          <span className="text-lg font-semibold tracking-wide">CrossWatch</span>
          <span className="text-xs text-zinc-400">Plex ↔︎ SIMKL</span>
        </div>
        <button className="btn subtle" onClick={() => setShowSettings(true)}><Settings size={16} className="mr-2"/>Settings</button>
      </div>

      {/* Main */}
      <div className="p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-4">
          <div className="rounded-2xl bg-zinc-900/50 ring-1 ring-zinc-800/70 p-6">
            <div className="flex items-center gap-3">
              <button className="btn" onClick={runSync}><PlayCircle size={16} className="mr-2"/>Run Sync</button>
              <div className="text-xs text-zinc-400">Make sure both providers are connected.</div>
            </div>
          </div>

          <div className="rounded-2xl bg-zinc-900/50 ring-1 ring-zinc-800/70 p-5 grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="flex items-center justify-between rounded-xl bg-zinc-900/50 ring-1 ring-zinc-800/70 px-4 py-3">
              <div className="flex items-center gap-2"><span className="font-medium">Plex</span></div>
              {plexConnected ? <Check size={18} className="text-emerald-400"/> : <XCircle size={18} className="text-rose-400"/>}
            </div>
            <div className="flex items-center justify-between rounded-xl bg-zinc-900/50 ring-1 ring-zinc-800/70 px-4 py-3">
              <div className="flex items-center gap-2"><span className="font-medium">SIMKL</span></div>
              {simklConnected ? <Check size={18} className="text-emerald-400"/> : <XCircle size={18} className="text-rose-400"/>}
            </div>
          </div>

          <div className="rounded-2xl bg-zinc-900/50 ring-1 ring-zinc-800/70 p-5">
            <div className="text-xs text-zinc-400 whitespace-pre-wrap leading-relaxed min-h-[120px]">{log || 'Logs will appear here…'}</div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-2xl bg-zinc-900/50 ring-1 ring-zinc-800/70 p-5">
            <div className="text-sm text-zinc-400 mb-3">Quick actions</div>
            <div className="flex flex-col gap-3">
              <button className="btn" onClick={() => setShowSettings(true)}><Settings size={16} className="mr-2"/>Open Settings</button>
              <button className="btn subtle" onClick={saveConfig}><Save size={16} className="mr-2"/>Save Settings</button>
            </div>
          </div>
        </div>
      </div>

      {/* Toast */}
      {showToast && (
        <div className="fixed right-4 bottom-4 z-[60]">
          <div className="rounded-xl bg-zinc-900/90 ring-1 ring-zinc-700 px-4 py-2 text-sm text-zinc-100 shadow-lg">
            {toast}
          </div>
        </div>
      )}

      {/* Settings Modal */}
      {showSettings && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          <div className="absolute inset-0 bg-black/60" onClick={() => setShowSettings(false)} />
          <div className="relative w-[min(1100px,96vw)] max-h-[90vh] overflow-auto rounded-3xl bg-zinc-950 ring-1 ring-zinc-800/70 p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="text-xl font-semibold flex items-center gap-2"><Settings size={18}/> Settings</div>
              <button className="btn icon subtle" onClick={() => setShowSettings(false)}><X size={16}/></button>
            </div>

            {/* General row */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label className="label">Sync mode</label>
                <select
                  className="input"
                  value={cfg.sync?.mode ?? 'mirror'}
                  onChange={(e) => setCfg(prev => ({ ...prev, sync: { ...(prev.sync ?? {}), mode: e.target.value } }))}
                >
                  <option value="mirror">mirror</option>
                  <option value="two-way">two-way</option>
                </select>
              </div>
              <div>
                <label className="label">Debug</label>
                <select
                  className="input"
                  value={String(cfg.debug ?? 'false')}
                  onChange={(e) => setCfg(prev => ({ ...prev, debug: e.target.value === 'true' }))}
                >
                  <option value="false">false</option>
                  <option value="true">true</option>
                </select>
              </div>
            </div>

            <div className="my-6 h-px w-full bg-zinc-800/80" />

            {/* Auth grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 items-start">
              {/* PLEX column */}
              <div className="space-y-2 min-w-0">
                <button
                  className="w-full flex items-center justify-between rounded-2xl px-4 py-3 bg-zinc-800/60 hover:bg-zinc-800 transition-colors"
                  onClick={() => setPlexOpen(o => !o)}
                >
                  <div className="flex items-center gap-2">
                    <div className={`transition-transform ${plexOpen ? '' : '-rotate-90'}`}><ChevronDown size={16}/></div>
                    <div className="section-title">Authentication — PLEX</div>
                  </div>
                  <Badge ok={plexConnected}/>
                </button>

                <div className={`overflow-hidden transition-[max-height,opacity] duration-300 ${plexOpen ? 'max-h-[800px] opacity-100' : 'max-h-0 opacity-0'}`}>
                  <div className="pt-3 space-y-4">
                    <div>
                      <label className="label">Account token</label>
                      <input
                        className="input"
                        placeholder="Plex account token"
                        value={cfg.plex?.account_token ?? ''}
                        onChange={(e) => setCfg(prev => ({ ...prev, plex: { ...(prev.plex ?? {}), account_token: e.target.value } }))}
                      />
                    </div>

                    <div className="rounded-xl bg-zinc-900/40 p-3 ring-1 ring-zinc-800/70">
                      <div className="text-xs text-zinc-400 mb-2">Create a PIN (auto-copied), we open plex.tv/link in a small window, and will poll until accepted.</div>
                      <div className="flex items-center gap-3">
                        <button className="btn" disabled={pinBusy} onClick={plexCreatePin}>
                          {pinBusy ? <RefreshCw className="spin mr-2" size={16}/> : <LinkIcon className="mr-2" size={16}/>}
                          Create PIN
                        </button>
                        <input className="input w-28 text-center tracking-[0.3em]" value={pinCode ?? ''} readOnly placeholder="PIN" />
                        <div className="grow" />
                        <button className="btn subtle" onClick={saveConfig}><Save size={16} className="mr-2"/>Save</button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* SIMKL column */}
              <div className="space-y-2 min-w-0">
                <button
                  className="w-full flex items-center justify-between rounded-2xl px-4 py-3 bg-zinc-800/60 hover:bg-zinc-800 transition-colors"
                  onClick={() => setSimklOpen(o => !o)}
                >
                  <div className="flex items-center gap-2">
                    <div className={`transition-transform ${simklOpen ? '' : '-rotate-90'}`}><ChevronDown size={16}/></div>
                    <div className="section-title">Authentication — SIMKL</div>
                  </div>
                  <Badge ok={simklConnected}/>
                </button>

                <div className={`overflow-hidden transition-[max-height,opacity] duration-300 ${simklOpen ? 'max-h-[800px] opacity-100' : 'max-h-0 opacity-0'}`}>
                  <div className="pt-3 space-y-4">
                    <div>
                      <label className="label">Client ID</label>
                      <input
                        className="input"
                        placeholder="SIMKL client_id"
                        value={cfg.simkl?.client_id ?? ''}
                        onChange={(e) => setCfg(prev => ({ ...prev, simkl: { ...(prev.simkl ?? {}), client_id: e.target.value } }))}
                      />
                    </div>
                    <div>
                      <label className="label">Client Secret</label>
                      <input
                        className="input"
                        placeholder="SIMKL client_secret"
                        value={cfg.simkl?.client_secret ?? ''}
                        onChange={(e) => setCfg(prev => ({ ...prev, simkl: { ...(prev.simkl ?? {}), client_secret: e.target.value } }))}
                      />
                    </div>
                    <div className="flex items-center gap-3">
                      <button className="btn" onClick={() => invoke('cmd_simkl_oauth')}>
                        <RefreshCw className="mr-2" size={16}/>Connect SIMKL
                      </button>
                      <div className="grow" />
                      <button className="btn subtle" onClick={saveConfig}><Save size={16} className="mr-2"/>Save</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
