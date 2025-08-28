import React, { useEffect, useMemo, useRef, useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { listen, UnlistenFn } from '@tauri-apps/api/event'

// ---------------- Types matching Rust ----------------

type PlexCfg = {
  account_token?: string | null
}

type SimklCfg = {
  client_id: string
  client_secret: string
  access_token?: string | null
  refresh_token?: string | null
  token_expires_at?: number | null
}

type BidirectionalCfg = {
  enabled: boolean
  mode: string
  source_of_truth: string
}

type ActivityCfg = {
  use_activity: boolean
  types: string[]
}

type SyncCfg = {
  enable_add: boolean
  enable_remove: boolean
  bidirectional: BidirectionalCfg
  activity: ActivityCfg
}

type RuntimeCfg = {
  debug: boolean
}

type Config = {
  plex?: PlexCfg
  simkl?: SimklCfg
  sync?: SyncCfg
  runtime?: RuntimeCfg
}

// ---------------- Defaults & helpers ----------------

const defaultCfg: Config = {
  plex: { account_token: '' },
  simkl: { client_id: '', client_secret: '', access_token: '', refresh_token: '', token_expires_at: 0 },
  runtime: { debug: false },
  sync: {
    enable_add: true,
    enable_remove: true,
    bidirectional: { enabled: false, mode: 'mirror', source_of_truth: 'plex' },
    activity: { use_activity: false, types: [] }
  }
}

function ensureCfgShape(c: Partial<Config>): Config {
  const merged: Config = {
    plex: { account_token: '', ...(c.plex || {}) },
    simkl: { client_id: '', client_secret: '', access_token: '', refresh_token: '', token_expires_at: 0, ...(c.simkl || {}) },
    runtime: { debug: false, ...(c.runtime || {}) },
    sync: {
      enable_add: true,
      enable_remove: true,
      bidirectional: { enabled: false, mode: 'mirror', source_of_truth: 'plex', ...(c.sync?.bidirectional || {}) },
      activity: { use_activity: false, types: [], ...(c.sync?.activity || {}) },
      ...(c.sync || {})
    }
  }
  return merged
}

const pill = (ok: boolean) => (
  <span style={{ padding: '2px 8px', borderRadius: 999, fontSize: 12, background: ok ? 'rgba(16,185,129,.15)' : 'rgba(239,68,68,.12)', color: ok ? '#10b981' : '#ef4444' }}>
    {ok ? 'Connected' : 'Not connected'}
  </span>
)

function Section({ title, right, children }: { title: string; right?: React.ReactNode; children: React.ReactNode }) {
  return (
    <div style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 16, padding: 16, marginBottom: 16 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <h3 style={{ margin: 0, fontSize: 16, letterSpacing: .5 }}>{title}</h3>
        <div>{right}</div>
      </div>
      {children}
    </div>
  )
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '220px 1fr', gap: 12, alignItems: 'center', margin: '10px 0' }}>
      <div style={{ opacity: .8 }}>{label}</div>
      <div>{children}</div>
    </div>
  )
}

function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input {...props} style={{ ...(props.style || {}), width: '100%', background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.12)', borderRadius: 10, padding: '10px 12px', color: 'white' }} />
  )
}

function Button(props: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  const { children, style, ...rest } = props
  return (
    <button {...rest} style={{ ...(style || {}), background: 'linear-gradient(180deg, rgba(59,130,246,0.25), rgba(59,130,246,0.15))', border: '1px solid rgba(59,130,246,0.35)', borderRadius: 12, padding: '10px 14px', color: 'white' }}>
      {children}
    </button>
  )
}

function GhostButton(props: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  const { children, style, ...rest } = props
  return (
    <button {...rest} style={{ ...(style || {}), background: 'transparent', border: '1px solid rgba(255,255,255,0.15)', borderRadius: 12, padding: '8px 12px', color: 'white' }}>
      {children}
    </button>
  )
}

// ---------------- App ----------------

export default function App() {
  const [view, setView] = useState<'main' | 'settings'>('main')
  const [cfg, setCfg] = useState<Config>(defaultCfg)
  const [log, setLog] = useState<string>('')
  const [loading, setLoading] = useState<boolean>(true)

  // collapsibles
  const [plexOpen, setPlexOpen] = useState<boolean>(false)     // default closed as requested
  const [simklOpen, setSimklOpen] = useState<boolean>(false)    // default closed

  // Plex pin state
  const [pinBusy, setPinBusy] = useState(false)
  const [pinCode, setPinCode] = useState<string>('')
  const [pinId, setPinId] = useState<number | null>(null)
  const [pinExp, setPinExp] = useState<number>(0)
  const [clientId, setClientId] = useState<string>('')
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    (async () => {
      try {
        const c = await invoke<Config>('cmd_read_config')
        setCfg(ensureCfgShape(c || {}))
      } catch (e: any) {
        setLog(l => `${l}\n[Init] Failed to read config: ${e?.toString?.() ?? e}`)
      } finally {
        setLoading(false)
      }
    })()

    let unlisten: UnlistenFn | null = null
    ;(async () => {
      unlisten = await listen('simkl_linked', async (event) => {
        const payload = (event as any)?.payload
        if (payload?.ok) {
          setLog(l => `${l}\n[SIMKL] Linked successfully.`)
          // refresh config (tokens written by backend)
          try {
            const c = await invoke<Config>('cmd_read_config')
            setCfg(ensureCfgShape(c || {}))
          } catch {}
        } else {
          setLog(l => `${l}\n[SIMKL] Link failed: ${payload?.error ?? 'Unknown error'}`)
        }
      })
    })()

    return () => {
      if (unlisten) unlisten()
      if (pollRef.current) clearTimeout(pollRef.current)
    }
  }, [])

  const plexConnected = !!cfg?.plex?.account_token
  const simklConnected = !!cfg?.simkl?.access_token

  async function saveConfig() {
    const shaped = ensureCfgShape(cfg)
    await invoke('cmd_write_config', { cfg: shaped })
    setLog(l => `${l}\n[Config] Saved.`)
  }

  // --------- Plex PIN flow ----------
  async function createPlexPin() {
    try {
      setPinBusy(true)
      const out = await invoke<{ id: number; code: string; expires_at: number; client_id: string }>('cmd_plex_create_pin')
      setPinId(out.id)
      setPinCode(out.code)
      setPinExp(out.expires_at)
      setClientId(out.client_id)

      // auto-copy pin (requested)
      try {
        await navigator.clipboard.writeText(out.code)
        setLog(l => `${l}\n[Plex] PIN: ${out.code} (copied). Expires at ${new Date(out.expires_at * 1000).toLocaleTimeString()}`)
      } catch {
        setLog(l => `${l}\n[Plex] PIN: ${out.code}. Expires at ${new Date(out.expires_at * 1000).toLocaleTimeString()} (copy failed)`)
      }

      // open browser small
      await invoke('cmd_open_external_sized', { url: 'https://plex.tv/link', width: 520, height: 720 })

      // start polling
      const poll = async () => {
        if (!pinId || !clientId) return
        if (Date.now() / 1000 > pinExp) {
          setPinBusy(false)
          setLog(l => `${l}\n[Plex] PIN expired; please create a new PIN.`)
          return
        }
        try {
          const token = await invoke<string>('cmd_plex_poll_pin', { id: pinId, clientId })
          if (token) {
            setLog(l => `${l}\n[Plex] Token received.`)
            setCfg(prev => ensureCfgShape({ ...prev, plex: { ...(prev.plex || {}), account_token: token } }))
            await saveConfig()
            setPinBusy(false)
            return
          }
        } catch (e: any) {
          setLog(l => `${l}\n[Plex] Poll error: ${e?.toString?.() ?? e}`)
        }
        pollRef.current = setTimeout(poll, 1500)
      }
      pollRef.current = setTimeout(poll, 1500)
    } catch (e: any) {
      setLog(l => `${l}\n[Plex] Error creating PIN: ${e?.toString?.() ?? e}`)
      setPinBusy(false)
    }
  }

  // --------- SIMKL Link (loopback) ----------
  function randomState(len = 16) {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
    return Array.from({ length: len }, () => chars[Math.floor(Math.random() * chars.length)]).join('')
  }

  async function linkSIMKL() {
    const id = cfg.simkl?.client_id?.trim() || ''
    const secret = cfg.simkl?.client_secret?.trim() || ''
    if (!id || !secret) {
      setLog(l => `${l}\n[SIMKL] Please fill Client ID and Secret first.`)
      return
    }
    try {
      await invoke('cmd_simkl_start_listener')
      const state = randomState()
      const url = `https://simkl.com/oauth/authorize?response_type=code&client_id=${encodeURIComponent(id)}&redirect_uri=${encodeURIComponent('http://127.0.0.1:8787/callback')}&state=${encodeURIComponent(state)}`
      await invoke('cmd_open_external_sized', { url, width: 520, height: 720 })
      setLog(l => `${l}\n[SIMKL] Waiting for authorization…`)
    } catch (e: any) {
      setLog(l => `${l}\n[SIMKL] Failed to start loopback listener: ${e?.toString?.() ?? e}`)
    }
  }

  // --------------- UI ---------------

  if (loading) {
    return (
      <div style={{ minHeight: '100vh', background: '#0b0f16', color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'Inter, system-ui, Arial' }}>
        Loading…
      </div>
    )
  }

  return (
    <div style={{ minHeight: '100vh', background: '#0b0f16', color: 'white', fontFamily: 'Inter, system-ui, Arial' }}>
      <div style={{ maxWidth: 1100, margin: '0 auto', padding: 24 }}>
        {/* Top bar */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18 }}>
          <h1 style={{ margin: 0, fontSize: 22, letterSpacing: .5 }}>CrossWatch</h1>
          <div style={{ display: 'flex', gap: 8 }}>
            <GhostButton onClick={() => setView('main')}>Main</GhostButton>
            <GhostButton onClick={() => setView('settings')}>Settings</GhostButton>
          </div>
        </div>

        {view === 'main' ? (
          <>
            <Section
              title="Sync"
              right={<div style={{ display: 'flex', gap: 10 }}>
                <div>Plex: {pill(plexConnected)}</div>
                <div>SIMKL: {pill(simklConnected)}</div>
              </div>}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <Button onClick={async () => {
                  try {
                    await invoke('cmd_write_config', { cfg: ensureCfgShape(cfg) }) // ensure most recent on disk
                    await invoke('cmd_run_sync')
                    setLog(l => `${l}\n[Sync] Script finished OK.`)
                  } catch (e: any) {
                    setLog(l => `${l}\n[Sync] Error: ${e?.toString?.() ?? e}`)
                  }
                }}>Run Sync</Button>
              </div>
            </Section>

            <Section title="Log">
              <pre style={{ whiteSpace: 'pre-wrap', background: 'rgba(0,0,0,0.35)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 12, padding: 12, minHeight: 140, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                {log.trim() || 'Ready.'}
              </pre>
            </Section>
          </>
        ) : (
          <>
            {/* Runtime + Sync settings */}
            <Section title="General settings">
              <Row label="Debug">
                <input
                  type="checkbox"
                  checked={!!cfg.runtime?.debug}
                  onChange={(e) => setCfg(ensureCfgShape({ ...cfg, runtime: { ...(cfg.runtime || {}), debug: e.target.checked } }))}
                />
              </Row>

              <Row label="Sync: allow add">
                <input
                  type="checkbox"
                  checked={!!cfg.sync?.enable_add}
                  onChange={(e) => setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), enable_add: e.target.checked } }))}
                />
              </Row>
              <Row label="Sync: allow remove">
                <input
                  type="checkbox"
                  checked={!!cfg.sync?.enable_remove}
                  onChange={(e) => setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), enable_remove: e.target.checked } }))}
                />
              </Row>

              <Row label="Sync mode">
                <select
                  value={cfg.sync?.bidirectional?.enabled ? 'two-way' : (cfg.sync?.bidirectional?.mode || 'mirror')}
                  onChange={(e) => {
                    const v = e.target.value
                    if (v === 'two-way') {
                      setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), bidirectional: { ...(cfg.sync?.bidirectional as any || {}), enabled: true } } }))
                    } else {
                      setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), bidirectional: { ...(cfg.sync?.bidirectional as any || {}), enabled: false, mode: 'mirror' } } }))
                    }
                  }}
                  style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.12)', borderRadius: 10, color: 'white', padding: '8px 10px' }}
                >
                  <option value="mirror">Mirror (one-way)</option>
                  <option value="two-way">Two-way</option>
                </select>
              </Row>

              <Row label="Activity: use activity">
                <input
                  type="checkbox"
                  checked={!!cfg.sync?.activity?.use_activity}
                  onChange={(e) =>
                    setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), activity: { ...(cfg.sync?.activity as any || {}), use_activity: e.target.checked } } }))
                  }
                />
              </Row>

              <Row label="Activity types">
                <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
                  <label style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                    <input
                      type="checkbox"
                      checked={(cfg.sync?.activity?.types || []).includes('watchlist')}
                      onChange={(e) => {
                        const on = e.target.checked
                        const types = new Set(cfg.sync?.activity?.types ?? [])
                        if (on) {
                          types.add('watchlist')
                        } else {
                          types.delete('watchlist')
                        }
                        setCfg(ensureCfgShape({ ...cfg, sync: { ...(cfg.sync as any), activity: { ...(cfg.sync?.activity as any || {}), types: Array.from(types) } } }))
                      }}
                    />
                    Watchlist
                  </label>
                </div>
              </Row>

              <div style={{ marginTop: 12 }}>
                <Button onClick={saveConfig}>Save settings</Button>
              </div>
            </Section>

            {/* Authentication row: two columns */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
              {/* Plex */}
              <Section
                title="Authentication — Plex"
                right={<div>{pill(plexConnected)}</div>}
              >
                {/* Collapsible header */}
                <div style={{ marginBottom: 8 }}>
                  <GhostButton onClick={() => setPlexOpen(v => !v)}>{plexOpen ? 'Hide' : 'Show'}</GhostButton>
                </div>
                {plexOpen && (
                  <div>
                    <Row label="Account token">
                      <Input
                        value={cfg.plex?.account_token || ''}
                        placeholder="Paste Plex token or let PIN flow fill it…"
                        onChange={(e) => setCfg(ensureCfgShape({ ...cfg, plex: { ...(cfg.plex || {}), account_token: e.target.value } }))}
                      />
                    </Row>
                    <div style={{ display: 'flex', gap: 8, alignItems: 'center', margin: '8px 0 2px' }}>
                      <Button onClick={createPlexPin} disabled={pinBusy}>
                        {pinBusy ? 'Waiting…' : 'Create PIN'}
                      </Button>
                      {!!pinCode && <span style={{ opacity: .85 }}>PIN: <b style={{ letterSpacing: 2 }}>{pinCode}</b></span>}
                    </div>
                    <div style={{ marginTop: 10 }}>
                      <Button onClick={saveConfig}>Save</Button>
                    </div>
                  </div>
                )}
              </Section>

              {/* SIMKL */}
              <Section
                title="Authentication — SIMKL"
                right={<div>{pill(simklConnected)}</div>}
              >
                <div style={{ marginBottom: 8 }}>
                  <GhostButton onClick={() => setSimklOpen(v => !v)}>{simklOpen ? 'Hide' : 'Show'}</GhostButton>
                </div>
                {simklOpen && (
                  <div>
                    <Row label="Client ID">
                      <Input
                        value={cfg.simkl?.client_id || ''}
                        onChange={(e) => setCfg(ensureCfgShape({ ...cfg, simkl: { ...(cfg.simkl || {}), client_id: e.target.value } }))}
                        placeholder="SIMKL client id"
                      />
                    </Row>
                    <Row label="Client Secret">
                      <Input
                        value={cfg.simkl?.client_secret || ''}
                        onChange={(e) => setCfg(ensureCfgShape({ ...cfg, simkl: { ...(cfg.simkl || {}), client_secret: e.target.value } }))}
                        placeholder="SIMKL client secret"
                      />
                    </Row>
                    <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 8 }}>
                      <Button onClick={linkSIMKL}>Link SIMKL</Button>
                    </div>
                    <div style={{ marginTop: 10 }}>
                      <Button onClick={saveConfig}>Save</Button>
                    </div>
                  </div>
                )}
              </Section>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

async function simklCreatePin(clientId: string) {
  const created = await invoke<any>('cmd_simkl_create_pin', { clientId, redirect: 'https://simkl.com/apps/crosswatch/connected/' })
  const code = created.code || created.user_code
  if (code) {
    setSimklPin(code)
    await navigator.clipboard.writeText(code)
    setSimklStatus('PIN copied. Approve at simkl.com/pin, then Start Polling.')
  } else {
    setSimklStatus('Failed to create SIMKL PIN.')
  }
}

async function simklPollOnce(clientId: string, code: string) {
  const r = await invoke<any>('cmd_simkl_poll_pin', { clientId, code })
  if (r && r.access_token) {
    setSimklStatus('Linked')
    setSimklPolling(false)
    // TODO: write into config like your existing save path if needed
  }
}
