
import React, { useEffect, useState } from 'react'
import { invoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'

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
  runtime?: { debug?: boolean }
  sync?: {
    enable_add?: boolean
    enable_remove?: boolean
    bidirectional?: { enabled: boolean, mode: string, source_of_truth: string }
    activity?: { use_activity: boolean, types: string[] }
  }
}

export default function App() {
  const [cfg, setCfg] = useState<Config | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  // SIMKL PIN state
  const [simklPin, setSimklPin] = useState<string | null>(null)
  const [simklStatus, setSimklStatus] = useState<string | null>(null)
  const [polling, setPolling] = useState(false)

  useEffect(() => { load() }, [])

  async function load() {
    try {
      const c = await invoke<Config>('cmd_read_config')
      setCfg(c)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  async function saveConfig() {
    if (!cfg) return
    setSaving(true)
    try {
      await invoke('cmd_write_config', { cfg })
    } catch (e) {
      console.error(e)
    } finally {
      setSaving(false)
    }
  }

  async function plexCreatePin() {
    try {
      const pin = await invoke<any>('cmd_plex_create_pin')
      const code: string = pin.code
      await navigator.clipboard.writeText(code)
      // open the link page in small window
      await invoke('cmd_open_external_sized', { url: 'https://plex.tv/link', width: 520, height: 720 })
      alert('Plex PIN copied to clipboard: ' + code)
    } catch (e) {
      alert('Plex PIN error: ' + e)
    }
  }

  async function plexPollPin(pinId: number) {
    try {
      const token = await invoke<string | null>('cmd_plex_poll_pin', { pinId })
      if (token) {
        const next = { ...(cfg || {}), plex: { account_token: token } }
        setCfg(next as any)
        await invoke('cmd_write_config', { cfg: next })
        alert('Plex linked')
      } else {
        alert('Not linked yet. Keep trying.')
      }
    } catch (e) {
      alert('Plex poll error: ' + e)
    }
  }

  async function simklCreatePin() {
    if (!cfg?.simkl?.client_id) {
      alert('Enter SIMKL Client ID first.')
      return
    }
    try {
      const redirect = 'https://simkl.com/apps/crosswatch/connected/'
      const created = await invoke<any>('cmd_simkl_create_pin', { clientId: cfg.simkl.client_id, redirect })
      const code = created.code || created.user_code
      if (!code) {
        alert('SIMKL did not return a code.')
        return
      }
      setSimklPin(code)
      await navigator.clipboard.writeText(code)
      setSimklStatus('Waiting for approval at simkl.com/pin…')
    } catch (e) {
      alert('SIMKL PIN error: ' + e)
    }
  }

  async function simklStartPolling() {
    if (!cfg?.simkl?.client_id || !simklPin) return
    if (polling) return
    setPolling(true)
    setSimklStatus('Polling…')
    const pollOnce = async () => {
      try {
        const r = await invoke<any>('cmd_simkl_poll_pin', { code: simklPin, clientId: cfg!.simkl!.client_id })
        if (r && (r as any).access_token) {
          const next = {
            ...(cfg || {}),
            simkl: {
              ...(cfg?.simkl || { client_id: '', client_secret: '' }),
              access_token: (r as any).access_token,
              refresh_token: (r as any).refresh_token || cfg?.simkl?.refresh_token || null,
            }
          }
          setCfg(next as any)
          await invoke('cmd_write_config', { cfg: next })
          setSimklStatus('Linked')
          setPolling(false)
        } else {
          // keep polling
          setTimeout(pollOnce, 2000)
        }
      } catch (e) {
        console.error(e)
        setTimeout(pollOnce, 3000)
      }
    }
    pollOnce()
  }

  function simklConnected(): boolean {
    return !!cfg?.simkl?.access_token
  }

  if (loading) return <div className='p-6 text-sm'>Loading…</div>

  return (
    <div className='p-4 text-sm font-sans'>
      <div className='flex items-center gap-2 mb-4'>
        <button className='px-3 py-2 rounded bg-black text-white' onClick={() => invoke('cmd_run_sync')}>
          Run Sync
        </button>
        <div className='ml-4 px-2 py-1 rounded bg-green-100 text-green-800'>
          Plex: {cfg?.plex?.account_token ? 'Connected' : 'Not connected'}
        </div>
        <div className='px-2 py-1 rounded ' style={{backgroundColor: simklConnected() ? '#dcfce7' : '#fee2e2', color: simklConnected() ? '#166534' : '#991b1b'}}>
          SIMKL: {simklConnected() ? 'Connected' : 'Not connected'}
        </div>
      </div>

      <div className='grid grid-cols-2 gap-8'>
        {/* PLEX */}
        <div className='border rounded p-4'>
          <div className='font-semibold mb-2'>Plex</div>
          <div className='flex gap-2 items-center mb-2'>
            <button className='px-3 py-2 rounded bg-gray-200' onClick={plexCreatePin}>Create PIN</button>
            <button className='px-3 py-2 rounded bg-gray-200' onClick={() => {
              const id = parseInt(window.prompt('Enter Plex PIN id from create response') || '0', 10)
              if (id) plexPollPin(id)
            }}>Poll PIN</button>
          </div>
        </div>

        {/* SIMKL */}
        <div className='border rounded p-4'>
          <div className='font-semibold mb-2'>SIMKL</div>
          <div className='mb-2'>
            <label className='block text-xs'>Client ID</label>
            <input className='border rounded px-2 py-1 w-full' value={cfg?.simkl?.client_id || ''} onChange={e => setCfg({...cfg!, simkl: {...(cfg?.simkl || {client_id:'', client_secret:''}), client_id: e.target.value}})} />
          </div>
          <div className='mb-2'>
            <label className='block text-xs'>Client Secret (optional)</label>
            <input className='border rounded px-2 py-1 w-full' value={cfg?.simkl?.client_secret || ''} onChange={e => setCfg({...cfg!, simkl: {...(cfg?.simkl || {client_id:'', client_secret:''}), client_secret: e.target.value}})} />
          </div>

          <div className='flex gap-2 items-center mb-2'>
            <button className='px-3 py-2 rounded bg-gray-200' onClick={simklCreatePin}>Create PIN</button>
            <button className='px-3 py-2 rounded bg-gray-200' disabled={!simklPin} onClick={simklStartPolling}>Start Polling</button>
            {simklPin && <code className='ml-2'>PIN: {simklPin}</code>}
          </div>
          {simklStatus && <div className='text-xs opacity-70'>{simklStatus}</div>}
        </div>
      </div>

      <div className='mt-6 flex gap-2'>
        <button className='px-3 py-2 rounded bg-blue-600 text-white disabled:opacity-50' disabled={saving} onClick={saveConfig}>
          Save
        </button>
        <div className='text-xs opacity-60 self-center'>Config path: %APPDATA%\CrossWatch\config.json (+ mirrored ./config.json)</div>
      </div>
    </div>
  )
}
