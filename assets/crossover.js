/**
 * Minimal UI helper: given the API payload from /api/sync/providers,
 * populate a <select id="sync-mode"> with available modes for current pair.
 * Expects two selects with ids #src-provider and #dst-provider.
 */
export async function populateSyncModes() {
  const res = await fetch('/api/sync/providers');
  const data = await res.json();
  const src = document.getElementById('src-provider')?.value?.toUpperCase();
  const dst = document.getElementById('dst-provider')?.value?.toUpperCase();
  const select = document.getElementById('sync-mode');
  if (!select || !src || !dst) return;

  const dir = data.directions.find(d => d.source === src && d.target === dst)
           || data.directions.find(d => d.source === dst && d.target === src); // fallback
  const modes = dir?.modes || [];
  select.innerHTML = '';
  modes.forEach(m => {
    const opt = document.createElement('option');
    opt.value = m;
    opt.textContent = m === 'two-way' ? 'Two-way (bidirectional)' : 'One-way';
    select.appendChild(opt);
  });
  if (modes.length === 0) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = 'Not supported';
    select.appendChild(opt);
  }
}
