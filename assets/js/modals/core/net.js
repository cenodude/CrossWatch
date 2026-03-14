async function readJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function errorMessage(res, data) {
  return (data && (data.error || data.message)) || `HTTP ${res.status} ${res.statusText}`;
}

export async function getJson(url, opts = {}) {
  const res = await fetch(url, { method: "GET", ...opts });
  const data = await readJson(res);
  if (!res.ok) throw new Error(errorMessage(res, data));
  return data || {};
}

export async function postJson(url, opts = {}) {
  const res = await fetch(url, { method: "POST", ...opts });
  const data = await readJson(res);
  if (!res.ok || (data && data.ok === false)) {
    throw new Error(errorMessage(res, data));
  }
  return data || {};
}
