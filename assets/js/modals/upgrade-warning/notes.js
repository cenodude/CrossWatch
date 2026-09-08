/* Release highlights from the standard CrossWatch release format. */
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;", "<":"&lt;", ">":"&gt;", '\"':"&quot;", "'":"&#39;"})[c]);
}

export function releaseNotesUrl(value, fallback = "https://github.com/cenodude/CrossWatch/releases") {
  try {
    const url = new URL(value);
    if (["https:", "http:"].includes(url.protocol) && !url.username && !url.password) return url.href;
  } catch {}
  return fallback;
}

// Escape each original token before adding markup; never process generated HTML.
export function renderInlineMarkup(value) {
  const source = String(value ?? "");
  const tokens = /(`[^`]+`|\*\*[^*]+\*\*|\[[^\]\n]+\]\([^\s)]+\))/g;
  let html = "", offset = 0;
  for (const match of source.matchAll(tokens)) {
    html += escapeHtml(source.slice(offset, match.index));
    const token = match[0];
    if (token.startsWith("`")) html += `<code>${escapeHtml(token.slice(1, -1))}</code>`;
    else if (token.startsWith("**")) html += `<strong>${escapeHtml(token.slice(2, -2))}</strong>`;
    else {
      const link = token.match(/^\[([^\]]+)\]\((.+)\)$/);
      const url = releaseNotesUrl(link[2], "");
      html += url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(link[1])}</a>` : escapeHtml(link[1]);
    }
    offset = match.index + token.length;
  }
  return html + escapeHtml(source.slice(offset));
}

export function parseReleaseNotes(source) {
  const highlights = [], note = [], wikiLinks = [];
  let section = "", current = null, child = false, fence = false;
  for (const raw of String(source || "").replace(/\r\n?/g, "\n").split("\n")) {
    const line = raw.trim();
    if (/^```|^~~~/.test(line)) { fence = !fence; continue; }
    if (fence) continue;
    const heading = line.match(/^#{1,6}\s+(.+)$/);
    if (heading) {
      const name = heading[1].replace(/[^\p{L}\p{N}\s]/gu, "").trim().toLowerCase();
      section = name === "highlights" ? "highlights" : name === "upgrade note" ? "upgrade" : name === "updated wiki" ? "wiki" : "";
      current = null;
      continue;
    }
    const upgrade = line.match(/^(?:\*\*)?Upgrade note:?(?:\*\*)?:?\s*(.*)$/i);
    if (upgrade) { section = "upgrade"; if (upgrade[1]) note.push(upgrade[1]); continue; }
    if (section === "upgrade") { note.push(line); continue; }
    if (section === "wiki") {
      const entry = line.replace(/^[-*+]\s+/, "");
      const link = entry.match(/^(.*?)\[([^\]\n]+)\]\(([^\s)]+)\)\s*$/);
      if (link) {
        const url = releaseNotesUrl(link[3], "");
        const title = link[1].replace(/\s*[-:]\s*$/, "").trim() || link[2];
        if (url && !wikiLinks.some(item => item.url === url)) wikiLinks.push({title, url});
      }
      continue;
    }
    if (section !== "highlights") continue;
    const bullet = raw.match(/^(\s*)[-*+]\s+(.+)$/);
    if (bullet) {
      child = bullet[1].length > 0;
      if (!child) { current = {text: bullet[2], children: []}; highlights.push(current); }
      else if (current) current.children.push(bullet[2]);
    } else if (line && /^\s+/.test(raw) && current) {
      if (child && current.children.length) current.children[current.children.length - 1] += ` ${line}`;
      else current.text += ` ${line}`;
    }
  }
  return {highlights, upgradeNote: note.join("\n").trim(), wikiLinks};
}
