function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderInlineMarkup(text) {
  const linkLabel = (url) => {
    const issue = String(url || "").match(/^https?:\/\/github\.com\/cenodude\/CrossWatch\/issues\/(\d+)\/?$/i);
    if (issue) return `#${issue[1]}`;
    const pull = String(url || "").match(/^https?:\/\/github\.com\/cenodude\/CrossWatch\/pull\/(\d+)\/?$/i);
    if (pull) return `PR #${pull[1]}`;
    return url;
  };

  let out = escapeHtml(text || "");
  out = out.replace(/`([^`]+)`/g, '<code>$1</code>');
  out = out.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/\*([^*]+)\*/g, "<em>$1</em>");
  out = out.replace(
    /(https?:\/\/[^\s<]+)/g,
    (_m, url) => `<a href="${url}" target="_blank" rel="noopener noreferrer">${escapeHtml(linkLabel(url))}</a>`,
  );
  return out;
}

export function renderNotesMarkup(src) {
  const lines = String(src || "").replace(/\r\n?/g, "\n").split("\n");
  const html = [];
  let listOpen = false;

  const closeList = () => {
    if (!listOpen) return;
    html.push("</ul>");
    listOpen = false;
  };

  for (const raw of lines) {
    const line = String(raw || "");
    const trimmed = line.trim();

    if (!trimmed) {
      closeList();
      continue;
    }

    const heading = trimmed.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      closeList();
      const level = Math.min(3, heading[1].length + 1);
      html.push(`<h${level}>${renderInlineMarkup(heading[2])}</h${level}>`);
      continue;
    }

    const bullet = line.match(/^(\s*)[-*]\s+(.+)$/);
    if (bullet) {
      if (!listOpen) {
        html.push('<ul class="notes-list">');
        listOpen = true;
      }
      const indent = Math.min(2, Math.floor((bullet[1] || "").length / 2));
      const cls = indent > 0 ? ` class="indent-${indent}"` : "";
      html.push(`<li${cls}>${renderInlineMarkup(bullet[2])}</li>`);
      continue;
    }

    closeList();
    html.push(`<p>${renderInlineMarkup(trimmed)}</p>`);
  }

  closeList();
  return html.join("");
}
