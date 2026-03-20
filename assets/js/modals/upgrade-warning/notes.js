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
  let quoteOpen = false;
  let codeFence = null;

  const closeList = () => {
    if (!listOpen) return;
    html.push("</ul>");
    listOpen = false;
  };

  const closeQuote = () => {
    if (!quoteOpen) return;
    html.push("</blockquote>");
    quoteOpen = false;
  };

  const closeCodeFence = () => {
    if (!codeFence) return;
    html.push("</code></pre>");
    codeFence = null;
  };

  for (const raw of lines) {
    const line = String(raw || "");
    const trimmed = line.trim();

    if (codeFence) {
      if (/^```/.test(trimmed)) {
        closeCodeFence();
      } else {
        html.push(`${escapeHtml(line)}\n`);
      }
      continue;
    }

    const fence = trimmed.match(/^```([\w-]+)?\s*$/);
    if (fence) {
      closeList();
      closeQuote();
      const lang = String(fence[1] || "").trim();
      const cls = lang ? ` class="lang-${escapeHtml(lang)}"` : "";
      html.push(`<pre class="notes-code"><code${cls}>`);
      codeFence = lang || true;
      continue;
    }

    if (!trimmed) {
      closeList();
      closeQuote();
      continue;
    }

    const heading = trimmed.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      closeList();
      closeQuote();
      const level = Math.min(3, heading[1].length + 1);
      html.push(`<h${level}>${renderInlineMarkup(heading[2])}</h${level}>`);
      continue;
    }

    const bullet = line.match(/^(\s*)[-*]\s+(.+)$/);
    if (bullet) {
      closeQuote();
      if (!listOpen) {
        html.push('<ul class="notes-list">');
        listOpen = true;
      }
      const indent = Math.min(2, Math.floor((bullet[1] || "").length / 2));
      const cls = indent > 0 ? ` class="indent-${indent}"` : "";
      html.push(`<li${cls}>${renderInlineMarkup(bullet[2])}</li>`);
      continue;
    }

    const quote = line.match(/^\s*>\s?(.*)$/);
    if (quote) {
      closeList();
      if (!quoteOpen) {
        html.push('<blockquote class="notes-quote">');
        quoteOpen = true;
      }
      html.push(`<p>${renderInlineMarkup(quote[1])}</p>`);
      continue;
    }

    closeList();
    closeQuote();
    html.push(`<p>${renderInlineMarkup(trimmed)}</p>`);
  }

  closeList();
  closeQuote();
  closeCodeFence();
  return html.join("");
}
