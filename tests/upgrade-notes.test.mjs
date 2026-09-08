import test from "node:test";
import assert from "node:assert/strict";
import {parseReleaseNotes, renderInlineMarkup, releaseNotesUrl} from "../assets/js/modals/upgrade-warning/notes.js";

test("standard release format separates upgrade notes, highlights and nested details", () => {
  const notes = parseReleaseNotes(`# CrossWatch v0.12.0
Intro text.
**Upgrade note:**
Existing pairs must rebuild their baselines.
The next sync will take longer.

## ✨ Highlights
- **Interactive Sync:** review changes before applying them.
  - Select the changes you want.
  - Correct titles and IDs.
    Individually or in bulk.
- **Saved mappings:** edit corrections in the Editor.
## 🔧 Fixes & Improvements
- **Fixed:** another issue.
`);
  assert.equal(notes.upgradeNote, "Existing pairs must rebuild their baselines.\nThe next sync will take longer.");
  assert.deepEqual(notes.highlights, [
    {text:"**Interactive Sync:** review changes before applying them.", children:["Select the changes you want.", "Correct titles and IDs. Individually or in bulk."]},
    {text:"**Saved mappings:** edit corrections in the Editor.", children:[]},
  ]);
});

test("older release notes without an upgrade note keep only the highlights", () => {
  const notes = parseReleaseNotes("# CrossWatch v0.11.7\r\nThanks to contributors.\r\n## ✨ Highlights\r\n\r\n- **Sync activity modal:** heatmap.\r\n- **Anime ID mapping:** opt-in.\r\n## 🔧 Fixes & Improvements\r\n- Other changes.");
  assert.equal(notes.upgradeNote, "");
  assert.equal(notes.highlights.length, 2);
});

test("missing sections and code samples do not become highlights", () => {
  assert.deepEqual(parseReleaseNotes("## Fixes\n- Fixed something"), {highlights:[], upgradeNote:"", wikiLinks:[]});
  assert.deepEqual(parseReleaseNotes("```md\n## Highlights\n- Example\n```"), {highlights:[], upgradeNote:"", wikiLinks:[]});
});

test("release markup escapes HTML and rejects executable links", () => {
  const html = renderInlineMarkup('**Mapping:** `<img src=x onerror=alert(1)>` [bad](javascript:alert) <script>alert(1)</script>');
  assert.match(html, /<strong>Mapping:<\/strong>/);
  assert.match(html, /&lt;img/);
  assert.doesNotMatch(html, /<script|<img|href=/);
  assert.match(renderInlineMarkup('[Docs](https://example.com/?a=1&b=2)'), /href="https:\/\/example.com\/\?a=1&amp;b=2"/);
  assert.equal(releaseNotesUrl('javascript:alert(1)', ''), '');
  assert.equal(releaseNotesUrl('https://user:pass@example.com', ''), '');
});


test("Updated Wiki accepts plain and bulleted links and stops at the next section", () => {
  const notes = parseReleaseNotes("## \u{1f4da} updated WIKI\r\nPlaylists - [https://wiki.crosswatch.app/playlists](https://wiki.crosswatch.app/playlists)\r\nCLI - [https://wiki.crosswatch.app/cli](https://wiki.crosswatch.app/cli)\r\nCLI - [https://wiki.crosswatch.app/cli/run-once](https://wiki.crosswatch.app/cli/run-once)\r\n- [Setup](https://wiki.crosswatch.app/setup)\r\n## Fixes\r\n- [Unrelated](https://example.com/)");
  assert.deepEqual(notes.wikiLinks, [
    {title:"Playlists", url:"https://wiki.crosswatch.app/playlists"},
    {title:"CLI", url:"https://wiki.crosswatch.app/cli"},
    {title:"CLI", url:"https://wiki.crosswatch.app/cli/run-once"},
    {title:"Setup", url:"https://wiki.crosswatch.app/setup"},
  ]);
  assert.deepEqual(notes.highlights, []);
  assert.equal(notes.upgradeNote, "");
});

test("Wiki links reject unsafe URLs, omit duplicates and ignore fenced examples", () => {
  const notes = parseReleaseNotes("## Updated Wiki\n[bad](javascript:alert)\n[bad](https://user:pass@example.com)\n[Docs](https://wiki.crosswatch.app/)\n- [Repeated](https://wiki.crosswatch.app/)\n```md\n[Example](https://example.com/)\n```\n## Highlights\n- New feature");
  assert.deepEqual(notes.wikiLinks, [{title:"Docs", url:"https://wiki.crosswatch.app/"}]);
  assert.equal(notes.highlights[0].text, "New feature");
});
