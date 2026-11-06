/* tests/diagnostic-details.browser.mjs */
/* CrossWatch - Diagnostic detail layout regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

const read = name => readFile(new URL("../" + name, import.meta.url), "utf8");
const analyzer = await read("assets/js/analyzer/index.js");
const events = await read("assets/js/events/index.js");
const css = (await Promise.all(["assets/themes/tokens.css", "assets/css/page-theme.css", "assets/css/pages.css", "assets/themes/flat.css", "assets/themes/original-coverage.css", "assets/js/analyzer/styles.css", "assets/js/events/styles.css"].map(read))).join("\n");

for (const theme of ["original", "flat-dark", "flat-light"]) {
  for (const width of [1280, 390]) {
    test(`${theme} diagnostic details at ${width}px`, async t => {
      const browser = await chromium.launch({headless:true});
      t.after(() => browser.close());
      const page = await browser.newPage({viewport:{width,height:1000}});
      await page.setContent(`<html data-cw-theme="${theme}"><head><style>${css}</style></head><body style="margin:0;background:var(--bg);color:var(--fg);font-family:Arial,sans-serif"><section class="an-page" style="max-width:500px"><h4>What happened</h4><div class="an-detail-reasons"><p>Multiple possible destination matches were found.</p></div><div id="an-test"></div></section><section id="page-events" style="max-width:500px"><div class="ev-app"><h4>Grouped timeline</h4><div id="ev-test"></div><h4>Event details</h4><div id="ev-raw"></div></div></section></body></html>`);
      await page.addScriptTag({content:analyzer.slice(analyzer.indexOf("const FEATURES"), analyzer.indexOf("export function mappingRowActions")).replace("export function matchDetailsHTML", "function matchDetailsHTML")});
      await page.addScriptTag({content:events.slice(events.indexOf("const failureDetailsHTML"), events.indexOf("const PAGE_SIZE"))});
      await page.evaluate(() => {
        const evidence = {candidate_count:12,truncated:true,candidates:[{title:"A possible movie match with a longer title",type:"episode",local_id:"17",season:1,episode:2,ids:{tmdb:"123",anidb:"456"},show_ids:{kitsu:"789"}}]};
        const row = {provider:"KODI",match_details:evidence};
        document.querySelector("#an-test").innerHTML = matchDetailsHTML(row, [{...row,provider:"kodi@default"}]);
        const failure = {event_type:"write_failed",run_id:"run1",item_key:"tmdb:1",reason_message:"The destination update failed.",hint_message:"Multiple possible destination matches were found. Check the candidate IDs before retrying.",match_details:evidence};
        const history = [failure,{...failure,event_type:"unresolved_recorded"}];
        document.querySelector("#ev-test").innerHTML = history.map(event => `<div class="ev-tl-body">${failureDetailsHTML(event,history)}</div>`).join("");
        document.querySelector("#ev-raw").innerHTML = failureDetailsHTML(history[1]);
        if (failureDetailsHTML({}) || matchDetailsHTML({})) throw new Error("Empty diagnostics rendered");
      });
      assert.equal(await page.locator("#ev-test .ev-failure-details").count(),1);
      assert.equal(await page.locator("details[open]").count(),0);
      assert.equal(await page.locator(".an-match-details").count(),1);
      for (const summary of await page.locator("details summary").all()) {
        await summary.focus();
        await page.keyboard.press("Enter");
      }
      assert.equal(await page.locator("details[open]").count(),3);
      const layout = await page.evaluate(() => {
        const list = getComputedStyle(document.querySelector(".an-match-details ul"));
        const summary = getComputedStyle(document.querySelector(".ev-match-details summary"));
        const paragraph = getComputedStyle(document.querySelector(".ev-failure-details p"));
        return {overflow:document.documentElement.scrollWidth > innerWidth,listPadding:list.paddingLeft,listStyle:list.listStyleType,opacity:summary.opacity,margin:paragraph.marginTop};
      });
      assert.deepEqual(layout,{overflow:false,listPadding:"0px",listStyle:"none",opacity:"1",margin:"4px"});
      if (process.env.CW_DIAGNOSTIC_SCREENSHOTS) {
        await mkdir(process.env.CW_DIAGNOSTIC_SCREENSHOTS,{recursive:true});
        await page.screenshot({path:`${process.env.CW_DIAGNOSTIC_SCREENSHOTS}/${theme}-${width}.png`,fullPage:true});
      }
    });
  }
}
