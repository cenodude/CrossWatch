/* tests/events-stats.browser.mjs */
/* CrossWatch - Activity statistics categories, charts and responsive rendering */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

for (const theme of ['original', 'flat-dark', 'flat-light']) {
  test(`complete activity statistics render in ${theme}`, async t => {
    const browser = await chromium.launch({headless: true});
    t.after(() => browser.close());
    const page = await browser.newPage({viewport: {width: 1440, height: 1100}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const values = {activities: 15, sync_runs: 4, scrobbles: 3, ratings: 2, captures: 2, backups: 2, audits: 2, delivery_skips: 2, failures: 5, blocked: 0, avg_duration: 10, success_rate: 50};
    const types = ['sync', 'watching', 'ratings', 'capture', 'backup', 'audit'].map((key, i) => ({key, label: ['Sync runs', 'Watching', 'Ratings', 'Captures', 'Backups', 'Audits'][i], value: i === 0 ? 4 : i === 1 ? 3 : 2}));
    const data = {ok: true, range: {bucket: 86400}, kpis: Object.fromEntries(Object.entries(values).map(([key, value]) => [key, {value, prev: 0, delta: null}])), types,
      outcomes: [{key: 'completed', label: 'Completed', value: 6}, {key: 'warning', label: 'Needs attention', value: 1}, {key: 'failed', label: 'Failed', value: 5}, {key: 'active', label: 'In progress', value: 1}, {key: 'skipped', label: 'Skipped', value: 1}, {key: 'stopped', label: 'Stopped before completion', value: 1}],
      trend: [{t: 1795132800, sync: 4, watching: 3, ratings: 2, capture: 2, backup: 2, audit: 2, watcher: 2, webhook: 3, failed: 5, rate: 50}],
      routes: [{source: 'PLEX', destination: 'SIMKL', volume: 2, runs: 0, scrobbles: 2, skipped: 2, success_rate: null}],
      duration_series: [{t: 1795132800, avg: 10, n: 3}], duration_percentiles: {p50: 10, p90: 10, p99: 10},
      failure_reasons: [{reason: 'backup_failed', label: 'Backup failed', count: 1, share: 20, delta: null}],
    };
    await page.route('http://stats.test/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname.startsWith('/assets/')) return route.fulfill({body: await readFile(new URL('..' + url.pathname, import.meta.url)), contentType: url.pathname.endsWith('.css') ? 'text/css' : 'text/javascript'});
      return route.fulfill({contentType: 'text/html', body: `<!doctype html><html ${theme === 'original' ? '' : `data-cw-theme="${theme}"`}><head><meta charset="utf-8"><link rel="stylesheet" href="/assets/themes/tokens.css"><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"><style>body{margin:16px;background:var(--cw-theme-bg);font-family:Arial,sans-serif}.material-symbols-rounded{display:none}</style></head><body><main id="page-events" class="cw-page-events"><div class="ev-app"><h1>Activity statistics</h1><div id="stats"></div></div></main></body></html>`});
    });
    await page.goto('http://stats.test/');
    await page.evaluate(async data => {
      window.statsData = data;
      window.statsView = (await import('/assets/js/events/stats.js')).default(document.querySelector('#stats'), {fetchJson: async () => window.statsData});
      await window.statsView.load();
    }, data);
    for (const label of ['Ratings', 'Captures', 'Backups', 'Audits', 'Skipped deliveries']) {
      const tile = page.locator('.stat-tile').filter({has: page.locator('.stat-tile-lb', {hasText: label})});
      assert.equal(await tile.locator('.stat-tile-val').innerText(), '2');
    }
    assert.equal(await page.locator('.stat-svg-trend rect[fill]').count(), 6);
    assert.match(await page.locator('.stat-rt-route').getAttribute('title'), /2 skipped/);
    assert.match(await page.locator('#stats').innerText(), /Stopped before completion/);
    assert.doesNotMatch(await page.locator('#stats').innerHTML(), /NaN|undefined/);
    for (const width of [1440, 390]) {
      await page.setViewportSize({width, height: 1100});
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      if (process.env.CW_EVENTS_SCREENSHOTS) {
        await mkdir(process.env.CW_EVENTS_SCREENSHOTS, {recursive: true});
        await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/statistics-${theme}-${width}.png`, fullPage: true});
      }
    }
    await page.evaluate(async () => {
      window.statsData = {ok: true, kpis: {}, types: [], outcomes: [], trend: [], routes: [], failure_reasons: []};
      await window.statsView.load();
    });
    assert.match(await page.locator('#stats').innerText(), /No activity in this period/);
    assert.doesNotMatch(await page.locator('#stats').innerHTML(), /NaN|undefined/);
    assert.deepEqual(errors, []);
  });
}
