/* tests/i18n-browser-setup.mjs */
/* CrossWatch - Gives browser test pages the real English translation catalogs and runtime */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import * as playwright from 'playwright';
import {i18nInitScript} from './i18n-test-helper.mjs';

const script = i18nInitScript(process.env.CW_TEST_LANG || 'en');

function decorate(browser) {
  if (!browser || browser.__cwI18n) return browser;
  browser.__cwI18n = true;
  const newContext = browser.newContext.bind(browser);
  browser.newContext = async (...args) => {
    const context = await newContext(...args);
    await context.addInitScript({content: script});
    return context;
  };
  browser.newPage = async (...args) => {
    const context = await browser.newContext(...args);
    const page = await context.newPage();
    page.on('close', () => context.close().catch(() => {}));
    return page;
  };
  return browser;
}

for (const name of ['chromium', 'webkit', 'firefox']) {
  const type = playwright[name];
  if (!type || type.__cwI18n) continue;
  type.__cwI18n = true;
  const launch = type.launch.bind(type);
  type.launch = async (...args) => decorate(await launch(...args));
}
