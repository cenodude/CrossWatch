/* tests/scheduler-backups.test.mjs */
/* CrossWatch - Scheduler banner backup compatibility tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const source = readFileSync(new URL("../assets/js/schedulerbanner.js", import.meta.url), "utf8");
const window = {};
vm.runInNewContext(source.replace(/\}\)\(\);\s*$/, "window.schedulingOn = schedulingOn;})();"), {
  window,
  document: {addEventListener() {}, querySelector() { return {}; }},
});

const job = {id: "backup", at: "08:00", active: true};

for (const [name, advanced, expected] of [
  ["canonical backup", {backup_jobs: [job]}, true],
  ["legacy backup", {backupJobs: [job]}, true],
  ["empty canonical with legacy backup", {backup_jobs: [], backupJobs: [job]}, true],
  ["disabled canonical takes precedence", {backup_jobs: [{...job, active: false}], backupJobs: [job]}, false],
  ["disabled legacy backup", {backup_jobs: [], backupJobs: [{...job, active: false}]}, false],
  ["invalid backup time", {backup_jobs: [{...job, at: "invalid"}]}, false],
  ["empty backup lists", {backup_jobs: [], backupJobs: []}, false],
]) {
  test(`scheduler state recognizes ${name}`, () => {
    assert.equal(window.schedulingOn({enabled: false, advanced: {enabled: false, ...advanced}}), expected);
    assert.equal(window.schedulingOn({scheduling: {enabled: false, advanced: {enabled: false, ...advanced}}}), expected);
  });
}

test("sync plans stay visible without backups", () => {
  assert.equal(window.schedulingOn({enabled: true}), true);
  assert.equal(window.schedulingOn({advanced: {enabled: true}}), true);
});
