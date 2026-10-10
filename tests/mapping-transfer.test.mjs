/* tests/mapping-transfer.test.mjs */
/* CrossWatch - Mapping import file format compatibility */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readMappingFile} from "../assets/js/editor/saved-mappings.js";

const file = data => new Blob([JSON.stringify(data)], {type:"application/json"});
const bundle = version => ({format:"crosswatch-mappings-blocks", version, records:[]});

test("mapping imports accept legacy files and version 3 groups", async () => {
  for (const version of [1, 2, 3]) {
    assert.deepEqual(await readMappingFile(file(bundle(version))), bundle(version));
  }
  const data = {...bundle(3), episode_groups:[{pair_id:"p1", group:{id:"finale", scrobble:true}}]};
  assert.deepEqual(await readMappingFile(file(data)), data);
});

test("unsupported formats and invalid group containers fail before confirmation", async () => {
  for (const data of [bundle(4), {...bundle(3), records:{}}, {...bundle(3), episode_groups:{}},
    {...bundle(3), episode_groups:null}, {...bundle(2), episode_groups:[{}]}, {records:[]}]) {
    await assert.rejects(readMappingFile(file(data)), /Mappings & blocks/);
  }
  await assert.rejects(readMappingFile(new Blob(["invalid json"])), /valid Mappings & blocks/);
  await assert.rejects(readMappingFile({size:20 * 1024 * 1024 + 1}), /20 MB/);
});
