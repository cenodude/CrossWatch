/* tests/profile-media-modal.test.mjs */
/* CrossWatch - Profile media modal watch status tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/profile-media-modal.js", import.meta.url), "utf8");

function modal(presence = null, type = "movie") {
  const context = {
    window: { CW: { ProfileDateTime: { format: (date) => date.toISOString().slice(0, 10) } } },
    navigator: { language: "en-US" },
    document: { documentElement: { dataset: {} } },
  };
  vm.runInNewContext(source.replace("  (window.CW ||= {}).ProfileMediaModal =", "  window.testModal = { state, movieProgressBox, overviewTab, episodesTab, seasonProgress, upNext, episodeSlot, livesCard, episodeDetail };\n  (window.CW ||= {}).ProfileMediaModal ="), context);
  const api = context.window.testModal;
  Object.assign(api.state, { item: { type, tmdb: "123" }, presence, progress: [] });
  return api;
}

const epoch = Date.parse("2026-01-02T12:00:00Z") / 1000;
const entry = (count, last_epoch = epoch, episodes = []) => ({ count, last_epoch, episodes, present: [{ provider: "TRAKT" }], missing: [] });

test("a scrobbled movie is watched without synced history", () => {
  const api = modal({ synced: null, scrobble: entry(1) });
  const html = api.movieProgressBox();
  assert.match(html, /<b>Watched<\/b>/);
  assert.match(html, /Watched once/);
  assert.match(html, /width:100%/);
  assert.match(html, /2026-01-02/);
  assert.doesNotMatch(html, /Not watched yet/);
  assert.match(api.livesCard(), /History<\/strong><span>Not synced/);
});

test("movie counts do not add overlapping sources and use the latest watch date", () => {
  const api = modal({ synced: entry(2, epoch - 86400), scrobble: entry(2) });
  assert.match(api.movieProgressBox(), /Watched 2 times/);
  assert.match(api.movieProgressBox(), /2026-01-02/);
  api.state.presence.scrobble.count = 3;
  assert.match(api.movieProgressBox(), /Watched 3 times/);
});

test("history-only, unwatched and absent presence retain their watch status", () => {
  assert.match(modal({ synced: entry(1), scrobble: null }).movieProgressBox(), /Watched once/);
  for (const presence of [{}, { synced: null, scrobble: null }]) {
    const html = modal(presence).movieProgressBox();
    assert.match(html, /Not watched yet/);
    assert.match(html, /Not started/);
    assert.match(html, /width:0%/);
  }
});

test("a current rewatch keeps playback progress alongside the completed scrobble", () => {
  const api = modal({ scrobble: entry(1) });
  api.state.progress = [{ provider: "TRAKT", instance: "default", pct: 35 }];
  assert.match(api.movieProgressBox(), /<b>35%<\/b>/);
  assert.match(api.movieProgressBox(), /Watched once/);
});

function show(presence) {
  const api = modal(presence, "show");
  api.state.meta = { detail: { number_of_episodes: 3, seasons: [{ season: 1, episode_count: 3 }] } };
  api.state.seasons.set("1", { episodes: [1, 2, 3].map((episode) => ({ episode, name: "Episode " + episode })) });
  return api;
}

test("scrobbled episodes drive totals, next episode and watched badges", () => {
  const api = show({ scrobble: entry(1, epoch, [{ season: 1, episode: 1, epoch }]) });
  assert.match(api.overviewTab(), /1\/3 episodes <b>33%/);
  assert.equal(api.upNext().label, "Continue watching");
  assert.equal(api.upNext().first.episode, 2);
  assert.match(api.seasonProgress(), /1\/3/);
  assert.equal((api.episodesTab().match(/cw-mm-eprow is-watched/g) || []).length, 1);
  assert.match(api.episodeSlot("Last watched", { season: 1, episode: 1 }), /Watched/);
});

test("episodes shared by history and scrobbles count once and retain the latest date", () => {
  const api = show({
    synced: entry(1, epoch - 86400, [{ season: 1, episode: 1, epoch: epoch - 86400 }]),
    scrobble: entry(2, epoch, [{ season: 1, episode: 1, epoch }, { season: 1, episode: 2, epoch }]),
  });
  assert.match(api.overviewTab(), /2\/3 episodes <b>67%/);
  assert.match(api.seasonProgress(), /2\/3/);
  assert.equal(api.upNext().first.episode, 3);
  const html = api.episodesTab();
  assert.equal((html.match(/cw-mm-eprow is-watched/g) || []).length, 2);
  assert.match(html, /title="2026-01-02"/);
  assert.doesNotMatch(html, /title="2026-01-01"/);
});

test("unwatched shows start at the first episode and history-only shows still advance", () => {
  const empty = show({});
  assert.equal(empty.upNext().label, "Start watching");
  assert.match(empty.overviewTab(), /0\/3 episodes/);
  const api = show({ synced: entry(1, epoch, [{ season: 1, episode: 1, epoch }]) });
  assert.equal(api.upNext().first.episode, 2);
  assert.match(api.overviewTab(), /1\/3 episodes/);
});


test("pending and failed watch status never imply an unwatched title", () => {
  for (const api of [modal(), show(null)]) {
    api.state.presenceLoading = true;
    assert.match(api.overviewTab(), /Checking watch status/);
    assert.doesNotMatch(api.overviewTab(), /Not watched yet|Not started|0\/3 episodes|Start watching/);
    api.state.presenceLoading = false;
    assert.match(api.overviewTab(), /Watch status unavailable/);
    assert.doesNotMatch(api.overviewTab(), /Not watched yet|Not started|0\/3 episodes|Start watching/);
  }
});

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return {promise, resolve};
}

function loadingModal() {
  const metas = [], requests = [], body = {innerHTML:"",scrollTop:0}, panel = {innerHTML:""};
  const root = {hidden:true,addEventListener(){},querySelector:selector => selector === ".cw-mm-body" ? body : selector === ".cw-mm-panel" ? panel : {style:{},focus(){}},querySelectorAll:()=>[]};
  const context = {
    window:{CW:{Meta:{get:() => {const d=deferred();metas.push(d);return d.promise;}},ProfileDateTime:{format:date=>date.toISOString()}}},
    navigator:{language:"en-US"},
    document:{createElement:()=>root,body:{appendChild(){}},addEventListener(){},documentElement:{dataset:{cwPermPlayback:"off"},classList:{add(){},remove(){}}}},
    fetch:url=>{const d=deferred();requests.push({url,...d});return d.promise;},
  };
  vm.runInNewContext(source.replace("  (window.CW ||= {}).ProfileMediaModal =", "  window.testExtra = { loadCollection, openPerson, onClick, state };\n  (window.CW ||= {}).ProfileMediaModal ="),context);
  return {api:context.window.CW.ProfileMediaModal,extra:context.window.testExtra,metas,requests,body,panel};
}

const tick = () => new Promise(resolve => setImmediate(resolve));
const response = data => ({ok:true,json:async()=>data});

test("metadata renders before slow presence and presence renders before slow metadata", async () => {
  for (const metadataFirst of [true,false]) {
    const f=loadingModal();
    const opening=f.api.open({type:"movie",tmdb:"1",title:"Example"});
    if (metadataFirst) f.metas[0].resolve({overview:"Description ready"});
    else f.requests[0].resolve(response({ok:true,scrobble:entry(1)}));
    await tick();
    assert.match(f.body.innerHTML, metadataFirst ? /Description ready/ : /Watched once/);
    assert.match(f.body.innerHTML, metadataFirst ? /Checking watch status/ : /Loading details/);
    if (metadataFirst) f.requests[0].resolve(response({ok:true,scrobble:entry(1)}));
    else f.metas[0].resolve({overview:"Description ready"});
    await opening;
    assert.match(f.body.innerHTML,/Description ready/);
    assert.match(f.body.innerHTML,/Watched once/);
  }
});

test("late responses from a closed title do not replace the current title", async () => {
  const f=loadingModal();
  const first=f.api.open({type:"movie",tmdb:"1",title:"First"});
  f.api.close();
  const second=f.api.open({type:"movie",tmdb:"2",title:"Second"});
  f.metas[1].resolve({title:"Second ready"});
  f.requests[1].resolve(response({ok:true}));
  await second;
  f.metas[0].resolve({title:"Stale title"});
  f.requests[0].resolve(response({ok:true,scrobble:entry(1)}));
  await first;
  assert.match(f.body.innerHTML,/Second ready/);
  assert.doesNotMatch(f.body.innerHTML,/Stale title|Watched once/);
});

test("shows prefetch the watched season before metadata and retry interrupted loads on reopen", async () => {
  const f=loadingModal();
  const item={type:"show",tmdb:"1",title:"Show"};
  const presence={ok:true,scrobble:entry(1,epoch,[{season:4,episode:2,epoch}])};
  const first=f.api.open(item);
  f.requests[0].resolve(response(presence));
  await tick();
  assert.match(f.requests[1].url,/season=4/);
  assert.match(f.body.innerHTML,/1 episode watched/);
  f.api.close();
  const second=f.api.open(item);
  f.requests[2].resolve(response(presence));
  await tick();
  assert.match(f.requests[3].url,/season=4/);
  f.metas[0].resolve(null);
  f.metas[1].resolve({detail:{number_of_episodes:3,seasons:[{season:4,episode_count:3}]}});
  f.requests[1].resolve(response({ok:true,episodes:[{episode:3,name:"Stale episode"}]}));
  f.requests[3].resolve(response({ok:true,episodes:[{episode:3,name:"Current episode"}]}));
  await Promise.all([first,second]);
  await tick();
  assert.match(f.panel.innerHTML,/Current episode/);
  assert.doesNotMatch(f.panel.innerHTML,/Stale episode/);
});


test("unmatched watches do not inflate progress or suggest an unwatched starting point", () => {
  const api = show({ scrobble: entry(2, epoch, [{ season: 4, episode: 1, epoch }, { season: 1, episode: 9, epoch }]) });
  assert.match(api.overviewTab(), /0\/3 episodes/);
  assert.match(api.overviewTab(), /2 watched episodes could not be matched/);
  assert.doesNotMatch(api.overviewTab(), /Start watching/);
  assert.equal(api.upNext().first, null);
  assert.match(api.seasonProgress(), /0\/3/);
});

test("episode details use full metadata and only that episode's provider records", () => {
  const api = show({ scrobble: entry(2, epoch, [
    { season: 1, episode: 1, epoch, present: [{ provider: "PLEX" }] },
    { season: 1, episode: 2, epoch, present: [{ provider: "TRAKT" }] },
  ]) });
  api.state.seasons.get("1").episodes[0] = { episode: 1, name: "Pilot", overview: "Full synopsis & ending", runtime: 42, vote_average: 8.2, air_date: "2026-01-01" };
  api.state.episode = { season: 1, episode: 1 };
  const html = api.episodeDetail();
  assert.match(html, /Back to series/);
  assert.match(html, /Pilot/);
  assert.match(html, /Full synopsis &amp; ending/);
  assert.match(html, /42m/);
  assert.match(html, /8.2\/10/);
  assert.match(html, /Watched/);
  assert.match(html, /PLEX/);
  assert.doesNotMatch(html, /TRAKT/);
  assert.match(api.episodesTab(), /<button[^>]+data-mm-episode="1:1"/);
});


test("episode credits show guests and their characters without series cast or empty sections", () => {
  const api = show({});
  api.state.episode = { season: 1, episode: 1 };
  api.state.meta.credits = { cast: [{ name: "Series regular" }] };
  const ep = api.state.seasons.get("1").episodes[0];
  assert.doesNotMatch(api.episodeDetail(), /Guest cast|writing credits|Series regular/);
  ep.guest_stars = [{ name: "Guest <One>", character: "A & B", profile_path: "/guest.jpg" }, { name: "Guest Two" }];
  ep.crew = [{ name: "Director One", job: "Director" }, { name: "Writer One", job: "Teleplay" }];
  const html = api.episodeDetail();
  assert.match(html, /Guest cast/);
  assert.match(html, /Guest &lt;One&gt;/);
  assert.match(html, /A &amp; B/);
  assert.match(html, /path=%2Fguest.jpg/);
  assert.match(html, /cw-mm-initials/);
  assert.match(html, /Director One/);
  assert.match(html, /Teleplay/);
  assert.doesNotMatch(html, /Series regular/);
});


test("stale upcoming episodes already watched are never recommended", () => {
  const api = show({ scrobble: entry(3, epoch, [1, 2, 3].map(episode => ({ season: 1, episode, epoch }))) });
  api.state.meta.detail.next_episode_to_air = { season_number: 1, episode_number: 2, name: "Stale upcoming", air_date: "2099-01-01" };
  assert.doesNotMatch(api.overviewTab(), /Stale upcoming|Upcoming/);
});

test("collection metadata is lazy and watch status is requested in a single batch", async () => {
  const f = loadingModal();
  const opening = f.api.open({ type: "movie", tmdb: "1", title: "Film" });
  f.metas[0].resolve({ detail: { belongs_to_collection: { id: 7, name: "Collection" } } });
  f.requests[0].resolve(response({ ok: true }));
  await opening;
  assert.equal(f.requests.length, 1);
  assert.match(f.body.innerHTML, /data-mm-collection/);
  const loading = f.extra.loadCollection();
  assert.match(f.requests[1].url, /collection=7/);
  f.requests[1].resolve(response({ ok: true, parts: [{ id: 1, title: "First" }, { id: 2, title: "Second" }] }));
  await tick();
  assert.match(f.requests[2].url, /movie-watch-status\?tmdb=1%2C2/);
  f.requests[2].resolve(response({ ok: true, watched: { "1": true, "2": null } }));
  await loading;
  assert.match(f.panel.innerHTML, /Status unavailable/);
  assert.doesNotMatch(f.panel.innerHTML.split('<details class="cw-collection-list"')[1], /Not watched|1 of 2 watched/);
});

test("leaving an actor ignores its late response and reuses metadata on reopening", async () => {
  const f = loadingModal();
  const opening = f.api.open({ type: "movie", tmdb: "1", title: "Film" });
  f.metas[0].resolve({ title: "Film" });
  f.requests[0].resolve(response({ ok: true }));
  await opening;
  const actor = f.extra.openPerson("12");
  assert.match(f.body.innerHTML, /Loading filmography/);
  f.extra.onClick({ target: { closest: selector => selector === "[data-mm-person-back]" ? {} : null } });
  f.requests[1].resolve(response({ ok: true, name: "Actor", credits: [] }));
  await actor;
  assert.doesNotMatch(f.body.innerHTML, /Also appears in/);
  await f.extra.openPerson("12");
  assert.equal(f.requests.length, 2);
  assert.match(f.body.innerHTML, /Actor/);
});
