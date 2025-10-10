/* helpers/api.js – request helpers with timeout */
(function(){ const JSON_HDR={"Content-Type":"application/json"};
  function to(ms=9000){const ac=new AbortController();const t=setTimeout(()=>ac.abort("timeout"),ms);return {signal:ac.signal, done:()=>clearTimeout(t)};}
  async function f(url,opt={},ms=9000){const tt=to(ms); try{return await fetch(url,{cache:"no-store",...opt,signal:tt.signal});} finally{tt.done();}}
  async function j(url,opt={},ms=9000){const r=await f(url,opt,ms); if(!r.ok) throw new Error(r.status+" "+r.statusText); const ct=r.headers.get("content-type")||""; return ct.includes("json")?r.json():r.text();}
  const Cache={cfg:null,setCfg(v){this.cfg=v;try{window._cfgCache=v;}catch{}},getCfg(){return this.cfg||window._cfgCache||null;}};
  const Config={async load(){const cfg=await j("/api/config"); Cache.setCfg(cfg); return cfg;}, async save(cfg){const r=await j("/api/config",{method:"POST",headers:JSON_HDR,body:JSON.stringify(cfg||{})}); Cache.setCfg(cfg); return r;}};
  const Pairs={async list(){return j("/api/pairs");}, async save(p){const has=!!(p&&p.id); const url=has?`/api/pairs/${encodeURIComponent(p.id)}`:"/api/pairs"; return j(url,{method:has?"PUT":"POST",headers:JSON_HDR,body:JSON.stringify(p||{})});}, async delete(id){if(!id)return; return j(`/api/pairs/${encodeURIComponent(id)}`,{method:"DELETE"});} };
  const Providers={ list(){return j("/api/sync/providers");}, html(){return j("/api/metadata/providers/html");} };
  const Status={ get(force=false){const q=force?`?ts=${Date.now()}`:""; return j(`/api/status${q}`);} };
  const Insights={ get(){return j("/api/insights");} };
  (window.CW ||= {}); Object.assign(window.CW,{API:{j,f,Config,Pairs,Providers,Status,Insights}, Cache});
})();