# _analyzer.py
from __future__ import annotations
from typing import Dict, Any, Iterable, Tuple, List, DefaultDict
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import json, threading, re, requests
from collections import defaultdict

from cw_platform.config_base import CONFIG as CONFIG_DIR

router = APIRouter(prefix="/api", tags=["analyzer"])
STATE_PATH = (CONFIG_DIR / "state.json")
CWS_DIR    = (CONFIG_DIR / ".cw_state")
CFG_PATH   = (CONFIG_DIR / "config.json")
_LOCK = threading.Lock()

def _cfg() -> Dict[str, Any]:
    try: return json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except Exception: return {}

def _tmdb_key() -> str: return ((_cfg().get("tmdb") or {}).get("api_key") or "").strip()
def _trakt_headers() -> Dict[str,str]:
    t=(_cfg().get("trakt") or {})
    h={"trakt-api-version":"2","trakt-api-key":(t.get("client_id") or "").strip()}
    tok=(t.get("access_token") or "").strip()
    if tok: h["Authorization"]=f"Bearer {tok}"
    return h

def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists(): raise HTTPException(404,"state.json not found")
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def _save_state(s: Dict[str, Any]) -> None:
    with _LOCK:
        tmp=STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")
        tmp.replace(STATE_PATH)

def _iter_items(s: Dict[str, Any]) -> Iterable[Tuple[str,str,str,Dict[str,Any]]]:
    for prov,pv in (s.get("providers") or {}).items():
        for feat in ("history","watchlist","ratings"):
            items=(((pv or {}).get(feat) or {}).get("baseline") or {}).get("items") or {}
            for k,it in items.items():
                yield prov,feat,k,(it or {})

def _bucket(s: Dict[str, Any], prov:str, feat:str):
    try: return s["providers"][prov][feat]["baseline"]["items"]
    except KeyError: return None

def _find_item(s: Dict[str, Any], prov:str, feat:str, key:str):
    b=_bucket(s,prov,feat)
    if not b or key not in b: return None,None
    return b,b[key]

def _counts(s: Dict[str, Any]) -> Dict[str, Dict[str,int]]:
    out={}
    for p,f,_,_ in _iter_items(s):
        r=out.setdefault(p,{"history":0,"watchlist":0,"ratings":0,"total":0})
        r[f]+=1; r["total"]+=1
    return out

def _collect_items(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    out=[]
    for p,f,k,it in _iter_items(s):
        out.append({"provider":p,"feature":f,"key":k,"type":it.get("type"),
                    "title":it.get("title"),"year":it.get("year"),"ids":it.get("ids") or {}})
    return out

def _rekey(items: Dict[str, Any], old_key:str, item:Dict[str,Any]) -> str:
    ids=item.get("ids") or {}
    for ns in ("imdb","tmdb","tvdb"):
        if ids.get(ns):
            new=f"{ns}:{ids[ns]}"
            if new!=old_key: items.pop(old_key,None); items[new]=item; return new
            break
    return old_key

_ID_RX = {
    "imdb": re.compile(r"^tt\d{5,}$"),
    "tmdb": re.compile(r"^\d+$"),
    "tvdb": re.compile(r"^\d+$"),
    "plex": re.compile(r"^\d+$"),
    "trakt": re.compile(r"^\d+$"),
    "simkl": re.compile(r"^\d+$"),
    "emby": re.compile(r"^[A-Za-z0-9-]{4,}$"),
}
def _read_cw_state() -> Dict[str, Any]:
    out={}
    if not (CWS_DIR.exists() and CWS_DIR.is_dir()): return out
    for p in sorted(CWS_DIR.glob("*.json")):
        try: out[p.name]=json.loads(p.read_text(encoding="utf-8"))
        except Exception: out[p.name]={"_error":"parse_error"}
    return out

def _class_key(it:Dict[str,Any])->Tuple[str,str,int|None]:
    return ((it.get("type") or "").lower(),
            (it.get("title") or "").strip().lower(),
            it.get("year"))

def _alias_keys(obj:Dict[str,Any])->List[str]:
    t=(obj.get("type") or "").lower()
    ids=dict(obj.get("ids") or {})
    out=[]; seen=set()
    if obj.get("_key"): out.append(obj["_key"])
    for ns in ("imdb","tmdb","tvdb","trakt","simkl","plex","emby","guid"):
        v=ids.get(ns)
        if v:
            vs=str(v); out.append(f"{ns}:{vs}")
            if t in ("movie","show","season","episode"):
                out.append(f"{t}:{ns}:{vs}")
    title=(obj.get("title") or "").strip().lower()
    year=obj.get("year")
    if title and year: out.append(f"t:{title}|y:{year}|ty:{t}")
    res=[]
    for k in out:
        if k not in seen:
            seen.add(k); res.append(k)
    return res

def _alias_index(items:Dict[str,Any])->Dict[str,str]:
    idx={}
    for k,v in items.items():
        vv=dict(v); vv["_key"]=k
        for ak in _alias_keys(vv): idx.setdefault(ak,k)
    return idx

# --- Pair awareness ----------------------------------------------------------
def _pair_map(cfg: Dict[str, Any], state: Dict[str, Any]) -> DefaultDict[Tuple[str, str], List[str]]:
    mp: DefaultDict[Tuple[str, str], List[str]] = defaultdict(list)
    pairs = cfg.get("pairs") or []

    def add(src: str, feat: str, dst: str):
        k = (src, feat)
        if dst not in mp[k]:
            mp[k].append(dst)

    for pr in pairs:
        src = str(pr.get("src") or pr.get("source") or "").upper().strip()
        dst = str(pr.get("dst") or pr.get("target") or "").upper().strip()
        if not (src and dst):
            continue

        if pr.get("enabled") is False:
            continue
        mode = str(pr.get("mode") or "one-way").lower()

        feats = pr.get("features")
        feats_list: List[str] = []
        if isinstance(feats, (list, tuple)):
            feats_list = [str(f).lower() for f in feats]
        elif isinstance(feats, dict):
            for name in ("history", "watchlist", "ratings"):
                f = feats.get(name)
                if isinstance(f, dict) and (f.get("enable") or f.get("enabled")):
                    feats_list.append(name)
        else:
            feats_list = ["history"]

        for f in feats_list:
            add(src, f, dst)
            if mode in ("two-way", "bi", "both", "mirror", "two", "two_way", "two way"):
                add(dst, f, src)
    return mp

def _indices_for(s:Dict[str,Any]) -> Dict[Tuple[str,str], Dict[str,str]]:
    out={}
    for p,f,_,_ in _iter_items(s):
        key=(p,f)
        if key in out: continue
        out[key]=_alias_index(_bucket(s,p,f) or {})
    return out

def _has_peer_by_pairs(s:Dict[str,Any], pairs:DefaultDict[Tuple[str,str],List[str]],
                       prov:str, feat:str, item_key:str, item:Dict[str,Any],
                       idx_cache:Dict[Tuple[str,str],Dict[str,str]]) -> bool:
    if feat not in ("history","watchlist","ratings"): return True
    targets = pairs.get((prov,feat.lower()), [])
    if not targets: return True
    vv=dict(item); vv["_key"]=item_key
    keys=set(_alias_keys(vv))
    for dst in targets:
        idx = idx_cache.get((dst,feat)) or {}
        if any(k in idx for k in keys): return True
    return False

# --- Problems + fixes --------------------------------------------------------

def _problems(s: Dict[str, Any]) -> List[Dict[str, Any]]:
    probs=[]
    CORE=("imdb","tmdb","tvdb")

    pairs=_pair_map(_cfg(), s)
    idx_cache=_indices_for(s)

    # missing_peer across configured pairs (all providers/features present in pairs)
    for (prov,feat), targets in pairs.items():
        src_items=_bucket(s,prov,feat) or {}
        union_targets=[idx_cache.get((t,feat)) or {} for t in targets]
        if not union_targets: continue
        merged_keys=set().union(*[set(d.keys()) for d in union_targets]) if union_targets else set()
        for k,v in src_items.items():
            if v.get("_ignore_missing_peer"): continue
            vv=dict(v); vv["_key"]=k
            if not any(ak in merged_keys for ak in _alias_keys(vv)):
                probs.append({"severity":"warn","type":"missing_peer","provider":prov,"feature":feat,"key":k,
                              "title":v.get("title"),"year":v.get("year"),"targets":targets})

    # id format + key/id mismatches
    for p,f,k,it in _iter_items(s):
        ids=it.get("ids") or {}
        for ns in CORE:
            v=ids.get(ns); rx=_ID_RX.get(ns)
            if v and rx and not rx.match(str(v)):
                probs.append({"severity":"warn","type":"invalid_id_format","provider":p,"feature":f,"key":k,"id_name":ns,"id_value":v})
        if ":" in k:
            ns,kid=k.split(":",1); base=kid.split("#",1)[0].strip(); val=str((ids.get(ns) or "")).strip()
            if ns in CORE:
                if not val: probs.append({"severity":"warn","type":"key_missing_ids","provider":p,"feature":f,"key":k,"id_name":ns,"expected":base})
                elif val!=base: probs.append({"severity":"info","type":"key_ids_mismatch","provider":p,"feature":f,"key":k,"id_name":ns,"expected":base,"got":val})

    # orphan keys from cw_state
    present={k for *_,k,_ in _iter_items(s)}
    for fname,body in _read_cw_state().items():
        for k in (body.get("items") or {}).keys():
            if k not in present: probs.append({"severity":"info","type":"shadow_orphan","source":fname,"key":k})
    return probs

def _peer_ids(s: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str,str]:
    t=(cur.get("title") or "").strip().lower(); y=cur.get("year"); ty=(cur.get("type") or "").lower(); out={}
    for _,_,_,it in _iter_items(s):
        if (it.get("title") or "").strip().lower()!=t: continue
        if it.get("year")!=y: continue
        if (it.get("type") or "").lower()!=ty: continue
        for k,v in (it.get("ids") or {}).items():
            if v and k not in out: out[k]=str(v)
    return out

def _norm(ns:str,v:str)->str|None:
    if v is None: return None
    s=str(v).strip()
    if ns=="imdb":
        m=re.search(r"(\d+)",s); return f"tt{m.group(1)}" if m else None
    if ns in ("tmdb","tvdb","trakt","plex","simkl"):
        m=re.search(r"(\d+)",s); return m.group(1) if m else None
    return s or None

def _apply_fix(s: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    t,prov,feat,key=body.get("type"),body.get("provider"),body.get("feature"),body.get("key")
    b,it=_find_item(s,prov,feat,key)
    if not it: raise HTTPException(404,"Item not found")
    ids=it.setdefault("ids",{}); ch=[]
    if t in ("key_missing_ids","key_ids_mismatch"):
        ns=body.get("id_name"); exp=body.get("expected")
        if not ns or not exp: raise HTTPException(400,"Missing id_name/expected")
        ids[ns]=exp; ch.append(f"ids.{ns}={exp}")
        new=_rekey(b,key,it)
        # mark resolved state
        pairs=_pair_map(_cfg(), s); idx=_indices_for(s)
        it["_ignore_missing_peer"]=not _has_peer_by_pairs(s,pairs,prov,feat,new,it,idx)
        return {"ok":True,"changes":ch,"new_key":new}
    if t=="invalid_id_format":
        ns=body.get("id_name"); val=body.get("id_value"); nv=_norm(ns,val)
        if not nv: raise HTTPException(400,"Cannot normalize")
        ids[ns]=nv; ch.append(f"ids.{ns}={nv}")
        new=_rekey(b,key,it)
        pairs=_pair_map(_cfg(), s); idx=_indices_for(s)
        it["_ignore_missing_peer"]=not _has_peer_by_pairs(s,pairs,prov,feat,new,it,idx)
        return {"ok":True,"changes":ch,"new_key":new}
    if t in ("missing_ids","missing_peer"):
        if ":" in key:
            nsb,kid=key.split(":",1); base=kid.split("#",1)[0].strip()
            if base: ids.setdefault(nsb,base)
        peer=_peer_ids(s,it)
        for ns,v in (peer or {}).items():
            if not ids.get(ns): ids[ns]=v
        new=_rekey(b,key,it)
        pairs=_pair_map(_cfg(), s); idx=_indices_for(s)
        it["_ignore_missing_peer"]=not _has_peer_by_pairs(s,pairs,prov,feat,new,it,idx)
        return {"ok":True,"changes":["ids merged from peers"],"new_key":new}
    raise HTTPException(400,"Unsupported fix")

# --- External lookups --------------------------------------------------------

def _tmdb(path:str,params:Dict[str,Any])->Dict[str,Any]:
    k=_tmdb_key()
    if not k: raise HTTPException(400,"tmdb.api_key missing in config.json")
    r=requests.get(f"https://api.themoviedb.org/3{path}",params={**(params or {}),"api_key":k},timeout=8)
    r.raise_for_status(); return r.json()

def _trakt(path:str,params:Dict[str,Any])->List[Dict[str,Any]]:
    h=_trakt_headers()
    if not h.get("trakt-api-key"): raise HTTPException(400,"trakt.client_id missing in config.json")
    r=requests.get(f"https://api.trakt.tv{path}",params=params,headers=h,timeout=8)
    r.raise_for_status(); return r.json()

def _anchor(key:str):
    if "#" not in key: return None
    m=re.search(r"#s(\d+)[eE](\d+)",key)
    return (int(m.group(1)),int(m.group(2))) if m else None

def _sig(ids:Dict[str,Any])->str:
    return "|".join([str(ids.get(x,"")) for x in ("imdb","tmdb","tvdb","trakt","plex","emby")])

def _suggest(s: Dict[str, Any], prov:str, feat:str, key:str) -> Dict[str,Any]:
    b,it=_find_item(s,prov,feat,key)
    if not it: raise HTTPException(404,"Item not found")
    title=(it.get("title") or "").strip(); year=it.get("year"); typ=(it.get("type") or "").lower()
    ids=it.get("ids") or {}; se=_anchor(key); out=[]; seen=set()
    def push(new_ids, why, conf, src):
        merged={**ids, **{k:v for k,v in (new_ids or {}).items() if v}}
        sig=_sig(merged)
        if sig in seen: return
        seen.add(sig); out.append({"ids":merged,"reason":why,"source":src,"confidence":round(conf,3)})
    if ":" in key:
        ns,rest=key.split(":",1); base=rest.split("#",1)[0].strip()
        if ns in ("imdb","tmdb","tvdb","trakt") and not ids.get(ns): push({ns:base},f"from key:{ns}",0.92,"key")
    peer=_peer_ids(s,it)
    if peer: push(peer,"from peers (title/year/type)",0.87,"peers")
    try:
        if ids.get("imdb"):
            f=_tmdb(f"/find/{ids['imdb']}",{"external_source":"imdb_id"})
            if typ=="movie" and f.get("movie_results"):
                tid=f["movie_results"][0]["id"]; ext=_tmdb(f"/movie/{tid}/external_ids",{}); push({"tmdb":tid,"imdb":ext.get("imdb_id")},"TMDB ext (movie)",0.98,"tmdb")
            elif typ in ("show","episode") and (f.get("tv_results") or f.get("tv_episode_results")):
                base=(f.get("tv_results") or [{"id":None}])[0]["id"]
                if typ=="show" and base:
                    ext=_tmdb(f"/tv/{base}/external_ids",{}); push({"tmdb":base,"imdb":ext.get("imdb_id"),"tvdb":ext.get("tvdb_id")},"TMDB ext (tv)",0.98,"tmdb")
                if typ=="episode" and base and se:
                    sN,eN=se; ext=_tmdb(f"/tv/{base}/season/{sN}/episode/{eN}/external_ids",{}); push({"tmdb":base,"imdb":ext.get("imdb_id"),"tvdb":ext.get("tvdb_id")},"TMDB ext (ep)",0.98,"tmdb")
    except Exception: pass
    try:
        if ids.get("tmdb"):
            if typ=="movie":
                ext=_tmdb(f"/movie/{ids['tmdb']}/external_ids",{}); push({"imdb":ext.get("imdb_id")},"TMDB ext (movie)",0.98,"tmdb")
            elif typ=="show":
                ext=_tmdb(f"/tv/{ids['tmdb']}/external_ids",{}); push({"imdb":ext.get("imdb_id"),"tvdb":ext.get("tvdb_id")},"TMDB ext (tv)",0.98,"tmdb")
            elif typ=="episode" and se:
                sN,eN=se; ext=_tmdb(f"/tv/{ids['tmdb']}/season/{sN}/episode/{eN}/external_ids",{}); push({"imdb":ext.get("imdb_id"),"tvdb":ext.get("tvdb_id")},"TMDB ext (ep)",0.98,"tmdb")
    except Exception: pass
    try:
        if title:
            if typ=="movie":
                r=_tmdb("/search/movie",{"query":title,"year":year or ""})
                if r.get("results"):
                    cand=r["results"][0]; tid=cand["id"]; yr=int((cand.get("release_date") or "0000")[:4] or 0)
                    conf=0.9-min(abs((year or 0)-yr),2)*0.05
                    ext=_tmdb(f"/movie/{tid}/external_ids",{}); push({"tmdb":tid,"imdb":ext.get("imdb_id")},"TMDB search(movie)",conf,"tmdb")
            elif typ in ("show","episode"):
                r=_tmdb("/search/tv",{"query":title,"first_air_date_year":year or ""})
                if r.get("results"):
                    base=r["results"][0]["id"]; ext=_tmdb(f"/tv/{base}/external_ids",{}); push({"tmdb":base,"imdb":ext.get("imdb_id"),"tvdb":ext.get("tvdb_id")},"TMDB search(tv)",0.88,"tmdb")
    except Exception: pass
    try:
        if title:
            t={"movie":"movie","show":"show","episode":"episode"}.get(typ,"movie,show")
            r=_trakt("/search/"+t,{"query":title,"year":year or ""})
            for e in (r or [])[:3]:
                ids2=(e.get("movie") or e.get("show") or e.get("episode") or {}).get("ids") or {}
                push({"trakt":ids2.get("trakt"),"imdb":ids2.get("imdb"),"tmdb":ids2.get("tmdb"),"tvdb":ids2.get("tvdb")},"Trakt search",0.86,"trakt")
    except Exception: pass
    miss=[]
    if not _tmdb_key(): miss.append("tmdb.api_key")
    if not _trakt_headers().get("trakt-api-key"): miss.append("trakt.client_id")
    return {"suggestions":out,"needs":miss}

# --- API ---------------------------------------------------------------------

@router.get("/analyzer/state", response_class=JSONResponse)
def api_state(): s=_load_state(); return {"counts":_counts(s),"items":_collect_items(s)}

@router.get("/analyzer/problems", response_class=JSONResponse)
def api_problems(): return {"problems":_problems(_load_state())}

@router.get("/analyzer/ratings-audit", response_class=JSONResponse)
def api_ratings_audit():
    s=_load_state()
    def _ratings_items(prov:str)->Dict[str,Any]:
        pv=(s.get("providers") or {}).get(prov) or {}
        return ((pv.get("ratings") or {}).get("baseline") or {}).get("items") or {}
    def _alias_keys_r(obj:Dict[str,Any])->List[str]:
        t=str(obj.get("type") or "").lower(); ids=dict(obj.get("ids") or {}); out=[]; seen=set()
        if obj.get("_key"): out.append(obj["_key"])
        for ns in ("trakt","imdb","tmdb","tvdb","slug"):
            v=ids.get(ns)
            if v:
                vs=str(v); out.append(f"{ns}:{vs}")
                if t in ("movie","show","season","episode"): out.append(f"{t}:{ns}:{vs}")
        res=[]; 
        for k in out:
            if k not in seen: seen.add(k); res.append(k)
        return res
    def _alias_index_r(items:Dict[str,Any])->Dict[str,str]:
        idx={}
        for k,v in items.items():
            vv=dict(v); vv["_key"]=k
            for ak in _alias_keys_r(vv): idx.setdefault(ak,k)
        return idx
    def _sus(items:Dict[str,Any]):
        o=[]
        for k,v in items.items():
            f=[]
            if not (v.get("title") or "").strip(): f.append("no_title")
            if not (v.get("year") or 0): f.append("no_year")
            if not (v.get("ids") or {}).get("trakt"): f.append("no_trakt_id")
            if f: o.append([k,v,f])
        return o
    plex=_ratings_items("PLEX"); trakt=_ratings_items("TRAKT")
    only_p=sorted(set(plex)-set(trakt)); only_t=sorted(set(trakt)-set(plex))
    pa=_alias_index_r(plex); ta=_alias_index_r(trakt)
    ap=[[k,next(ta[a] for a in _alias_keys_r({"_key":k,**plex[k]}) if a in ta)] for k in only_p if any(a in ta for a in _alias_keys_r({"_key":k,**plex[k]}))]
    at=[[k,next(pa[a] for a in _alias_keys_r({"_key":k,**trakt[k]}) if a in pa)] for k in only_t if any(a in pa for a in _alias_keys_r({"_key":k,**trakt[k]}))]
    return {"counts":{"plex":len(plex),"trakt":len(trakt),"only_in_plex":len(only_p),"only_in_trakt":len(only_t),
                      "alias_hits_from_plex":len(ap),"alias_hits_from_trakt":len(at),
                      "plex_suspects":len(_sus(plex)),"trakt_suspects":len(_sus(trakt))},
            "only_in_plex":only_p,"only_in_trakt":only_t,"alias_hits_from_plex":ap,"alias_hits_from_trakt":at,
            "plex_suspects":_sus(plex),"trakt_suspects":_sus(trakt)}

@router.post("/analyzer/suggest", response_class=JSONResponse)
def api_suggest(payload:Dict[str,Any]): s=_load_state(); p=payload or {}; return _suggest(s,p.get("provider"),p.get("feature"),p.get("key"))

@router.post("/analyzer/fix", response_class=JSONResponse)
def api_fix(payload:Dict[str,Any]): s=_load_state(); r=_apply_fix(s,payload); _save_state(s); return r

@router.patch("/analyzer/item", response_class=JSONResponse)
def api_edit(payload:Dict[str,Any]):
    for f in ("provider","feature","key","updates"):
        if f not in payload: raise HTTPException(400,f"Missing {f}")
    s=_load_state(); b=_bucket(s,payload["provider"],payload["feature"])
    if not b or payload["key"] not in b: raise HTTPException(404,"Item not found")
    it=b[payload["key"]]; up=payload["updates"]
    if "title" in up: it["title"]=up["title"]
    if "year" in up: it["year"]=up["year"]
    if "ids" in up and isinstance(up["ids"],dict):
        ids=it.setdefault("ids",{})
        for k,v in up["ids"].items():
            if v is None: ids.pop(k,None)
            elif v!="": ids[k]=v
    new=_rekey(b,payload["key"],it)
    pairs=_pair_map(_cfg(), s); idx=_indices_for(s)
    it["_ignore_missing_peer"]=not _has_peer_by_pairs(s,pairs,payload["provider"],payload["feature"],new,it,idx)
    _save_state(s); return {"ok":True,"new_key":new}

@router.delete("/analyzer/item", response_class=JSONResponse)
def api_delete(payload:Dict[str,Any]):
    for f in ("provider","feature","key"):
        if f not in payload: raise HTTPException(400,f"Missing {f}")
    s=_load_state(); b=_bucket(s,payload["provider"],payload["feature"])
    if not b or payload["key"] not in b: raise HTTPException(404,"Item not found")
    b.pop(payload["key"],None); _save_state(s); return {"ok":True}
