# tests/test_plex_history_recovery.py
# CrossWatch - Plex history recovery tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from copy import deepcopy
from types import SimpleNamespace as NS
import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from providers.sync.plex import _common as common, _history as history, _recovery as recovery
from services import plex_history_recovery as service


@pytest.fixture
def plex(monkeypatch):
    state = NS(rows=[], library=[], searches=[], calls=[])
    def read(**kwargs):
        state.calls.append(kwargs)
        return state.rows
    server = NS(history=read)
    state.adapter = NS(client=NS(server=server, user_account_id=17), config={"plex": {}},
                       libraries=lambda **kw: [NS(key="1", type="movie", title="Movies")])
    monkeypatch.setattr(common, "home_scope_enter", lambda _: (False, False, 17, None))
    monkeypatch.setattr(common, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(common, "hydrate_external_ids", lambda *a: {})
    monkeypatch.setattr(common, "_hydrate_show_ids_from_episode_rk", lambda *a: {})
    monkeypatch.setattr(common, "plex_context", lambda: {"baseurl":"http://plex", "token":"token", "account_token":"token"})
    monkeypatch.setattr(history, "_fetch_section_guid_rows", lambda *a, **kw: (state.library, 1))
    def discover(*a, **kw):
        state.searches.append(a)
        return {"type":"movie", "title":"Removed", "year":2000, "Guid":[{"id":"tmdb://99"}]}
    monkeypatch.setattr(common, "_discover_search_title", discover)
    return state


def raw(rk="42", **kwargs):
    return NS(**dict(type="movie", ratingKey=rk, title="Removed", year=2000,
                     viewedAt=1700000000, accountID=17, **kwargs))


def scan(plex, **kwargs):
    return recovery.scan(plex.adapter, progress=lambda *a: None, check_cancel=lambda: None, **kwargs)


def test_only_removed_history_and_rewatches_without_persistent_memo(plex, monkeypatch):
    plex.library = [{"type":"movie", "ratingKey":"1", "title":"Current", "Guid":[{"id":"tmdb://1"}]}]
    known = raw("2", guid="tmdb://2")
    repeated = deepcopy(known); repeated.viewedAt += 100
    plex.rows = [raw("1"), known, repeated, raw("3")]
    before = deepcopy(common._FBGUID_MEMO)
    monkeypatch.setattr(common, "_fb_cache_load", lambda: pytest.fail("Recovery read the persistent memo"))
    result = scan(plex)
    assert len(result) == 3
    assert len(plex.searches) == 1
    assert result[0]["item"]["watched_at"] != result[1]["item"]["watched_at"]
    assert common._FBGUID_MEMO == before
    assert plex.calls == [{"accountID":17}]


def test_readded_identity_and_other_user_are_excluded(plex):
    plex.library = [{"type":"movie", "ratingKey":"new", "Guid":[{"id":"tmdb://2"}]}]
    wrong = raw("3"); wrong.accountID=18
    plex.rows = [raw("old", guid="tmdb://2"), wrong]
    assert scan(plex) == []
    assert not plex.searches


@pytest.mark.parametrize("timestamp", [0, 1, None])
def test_recovery_keeps_epoch_separate_from_missing_date(plex, timestamp):
    row = raw("2", guid="tmdb://2")
    row.viewedAt = timestamp
    plex.rows = [row]

    result = scan(plex)

    assert len(result) == 1
    assert result[0]["item"]["watched_at"] == (history._iso(timestamp) if timestamp is not None else None)


def test_title_guess_rewatches_keep_the_review_requirement(plex):
    first=raw();repeat=deepcopy(first);repeat.viewedAt+=100
    plex.rows=[first,repeat,raw("known",guid="tmdb://1")]
    result=scan(plex)
    assert len(plex.searches)==1
    assert [r["recovery_requires_review"] for r in result]==[True,True,False]
    assert result[0]["recovery_match"]==result[1]["recovery_match"]
    assert result[0]["item"]["watched_at"]!=result[1]["item"]["watched_at"]


def test_live_activity_reports_lookups_outcomes_and_counts_without_other_user_titles(plex,monkeypatch):
    updates=[]
    plex.library=[{"type":"movie","ratingKey":"1"}]
    private=raw("private");private.accountID=18;private.title="Other user's title"
    missing=raw("missing");missing.title="Unknown movie"
    plex.rows=[raw("1"),raw("2",guid="tmdb://2"),raw("3"),private,missing]
    def discover(token,title,*a,**kw):
        assert updates[-1][4]["current_action"]=="Searching Plex for a title match"
        assert updates[-1][4]["checked"] in (2,4)
        return None if title=="Unknown movie" else {"type":"movie","title":title,"Guid":[{"id":"tmdb://99"}]}
    monkeypatch.setattr(common,"_discover_search_title",discover)
    result=recovery.scan(plex.adapter,progress=lambda *a:updates.append(a),check_cancel=lambda:None)
    final=updates[-1][4]
    assert (final["checked"],final["found"],final["skipped"])==(5,3,2)
    assert len(result)==3
    events=[u[4]["event"] for u in updates if len(u)==5 and "event" in u[4]]
    assert [e["result"] for e in events]==["Skipped: still in Plex","Found for review","Found: title guess needs review","Found: needs a match"]
    assert "Other user's title" not in str(updates)


def test_live_episode_title_includes_specials_coordinates():
    label=recovery._history_label(NS(type="episode",title="Special",grandparentTitle="Series",parentIndex=0,index=1))
    assert "Series" in label and "S0 E1" in label and "Special" in label


def test_failed_library_read_never_turns_into_removed_items(plex, monkeypatch):
    def fail(*a, **kw): raise RuntimeError("library unavailable")
    monkeypatch.setattr(history,"_fetch_section_guid_rows",fail)
    with pytest.raises(RuntimeError, match="library unavailable"):
        scan(plex)
    assert plex.calls == []


def test_wrong_user_scope_stops_scan(plex, monkeypatch):
    monkeypatch.setattr(common,"home_scope_enter",lambda _: (True,False,17,None))
    with pytest.raises(RuntimeError,match="user scope"):
        scan(plex)


def test_missing_matches_are_reviewable_not_importable(plex, monkeypatch):
    monkeypatch.setattr(common,"_discover_search_title",lambda *a,**kw: None)
    plex.rows=[raw()]
    result=scan(plex)
    assert len(result)==1
    assert not result[0]["recovery_valid"]


@pytest.mark.parametrize("parent_index", [0, None])
def test_special_episode_keeps_coordinates_and_date(plex, parent_index):
    episode=NS(type="episode",ratingKey="88",title="Special",grandparentTitle="Series",
               grandparentGuid="tvdb://10",parentIndex=parent_index,seasonNumber=0,index=1,viewedAt=1700000000,accountID=17)
    plex.rows=[episode]
    row=scan(plex)[0]
    assert row["item"]["season"]==0 and row["item"]["episode"]==1
    assert row["item"]["show_ids"]["tvdb"]=="10"
    assert row["recovery_valid"]


def test_cancellation_stops_before_network(plex):
    def cancelled(): raise InterruptedError()
    with pytest.raises(InterruptedError):
        recovery.scan(plex.adapter, progress=lambda *a: None, check_cancel=cancelled)
    assert plex.calls==[]


def test_recovery_row_limit_does_not_publish_partial_results(plex):
    plex.rows=[raw("1",guid="tmdb://1"),raw("2",guid="tmdb://2")]
    with pytest.raises(RuntimeError,match="Narrow"):
        scan(plex,max_rows=1)


@pytest.fixture
def api(monkeypatch, config_base):
    from services import importer
    from api import syncAPI
    cfg={"plex":{"baseurl":"http://plex","token":"test","fallback_GUID":True},
         "crosswatch":{"connected":True,"root_dir":str(config_base / "tracker")}}
    monkeypatch.setattr(service,"load_config",lambda:deepcopy(cfg))
    monkeypatch.setattr(importer,"load_config",lambda:deepcopy(cfg))
    monkeypatch.setattr(service,"request_user",lambda _:None)
    monkeypatch.setattr(importer,"request_user",lambda _:None)
    monkeypatch.setattr(service,"user_can_access_instance",lambda *a:True)
    monkeypatch.setattr(importer,"user_can_access_instance",lambda *a:True)
    monkeypatch.setattr(syncAPI,"_is_sync_running",lambda:False)
    rt=({}, {}, threading.Lock())
    monkeypatch.setattr(syncAPI,"_rt",lambda:rt)
    service.JOBS.clear()
    job=service.Job("local","default","default",service._source_hash(cfg,"default"),
                    target_hash=service._target_hash(cfg,"default"),status="review")
    job.raw=[dict(feature="history", item=dict(type="movie",title=f"Old {i}",ids={"tmdb":str(i+1)},
                 watched_at=f"2020-01-0{i+1}T12:00:00Z"), recovery_match="Known identity", recovery_valid=True, original_title=f"Old {i}") for i in range(3)]
    service._shape(job,cfg)
    service.JOBS[job.id]=job
    app=FastAPI();app.include_router(service.router)
    yield NS(client=TestClient(app),job=job,cfg=cfg,base=config_base)
    service.JOBS.clear()


def test_import_only_selected_events_to_real_tracker_and_repeat_is_skipped(api):
    from services import importer
    url=f"/api/import/plex-recovery/{api.job.id}"
    payload=dict(import_id=api.job.id,revision=0,target_instance="default",mode="selected",row_ids=["1"])
    response=api.client.post(url+"/commit",json=payload)
    assert response.status_code==200,response.text
    assert response.json()["applied"]==1
    stored=importer._existing_keys(api.cfg,"default")["history"]
    assert len(stored)==1
    assert next(iter(stored.values()))["ids"]["tmdb"]=="2"
    assert next(iter(stored.values()))["watched_at"]=="2020-01-02T12:00:00Z"
    receipt=api.client.get(url+"/receipt").json()
    assert receipt["keys"]==list(stored)
    payload["revision"]=response.json()["revision"]
    assert api.client.post(url+"/commit",json=payload).json()["applied"]==0
    assert len(importer._existing_keys(api.cfg,"default")["history"])==1
    assert api.cfg["plex"]["fallback_GUID"] is True


def test_bulk_import_skips_title_guesses_but_explicit_selection_accepts_them(api):
    from services import importer
    api.job.raw[1]["recovery_requires_review"]=True
    service._shape(api.job,api.cfg)
    url=f"/api/import/plex-recovery/{api.job.id}"
    preview=api.client.get(url+"/rows").json()
    guessed=next(r for r in preview["rows"] if r["id"]=="1")
    assert guessed["status"]=="needs_review"
    assert guessed["default_included"] is False
    assert preview["summary"]["ready"]==2
    payload=dict(import_id=api.job.id,revision=0,target_instance="default",mode="ready")
    result=api.client.post(url+"/commit",json=payload)
    assert result.status_code==200,result.text
    assert result.json()["applied"]==2
    assert api.job.rows[1]["status"]=="needs_review"
    assert {i["ids"]["tmdb"] for i in importer._existing_keys(api.cfg,"default")["history"].values()}=={"1","3"}
    payload.update(revision=result.json()["revision"],mode="selected",row_ids=["1"],status="needs_review")
    result=api.client.post(url+"/commit",json=payload)
    assert result.status_code==200,result.text
    assert result.json()["applied"]==1
    assert api.job.rows[1]["status"]=="exists"
    assert len(api.client.get(url+"/receipt").json()["keys"])==3


def test_bulk_and_individual_selections_can_be_combined_without_approving_other_guesses(api):
    api.job.raw[1]["recovery_requires_review"]=True
    api.job.raw[2]["recovery_requires_review"]=True
    url=f"/api/import/plex-recovery/{api.job.id}"
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=0,mode="ready",row_ids=["1"]))
    assert result.status_code==200,result.text
    assert result.json()["applied"]==2
    assert [r["status"] for r in api.job.rows]==["exists","exists","needs_review"]


def test_saving_a_match_makes_a_guess_eligible_for_bulk_import(api):
    api.job.raw[1]["recovery_requires_review"]=True
    service._shape(api.job,api.cfg)
    url=f"/api/import/plex-recovery/{api.job.id}"
    response=api.client.post(url+"/match",json=dict(revision=0,row_id="1",title="Confirmed",tmdb="2"))
    assert response.status_code==200,response.text
    assert api.job.rows[1]["status"]=="ready"
    assert not api.job.raw[1]["recovery_requires_review"]
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=1,mode="ready"))
    assert result.json()["applied"]==3


def test_accept_title_matches_across_pages_without_importing(api):
    from services import importer
    template = api.job.raw[0]
    api.job.raw = [dict(deepcopy(template), original_title=f"Guess {i}", recovery_requires_review=True,
                        item=dict(template["item"], title=f"Guess {i}", ids={"tmdb":str(i+1)})) for i in range(65)]
    service._shape(api.job, api.cfg)
    url = f"/api/import/plex-recovery/{api.job.id}"
    response = api.client.post(url+"/accept-title-matches", json=dict(revision=0))
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 65
    assert all(row["status"] == "ready" for row in api.job.rows)
    assert not importer._existing_keys(api.cfg, "default")["history"]
    assert not api.job.receipt
    result = api.client.post(url+"/commit", json=dict(import_id=api.job.id, revision=1, mode="ready"))
    assert result.json()["applied"] == 65


def test_accept_title_matches_respects_search_and_skips_invalid_and_existing(api):
    from services import importer
    url = f"/api/import/plex-recovery/{api.job.id}"
    api.client.post(url+"/commit", json=dict(import_id=api.job.id, revision=0, mode="selected", row_ids=["0"]))
    for raw in api.job.raw:
        raw["recovery_requires_review"] = True
    missing = deepcopy(api.job.raw[2])
    missing["item"].update(type="episode", title="Old missing", season=0, episode=None, show_ids={"tmdb":"5"})
    missing["recovery_valid"] = False
    invalid = deepcopy(api.job.raw[2])
    invalid["original_title"] = "Old invalid"
    invalid["item"].update(title="Old invalid", ids={"tmdb":"6"}, watched_at=None)
    api.job.raw.extend([missing, invalid])
    api.job.raw[1]["item"]["title"] = "Distinctive"
    response = api.client.post(url+"/accept-title-matches", json=dict(revision=1, q="Distinctive"))
    assert response.json()["accepted"] == 1
    assert [row["status"] for row in api.job.rows][:3] == ["exists", "ready", "needs_review"]
    assert api.job.raw[3]["recovery_requires_review"]
    assert api.job.raw[4]["recovery_requires_review"]
    response = api.client.post(url+"/accept-title-matches", json=dict(revision=2))
    assert response.json()["accepted"] == 1
    assert api.job.raw[0]["recovery_requires_review"]
    assert api.job.raw[3]["recovery_requires_review"]
    assert api.job.raw[4]["recovery_requires_review"]
    assert len(importer._existing_keys(api.cfg, "default")["history"]) == 1


@pytest.mark.parametrize("block", ["revision", "running", "settings", "owner", "closed"])
def test_accept_title_matches_rejects_stale_or_unavailable_recovery(api, monkeypatch, block):
    api.job.raw[1]["recovery_requires_review"] = True
    if block == "running":
        api.job.auto_thread = NS(is_alive=lambda:True)
    elif block == "settings":
        api.cfg["plex"]["baseurl"] = "http://different"
    elif block == "owner":
        api.job.owner = "someone-else"
    elif block == "closed":
        api.job.cancel.set()
    response = api.client.post(f"/api/import/plex-recovery/{api.job.id}/accept-title-matches",
                               json=dict(revision=99 if block == "revision" else 0))
    assert response.status_code == (404 if block == "owner" else 409)
    assert api.job.raw[1]["recovery_requires_review"]


def test_preview_and_cancel_do_not_write_tracker(api):
    from services import importer
    url=f"/api/import/plex-recovery/{api.job.id}"
    assert api.client.get(url+"/rows").json()["summary"]["ready"]==3
    assert api.client.delete(url).status_code==200
    assert not importer._existing_keys(api.cfg,"default")["history"]


def test_running_scan_can_be_cancelled_and_releases_sync_slot(api,monkeypatch):
    from api import syncAPI
    from providers.sync import _mod_PLEX
    from services import importer
    entered=threading.Event();release=threading.Event()
    rt=syncAPI._rt()
    monkeypatch.setattr(syncAPI,"_is_sync_running",lambda:bool(rt[1].get("SYNC") and rt[1]["SYNC"].is_alive()))
    monkeypatch.setattr(_mod_PLEX,"PLEXModule",lambda _:NS(client=NS(server=None,session=None)))
    def read(*a,check_cancel,progress,**kw):
        for index in range(12):
            progress("matching","Matching history",index+1,20,
                     dict(checked=index+1,found=index+1,skipped=0,current_item=f"Movie {index}",current_action="",
                          event=dict(title=f"Movie {index}",result="Found for review")))
        entered.set()
        assert release.wait(3)
        check_cancel()
        return []
    monkeypatch.setattr(recovery,"scan",read)
    assert api.client.delete(f"/api/import/plex-recovery/{api.job.id}").status_code == 200
    response=api.client.post("/api/import/plex-recovery",json={})
    assert response.status_code==200,response.text
    url="/api/import/plex-recovery/"+response.json()["id"]
    thread=rt[1]["SYNC"]
    try:
        assert entered.wait(3)
        live=api.client.get(url).json()
        assert live["activity"]["found"]==12
        assert live["imported"]==0
        assert [item["title"] for item in live["recent"]]==[f"Movie {index}" for index in range(4,12)]
        assert api.client.post("/api/import/plex-recovery",json={}).status_code==409
        assert api.client.delete(url).status_code==200
    finally:
        release.set();thread.join(3)
    assert not thread.is_alive()
    assert "SYNC" not in rt[1]
    assert api.client.get(url).json()["status"]=="cancelled"
    assert not importer._existing_keys(api.cfg,"default")["history"]


def test_commit_rejects_changed_destination_revision_and_source(api):
    url=f"/api/import/plex-recovery/{api.job.id}/commit"
    payload=dict(import_id=api.job.id,revision=0,target_instance="elsewhere")
    assert api.client.post(url,json=payload).status_code==400
    payload.update(target_instance="default",revision=99)
    assert api.client.post(url,json=payload).status_code==409
    payload["revision"]=0;api.cfg["plex"]["username"]="another user"
    assert api.client.post(url,json=payload).status_code==409


def test_recovery_is_private_and_requires_instance_access(api, monkeypatch):
    url=f"/api/import/plex-recovery/{api.job.id}"
    monkeypatch.setattr(service,"request_user",lambda _: {"id":"other","is_admin":True})
    assert api.client.get(url).status_code==404
    monkeypatch.setattr(service,"request_user",lambda _: None)
    monkeypatch.setattr(service,"user_can_access_instance",lambda *a:False)
    assert api.client.get(url+"/rows").status_code==403


def test_changed_tracker_settings_require_a_new_preview(api):
    api.cfg["crosswatch"]["root_dir"] += "-changed"
    response=api.client.post(f"/api/import/plex-recovery/{api.job.id}/commit",
                            json=dict(import_id=api.job.id,revision=0,target_instance="default"))
    assert response.status_code==409
    assert "Tracker settings changed" in response.json()["detail"]


def test_editing_match_preserves_watch_event_and_invalidates_revision(api):
    url=f"/api/import/plex-recovery/{api.job.id}"
    response=api.client.post(url+"/match",json=dict(revision=0,row_id="1",title="Corrected",tmdb="42"))
    assert response.status_code==200,response.text
    assert api.job.raw[1]["item"]["watched_at"]=="2020-01-02T12:00:00Z"
    assert api.job.rows[1]["id"]=="1"
    assert api.job.raw[1]["item"]["ids"]=={"tmdb":"42"}
    assert api.job.revision==1


def test_cache_cleanup_preserves_legacy_recovery_memos(tmp_path,monkeypatch):
    from api import maintenanceAPI
    state=tmp_path/".cw_state";state.mkdir()
    memo=state/"plex_fallback_memo.cw2_test.json";memo.write_text('{"saved":true}')
    (state/"temporary.json").write_text("{}")
    monkeypatch.setattr(maintenanceAPI,"_cw",lambda:(tmp_path/"cache",tmp_path,state,None,None,None))
    assert "plex_fallback_memo.cw2_test.json" not in {r["name"] for r in maintenanceAPI._scan_provider_cache()["files"]}
    maintenanceAPI.clear_cache()
    assert memo.exists()


@pytest.mark.parametrize("guessed", [False, True])
def test_episode_recovery_keeps_distinct_episodes_with_only_the_latest_date(plex, api, monkeypatch, guessed):
    def episode(season, number, date=1664057618):
        return NS(type="episode", ratingKey=f"{season}-{number}", grandparentRatingKey="show-1",
                  title=f"Episode {number}", grandparentTitle="Better Call Saul", parentIndex=season,
                  index=number, viewedAt=date, accountID=17,
                  grandparentGuid="" if guessed else "tmdb://60059")
    def discover(*a, **kw):
        plex.searches.append(kw)
        return {"type":"show", "title":"Better Call Saul", "Guid":[{"id":"tmdb://60059"}]}
    monkeypatch.setattr(common, "_discover_search_title", discover)
    plex.rows=[episode(6,1),episode(6,4),episode(6,5),episode(5,1),episode(6,1),episode(6,1,1664057718)]
    api.job.raw=scan(plex)
    service._shape(api.job,api.cfg)
    expected="needs_review" if guessed else "ready"
    assert len(api.job.rows)==4
    assert [row["status"] for row in api.job.rows]==[expected]*4
    assert len({row["key"] for row in api.job.rows})==4
    assert api.job.rows[0]["id"]=="5"
    assert api.job.rows[0]["item"]["watched_at"]==history._iso(1664057718)
    assert [(row["item"]["season"],row["item"]["episode"]) for row in api.job.raw]==[(6,1),(6,4),(6,5),(5,1),(6,1),(6,1)]
    url=f"/api/import/plex-recovery/{api.job.id}"
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=0,mode="selected",row_ids=[str(i) for i in range(6)]))
    assert result.status_code==200,result.text
    assert result.json()["applied"]==4
    assert len(api.client.get(url+"/receipt").json()["keys"])==4


def test_season_number_fallback_does_not_reuse_another_seasons_match(plex, api):
    plex.rows=[NS(type="episode",ratingKey=str(season),title="Episode one",grandparentTitle="Series",
                  grandparentGuid="tmdb://60059",parentIndex=None,seasonNumber=season,index=1,
                  viewedAt=1664057618,accountID=17) for season in (0,1,6)]
    api.job.raw=scan(plex)
    service._shape(api.job,api.cfg)
    assert [row["item"]["season"] for row in api.job.raw]==[0,1,6]
    assert [row["status"] for row in api.job.rows]==["ready"]*3


@pytest.fixture
def unresolved_series(api):
    api.job.raw=[]
    for source, title, season, episode, date in [
        ("uk", "The Office", 0, 1, "2020-01-01T12:00:00Z"),
        ("uk", "The Office", 1, 1, "2020-01-02T12:00:00Z"),
        ("uk", "The Office", 1, 1, "2020-01-03T12:00:00Z"),
        ("uk", "The Office", 1, 1, "2020-01-03T12:00:00Z"),
        ("us", "The Office", 1, 1, "2020-01-04T12:00:00Z"),
    ]:
        api.job.raw.append(dict(feature="history",item=dict(type="episode",series_title=title,title=title,
            season=season,episode=episode,ids={},show_ids={},watched_at=date),original_title=title,
            original_group=source,original_year=2001 if source=="uk" else 2005,
            recovery_valid=False,recovery_match="Missing identity"))
    service._shape(api.job,api.cfg)
    return api


def test_bulk_groups_original_series_across_seasons_and_rewatches(unresolved_series):
    api=unresolved_series
    url=f"/api/import/plex-recovery/{api.job.id}"
    context=api.client.get(url+"/match-groups",params=dict(revision=0)).json()
    assert len(context["rows"])==2
    assert [g["recovery_count"] for g in context["rows"]]==[2,1]
    first=context["rows"][0]
    dates=[r["item"]["watched_at"] for r in api.job.raw]
    result=api.client.post(url+"/match-groups",json=dict(revision=0,edits=[dict(group_id=first["id"],title="The Office (UK)",ids={"tmdb":"2996"})]))
    assert result.status_code==200,result.text
    assert result.json()["matched_events"]==2
    assert [r["status"] for r in api.job.rows]==["ready","ready","missing_identity"]
    assert [r["item"]["watched_at"] for r in api.job.raw]==dates
    assert [r["item"]["season"] for r in api.job.raw]==[0,1,1,1,1]
    assert api.job.raw[-1]["item"]["show_ids"]=={}
    from services import importer
    assert not importer._existing_keys(api.cfg,"default")["history"]
    remaining=api.client.get(url+"/match-groups",params=dict(revision=1)).json()
    assert len(remaining["rows"])==1
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=1,mode="ready"))
    assert result.json()["applied"]==2


def test_bulk_match_is_atomic_and_rejects_stale_or_foreign_groups(unresolved_series):
    api=unresolved_series
    url=f"/api/import/plex-recovery/{api.job.id}"
    groups=api.client.get(url+"/match-groups",params=dict(revision=0)).json()["rows"]
    edits=[dict(group_id=g["id"],title="Confirmed",ids={"tmdb":"10"}) for g in groups]
    before=deepcopy(api.job.raw)
    edits[1]["ids"]={"imdb":"invalid"}
    assert api.client.post(url+"/match-groups",json=dict(revision=0,edits=edits)).status_code==400
    assert api.job.raw==before and api.job.revision==0
    edits[1]["group_id"]="foreign"
    assert api.client.post(url+"/match-groups",json=dict(revision=0,edits=edits)).status_code==409
    assert api.job.raw==before
    assert api.client.post(url+"/match-groups",json=dict(revision=2,edits=edits[:1])).status_code==409


def test_bulk_matching_preserves_explicit_matches_and_missing_numbering(unresolved_series):
    api=unresolved_series
    api.job.raw[0].update(recovery_valid=True,recovery_requires_review=False)
    api.job.raw[0]["item"]["show_ids"]={"tmdb":"5"}
    api.job.raw[1]["item"]["season"]=None
    service._shape(api.job,api.cfg)
    url=f"/api/import/plex-recovery/{api.job.id}"
    group=api.client.get(url+"/match-groups",params=dict(revision=0)).json()["rows"][0]
    assert group["recovery_count"]==2
    result=api.client.post(url+"/match-groups",json=dict(revision=0,edits=[dict(group_id=group["id"],title="Confirmed",ids={"tmdb":"10"})]))
    assert result.status_code==200,result.text
    assert api.job.raw[0]["item"]["show_ids"]=={"tmdb":"5"}
    assert api.job.rows[1]["status"]=="missing_identity"
    assert api.job.raw[1]["item"]["season"] is None


def test_catalog_search_uses_configured_accessible_profiles_and_caches_suggestions(unresolved_series,monkeypatch):
    from services import interactive_sync_mapping
    api=unresolved_series
    api.cfg["trakt"]={"client_id":"client","access_token":"token"}
    api.cfg["tmdb"]={"api_key":"metadata"}
    calls=[]
    def search(cfg,row,q,**kwargs):
        calls.append((row,q,kwargs))
        return dict(catalog="TRAKT",results=[dict(title="The Office",year=2001,ids={"tmdb":"2996"},exact_title=True)])
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    url=f"/api/import/plex-recovery/{api.job.id}"
    context=api.client.get(url+"/match-groups",params=dict(revision=0)).json()
    labels=[c["label"] for c in context["catalogs"]]
    assert "TRAKT catalog" in labels and "TMDb metadata" in labels
    catalog=next(c["id"] for c in context["catalogs"] if c["label"]=="TRAKT catalog")
    for group in context["rows"]:
        result=api.client.get(url+"/match-search",params=dict(revision=0,group_id=group["id"],q="The Office",catalog=catalog))
        assert result.status_code==200,result.text
    assert len(calls)==1
    assert calls[0][0]["provider"]=="TRAKT" and calls[0][0]["item"]["type"]=="show"
    assert all(row["status"]=="missing_identity" for row in api.job.rows)
    monkeypatch.setattr(service,"user_can_access_instance",lambda cfg,user,provider,instance:provider!="TRAKT")
    result=api.client.get(url+"/match-search",params=dict(revision=0,group_id=context["rows"][0]["id"],q="The Office",catalog=catalog))
    assert result.status_code==403


def test_search_result_is_discarded_if_recovery_changes_during_lookup(unresolved_series,monkeypatch):
    from services import interactive_sync_mapping
    api=unresolved_series
    api.cfg["tmdb"]={"api_key":"metadata"}
    url=f"/api/import/plex-recovery/{api.job.id}"
    group=api.client.get(url+"/match-groups",params=dict(revision=0)).json()["rows"][0]
    def search(*a,**kw):
        api.job.revision+=1
        return dict(results=[])
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    result=api.client.get(url+"/match-search",params=dict(revision=0,group_id=group["id"],q="The Office",catalog="tmdb"))
    assert result.status_code==409
    assert not api.job.match_cache


def test_original_plex_groups_do_not_use_guessed_show_ids(plex,monkeypatch):
    def discover(*a,**kw):
        return {"type":"show","title":"The Office","Guid":[{"id":"tmdb://2316"}]}
    monkeypatch.setattr(common,"_discover_search_title",discover)
    plex.rows=[NS(type="episode",ratingKey=str(i),title="Episode",grandparentTitle="The Office",grandparentRatingKey=show,
                  parentIndex=season,index=1,viewedAt=1664057618+i,accountID=17)
               for i,(show,season) in enumerate([("uk",0),("uk",1),("us",1)])]
    result=scan(plex)
    assert result[0]["original_group"]==result[1]["original_group"]
    assert result[0]["original_group"]!=result[2]["original_group"]
    assert all(r["original_year"] is None for r in result)


def test_bulk_matching_without_catalogs_keeps_manual_matching_available(unresolved_series):
    api=unresolved_series
    api.cfg["plex"]["pms_token"]=api.cfg["plex"].pop("token")
    api.job.source_hash=service._source_hash(api.cfg,"default")
    url=f"/api/import/plex-recovery/{api.job.id}"
    context=api.client.get(url+"/match-groups",params=dict(revision=0)).json()
    assert context["catalogs"]==[]
    group=context["rows"][0]
    result=api.client.post(url+"/match-groups",json=dict(revision=0,edits=[dict(group_id=group["id"],title="Manual series",ids={"tvdb":"10"})]))
    assert result.status_code==200,result.text
    assert result.json()["matched_events"]==2


def test_bulk_matching_rechecks_existing_tracker_events(unresolved_series,monkeypatch):
    from services import importer
    api=unresolved_series
    api.job.raw[0]["recovery_valid"]=True
    api.job.raw[0]["recovery_requires_review"]=True
    api.job.raw[0]["item"]["show_ids"]={"tmdb":"10"}
    service._shape(api.job,api.cfg)
    existing_item=deepcopy(api.job.raw[0]["item"])
    existing_key=importer._key_for("history",existing_item)
    url=f"/api/import/plex-recovery/{api.job.id}"
    context=api.client.get(url+"/match-groups",params=dict(revision=0)).json()
    monkeypatch.setattr(importer,"_existing_keys",lambda *a:dict(history={existing_key:existing_item},ratings={},watchlist={}))
    result=api.client.post(url+"/match-groups",json=dict(revision=0,edits=[dict(group_id=context["rows"][0]["id"],title="Confirmed",ids={"tmdb":"10"})]))
    assert result.status_code==200,result.text
    assert api.job.rows[0]["status"]=="exists"
    assert api.job.rows[0]["default_included"] is False


@pytest.mark.parametrize("guessed", [False, True])
def test_movie_recovery_combines_dates_and_manual_match_keeps_them_combined(api, guessed):
    raw=deepcopy(api.job.raw[0])
    raw.update(original_title="Gravity",original_group="gravity",recovery_requires_review=guessed)
    raw["item"].update(title="Gravity",ids={"tmdb":"49047"})
    dates=["2022-01-17T16:00:13Z",None,"2022-01-18T08:00:16Z","2022-01-18T00:00:13Z"]
    api.job.raw=[dict(deepcopy(raw),item={**deepcopy(raw["item"]),"watched_at":date}) for date in dates]
    service._shape(api.job,api.cfg)
    url=f"/api/import/plex-recovery/{api.job.id}"
    preview=api.client.get(url+"/rows").json()
    assert preview["summary"]["total"]==1
    assert len(preview["rows"])==1
    row=preview["rows"][0]
    assert row["id"]=="2" and row["watched_at"]==dates[2]
    result=api.client.post(url+"/match",json=dict(revision=0,row_id=row["id"],title="Gravity",tmdb="49047"))
    assert result.status_code==200,result.text
    assert len(api.job.rows)==1
    assert all(not r["recovery_requires_review"] for r in api.job.raw)
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=1,mode="ready"))
    assert result.json()["applied"]==1
    from services import importer
    stored=importer._existing_keys(api.cfg,"default")["history"]
    assert len(stored)==1
    assert next(iter(stored.values()))["watched_at"]==dates[2]
    assert api.client.get(url+"/receipt").json()["keys"]==list(stored)


def test_bulk_matches_merge_different_sources_resolving_to_same_movie(api):
    for index, raw in enumerate(api.job.raw):
        raw.update(original_group=f"source-{index}",recovery_requires_review=True)
    service._shape(api.job,api.cfg)
    url=f"/api/import/plex-recovery/{api.job.id}"
    groups=api.client.get(url+"/match-groups",params=dict(revision=0)).json()["rows"]
    result=api.client.post(url+"/match-groups",json=dict(revision=0,edits=[dict(group_id=g["id"],title="One movie",ids={"tmdb":"10"}) for g in groups]))
    assert result.status_code==200,result.text
    assert len(api.job.rows)==1
    assert api.job.rows[0]["item"]["watched_at"]=="2020-01-03T12:00:00Z"
    assert api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=1,mode="ready")).json()["applied"]==1


def test_title_already_in_tracker_is_not_recovered_again_with_another_date(api):
    from services import importer
    url=f"/api/import/plex-recovery/{api.job.id}"
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=0,mode="selected",row_ids=["0"]))
    assert result.json()["applied"]==1
    api.job.raw[0]["item"]["watched_at"]="2025-01-01T00:00:00Z"
    service._shape(api.job,api.cfg)
    assert api.job.rows[0]["status"]=="exists"
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=1,mode="selected",row_ids=["0"]))
    assert result.json()["applied"]==0
    stored=importer._existing_keys(api.cfg,"default")["history"]
    assert len(stored)==1
    assert next(iter(stored.values()))["watched_at"]=="2020-01-01T12:00:00Z"


def test_explicit_guesses_for_same_title_import_only_latest_selected_date(api):
    from services import importer
    for index,raw in enumerate(api.job.raw):
        raw.update(original_group=f"source-{index}",recovery_requires_review=True)
        raw["item"]["ids"]={"tmdb":"10"}
    service._shape(api.job,api.cfg)
    assert len(api.job.rows)==3
    url=f"/api/import/plex-recovery/{api.job.id}"
    result=api.client.post(url+"/commit",json=dict(import_id=api.job.id,revision=0,mode="selected",row_ids=["0","1","2"]))
    assert result.status_code==200,result.text
    assert result.json()["applied"]==1
    stored=importer._existing_keys(api.cfg,"default")["history"]
    assert len(stored)==1
    assert next(iter(stored.values()))["watched_at"]=="2020-01-03T12:00:00Z"
    assert api.client.get(url+"/receipt").json()["keys"]==list(stored)


@pytest.mark.parametrize("state", ["reading", "review", "error", "cancelled"])
def test_only_one_recovery_session_can_be_open(api, state):
    api.job.status = state
    result = api.client.post("/api/import/plex-recovery", json={})
    assert result.status_code == 409
    assert len(service.JOBS) == 1
    assert api.client.get("/api/import/plex-recovery/active").json()["job"]["id"] == api.job.id


def test_resume_does_not_extend_expiry_or_expose_another_users_recovery(api, monkeypatch):
    url = "/api/import/plex-recovery/active"
    api.job.touched -= 30
    touched = api.job.touched
    assert api.client.get(url).json()["job"]["revision"] == api.job.revision
    assert api.job.touched == touched
    monkeypatch.setattr(service, "request_user", lambda _: {"id":"other", "is_admin":True})
    assert api.client.get(url).json() == {"job":None, "busy":True}
    assert api.client.post("/api/import/plex-recovery", json={}).status_code == 409
    assert api.client.delete(f"/api/import/plex-recovery/{api.job.id}").status_code == 404


def test_resume_respects_instance_access_and_expiry(api, monkeypatch):
    url = "/api/import/plex-recovery/active"
    monkeypatch.setattr(service, "user_can_access_instance", lambda *a: False)
    assert api.client.get(url).json() == {"job":None, "busy":True}
    monkeypatch.setattr(service, "user_can_access_instance", lambda *a: True)
    api.job.touched -= service.RECOVERY_TTL_SECONDS + 1
    assert api.client.get(url).json() == {"job":None, "busy":False}


def test_recovery_expires_after_24_idle_hours_and_review_activity_renews_it(api, monkeypatch):
    clock = NS(now=100_000)
    monkeypatch.setattr(service, "time", NS(time=lambda: clock.now))
    assert service.RECOVERY_TTL_SECONDS == 24 * 60 * 60
    assert service.importer.PREVIEW_TTL_SECONDS == 30 * 60
    api.job.touched = clock.now
    clock.now += 23 * 60 * 60
    active_url = "/api/import/plex-recovery/active"
    assert api.client.get(active_url).json()["job"]["id"] == api.job.id
    assert api.job.touched == 100_000
    assert api.client.get(f"/api/import/plex-recovery/{api.job.id}/rows").status_code == 200
    renewed = clock.now
    assert api.job.touched == renewed
    clock.now += 23 * 60 * 60
    assert api.client.get(active_url).json()["job"]["id"] == api.job.id
    assert api.job.touched == renewed
    clock.now += 60 * 60 + 1
    assert api.client.get(active_url).json() == {"job": None, "busy": False}


@pytest.mark.parametrize("running", ["scan", "auto_match"])
def test_running_recovery_work_does_not_expire(api, running):
    api.job.touched -= service.RECOVERY_TTL_SECONDS + 1
    if running == "scan":
        api.job.status = "reading"
    else:
        api.job.auto_thread = NS(is_alive=lambda: True)
    service._prune()
    assert api.job.id in service.JOBS


def test_close_recovery_discards_preview_before_expiry(api):
    url = f"/api/import/plex-recovery/{api.job.id}"
    before = deepcopy(service.importer._existing_keys(api.cfg, "default"))
    assert api.client.delete(url).status_code == 200
    assert api.job.id not in service.JOBS
    assert api.client.get("/api/import/plex-recovery/active").json() == {"job": None, "busy": False}
    assert api.client.get(url + "/rows").status_code == 404
    assert service.importer._existing_keys(api.cfg, "default") == before


def test_background_scan_can_be_resumed_from_another_client_and_imported(api, monkeypatch):
    from api import syncAPI
    from providers.sync import _mod_PLEX
    entered, release = threading.Event(), threading.Event()
    original = deepcopy(api.job.raw)
    assert api.client.delete(f"/api/import/plex-recovery/{api.job.id}").status_code == 200
    monkeypatch.setattr(_mod_PLEX, "PLEXModule", lambda _: NS(client=NS(server=None,session=None)))
    def read(*a, **kw):
        entered.set()
        assert release.wait(3)
        return original
    monkeypatch.setattr(recovery, "scan", read)
    response = api.client.post("/api/import/plex-recovery", json={})
    assert response.status_code == 200
    thread = syncAPI._rt()[1]["SYNC"]
    try:
        assert entered.wait(3)
        second = TestClient(api.client.app)
        current = second.get("/api/import/plex-recovery/active").json()["job"]
        assert current["id"] == response.json()["id"]
        assert current["status"] == "reading"
        assert second.post("/api/import/plex-recovery", json={}).status_code == 409
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive()
    current = second.get("/api/import/plex-recovery/active").json()["job"]
    assert current["status"] == "review"
    assert not service.importer._existing_keys(api.cfg,"default")["history"]
    url = f"/api/import/plex-recovery/{current['id']}"
    result = second.post(url+"/commit", json={"import_id":current["id"], "revision":current["revision"], "mode":"ready"})
    assert result.status_code == 200, result.text
    assert result.json()["applied"] == 3
    resumed = api.client.get("/api/import/plex-recovery/active").json()["job"]
    assert resumed["imported"] == 3
    assert len(api.client.get(url+"/receipt").json()["keys"]) == 3
    assert api.client.delete(url).status_code == 200
    assert api.client.get("/api/import/plex-recovery/active").json() == {"job":None,"busy":False}
    assert len(service.importer._existing_keys(api.cfg,"default")["history"]) == 3


@pytest.fixture
def auto_api(unresolved_series, monkeypatch):
    from services import interactive_sync_mapping
    api = unresolved_series
    api.cfg["tmdb"] = {"api_key":"test"}
    api.calls = []
    monkeypatch.setattr(service, "AUTO_MATCH_INTERVAL", 0)
    def search(cfg, route, title, **kw):
        api.calls.append((title, kw["catalog"]))
        return {"results":[{"title":"The Office", "year":2001, "ids":{"tmdb":"2996"}, "exact_title":True}]}
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", search)
    api.auto_url = f"/api/import/plex-recovery/{api.job.id}/auto-match"
    api.groups = [g["id"] for g in service._match_groups(api.job)]
    api.auto_payload = dict(revision=0, catalog="tmdb", group_ids=api.groups)
    yield api
    api.job.auto_stop.set()
    if api.job.auto_thread:
        api.job.auto_thread.join(3)


def test_background_auto_match_groups_shows_and_retains_only_reviewable_suggestions(auto_api):
    api = auto_api
    before = deepcopy(api.job.raw)
    result = api.client.post(api.auto_url, json=api.auto_payload)
    assert result.status_code == 200, result.text
    api.job.auto_thread.join(3)
    state = api.client.get(api.auto_url).json()
    assert state["auto_match"]["status"] == "complete"
    assert state["auto_match"]["done"] == 2
    assert state["auto_match"]["matched"] == 1
    assert len(api.calls) == 1
    assert state["suggestions"][api.groups[0]]["match"]["ids"] == {"tmdb":"2996"}
    assert state["suggestions"][api.groups[1]]["match"] is None
    assert api.job.raw == before
    assert api.job.revision == 0
    assert not service.importer._existing_keys(api.cfg,"default")["history"]
    second = TestClient(api.client.app)
    resumed = second.get(f"/api/import/plex-recovery/{api.job.id}/match-groups", params={"revision":0}).json()
    assert resumed["suggestions"] == state["suggestions"]
    assert second.post(api.auto_url, json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert len(api.calls) == 1
    assert api.job.auto["cached"] == 1
    assert api.job.auto["total"] == 1


def test_auto_match_retries_only_unresolved_groups_with_another_catalog(auto_api, monkeypatch):
    from services import interactive_sync_mapping
    api = auto_api
    assert api.client.post(api.auto_url, json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    api.cfg["trakt"] = {"client_id":"test", "access_token":"test"}
    catalogs = service._match_catalogs(api.cfg, None)
    catalog = next(key for key,value in catalogs.items() if value["route"]["provider"] == "TRAKT")
    def search(cfg, route, title, **kw):
        api.calls.append((title, route["provider"]))
        return {"results":[{"title":"The Office", "year":2005, "ids":{"tmdb":"2316"}, "exact_title":True}]}
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", search)
    assert api.client.post(api.auto_url, json={**api.auto_payload,"catalog":catalog}).status_code == 200
    api.job.auto_thread.join(3)
    assert len(api.calls) == 2
    assert api.job.auto["total"] == 1
    assert api.job.suggestions[api.groups[0]]["match"]["ids"] == {"tmdb":"2996"}
    assert api.job.suggestions[api.groups[1]]["match"]["ids"] == {"tmdb":"2316"}


def test_auto_match_stop_blocks_parallel_work_and_resumes_without_repeating_completed_titles(auto_api, monkeypatch):
    from services import interactive_sync_mapping
    api = auto_api
    api.job.raw[-1]["original_title"] = "Different Show"
    service._shape(api.job,api.cfg)
    entered, release = threading.Event(), threading.Event()
    def search(cfg, route, title, **kw):
        api.calls.append(title)
        if len(api.calls) == 2:
            entered.set()
            assert release.wait(3)
        return {"results":[{"title":title, "year":2001, "ids":{"tmdb":"2996"}, "exact_title":True}]}
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", search)
    assert api.client.post(api.auto_url, json=api.auto_payload).status_code == 200
    assert entered.wait(3)
    try:
        assert api.client.post(api.auto_url, json=api.auto_payload).status_code == 409
        assert api.client.post(f"/api/import/plex-recovery/{api.job.id}/commit", json={"revision":0,"import_id":api.job.id,"mode":"ready"}).status_code == 409
        edit={"revision":0,"edits":[{"group_id":api.groups[0],"title":"Office","ids":{"tmdb":"2996"}}]}
        assert api.client.post(f"/api/import/plex-recovery/{api.job.id}/match-groups",json=edit).status_code == 409
        assert api.client.delete(api.auto_url).status_code == 200
    finally:
        release.set()
        api.job.auto_thread.join(3)
    assert api.job.auto["status"] == "stopped"
    assert api.job.auto["done"] == 1
    assert api.job.suggestions[api.groups[0]]["match"]
    assert api.client.post(api.auto_url, json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert api.calls == ["The Office","Different Show","Different Show"]
    assert api.job.auto["status"] == "complete"


def test_auto_match_honors_retry_after_and_does_not_cache_rate_limit_errors(auto_api, monkeypatch):
    from services import interactive_sync_mapping
    from fastapi import HTTPException
    api = auto_api
    def search(*a, **kw):
        api.calls.append(1)
        if len(api.calls) == 1:
            raise HTTPException(429,"Rate limited",headers={"Retry-After":"12"})
        return {"results":[]}
    waits=[]
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    monkeypatch.setattr(api.job.auto_stop,"wait",lambda delay: waits.append(delay) or False)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert any(11 < delay <= 12 for delay in waits)
    assert api.job.auto["status"] == "complete"
    assert len(api.calls) == 2


@pytest.mark.parametrize("close_recovery",[False,True])
def test_auto_match_discards_inflight_result_after_settings_change_or_close(auto_api,monkeypatch,close_recovery):
    from services import interactive_sync_mapping
    api=auto_api
    entered,release=threading.Event(),threading.Event()
    def search(*a,**kw):
        entered.set()
        assert release.wait(3)
        return {"results":[{"title":"The Office","year":2001,"ids":{"tmdb":"2996"},"exact_title":True}]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    assert entered.wait(3)
    try:
        if close_recovery:
            assert api.client.delete(f"/api/import/plex-recovery/{api.job.id}").status_code == 200
            assert api.client.post("/api/import/plex-recovery",json={}).status_code == 409
        else:
            api.cfg["tmdb"]["api_key"]="changed"
    finally:
        release.set()
        api.job.auto_thread.join(3)
    assert not api.job.suggestions
    assert not api.job.match_cache
    assert (api.job.id in service.JOBS) is not close_recovery
    if not close_recovery:
        assert api.job.auto["status"] == "paused"
        assert "settings changed" in api.job.auto["message"].lower()


def test_auto_match_rejects_foreign_sessions_bad_groups_and_unavailable_catalogs(auto_api,monkeypatch):
    api=auto_api
    assert api.client.post(api.auto_url,json={**api.auto_payload,"catalog":"missing"}).status_code == 403
    assert api.client.post(api.auto_url,json={**api.auto_payload,"group_ids":["missing"]}).status_code == 409
    assert api.client.post(api.auto_url,json={**api.auto_payload,"revision":99}).status_code == 409
    monkeypatch.setattr(service,"request_user",lambda _: {"id":"other","is_admin":True})
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 404
    assert api.client.get(api.auto_url).status_code == 404
    assert api.client.delete(api.auto_url).status_code == 404
    assert not api.calls



def test_recovery_language_retries_unresolved_titles_and_shares_only_same_language_cache(auto_api, monkeypatch):
    from services import interactive_sync_mapping
    api = auto_api
    calls = []
    def search(cfg, route, title, **kwargs):
        calls.append((title, route["metadata_language"]))
        return {"results": []}
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", search)
    assert api.client.post(api.auto_url, json={**api.auto_payload, "language": "en-US"}).status_code == 200
    api.job.auto_thread.join(3)
    assert len(calls) == 1
    assert api.client.post(api.auto_url, json={**api.auto_payload, "language": "nl-NL"}).status_code == 200
    api.job.auto_thread.join(3)
    assert len(calls) == 2
    assert [c[1] for c in calls] == ["en-US", "nl-NL"]
    groups = service._match_groups(api.job)
    url = f"/api/import/plex-recovery/{api.job.id}/match-search"
    params = dict(revision=0, catalog="tmdb", group_id=groups[0]["id"], q=groups[0]["item"]["title"])
    assert api.client.get(url, params={**params, "language": "nl-NL"}).status_code == 200
    assert len(calls) == 2
    assert api.client.get(url, params={**params, "language": "de-DE"}).status_code == 200
    assert len(calls) == 3
    assert api.client.post(api.auto_url, json={**api.auto_payload, "language": "nl-NL"}).status_code == 200
    api.job.auto_thread.join(3)
    assert len(calls) == 3
    context = api.client.get(f"/api/import/plex-recovery/{api.job.id}/match-groups", params={"revision": 0}).json()
    assert context["auto_match"]["language"] == "nl-NL"
    assert next(c for c in context["catalogs"] if c["id"] == "tmdb")["supports_language"] is True
    assert all(not c["supports_language"] for c in context["catalogs"] if c["id"] != "tmdb")
    assert api.client.post(api.auto_url, json={**api.auto_payload, "language": "invalid"}).status_code == 422
    assert api.client.get(url, params={**params, "language": "invalid"}).status_code == 422


def test_auto_match_movies_keep_ambiguous_and_invalid_suggestions_unresolved(api, monkeypatch):
    from services import interactive_sync_mapping
    api.cfg["tmdb"]={"api_key":"test"}
    for raw in api.job.raw:
        raw["recovery_valid"]=False
    service._shape(api.job,api.cfg)
    calls=[]
    def search(cfg,row,title,**kw):
        calls.append(title)
        candidate={"title":title,"exact_title":True,"ids":{"tmdb":"12"}}
        if title == "Old 1":
            return {"results":[candidate,{**candidate,"ids":{"tmdb":"13"}}]}
        if title == "Old 2":
            candidate["ids"]={"tmdb":"invalid"}
        return {"results":[candidate]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    monkeypatch.setattr(service,"AUTO_MATCH_INTERVAL",0)
    groups=service._match_groups(api.job)
    payload={"revision":0,"catalog":"tmdb","group_ids":[g["id"] for g in groups]}
    url=f"/api/import/plex-recovery/{api.job.id}/auto-match"
    assert api.client.post(url,json=payload).status_code == 200
    api.job.auto_thread.join(3)
    assert len(calls) == 3
    assert api.job.auto["matched"] == 1
    assert all(row["status"] == "missing_identity" for row in api.job.rows)
    assert api.client.post(url,json=payload).status_code == 200
    api.job.auto_thread.join(3)
    assert len(calls) == 3
    assert api.job.auto["cached"] == 2


def test_auto_match_long_rate_limit_remains_in_effect_when_resumed(auto_api, monkeypatch):
    from fastapi import HTTPException
    from services import interactive_sync_mapping
    api=auto_api
    def search(*a,**kw):
        api.calls.append(1)
        raise HTTPException(429,"Rate limited",headers={"Retry-After":"3600"})
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert api.job.auto["status"] == "paused"
    assert not api.job.auto_attempts
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert len(api.calls) == 1
    assert api.job.auto["status"] == "paused"
    assert "Retry in" in api.job.auto["message"]



def test_auto_match_ignores_unrelated_config_writes_and_keeps_shared_cache(auto_api, monkeypatch):
    from services import interactive_sync_mapping
    api=auto_api
    def search(cfg,route,title,**kw):
        api.calls.append(title)
        api.cfg["auth"]={"sessions":["new-login"],"trakt":{"access_token":"refreshed"}}
        api.cfg["simkl"]={"access_token":"new-token","client_id":"test"}
        api.cfg["ui"]={"theme":"flat-light"}
        return {"results":[]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert api.job.auto["status"] == "complete"
    assert api.job.auto["done"] == 2
    assert len(api.calls) == 1
    api.cfg["auth"]["sessions"]=[]
    api.cfg["simkl"]["access_token"]="another-token"
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    assert api.job.auto["cached"] == 2
    result=api.client.get(f"/api/import/plex-recovery/{api.job.id}/match-search",params={
        "revision":0,"catalog":"tmdb","group_id":api.groups[0],"q":"The Office"})
    assert result.status_code == 200
    assert result.json()["results"] == []
    assert len(api.calls) == 1


def test_selected_catalog_changes_invalidate_manual_cache_but_other_instances_do_not(auto_api,monkeypatch):
    from services import interactive_sync_mapping
    api=auto_api
    api.cfg["trakt"]={"instances":{"first":{"client_id":"first","access_token":"token"},
                                    "second":{"client_id":"second","access_token":"other"}}}
    choices=service._match_catalogs(api.cfg,None)
    catalog=next(key for key,value in choices.items() if value["route"]=={"provider":"TRAKT","instance":"first"})
    def search(cfg,row,q,**kw):
        api.calls.append(q)
        api.cfg["auth"]={"session":"login"}
        return {"results":[]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    url=f"/api/import/plex-recovery/{api.job.id}/match-search"
    params={"revision":0,"catalog":catalog,"group_id":api.groups[0],"q":"The Office"}
    assert api.client.get(url,params=params).status_code == 200
    api.cfg["trakt"]["instances"]["second"]["access_token"]="refreshed"
    api.cfg["tmdb"]["api_key"]="unused-key"
    assert api.client.get(url,params=params).status_code == 200
    assert len(api.calls) == 1
    api.cfg["trakt"]["instances"]["first"]["client_id"]="changed"
    assert api.client.get(url,params=params).status_code == 200
    assert len(api.calls) == 2


@pytest.mark.parametrize("ids",[{"trakt":"123"},{"simkl":"456"},{"tmdb":"invalid"}])
def test_auto_match_cache_keeps_missing_identity_warning_in_manual_search(auto_api,monkeypatch,ids):
    from services import interactive_sync_mapping
    api=auto_api
    def search(*a,**kw):
        api.calls.append(1)
        return {"results":[{"title":"The Office","year":2001,"exact_title":True,"ids":ids}]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    api.job.auto_thread.join(3)
    result=api.client.get(f"/api/import/plex-recovery/{api.job.id}/match-search",params={
        "revision":0,"catalog":"tmdb","group_id":api.groups[0],"q":"The Office"})
    assert result.status_code == 200
    assert "TMDb, TVDB or IMDb" in result.json()["results"][0]["mapping_unavailable"]
    assert len(api.calls) == 1
    assert api.job.auto["matched"] == 0


def test_closed_auto_match_is_hidden_until_worker_releases_single_recovery_slot(auto_api,monkeypatch):
    from services import interactive_sync_mapping
    api=auto_api
    entered,release=threading.Event(),threading.Event()
    def search(*a,**kw):
        entered.set()
        assert release.wait(3)
        return {"results":[]}
    monkeypatch.setattr(interactive_sync_mapping,"search_candidates",search)
    assert api.client.post(api.auto_url,json=api.auto_payload).status_code == 200
    assert entered.wait(3)
    try:
        assert api.client.delete(f"/api/import/plex-recovery/{api.job.id}").status_code == 200
        assert api.client.get("/api/import/plex-recovery/active").json() == {"job":None,"busy":True,"stopping":True}
        result=api.client.post("/api/import/plex-recovery",json={})
        assert result.status_code == 409
        assert "still stopping" in result.json()["detail"]
    finally:
        release.set()
        api.job.auto_thread.join(3)
    assert api.client.get("/api/import/plex-recovery/active").json() == {"job":None,"busy":False}
    assert not api.job.suggestions
    assert not service.importer._existing_keys(api.cfg,"default")["history"]
