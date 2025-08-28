#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, time::Duration};
use tauri::AppHandle;
use tauri_plugin_opener::OpenerExt;
use tauri_plugin_shell::ShellExt;
use directories::ProjectDirs;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct PlexCfg { account_token: Option<String> }

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct SimklCfg {
  client_id: String,
  client_secret: String,
  access_token: Option<String>,
  refresh_token: Option<String>,
  token_expires_at: Option<i64>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Config {
  plex: Option<PlexCfg>,
  simkl: Option<SimklCfg>,
  #[serde(default)] sync: Option<serde_json::Value>,
  #[serde(default)] runtime: Option<serde_json::Value>,
  #[serde(flatten)] rest: serde_json::Value
}

fn config_dir() -> PathBuf {
  if let Some(pd) = ProjectDirs::from("app","cenodude","crosswatch") {
    return pd.config_dir().to_path_buf();
  }
  PathBuf::from("./config")
}

fn config_path() -> PathBuf {
  let mut p = config_dir();
  fs::create_dir_all(&p).ok();
  p.push("config.json");
  p
}

fn read_cfg() -> Result<Config> {
  let p = config_path();
  if !p.exists() { Ok(Config::default()) }
  else { Ok(serde_json::from_str(&fs::read_to_string(p)?)?) }
}

fn write_cfg(cfg: &Config) -> Result<()> {
  let p = config_path();
  if let Some(dir) = p.parent() { fs::create_dir_all(dir)?; }
  fs::write(p, serde_json::to_string_pretty(cfg)?)?;
  Ok(())
}

#[tauri::command] async fn cmd_read_config() -> Result<Config, String> { read_cfg().map_err(|e| e.to_string()) }
#[tauri::command] async fn cmd_write_config(cfg: Config) -> Result<(), String> { write_cfg(&cfg).map_err(|e| e.to_string()) }

#[tauri::command]
async fn cmd_open_url(app: AppHandle, url: String) -> Result<(), String> {
  app.opener().open_url(url, None::<String>).map_err(|e| e.to_string())
}

#[derive(Serialize)]
struct PinCreateOut { id: i64, code: String, expires_at: i64 }

fn plex_headers(cid: &str) -> reqwest::header::HeaderMap {
  use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
  let mut h = HeaderMap::new();
  h.insert(ACCEPT, HeaderValue::from_static("application/json"));
  h.insert(USER_AGENT, HeaderValue::from_static("CrossWatch/0.3.0"));
  h.insert("X-Plex-Product", HeaderValue::from_static("CrossWatch"));
  h.insert("X-Plex-Version", HeaderValue::from_static("0.3.0"));
  h.insert("X-Plex-Client-Identifier", HeaderValue::from_str(cid).unwrap());
  h.insert("X-Plex-Device", HeaderValue::from_static("Desktop"));
  h.insert("X-Plex-Platform", HeaderValue::from_static("Rust"));
  h
}

fn iso_to_epoch(iso: &str) -> Option<i64> {
  // Accepts e.g. "2025-08-27T23:00:30Z"
  chrono::DateTime::parse_from_rfc3339(iso)
    .ok()
    .map(|dt| dt.with_timezone(&chrono::Utc).timestamp())
}

#[tauri::command]
async fn cmd_plex_create_pin() -> Result<PinCreateOut, String> {
  // Mirror plex_token_helper.py: /pins.json with strong=true
  use rand::{distributions::Alphanumeric, Rng};
  let cid: String = rand::thread_rng().sample_iter(&Alphanumeric).take(10).map(char::from).collect();
  let cid = format!("crosswatch-{}", cid.to_lowercase());

  let client = reqwest::Client::new();
  let res = client.post("https://plex.tv/pins.json")
    .headers(plex_headers(&cid))
    .form(&[("strong","true")])
    .send().await.map_err(|e| e.to_string())?;
  if !res.status().is_success() { return Err(format!("Create PIN failed: {}", res.status())); }

  let js: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;
  let pin = js.get("pin").cloned().unwrap_or(js);
  let id = pin.get("id").and_then(|v| v.as_i64()).ok_or("missing pin.id")?;
  let code = pin.get("code").and_then(|v| v.as_str()).ok_or("missing pin.code")?.to_string();
  let exp_iso = pin.get("expires_at").and_then(|v| v.as_str()).ok_or("missing pin.expires_at")?;
  let exp = iso_to_epoch(exp_iso).ok_or("bad expires_at")?;
  Ok(PinCreateOut { id, code, expires_at: exp })
}

#[tauri::command]
async fn cmd_plex_poll_pin(id: i64) -> Result<String, String> {
  let client = reqwest::Client::new();
  loop {
    let res = client.get(format!("https://plex.tv/pins/{id}.json"))
      .headers(plex_headers("crosswatch"))
      .send().await.map_err(|e| e.to_string())?;
    if !res.status().is_success() { return Err(format!("Poll failed: {}", res.status())); }
    let js: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;
    let pin = js.get("pin").cloned().unwrap_or(js);
    // try both styles
    if let Some(t) = pin.get("auth_token").and_then(|v| v.as_str()) {
      // save
      let mut cfg = read_cfg().map_err(|e| e.to_string())?;
      let mut plex = cfg.plex.unwrap_or_default();
      plex.account_token = Some(t.to_string());
      cfg.plex = Some(plex);
      write_cfg(&cfg).map_err(|e| e.to_string())?;
      return Ok(t.to_string());
    }
    if let Some(t) = pin.get("authToken").and_then(|v| v.as_str()) {
      let mut cfg = read_cfg().map_err(|e| e.to_string())?;
      let mut plex = cfg.plex.unwrap_or_default();
      plex.account_token = Some(t.to_string());
      cfg.plex = Some(plex);
      write_cfg(&cfg).map_err(|e| e.to_string())?;
      return Ok(t.to_string());
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
  }
}

#[tauri::command]
async fn cmd_simkl_oauth(app: AppHandle) -> Result<(), String> {
  let cfg = read_cfg().map_err(|e| e.to_string())?;
  let simkl = cfg.simkl.clone().ok_or_else(|| "Add simkl.client_id/client_secret in settings and Save first.".to_string())?;
  let client = reqwest::Client::new();

  let redirect = "http://127.0.0.1:8787/callback";
  let auth_url = format!("https://api.simkl.com/oauth/authorize?response_type=code&client_id={}&redirect_uri={}", simkl.client_id, urlencoding::encode(redirect).into_owned());
  let _ = app.opener().open_url(&auth_url, None::<String>);

  let server = tiny_http::Server::http("127.0.0.1:8787").map_err(|e| e.to_string())?;
  let code: String = loop {
    if let Ok(Some(req)) = server.try_recv() {
      let url = req.url().to_string();
      if url.starts_with("/callback") {
        let query = url.split('?').nth(1).unwrap_or("");
        let params: std::collections::HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();
        if let Some(c) = params.get("code") {
          let _ = req.respond(tiny_http::Response::from_string("SIMKL linked. You can close this window."));
          break c.clone();
        }
      }
      let _ = req.respond(tiny_http::Response::from_string("OK"));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  };

  #[derive(Deserialize)] struct TokResp { access_token: String, refresh_token: String, expires_in: i64 }
  let token_resp = client.post("https://api.simkl.com/oauth/token")
    .header("Content-Type", "application/json")
    .json(&serde_json::json!({
      "client_id": simkl.client_id,
      "client_secret": simkl.client_secret,
      "grant_type": "authorization_code",
      "redirect_uri": redirect,
      "code": code
    }))
    .send().await.map_err(|e| e.to_string())?;
  if !token_resp.status().is_success() { return Err(format!("SIMKL token exchange failed: {}", token_resp.status())); }
  let tr: TokResp = token_resp.json().await.map_err(|e| e.to_string())?;

  let mut cfg2 = read_cfg().map_err(|e| e.to_string())?;
  let mut s = cfg2.simkl.unwrap_or_default();
  s.access_token = Some(tr.access_token);
  s.refresh_token = Some(tr.refresh_token);
  s.token_expires_at = Some( (chrono::Utc::now().timestamp()) + tr.expires_in );
  cfg2.simkl = Some(s);
  write_cfg(&cfg2).map_err(|e| e.to_string())?;
  Ok(())
}

#[tauri::command]
async fn cmd_run_sync(app: AppHandle) -> Result<(), String> {
  let status = app.shell()
    .command("python")
    .args(["resources/python/plex_simkl_watchlist_sync.py", "--sync"])
    .status().await.map_err(|e| e.to_string())?;
  if !status.success() { return Err(format!("Python exited with code {:?}", status.code())); }
  Ok(())
}

fn main() {
  tauri::Builder::default()
    .plugin(tauri_plugin_opener::init())
    .plugin(tauri_plugin_shell::init())
    .invoke_handler(tauri::generate_handler![
      cmd_read_config, cmd_write_config,
      cmd_open_url,
      cmd_plex_create_pin, cmd_plex_poll_pin,
      cmd_simkl_oauth, cmd_run_sync
    ])
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
