#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, time::Duration};
use tauri::{Manager, AppHandle};
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
  if let Some(pd) = ProjectDirs::from("app","plex-simkl","desktop") {
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
  if !p.exists() {
    Ok(Config::default())
  } else {
    let s = fs::read_to_string(p)?;
    Ok(serde_json::from_str(&s)?)
  }
}

fn write_cfg(cfg: &Config) -> Result<()> {
  let p = config_path();
  if let Some(dir) = p.parent() { fs::create_dir_all(dir)?; }
  let s = serde_json::to_string_pretty(cfg)?;
  fs::write(p, s)?;
  Ok(())
}

#[tauri::command]
async fn cmd_read_config() -> Result<Config, String> {
  read_cfg().map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_write_config(cfg: Config) -> Result<(), String> {
  write_cfg(&cfg).map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_plex_pin_flow(app: AppHandle) -> Result<(), String> {
  #[derive(Deserialize)] struct PinResp { id: i64 }
  #[derive(Deserialize)] struct PollResp { auth_token: Option<String> }

  let client = reqwest::Client::new();
  let create = client.post("https://plex.tv/pins.json")
    .header("X-Plex-Product", "PlexSIMKLDesktop")
    .header("X-Plex-Version", "0.2.0")
    .header("X-Plex-Client-Identifier", "plex-simkl-desktop")
    .form(&[("strong", "true")])
    .send().await.map_err(|e| e.to_string())?;
  let pin: PinResp = create.json().await.map_err(|e| e.to_string())?;

  let _ = app.opener().open_url("https://plex.tv/link");

  let mut token: Option<String> = None;
  for _ in 0..60 {
    let poll = client.get(format!("https://plex.tv/pins/{id}.json", id=pin.id))
      .header("X-Plex-Product", "PlexSIMKLDesktop")
      .header("X-Plex-Version", "0.2.0")
      .header("X-Plex-Client-Identifier", "plex-simkl-desktop")
      .send().await.map_err(|e| e.to_string())?;
    let pr: PollResp = poll.json().await.map_err(|e| e.to_string())?;
    if let Some(t) = pr.auth_token { token = Some(t); break; }
    tokio::time::sleep(Duration::from_millis(1500)).await;
  }

  let t = token.ok_or_else(|| "Plex token not granted (timeout)".to_string())?;
  let mut cfg = read_cfg().map_err(|e| e.to_string())?;
  let mut plex = cfg.plex.unwrap_or_default();
  plex.account_token = Some(t);
  cfg.plex = Some(plex);
  write_cfg(&cfg).map_err(|e| e.to_string())?;
  Ok(())
}

#[tauri::command]
async fn cmd_simkl_oauth(app: AppHandle) -> Result<(), String> {
  let cfg = read_cfg().map_err(|e| e.to_string())?;
  let simkl = cfg.simkl.clone().ok_or_else(|| "Add simkl.client_id/client_secret in settings and Save first.".to_string())?;
  let client = reqwest::Client::new();

  let redirect = "http://127.0.0.1:8787/callback";
  let auth_url = format!("https://api.simkl.com/oauth/authorize?response_type=code&client_id={}&redirect_uri={}", simkl.client_id, urlencoding::encode(redirect).into_owned());
  let _ = app.opener().open_url(&auth_url);

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
  let sh = app.shell();
  let mut cmd = sh.command("python");
  cmd.args(["resources/python/plex_simkl_watchlist_sync.py", "--sync"]);
  let status = cmd.status().map_err(|e| e.to_string())?;
  if !status.success() {
    return Err(format!("Python exited with code {:?}", status.code()));
  }
  Ok(())
}

fn main() {
  tauri::Builder::default()
    .plugin(tauri_plugin_opener::init())
    .plugin(tauri_plugin_shell::init())
    .invoke_handler(tauri::generate_handler![
      cmd_read_config, cmd_write_config,
      cmd_plex_pin_flow, cmd_simkl_oauth, cmd_run_sync
    ])
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
