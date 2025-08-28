#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{fs, path::{Path, PathBuf}};

use tauri::{AppHandle, Emitter};
use serde::{Serialize, Deserialize};
use tauri_plugin_shell::ShellExt;
use tauri_plugin_opener::OpenerExt;
use reqwest::header::{HeaderMap, HeaderValue};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PlexCfg {
  #[serde(default)] account_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SimklCfg {
  #[serde(default)] client_id: String,
  #[serde(default)] client_secret: String,
  #[serde(default)] access_token: Option<String>,
  #[serde(default)] refresh_token: Option<String>,
  #[serde(default)] token_expires_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BidirectionalCfg {
  #[serde(default)] enabled: bool,
  #[serde(default)] mode: String,
  #[serde(default)] source_of_truth: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ActivityCfg {
  #[serde(default)] use_activity: bool,
  #[serde(default)] types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SyncCfg {
  #[serde(default)] enable_add: bool,
  #[serde(default)] enable_remove: bool,
  #[serde(default)] bidirectional: BidirectionalCfg,
  #[serde(default)] activity: ActivityCfg,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeCfg {
  #[serde(default)] debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Config {
  #[serde(default)] plex: Option<PlexCfg>,
  #[serde(default)] simkl: Option<SimklCfg>,
  #[serde(default)] sync: Option<SyncCfg>,
  #[serde(default)] runtime: Option<RuntimeCfg>,
}

// Convenient alias for Send+Sync errors so spawned futures are Send
type AnyErr = Box<dyn std::error::Error + Send + Sync>;

// ---------------- Config locations ----------------

fn user_config_dir() -> PathBuf {
  #[cfg(target_os = "windows")]
  {
    if let Ok(p) = std::env::var("APPDATA") {
      return PathBuf::from(p).join("CrossWatch");
    }
  }
  #[cfg(target_os = "macos")]
  {
    if let Ok(home) = std::env::var("HOME") {
      return PathBuf::from(home).join("Library").join("Application Support").join("CrossWatch");
    }
  }
  #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
  {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
      return PathBuf::from(xdg).join("CrossWatch");
    }
    if let Ok(home) = std::env::var("HOME") {
      return PathBuf::from(home).join(".config").join("CrossWatch");
    }
  }
  PathBuf::from("config")
}

fn project_root_cfg() -> PathBuf {
  PathBuf::from("config.json")
}

fn legacy_cfg_candidates() -> Vec<PathBuf> {
  vec![
    PathBuf::from("config.json"),
    PathBuf::from("resources/config.json"),
    PathBuf::from("src-tauri/resources/config.json"),
  ]
}

fn cfg_path() -> PathBuf {
  let base = user_config_dir();
  let _ = fs::create_dir_all(&base);
  base.join("config.json")
}

fn read_cfg() -> Result<Config, AnyErr> {
  let p = cfg_path();
  if p.exists() {
    let s = fs::read_to_string(p)?;
    let cfg: Config = serde_json::from_str(&s)?;
    return Ok(cfg);
  }
  for cand in legacy_cfg_candidates() {
    if cand.exists() {
      let s = fs::read_to_string(&cand)?;
      let cfg: Config = serde_json::from_str(&s)?;
      let newp = cfg_path();
      if let Some(dir) = newp.parent() { let _ = fs::create_dir_all(dir); }
      fs::write(&newp, &s)?;
      let _ = fs::write(project_root_cfg(), &s);
      if cand.to_string_lossy().contains("src-tauri") {
        let _ = fs::remove_file(&cand);
      }
      return Ok(cfg);
    }
  }
  Ok(Config::default())
}

fn write_cfg(cfg: &Config) -> Result<(), AnyErr> {
  let p = cfg_path();
  if let Some(dir) = p.parent() { let _ = fs::create_dir_all(dir); }
  let s = serde_json::to_string_pretty(cfg)?;
  fs::write(&p, &s)?;
  let _ = fs::write(project_root_cfg(), &s);
  for cand in legacy_cfg_candidates() {
    if cand.to_string_lossy().contains("src-tauri") && cand.exists() {
      let _ = fs::remove_file(cand);
    }
  }
  Ok(())
}

// ---------------- Utilities ----------------

#[tauri::command]
async fn cmd_read_config() -> Result<Config, String> {
  read_cfg().map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_write_config(cfg: Config) -> Result<(), String> {
  write_cfg(&cfg).map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_open_url(app: AppHandle, url: String) -> Result<(), String> {
  app.opener().open_url(url, None::<String>).map_err(|e| e.to_string())
}

#[tauri::command]
async fn cmd_open_external_sized(app: AppHandle, url: String, width: Option<u32>, height: Option<u32>) -> Result<(), String> {
  let w = width.unwrap_or(520);
  let h = height.unwrap_or(720);

  // Edge
  let edge_candidates = [
    std::env::var("PROGRAMFILES(X86)").ok().map(|p| format!(r"{}\Microsoft\Edge\Application\msedge.exe", p)),
    std::env::var("PROGRAMFILES").ok().map(|p| format!(r"{}\Microsoft\Edge\Application\msedge.exe", p)),
  ];
  let args_edge = vec!["--new-window".into(), format!("--window-size={},{}", w, h), url.clone()];
  for p in edge_candidates.into_iter().flatten() {
    if Path::new(&p).exists() {
      AppHandle::shell(&app).command(p).args(args_edge.clone()).spawn().map_err(|e| e.to_string())?;
      return Ok(());
    }
  }

  // Chrome
  let chrome_candidates = [
    std::env::var("PROGRAMFILES(X86)").ok().map(|p| format!(r"{}\Google\Chrome\Application\chrome.exe", p)),
    std::env::var("PROGRAMFILES").ok().map(|p| format!(r"{}\Google\Chrome\Application\chrome.exe", p)),
  ];
  let args_chrome = vec!["--new-window".into(), format!("--window-size={},{}", w, h), url.clone()];
  for p in chrome_candidates.into_iter().flatten() {
    if Path::new(&p).exists() {
      AppHandle::shell(&app).command(p).args(args_chrome.clone()).spawn().map_err(|e| e.to_string())?;
      return Ok(());
    }
  }

  // Fallback
  app.opener().open_url(url, None::<String>).map_err(|e| e.to_string())
}

// ---------- Plex PIN helpers ----------

fn plex_headers(client_id: &str) -> HeaderMap {
  let mut h = HeaderMap::new();
  h.insert("Accept", HeaderValue::from_static("application/json"));
  h.insert("User-Agent", HeaderValue::from_static("CrossWatch/0.3.0 (+https://example.local)"));
  h.insert("X-Plex-Product", HeaderValue::from_static("CrossWatch"));
  h.insert("X-Plex-Version", HeaderValue::from_static("0.3.0"));
  h.insert("X-Plex-Client-Identifier", HeaderValue::from_str(client_id).unwrap_or(HeaderValue::from_static("crosswatch")));
  h.insert("X-Plex-Device", HeaderValue::from_static("Desktop"));
  h.insert("X-Plex-Device-Name", HeaderValue::from_static("CrossWatch"));
  h.insert("X-Plex-Platform", HeaderValue::from_static("Tauri"));
  h
}

fn unwrap_pin(v: &serde_json::Value) -> serde_json::Value {
  if let Some(pin) = v.get("pin") { pin.clone() } else { v.clone() }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct PlexPinCreateOut {
  id: i64,
  code: String,
  expires_at: i64, // epoch
  client_id: String,
}

#[tauri::command]
async fn cmd_plex_create_pin() -> Result<PlexPinCreateOut, String> {
  use rand::{distributions::Alphanumeric, Rng};
  let client_id: String = rand::thread_rng().sample_iter(&Alphanumeric).take(12).map(char::from).collect();
  let client = reqwest::Client::new();
  let resp = client.post("https://plex.tv/pins.json")
    .headers(plex_headers(&client_id))
    .send().await.map_err(|e| e.to_string())?;
  if !resp.status().is_success() { return Err(format!("Plex PIN create failed: {}", resp.status())); }
  let raw: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
  let pin = unwrap_pin(&raw);
  let id = pin.get("id").and_then(|x| x.as_i64()).ok_or("Missing id")?;
  let code = pin.get("code").and_then(|x| x.as_str()).unwrap_or("").to_string();
  let exp_iso = pin.get("expires_at").and_then(|x| x.as_str()).unwrap_or("");
  let exp_epoch = chrono::DateTime::parse_from_rfc3339(exp_iso).map(|dt| dt.timestamp()).unwrap_or(0);
  Ok(PlexPinCreateOut{ id, code, expires_at: exp_epoch, client_id })
}

#[allow(non_snake_case)]
#[tauri::command]
async fn cmd_plex_poll_pin(id: i64, clientId: String) -> Result<String, String> {
  let client = reqwest::Client::new();
  let resp = client.get(&format!("https://plex.tv/pins/{}.json", id))
    .headers(plex_headers(&clientId))
    .send().await.map_err(|e| e.to_string())?;
  if !resp.status().is_success() { return Err(format!("Plex PIN poll failed: {}", resp.status())); }
  let raw: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
  let pin = unwrap_pin(&raw);
  let token = pin.get("auth_token").and_then(|x| x.as_str())
                .or_else(|| pin.get("authToken").and_then(|x| x.as_str()))
                .unwrap_or("");
  Ok(token.to_string())
}

// ---------- SIMKL OAuth (loopback via background task) ----------

#[tauri::command]
async fn cmd_simkl_start_listener(app: AppHandle) -> Result<(), String> {
  use tiny_http::{Server, Response, StatusCode};

  let server = Server::http("127.0.0.1:8787").map_err(|e| format!("Bind 127.0.0.1:8787 failed: {e}"))?;
  tauri::async_runtime::spawn(async move {
    if let Ok(rq) = server.recv() {
      let path_q = rq.url().to_string();
      let parsed = url::Url::parse(&format!("http://localhost{}", path_q));
      let mut ok = false;
      let mut err: Option<String> = None;

      if let Ok(u) = parsed {
        let code = u.query_pairs().find(|(k, _)| k == "code").map(|(_, v)| v.to_string());
        if let Some(code) = code {
          let cfg = read_cfg();
          match cfg {
            Ok(cfg) => {
              if let Some(sim) = cfg.simkl {
                if !sim.client_id.is_empty() && !sim.client_secret.is_empty() {
                  let client = reqwest::Client::new();
                  let token_resp = client.post("https://api.simkl.com/oauth/token")
                    .header("Content-Type", "application/json")
                    .json(&serde_json::json!({
                      "client_id": sim.client_id,
                      "client_secret": sim.client_secret,
                      "grant_type": "authorization_code",
                      "redirect_uri": "http://127.0.0.1:8787/callback",
                      "code": code
                    }))
                    .send().await;

                  if let Ok(resp) = token_resp {
                    if resp.status().is_success() {
                      #[derive(serde::Deserialize)]
                      struct TokResp { access_token: String, refresh_token: String, expires_in: i64 }
                      if let Ok(tr) = resp.json::<TokResp>().await {
                        let mut cfg2 = read_cfg().unwrap_or_default();
                        let mut s = cfg2.simkl.unwrap_or_default();
                        s.access_token = Some(tr.access_token);
                        s.refresh_token = Some(tr.refresh_token);
                        s.token_expires_at = Some( chrono::Utc::now().timestamp() + tr.expires_in );
                        cfg2.simkl = Some(s);
                        let _ = write_cfg(&cfg2);
                        let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL linked. You can close this window.</h3></body></html>").with_status_code(StatusCode(200)));
                        ok = true;
                      } else {
                        let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL token parse failed.</h3></body></html>").with_status_code(StatusCode(500)));
                        err = Some("SIMKL token parse failed".into());
                      }
                    } else {
                      let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL token exchange failed.</h3></body></html>").with_status_code(StatusCode(400)));
                      err = Some(format!("SIMKL token exchange failed: {}", resp.status()));
                    }
                  } else {
                    let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL token request failed.</h3></body></html>").with_status_code(StatusCode(500)));
                    err = Some("SIMKL token request failed".into());
                  }
                } else {
                  let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL client_id/secret missing.</h3></body></html>").with_status_code(StatusCode(400)));
                  err = Some("SIMKL client_id/secret missing".into());
                }
              } else {
                let _ = rq.respond(Response::from_string("<html><body><h3>SIMKL not configured.</h3></body></html>").with_status_code(StatusCode(400)));
                err = Some("SIMKL not configured".into());
              }
            }
            Err(e) => {
              let _ = rq.respond(Response::from_string("<html><body><h3>Config read failed.</h3></body></html>").with_status_code(StatusCode(500)));
              err = Some(format!("Config read failed: {e}"));
            }
          }
        } else {
          let _ = rq.respond(Response::from_string("<html><body><h3>No code in callback.</h3></body></html>").with_status_code(StatusCode(400)));
          err = Some("No code in callback".into());
        }
      } else {
        let _ = rq.respond(Response::from_string("<html><body><h3>Callback parse failed.</h3></body></html>").with_status_code(StatusCode(400)));
        err = Some("Callback parse failed".into());
      }

      let _ = app.emit("simkl_linked", if ok {
        serde_json::json!({"ok": true})
      } else {
        serde_json::json!({"ok": false, "error": err.unwrap_or_else(|| "Unknown error".into())})
      });
    } else {
      let _ = app.emit("simkl_linked", serde_json::json!({"ok": false, "error": "Listener recv failed"}));
    }
  });

  Ok(())
}

#[tauri::command]
async fn cmd_run_sync(app: AppHandle) -> Result<(), String> {
  let cfg = read_cfg().map_err(|e| e.to_string())?;
  write_cfg(&cfg).map_err(|e| e.to_string())?;

  let mut cmd = app.shell().command("python");
  cmd = cmd.args(["resources/python/plex_simkl_watchlist_sync.py", "--sync"]);
  let status = cmd.status().await.map_err(|e| e.to_string())?;
  if !status.success() {
    return Err(format!("Sync script failed with {:?}", status.code()));
  }
  Ok(())
}


// ======= SIMKL PIN FLOW =======
// GET https://api.simkl.com/oauth/pin?client_id=...&redirect=...
// GET https://api.simkl.com/oauth/pin/{code}?client_id=...
// After user approves at https://simkl.com/pin

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct SimklPinCreate {
  code: Option<String>,
  user_code: Option<String>,
  authorize_url: Option<String>,
  verification_url: Option<String>,
  expires_in: Option<i64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct SimklTokenResp {
  access_token: Option<String>,
  refresh_token: Option<String>,
  expires_in: Option<i64>,
  token_type: Option<String>,
  result: Option<String>,
}

#[tauri::command]
async fn cmd_simkl_create_pin(app: tauri::AppHandle, client_id: String, redirect: Option<String>) -> Result<SimklPinCreate, String> {
  let mut url = format!("https://api.simkl.com/oauth/pin?client_id={}", urlencoding::encode(&client_id));
  if let Some(r) = redirect {
    url.push_str("&redirect=");
    url.push_str(&urlencoding::encode(&r));
  }
  let res = reqwest::Client::new().get(&url)
    .header("Accept", "application/json")
    .send().await.map_err(|e| e.to_string())?;
  let text = res.text().await.map_err(|e| e.to_string())?;
  let mut created: SimklPinCreate = serde_json::from_str(&text).unwrap_or_default();
  if created.code.is_none() { created.code = created.user_code.clone(); }
  // open pin page in a small window
  let _ = cmd_open_external_sized(app, "https://simkl.com/pin".to_string(), Some(520), Some(720));
  Ok(created)
}

#[tauri::command]
async fn cmd_simkl_poll_pin(code: String, client_id: String) -> Result<SimklTokenResp, String> {
  let url = format!("https://api.simkl.com/oauth/pin/{}?client_id={}", urlencoding::encode(&code), urlencoding::encode(&client_id));
  let res = reqwest::Client::new().get(&url)
    .header("Accept", "application/json")
    .send().await.map_err(|e| e.to_string())?;
  let text = res.text().await.map_err(|e| e.to_string())?;
  let tok: SimklTokenResp = serde_json::from_str(&text).unwrap_or_default();
  Ok(tok)
}

fn main() {
  tauri::Builder::default()
    .plugin(tauri_plugin_opener::init())
    .plugin(tauri_plugin_shell::init())
    .invoke_handler(tauri::generate_handler![cmd_read_config, cmd_write_config,
      cmd_open_url, cmd_open_external_sized,
      cmd_plex_create_pin, cmd_plex_poll_pin,
      cmd_simkl_start_listener,
      cmd_run_sync, cmd_simkl_create_pin, cmd_simkl_poll_pin])
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
