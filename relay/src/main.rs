// Shizuku relay: MCP gateway (HTTP) + device link (WebSocket).
// One phone. One connection. Commands run serially on the device.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    time::timeout,
};

const OUT_CAP: usize = 256 * 1024;
const SHOT_CAP: usize = 8 * 1024 * 1024;
const IDLE_SECS: u64 = 300;
const EXEC_SECS: u64 = 75;

#[derive(Clone)]
struct App {
    mcp_token: Arc<str>,
    relay_token: Arc<str>,
    link: Arc<Mutex<Option<Link>>>,
    exec: Arc<Mutex<()>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Reply>>>>,
    seq: Arc<AtomicU64>,
}

enum Out {
    Text(String),
    Close,
}

struct Link {
    id: u64,
    tx: mpsc::UnboundedSender<Out>,
    device: Arc<Mutex<String>>,
}

struct Reply {
    ok: bool,
    out: String,
    code: i64,
}

fn state(mcp: &str, relay: &str) -> App {
    App {
        mcp_token: mcp.into(),
        relay_token: relay.into(),
        link: Arc::new(Mutex::new(None)),
        exec: Arc::new(Mutex::new(())),
        pending: Arc::new(Mutex::new(HashMap::new())),
        seq: Arc::new(AtomicU64::new(1)),
    }
}

fn router(st: App) -> Router {
    // The gate runs before the WebSocket extractor, so a bad token gives 401, not 400.
    let relay = get(relay_ws).layer(axum::middleware::from_fn_with_state(
        st.clone(),
        relay_gate,
    ));
    // Auth runs before the handler, so the body is never read for a bad token.
    let mcp = post(mcp_post)
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .layer(axum::middleware::from_fn_with_state(st.clone(), mcp_gate));
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(health))
        .route("/mcp", mcp)
        .route("/relay", relay)
        .with_state(st)
}

async fn mcp_gate(
    State(st): State<App>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if bearer(&headers).is_some_and(|t| ct_eq(t, &st.mcp_token)) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

async fn relay_gate(
    State(st): State<App>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // Bearer only: a token in the URL would land in proxy logs.
    if bearer(&headers).is_some_and(|t| ct_eq(t, &st.relay_token)) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

fn bearer(h: &HeaderMap) -> Option<&str> {
    h.get(AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ").map(str::trim)
}

// Compare without an early exit, so timing does not leak the token prefix.
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn root() -> impl IntoResponse {
    "shizuku-relay 0.1.0\nmcp: POST /mcp (Bearer)\nrelay: GET /relay (Bearer)\n"
}

async fn health() -> Response {
    // No device data for anonymous callers.
    Json(json!({"ok": true})).into_response()
}

async fn mcp_post(State(st): State<App>, headers: HeaderMap, body: String) -> Response {
    if !bearer(&headers).is_some_and(|t| ct_eq(t, &st.mcp_token)) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let req: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid json").into_response(),
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req["method"].as_str().unwrap_or("");
    let params = &req["params"];

    if method.starts_with("notifications/") || req.get("id").is_none() {
        return StatusCode::ACCEPTED.into_response();
    }

    let out = match method {
        "initialize" => json!({
            "protocolVersion": params["protocolVersion"].as_str().unwrap_or("2025-06-18"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "shizuku-relay", "version": "0.1.0"}
        }),
        "ping" => json!({}),
        "tools/list" => json!({"tools": tools()}),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = &params["arguments"];
            return match run_tool(&st, name, args).await {
                Ok(content) => Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {"content": content, "isError": false}
                }))
                .into_response(),
                Err(e) => Json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {"content": [{"type": "text", "text": e}], "isError": true}
                }))
                .into_response(),
            };
        }
        _ => {
            return Json(json!({
                "jsonrpc": "2.0", "id": id,
                "error": {"code": -32601, "message": format!("unknown method: {method}")}
            }))
            .into_response()
        }
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": out})).into_response()
}

// ---------- device link ----------

async fn relay_ws(State(st): State<App>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| relay_loop(st, socket))
}

async fn relay_loop(st: App, mut socket: WebSocket) {
    let id = st.seq.fetch_add(1, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::unbounded_channel::<Out>();
    let device = Arc::new(Mutex::new(String::new()));
    let old = st.link.lock().await.replace(Link {
        id,
        tx,
        device: device.clone(),
    });
    if let Some(old) = old {
        let _ = old.tx.send(Out::Close);
    }

    let mut last = Instant::now();
    loop {
        tokio::select! {
            frame = timeout(Duration::from_secs(30), socket.recv()) => {
                match frame {
                    Err(_) => {
                        // 30 s of silence: probe, and drop a link that has been idle too long.
                        if last.elapsed() > Duration::from_secs(IDLE_SECS) { break; }
                        if socket.send(Message::Ping(Vec::new().into())).await.is_err() { break; }
                    }
                    Ok(None) => break,
                    Ok(Some(Err(_))) => break,
                    Ok(Some(Ok(msg))) => {
                        last = Instant::now();
                        match msg {
                            Message::Text(t) => {
                                if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                                    if let Some(h) = v.get("hello") {
                                        *device.lock().await = h.to_string().chars().take(200).collect();
                                    } else if let (Some(rid), Some(ok)) = (
                                        v["id"].as_u64(),
                                        v.get("ok").and_then(Value::as_bool),
                                    ) {
                                        let r = Reply {
                                            ok,
                                            out: v["out"].as_str().unwrap_or("").to_string(),
                                            code: v["code"].as_i64().unwrap_or(-1),
                                        };
                                        if let Some(tx) = st.pending.lock().await.remove(&rid) {
                                            let _ = tx.send(r);
                                        }
                                    }
                                }
                            }
                            Message::Ping(p) => { let _ = socket.send(Message::Pong(p)).await; }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                }
            }
            Some(out) = rx.recv() => {
                let sent = match out {
                    Out::Text(text) => socket.send(Message::Text(text.into())).await.is_ok(),
                    Out::Close => {
                        let _ = socket.send(Message::Close(None)).await;
                        false
                    }
                };
                if !sent { break; }
            }
        }
    }

    let was_current = {
        let mut link = st.link.lock().await;
        let current = link.as_ref().is_some_and(|l| l.id == id);
        if current {
            *link = None;
        }
        current
    };
    // Wake in-flight calls at once; their senders drop with the map.
    if was_current {
        st.pending.lock().await.clear();
    }
}

async fn exec_capped(st: &App, cmd: &str, secs: u64, cap: usize) -> Result<String, String> {
    if st.link.lock().await.is_none() {
        return Err("Device offline. Open the app, grant Shizuku, press Start.".into());
    }
    let _serial = st.exec.lock().await;

    let tx = {
        let link = st.link.lock().await;
        match link.as_ref() {
            Some(l) => l.tx.clone(),
            None => return Err("Device offline. Open the app and press Start.".into()),
        }
    };
    let id = st.seq.fetch_add(1, Ordering::SeqCst);
    let (otx, orx) = oneshot::channel();
    st.pending.lock().await.insert(id, otx);
    if tx.send(Out::Text(json!({"id": id, "cmd": cmd}).to_string())).is_err() {
        st.pending.lock().await.remove(&id);
        return Err("Device link closed.".into());
    }

    match timeout(Duration::from_secs(secs), orx).await {
        Ok(Ok(r)) if r.ok => Ok(cap_text(r.out, cap)),
        Ok(Ok(r)) => Err(cap_text(
            format!("exit {}\n{}", r.code, r.out.trim_end()),
            cap.min(8192),
        )),
        Ok(Err(_)) => Err("Device link closed.".into()),
        Err(_) => {
            st.pending.lock().await.remove(&id);
            Err(format!("Device timeout after {secs}s."))
        }
    }
}

async fn exec(st: &App, cmd: &str, secs: u64) -> Result<String, String> {
    exec_capped(st, cmd, secs, OUT_CAP).await
}

fn cap_text(s: String, max: usize) -> String {
    if s.len() <= max {
        return s;
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}\n[truncated at {} bytes]", &s[..cut], max)
}

// ---------- tools ----------

fn tools() -> Value {
    json!([
        {
            "name": "doctor",
            "description": "Preflight check. Run this first to test the device link.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "adb_shell",
            "description": "Run one shell command with shell (adb) rights via Shizuku. Output cap 256 KB. Commands run one at a time.",
            "inputSchema": {
                "type": "object",
                "properties": {"command": {"type": "string", "description": "Shell command, e.g. 'dumpsys battery'"}},
                "required": ["command"]
            }
        },
        {
            "name": "device_info",
            "description": "Model, Android version, SDK, ABI and battery level.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "ui_dump",
            "description": "List up to 150 on-screen elements: text, resource-id, class, clickable flag, center x,y. Fails on a locked or animated screen.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "screenshot",
            "description": "Capture the screen and return a PNG image. Fails if the screen is locked or off.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "input",
            "description": "Tap, swipe, press a key or type text. Text: ASCII letters, digits and .,:/@_+- only. Spaces become %s.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["tap", "swipe", "key", "text"]},
                    "x": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "y": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "x2": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "y2": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "duration_ms": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "key": {"type": "string", "description": "Key name, e.g. HOME, BACK, ENTER"},
                    "text": {"type": "string", "description": "ASCII text to type"}
                },
                "required": ["action"]
            }
        },
        {
            "name": "current_app",
            "description": "Foreground activity and window focus. Shows which app is in front.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "screen_state",
            "description": "Screen on/off, wakefulness and keyguard state.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "observe",
            "description": "One snapshot: screen state, foreground app and up to 150 UI elements. A failed part does not hide the others.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "notifications",
            "description": "Recent notification titles and text. Cap 60 lines / 4 KB. Some apps hide the text.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ])
}

fn text(s: String) -> Vec<Value> {
    vec![json!({"type": "text", "text": s})]
}

async fn run_tool(st: &App, name: &str, a: &Value) -> Result<Vec<Value>, String> {
    match name {
        "doctor" => {
            let out = exec(
                st,
                "echo uid=$(id -u); echo sdk=$(getprop ro.build.version.sdk); echo sh=$(command -v sh)",
                EXEC_SECS,
            )
            .await?;
            Ok(text(out))
        }
        "adb_shell" => {
            let cmd = a["command"].as_str().unwrap_or("").trim();
            if cmd.is_empty() {
                return Err("command is required".into());
            }
            if cmd.len() > 4096 {
                return Err("command too long (max 4096 bytes)".into());
            }
            Ok(text(exec(st, cmd, EXEC_SECS).await?))
        }
        "device_info" => {
            let cmd = "echo model=$(getprop ro.product.model); \
                       echo android=$(getprop ro.build.version.release); \
                       echo sdk=$(getprop ro.build.version.sdk); \
                       echo abi=$(getprop ro.product.cpu.abi); \
                       dumpsys battery | grep -E 'level|status|temperature'";
            Ok(text(exec(st, cmd, EXEC_SECS).await?))
        }
        "ui_dump" => {
            let out = exec(
                st,
                "uiautomator dump /data/local/tmp/.relay_ui.xml >/dev/null 2>&1; \
                 cat /data/local/tmp/.relay_ui.xml; rm -f /data/local/tmp/.relay_ui.xml",
                EXEC_SECS,
            )
            .await?;
            let rows = parse_ui(&out);
            if rows.is_empty() {
                return Err("No UI elements. Screen may be locked or moving. Try screenshot.".into());
            }
            Ok(text(rows))
        }
        "screenshot" => {
            let out = exec_capped(
                st,
                "screencap -p /data/local/tmp/.relay_shot.png && \
                 base64 /data/local/tmp/.relay_shot.png | tr -d '\\n'; \
                 rm -f /data/local/tmp/.relay_shot.png",
                EXEC_SECS,
                SHOT_CAP,
            )
            .await?;
            let data: String = out.split_whitespace().collect();
            if data.len() < 64 {
                return Err("Empty screenshot. Screen may be off or locked.".into());
            }
            Ok(vec![json!({"type": "image", "data": data, "mimeType": "image/png"})])
        }
        "input" => Ok(text(exec(st, &build_input(a)?, EXEC_SECS).await?)),
        "current_app" => Ok(text(
            exec(
                st,
                "dumpsys activity activities | grep -E 'mResumedActivity|topResumedActivity' | head -3; \
                 dumpsys window | grep -E 'mCurrentFocus|mFocusedApp' | head -3",
                EXEC_SECS,
            )
            .await?,
        )),
        "screen_state" => Ok(text(
            exec(
                st,
                "dumpsys power | grep -E 'mWakefulness=|Display Power' | head -4; \
                 dumpsys window | grep -E 'mShowingLockscreen|isKeyguardShowing|mDreamingLockscreen' | head -4",
                EXEC_SECS,
            )
            .await?,
        )),
        "observe" => {
            // One command, so the phone asks for approval once.
            let out = exec(
                st,
                "echo '== screen'; (dumpsys power | grep -E 'mWakefulness=|Display Power' | head -4) 2>&1; \
                 echo '== app'; (dumpsys activity activities | grep -E 'mResumedActivity|topResumedActivity' | head -2) 2>&1; \
                 echo '== ui'; uiautomator dump /data/local/tmp/.relay_ui.xml >/dev/null 2>&1; \
                 cat /data/local/tmp/.relay_ui.xml 2>/dev/null; rm -f /data/local/tmp/.relay_ui.xml",
                EXEC_SECS,
            )
            .await?;
            let (head, xml) = out.split_once("== ui\n").unwrap_or((out.as_str(), ""));
            let rows = parse_ui(xml);
            let ui = if rows.is_empty() { "(no elements)".to_string() } else { rows };
            Ok(text(format!("{}\n== ui\n{}", head.trim_end(), ui.trim_end())))
        }
        "notifications" => {
            let out = exec_capped(
                st,
                "dumpsys notification --noredact 2>/dev/null | grep -E 'android.title=|android.text=' | tail -60",
                EXEC_SECS,
                4096,
            )
            .await?;
            Ok(text(if out.trim().is_empty() {
                "No notification text. Some apps hide it.".to_string()
            } else {
                out
            }))
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

fn num(a: &Value, k: &str) -> Option<i64> {
    a[k].as_i64().filter(|v| (0..=10000).contains(v))
}

fn build_input(a: &Value) -> Result<String, String> {
    match a["action"].as_str().unwrap_or("") {
        "tap" => Ok(format!(
            "input tap {} {}",
            num(a, "x").ok_or("x is required (0..10000)")?,
            num(a, "y").ok_or("y is required (0..10000)")?
        )),
        "swipe" => {
            let ms = a["duration_ms"].as_i64().filter(|v| (0..=10000).contains(v)).unwrap_or(300);
            Ok(format!(
                "input swipe {} {} {} {} {}",
                num(a, "x").ok_or("x is required")?,
                num(a, "y").ok_or("y is required")?,
                num(a, "x2").ok_or("x2 is required")?,
                num(a, "y2").ok_or("y2 is required")?,
                ms
            ))
        }
        "key" => {
            let k = a["key"].as_str().unwrap_or("");
            let ok = !k.is_empty()
                && k.len() <= 40
                && k.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if !ok {
                return Err("key must be A-Z, 0-9 or _, e.g. HOME".into());
            }
            Ok(format!("input keyevent {k}"))
        }
        "text" => {
            let t = a["text"].as_str().unwrap_or("");
            if t.is_empty() {
                return Err("text is required".into());
            }
            if !t
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || " .,:/@_+-".contains(c))
            {
                return Err("text allows ASCII letters, digits and .,:/@_+- only".into());
            }
            Ok(format!("input text {}", t.replace(' ', "%s")))
        }
        _ => Err("action must be tap, swipe, key or text".into()),
    }
}

fn attr<'a>(tag: &'a str, key: &str) -> String {
    let pat = format!("{key}=\"");
    let Some(i) = tag.find(&pat) else {
        return String::new();
    };
    let rest = &tag[i + pat.len()..];
    let end = rest.find('"').unwrap_or(rest.len());
    unescape(&rest[..end])
}

fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#10;", " ")
        .replace("&amp;", "&")
}

fn center(bounds: &str) -> Option<(i32, i32)> {
    let n: Vec<i32> = bounds
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect();
    (n.len() == 4).then(|| ((n[0] + n[2]) / 2, (n[1] + n[3]) / 2))
}

fn parse_ui(xml: &str) -> String {
    let mut out = String::new();
    for (i, seg) in xml.split("<node ").skip(1).enumerate() {
        if i >= 150 {
            break;
        }
        let tag = &seg[..seg.find('>').unwrap_or(seg.len())];
        let text = attr(tag, "text");
        let desc = attr(tag, "content-desc");
        let label = if text.is_empty() { desc } else { text };
        let id = attr(tag, "resource-id");
        let class = attr(tag, "class");
        let short = class.rsplit('.').next().unwrap_or("").to_string();
        let clickable = if attr(tag, "clickable") == "true" { "T" } else { "-" };
        let pos = center(&attr(tag, "bounds"))
            .map(|(x, y)| format!("{x},{y}"))
            .unwrap_or_else(|| "-".into());
        out.push_str(&format!(
            "{} | {} | {} | {} | {}\n",
            if label.is_empty() { "-" } else { label.trim() },
            if id.is_empty() { "-" } else { &id },
            if short.is_empty() { "-" } else { &short },
            clickable,
            pos
        ));
    }
    out
}

#[tokio::main]
async fn main() {
    let mcp = std::env::var("MCP_HTTP_TOKEN").unwrap_or_default();
    let relay = std::env::var("RELAY_TOKEN").unwrap_or_default();
    if mcp.len() < 32 || relay.len() < 32 {
        eprintln!("Set MCP_HTTP_TOKEN and RELAY_TOKEN (32+ chars each).");
        std::process::exit(1);
    }
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(10_000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("bind");
    eprintln!("shizuku-relay on :{port}");
    axum::serve(listener, router(state(&mcp, &relay)))
        .await
        .expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> App {
        state("mcp-token-0123456789", "relay-token-0123456789")
    }

    async fn call(body: Value, token: &str) -> (StatusCode, Value) {
        let res = router(test_state())
            .oneshot(
                Request::post("/mcp")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    #[test]
    fn token_compare() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "ab"));
    }

    #[test]
    fn input_is_guarded() {
        assert_eq!(
            build_input(&json!({"action": "tap", "x": 10, "y": 20})).unwrap(),
            "input tap 10 20"
        );
        assert!(build_input(&json!({"action": "tap", "x": 10, "y": 99999})).is_err());
        assert!(build_input(&json!({"action": "text", "text": "hi; rm -rf /"})).is_err());
        assert_eq!(
            build_input(&json!({"action": "text", "text": "hello world"})).unwrap(),
            "input text hello%sworld"
        );
        assert!(build_input(&json!({"action": "key", "key": "HOME;reboot"})).is_err());
    }

    #[test]
    fn ui_xml_is_parsed() {
        let xml = r#"<hierarchy><node text="OK" resource-id="a.b:id/ok" class="android.widget.Button" clickable="true" bounds="[0,10][100,30]"><node text="&amp;more" class="android.widget.TextView" clickable="false" bounds="[4,4][8,8]"/></hierarchy>"#;
        let rows = parse_ui(xml);
        assert!(rows.contains("OK | a.b:id/ok | Button | T | 50,20"), "{rows}");
        assert!(rows.contains("&more | - | TextView | - | 6,6"), "{rows}");
        assert_eq!(parse_ui("<x/>"), "");
    }

    #[tokio::test]
    async fn mcp_needs_token() {
        let (status, _) = call(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}), "bad").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn mcp_lists_tools_and_reports_offline() {
        let (status, body) = call(
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}),
            "mcp-token-0123456789",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["serverInfo"]["name"], "shizuku-relay");

        let (_, body) = call(
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            "mcp-token-0123456789",
        )
        .await;
        let names: Vec<&str> = body["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 10);
        assert!(names.contains(&"screenshot"));

        let (_, body) = call(
            json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "doctor", "arguments": {}}}),
            "mcp-token-0123456789",
        )
        .await;
        assert_eq!(body["result"]["isError"], true);
        assert!(body["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("offline"));
    }

    #[tokio::test]
    async fn mcp_unknown_method() {
        let (_, body) = call(
            json!({"jsonrpc": "2.0", "id": 9, "method": "nope"}),
            "mcp-token-0123456789",
        )
        .await;
        assert_eq!(body["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn exec_round_trip_over_websocket() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message as WsMsg;

        let st = test_state();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = st.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router(served)).await;
        });

        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut req = format!("ws://{addr}/relay").into_client_request().unwrap();
        req.headers_mut()
            .insert("authorization", "Bearer relay-token-0123456789".parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws.send(WsMsg::text(r#"{"hello":{"model":"SM-A346E"}}"#))
            .await
            .unwrap();
        for _ in 0..100 {
            if st.link.lock().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(st.link.lock().await.is_some());

        let caller = st.clone();
        let task = tokio::spawn(async move { exec(&caller, "echo hi", 5).await });
        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let cmd: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(cmd["cmd"], "echo hi");

        ws.send(WsMsg::text(
            json!({"id": cmd["id"], "ok": true, "out": "hi\n", "code": 0}).to_string(),
        ))
        .await
        .unwrap();

        assert_eq!(task.await.unwrap().unwrap(), "hi\n");
        assert!(st.pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn relay_rejects_bad_token() {
        let res = router(test_state())
            .oneshot(
                Request::get("/relay?token=wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
}
