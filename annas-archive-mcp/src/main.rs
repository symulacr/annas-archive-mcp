//! Raw NDJSON JSON-RPC dispatch loop (cycle-29/MX): the rmcp ServiceExt machinery (lab37-measured ~84% of hot-path cost) replaced by a direct read/dispatch/write loop over newline-delimited JSON-RPC frames, byte-identical to the rmcp 3.1.4 build (golden-transcript verified: envelope key order, error codes, tool_result shapes, serde param structs).
use annas_archive_api::{AnnasArchiveClient, Error, PayloadProfile, SearchOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env,
    error::Error as StdError,
    io::{self, Write},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, Sleep, sleep_until};

// ===== tool parameter/result schemas =====

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchParams {
    pub query: String,
    pub page: Option<u32>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DetailsParams {
    pub md5: String,
    pub profile: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadParams {
    pub md5: String,
    pub path_index: Option<u32>,
    pub domain_index: Option<u32>,
}
/// `prewarm` result; `auto` > 0 = the post-serve background warm-up already completed in this process (its elapsed ms).
#[derive(Debug, Serialize, Deserialize)]
pub struct PrewarmReport {
    pub warmed: bool,
    pub elapsed_ms: u64,
    pub auto: u64,
}

// ===== stdio frame watchdog =====

// rmcp's stdio transport has no read deadline, so a client holding a JSON-RPC frame open wedges the session forever; this wraps stdin with a per-frame deadline and, per the MCP stdio spec, errors the read so the dispatch loop ends and the host client restarts us. The open-frame guard keeps normal idle-between-frames parking safe.

pub struct TimeoutAsyncRead<R> {
    inner: R,
    timeout: Duration,
    sleep: Pin<Box<Sleep>>,
    /// True while bytes have been delivered past the last `\n` (open frame) with the deadline armed; false when no frame is open (idle-safe).
    frame_open: bool,
}

impl<R> TimeoutAsyncRead<R> {
    pub fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: Box::pin(sleep_until(Instant::now() + timeout)),
            frame_open: false,
        }
    }
}

fn frame_timeout_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "stdio frame left open beyond timeout (stalled/partial frame)",
    )
}

impl<R: AsyncRead + Unpin> AsyncRead for TimeoutAsyncRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // The deadline is polled with this poller's waker before AND after the inner poll: once the stream has delivered its buffered bytes the outer loop may never repoll us, so a real OS pipe block could otherwise outlast the deadline.
        if self.frame_open && std::future::Future::poll(self.sleep.as_mut(), cx).is_ready() {
            return Poll::Ready(Err(frame_timeout_error()));
        }
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let filled = buf.filled();
                if filled.is_empty() {
                    return Poll::Ready(Ok(())); // EOF: session ends
                }
                // Open frame iff bytes extend past the last `\n`; every
                // open-frame delivery re-arms the deadline, so multi-read
                // frames never keep a stale absolute deadline.
                self.frame_open = !matches!(filled.iter().rposition(|&b| b == b'\n'), Some(p) if p + 1 == filled.len());
                if self.frame_open {
                    let timeout = self.timeout;
                    self.sleep.as_mut().reset(Instant::now() + timeout);
                    if std::future::Future::poll(self.sleep.as_mut(), cx).is_ready() {
                        return Poll::Ready(Err(frame_timeout_error()));
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                // No state write needed: an error ends this reader's use, and frame_open only gates deadline checks, never later polls.
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                if self.frame_open && std::future::Future::poll(self.sleep.as_mut(), cx).is_ready()
                {
                    return Poll::Ready(Err(frame_timeout_error()));
                }
                Poll::Pending
            }
        }
    }
}

/// Effective watchdog timeout: `MCP_STDIO_READ_TIMEOUT_MS` if set, else 30 s; invalid values fall back to the default.
pub fn stdio_read_timeout() -> Duration {
    Duration::from_millis(
        std::env::var("MCP_STDIO_READ_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(30_000),
    )
}

// ===== server =====

#[derive(Clone)]
pub struct AnnasArchiveServer {
    pub client: Arc<AnnasArchiveClient>,
    /// Elapsed ms of a successful auto-prewarm (0 = none finished yet).
    pub auto_prewarm_ms: Arc<AtomicU64>,
}

impl AnnasArchiveServer {
    /// Production constructor with env-derived capability defaults; env is read ONLY here (mcp layer). Unset env ⇒ byte-identical to the plain api-crate `AnnasArchiveClient` construction (all capabilities default-off). `cookie_file` (--cookies <path>): import a browser-exported cookie file into the file-backed jar and prewarm the session ahead of the first tool call; import or prewarm failure is fatal (a stale export must not degrade silently into challenge errors).
    pub async fn from_env(
        api_key: Option<String>,
        cookie_file: Option<String>,
    ) -> Result<Self, Error> {
        if let Some(path) = cookie_file {
            let jar_path = jar_state_path();
            let source = annas_archive_api::CookieImport::detect(std::path::Path::new(&path));
            let (jar, count) =
                annas_archive_api::import_cookies(&jar_path, source).map_err(|e| Error::Api {
                    message: format!("cookie import from {path} failed: {e}"),
                })?;
            let mut client = env_client(api_key).await?;
            client = client.with_shared_jar(jar);
            client.prewarm_session().await?;
            eprintln!("annas-archive-mcp: imported {count} cookies from {path}");
            return Ok(Self {
                client: Arc::new(client),
                auto_prewarm_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            });
        }
        Ok(Self {
            client: Arc::new(env_client(api_key).await?),
            auto_prewarm_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Fire the pool warm-up in the background, at most once per process; errors are ignored (flag stays 0).
    pub fn spawn_auto_prewarm(&self) {
        let client = self.client.clone();
        let flag = self.auto_prewarm_ms.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            if client.prewarm().await.is_ok() {
                flag.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            }
        });
    }
}

/// Tool-call outcome, ready for the wire: text content carrying compact JSON; tool failures are caller-visible is_error results, not protocol errors.
struct ToolText {
    text: String,
    is_error: bool,
}

/// Serialize outcome → ToolText; serializer failure (never in practice) → is_error.
fn into_text(serialized: Result<String, impl std::fmt::Display>, is_error: bool) -> ToolText {
    match serialized {
        Ok(text) => ToolText { text, is_error },
        Err(e) => ToolText {
            text: e.to_string(),
            is_error: true,
        },
    }
}

fn tool_text<T: Serialize>(result: Result<T, Error>) -> ToolText {
    match result {
        Ok(value) => into_text(serde_json::to_string(&value), false),
        Err(e) => {
            // Blocked gets actionable guidance; every other kind keeps the
            // exact three-field payload (golden-pinned byte contract).
            let guidance = if e.name() == "Blocked" {
                Some(
                    "Search is blocked by the site's DDoS-Guard. Options: (1) set AA_LOCAL_ES_URL to a local Elasticsearch mirror, (2) pass --cookies with browser-minted cookies, (3) use get_details with a known md5",
                )
            } else {
                None
            };
            into_text(
                serde_json::to_string(&json!({
                    "error": e.to_string(), "kind": e.name(), "retryable": e.is_retryable(),
                    "guidance": if guidance.is_some() { json!(guidance) } else { serde_json::Value::Null },
                })),
                true,
            )
        }
    }
}

/// Shared tool plumbing: parse arguments, deserialize `T`, api call, result envelope. `fmt` serializes success (verbatim for pre-shaped text).
impl AnnasArchiveServer {
    async fn run_tool<T: serde::de::DeserializeOwned, U: Serialize>(
        &self,
        args: Option<&Value>,
        fmt: fn(&U) -> Result<String, serde_json::Error>,
        f: impl AsyncFnOnce(T) -> Result<U, Error>,
    ) -> ToolText {
        // Absent/null arguments ≡ {} (rmcp: T::deserialize(json!({}))).
        let empty = Value::Object(Default::default());
        match serde_json::from_value::<T>(args.unwrap_or(&empty).clone()) {
            Ok(p) => match f(p).await {
                Ok(value) => into_text(fmt(&value), false),
                Err(e) => tool_text(Err::<(), _>(e)),
            },
            Err(e) => arg_error(&e),
        }
    }

    async fn tool_search(&self, args: Option<&Value>) -> ToolText {
        self.run_tool::<SearchParams, _>(
            args,
            serde_json::to_string,
            |p: SearchParams| async move {
                self.client
                    .search(SearchOptions {
                        page: p.page,
                        ..SearchOptions::new(&p.query)
                    })
                    .await
            },
        )
        .await
    }

    async fn tool_get_details(&self, args: Option<&Value>) -> ToolText {
        self.run_tool::<DetailsParams, String>(
            args,
            |s: &String| Ok(s.clone()),
            |p: DetailsParams| async move {
                let profile = PayloadProfile::from_arg(p.profile.as_deref());
                self.client
                    .get_details(&p.md5)
                    .await
                    .map(|d| annas_archive_api::shape_details(&d, profile))
            },
        )
        .await
    }

    async fn tool_get_download_url(&self, args: Option<&Value>) -> ToolText {
        self.run_tool::<DownloadParams, _>(
            args,
            serde_json::to_string,
            |p: DownloadParams| async move {
                self.client
                    .get_download_url(&p.md5, p.path_index, p.domain_index)
                    .await
            },
        )
        .await
    }

    async fn tool_get_membership_status(&self) -> ToolText {
        tool_text(
            self.client
                .membership_status()
                .await
                .map(|t| json!({ "tier": t.as_str() })),
        )
    }

    async fn tool_prewarm(&self) -> ToolText {
        // Local-ES backend: search never touches the mirrors; a mirror HEAD
        // would probe an unrelated (and possibly unreachable) endpoint.
        if matches!(
            self.client.search_backend(),
            annas_archive_api::SearchBackend::LocalEs { .. }
        ) {
            return tool_text(Ok(PrewarmReport {
                warmed: true,
                elapsed_ms: 0,
                auto: self.auto_prewarm_ms.load(Ordering::Relaxed),
            }));
        }
        let started = std::time::Instant::now();
        tool_text(self.client.prewarm().await.map(|(): ()| PrewarmReport {
            warmed: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            auto: self.auto_prewarm_ms.load(Ordering::Relaxed),
        }))
    }
}

/// Missing/invalid arguments surface as an is_error result with the exact rmcp deserialization message (golden-transcript parity).
fn arg_error(e: &serde_json::Error) -> ToolText {
    ToolText {
        text: format!("failed to deserialize parameters: {e}"),
        is_error: true,
    }
}

// ===== tools/list payload =====

/// Byte-identical copy of the rmcp 3.1.4 `ListToolsResult` serialization for
/// this server's 5 tools (captured from the golden transcript; schema text is
/// generated from the same field set, so it only changes if a tool's params
/// or annotations change — the tests pin the exact bytes).
const TOOLS_LIST_BODY: &[u8] = br#"{"tools":[{"name":"get_details","description":"Get detailed metadata for an item by its MD5 hash. Optional 'profile' arg: \"compact\" (default) returns ~1 KB with 300-char description, doi/isbn13 identifiers and one IPFS URL; \"full\" returns complete metadata incl. every identifier scheme; \"mini\" returns 8 core fields for search-result triage.","inputSchema":{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"md5":{"description":"MD5 hash of the item to get details for","type":"string"},"profile":{"description":"Output payload profile: compact (default, ~990 B: 300-char description, doi/isbn13 identifiers, 1 IPFS URL, download counts), full (complete metadata incl. all identifier schemes), or mini (8 core fields for search triage)","type":["string","null"]}},"required":["md5"],"type":"object"},"annotations":{"title":"Get item details","readOnlyHint":true,"openWorldHint":true}},{"name":"get_download_url","description":"Get a fast download URL for an item (requires ANNAS_ARCHIVE_API_KEY environment variable)","inputSchema":{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"domain_index":{"description":"Domain index for download source selection","format":"uint32","minimum":0,"type":["integer","null"]},"md5":{"description":"MD5 hash of the item to download","type":"string"},"path_index":{"description":"Path index for download source selection","format":"uint32","minimum":0,"type":["integer","null"]}},"required":["md5"],"type":"object"},"annotations":{"title":"Get download URL","readOnlyHint":true,"openWorldHint":true}},{"name":"get_membership_status","description":"Report the membership tier implied by the configured ANNAS_ARCHIVE_API_KEY: active, no_membership, or invalid_key. Quota-safe by design: the probe's response body is discarded unread, so calling this never spends a download mint.","inputSchema":{"properties":{},"type":"object"},"annotations":{"title":"Get membership status","readOnlyHint":true,"openWorldHint":true}},{"name":"prewarm","description":"Warm the connection to Anna's Archive ahead of the first search or details call (recommended before batch operations); reports elapsed time and the mirror used","inputSchema":{"properties":{},"type":"object"},"annotations":{"title":"Prewarm connection","readOnlyHint":true,"openWorldHint":true}},{"name":"search","description":"Search Anna's Archive for books, papers, magazines, comics, and other documents","inputSchema":{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"page":{"description":"Page number (starts at 1)","format":"uint32","minimum":0,"type":["integer","null"]},"query":{"description":"Search query for books, papers, magazines, comics, etc.","type":"string"}},"required":["query"],"type":"object"},"annotations":{"title":"Search the archive","readOnlyHint":true,"openWorldHint":true}}]}"#;

// ===== JSON-RPC envelope (rmcp wire parity) =====

/// Envelope prefix `{"jsonrpc":"2.0","id":<id>,<key>` (no closing brace); body closed by the caller. The id `Value` is re-serialized with serde_json's escapes (rmcp echoes the parsed id value unchanged; absent id → `null`).
fn envelope_into(response: &mut Vec<u8>, id: &Value, key: &[u8], body: &[u8]) {
    response.clear();
    response.extend_from_slice(br#"{"jsonrpc":"2.0","id":"#);
    let _ = serde_json::to_writer(&mut *response, id);
    response.extend_from_slice(b",");
    response.extend_from_slice(key);
    if !body.is_empty() {
        response.extend_from_slice(body);
        response.push(b'}');
        response.push(b'\n');
    }
}

/// Result envelope `{"jsonrpc":"2.0","id":<id>,"result":<body>}` + `\n`.
fn envelope_ok(response: &mut Vec<u8>, id: &Value, body: &[u8]) {
    envelope_into(response, id, br#""result":"#, body);
}

/// Tool-result envelope: text content carrying `text`; tool failures are caller-visible is_error results, not protocol errors.
fn envelope_text(response: &mut Vec<u8>, id: &Value, t: ToolText) {
    envelope_into(
        response,
        id,
        br#""result":{"content":[{"type":"text","text":"#,
        &[],
    );
    write_json_string(response, &t.text);
    response.extend_from_slice(if t.is_error {
        br#"}],"isError":true}}"#.as_slice()
    } else {
        br#"}],"isError":false}}"#.as_slice()
    });
    response.push(b'\n');
}

fn protocol_error(response: &mut Vec<u8>, id: &Value, code: i32, message: &str) {
    // rmcp's error envelope: {"jsonrpc":"2.0","id":..,"error":{"code":..,"message":".."}}
    let mut body = Vec::with_capacity(message.len() + 24);
    body.extend_from_slice(br#"{"code":"#);
    body.extend_from_slice(code.to_string().as_bytes());
    body.extend_from_slice(br#","message":"#);
    write_json_string(&mut body, message);
    body.push(b'}');
    envelope_into(response, id, br#""error":"#, &body);
}

/// Append `s` as a JSON string literal (quote + escapes), matching serde_json's escape set exactly.
fn write_json_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\u{08}' => out.extend_from_slice(b"\\b"),
            '\u{0c}' => out.extend_from_slice(b"\\f"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes())
            }
            c => out.extend_from_slice(c.to_string().as_bytes()),
        }
    }
    out.push(b'"');
}

// ===== dispatch loop =====

/// Serve one stdio session (NDJSON read/dispatch/write); returns on stdin EOF, watchdog frame-timeout error, or write failure. rmcp 3.1.4 parity: pre-`initialize` requests → -32602 + session end; `initialize` → V_2024_11_05 envelope then auto-prewarm on first post-handshake frame; notifications & unknown methods without id → no response; unknown method with id → -32601; unknown tool → -32602 "tool not found"; malformed JSON lines ignored; batching/cancellation unsupported.
pub async fn serve_session<R: AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin>(
    server: &AnnasArchiveServer,
    stdin: R,
    mut stdout: W,
) -> io::Result<()> {
    // The watchdog wraps the raw stdin before the framing layer, exactly as the rmcp build composed `TimeoutAsyncRead` into its transport.
    let mut stdin = TimeoutAsyncRead::new(stdin, stdio_read_timeout());
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let mut initialized = false;
    let mut response = Vec::with_capacity(256);
    let null_id = Value::Null;

    loop {
        // Pull bytes until a complete newline-terminated frame is in `buf`; the reader errors the read when a frame stays open past the deadline.
        let frame_end = loop {
            if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                break pos + 1;
            }
            let n = stdin.read(&mut chunk).await?;
            if n == 0 {
                // EOF: trailing bytes without a newline are an open frame the client abandoned — rmcp ends the session without a response.
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let line = std::str::from_utf8(&buf[..frame_end])
            .unwrap_or("")
            .trim_end_matches(['\n', '\r'])
            .to_owned();
        buf.drain(..frame_end);
        let line = line.as_str();
        if line.trim().is_empty() {
            continue;
        }
        // One parse per frame drives every arm: id echo, method, tools/call params.
        let frame = serde_json::from_str::<Value>(line).ok();

        // Auto-prewarm: spawned with the initialize reply, so the response is never delayed (rmcp spawned it post-serve).
        if !initialized {
            // Before initialize only a parseable `initialize` is answered; anything else (malformed lines included) ends the session: -32602 on the wire + rmcp's stderr/exit code.
            let is_init = frame
                .as_ref()
                .is_some_and(|f| f.get("method").and_then(Value::as_str) == Some("initialize"));
            if !is_init {
                let id = frame.as_ref().and_then(|f| f.get("id")).unwrap_or(&null_id);
                protocol_error(
                    &mut response,
                    id,
                    -32602,
                    "request _meta is missing or has malformed required fields: io.modelcontextprotocol/protocolVersion, io.modelcontextprotocol/clientCapabilities",
                );
                write_line(&mut stdout, &response).await?;
                return Err(io::Error::other(format!(
                    "expect initialized request, but received: {line}"
                )));
            }
            let frame = frame.unwrap();
            envelope_ok(
                &mut response,
                frame.get("id").unwrap_or(&null_id),
                br#"{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"rmcp","version":"3.1.4"},"instructions":"Access Anna's Archive to search for and get information about books, papers, magazines, comics, and other documents. Use get_download_url only if you have an API key configured."}"#,
            );
            write_line(&mut stdout, &response).await?;
            initialized = true;
            server.spawn_auto_prewarm();
            continue;
        }
        // Malformed JSON lines are ignored post-handshake (rmcp parity); a frame without `method` is not a request/notification (e.g. a bare client response) and is likewise dropped.
        let Some(frame) = frame else {
            continue;
        };
        let Some(method) = frame.get("method").and_then(Value::as_str) else {
            continue;
        };
        let id = frame.get("id").unwrap_or(&null_id);

        match method {
            "ping" => envelope_ok(&mut response, id, br#"{}"#),
            "tools/list" => envelope_ok(&mut response, id, TOOLS_LIST_BODY),
            // Spec-valid empty capability listings: rmcp 0.14 advertises no
            // prompts/resources/completions/logging capabilities, so an empty
            // list is the correct reply for each of these requests.
            "prompts/list" => envelope_ok(&mut response, id, br#"{"prompts":[]}"#),
            "resources/list" => envelope_ok(&mut response, id, br#"{"resources":[]}"#),
            "resources/templates/list" => {
                envelope_ok(&mut response, id, br#"{"resourceTemplates":[]}"#)
            }
            "tools/call" => {
                let params = frame.get("params").unwrap_or(&null_id);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = match params.get("arguments") {
                    None | Some(Value::Null) => None, // null ≡ absent (rmcp: None)
                    Some(v) => Some(v),
                };
                match name {
                    "search" => {
                        envelope_text(&mut response, id, server.tool_search(arguments).await)
                    }
                    "get_details" => {
                        envelope_text(&mut response, id, server.tool_get_details(arguments).await)
                    }
                    "get_download_url" => envelope_text(
                        &mut response,
                        id,
                        server.tool_get_download_url(arguments).await,
                    ),
                    "get_membership_status" => {
                        envelope_text(&mut response, id, server.tool_get_membership_status().await)
                    }
                    "prewarm" => envelope_text(&mut response, id, server.tool_prewarm().await),
                    // rmcp: unknown tool name → -32602 "tool not found"; the router only knows "tools/call" → -32601 when name is empty.
                    _ => {
                        let (code, msg) = if name.is_empty() {
                            (-32601, "tools/call")
                        } else {
                            (-32602, "tool not found")
                        };
                        protocol_error(&mut response, id, code, msg);
                    }
                }
            }
            // Unknown method: -32601 only when an id was carried; notifications (even carrying an id on known methods aside) get no response — rmcp keys the reply on the request variant, not the id.
            _ => {
                if frame.get("id").is_none() {
                    continue;
                }
                protocol_error(&mut response, id, -32601, method);
            }
        }
        write_line(&mut stdout, &response).await?;
    }
}

async fn write_line<W: tokio::io::AsyncWrite + Unpin>(
    stdout: &mut W,
    bytes: &[u8],
) -> io::Result<()> {
    stdout.write_all(bytes).await?;
    stdout.flush().await
}

/// Env-derived capability defaults. Read ONLY here (mcp layer): the api crate stays env-free by design. Unset env ⇒ byte-identical to the plain `new()` construction (all capabilities default-off). `AA_LENIENT_RECORDS=1` → with_lenient_records(true). `AA_REQUEST_COALESCING=1` → with_request_coalescing(true). `AA_DYNAMIC_MIRRORS=1` → with_dynamic_mirrors(true); async + fallible (CT-log probes), so this must run inside the async entrypoint — errors bubble to `main` rather than silently degrading to the hardcoded list. `AA_LOCAL_ES_URL=<url>` → with_search_backend(LocalEs): search goes to a local Elasticsearch instance (aa_derived_mirror_metadata index) instead of the mirrors.
pub(crate) async fn env_client(api_key: Option<String>) -> Result<AnnasArchiveClient, Error> {
    let mut client = AnnasArchiveClient::new(api_key);
    if let Ok(url) = env::var("AA_LOCAL_ES_URL") {
        let url = url.trim();
        if !url.is_empty() {
            client = client.with_search_backend(annas_archive_api::SearchBackend::LocalEs {
                url: url.to_string(),
            });
        }
    }
    if env_enabled("AA_LENIENT_RECORDS") {
        client = client.with_lenient_records(true);
    }
    if env_enabled("AA_REQUEST_COALESCING") {
        client = client.with_request_coalescing(true);
    }
    if env_enabled("AA_DYNAMIC_MIRRORS") {
        client = client.with_dynamic_mirrors(true).await?;
    }
    Ok(client)
}

fn env_enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1")
}

/// Jar file backing the imported session: fixed location so restarts reuse
/// the imported cookies without re-importing.
fn jar_state_path() -> std::path::PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(|h| std::path::PathBuf::from(h).join(".local/state"))
                .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        })
        .join("annas-archive-mcp")
        .join("cookies.json")
}

// ===== sync stdio writer =====

/// AsyncWrite adapter over std stdout: direct blocking write(2) instead of tokio's blocking-pool channel (lab37: a large share of per-frame dispatch cost). Response-sized stdio writes never block (the host is reading).
pub struct SyncStdout(std::io::Stdout);

impl tokio::io::AsyncWrite for SyncStdout {
    // Direct blocking write(2)/flush, bypassing tokio's blocking pool.
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.get_mut().0.write_all(buf).map(|_| buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().0.flush())
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// ===== entrypoint =====

// Entry-point plumbing is intentionally not driven by tests (#[path]-included by the test bin).
#[cfg_attr(test, allow(dead_code))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    // Run on a helper so every exit path ends in std::process::exit: the tokio Stdin blocking reader stays parked in read() after the session ends and would hang runtime shutdown.
    std::process::exit(match run().await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("annas-archive-mcp: {e}");
            1
        }
    });
}

async fn run() -> Result<(), Box<dyn StdError>> {
    let api_key = env::var("ANNAS_ARCHIVE_API_KEY").ok();
    // --cookies <file> (fix1-cookies): import a browser-exported cookie file
    // (Netscape cookies.txt or Chrome DevTools JSON) at startup so the
    // DDoS-Guard session is replayed without re-solving the challenge.
    let cookie_file = std::env::args()
        .skip(1)
        .position(|arg| arg == "--cookies")
        .and_then(|i| std::env::args().nth(i + 2));
    let server = AnnasArchiveServer::from_env(api_key, cookie_file).await?;
    serve_session(&server, tokio::io::stdin(), SyncStdout(std::io::stdout())).await?;
    Ok(())
}
