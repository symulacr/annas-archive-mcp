use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::{Client, Response, cookie::Jar};
use serde::Deserialize;
use serde::Serialize;
use serde::de::Deserializer;
use tokio::sync::OnceCell;

use crate::error::{Error, ParseKind, all_domains_error, api_error, blocked, parse_error};
use crate::scraper::parse_search_results;
use crate::types::{
    DownloadInfo, DownloadSource, Identifiers, IpfsInfo, ItemDetails, SearchOptions, SearchResponse,
};

/// Official mirrors, fastest p50 first; `with_dynamic_mirrors` may append CT-log discoveries.
pub(crate) const DOMAINS: &[&str] = &["annas-archive.gd", "annas-archive.gl", "annas-archive.pk"];

/// Merged-mirror cap.
const MAX_DOMAINS: usize = 5;

/// Bodies below this are parked-domain "for sale" pages served with HTTP 200; real bodies sit far above; fast_download responses are exempt.
const MIN_DOCUMENT_BODY_BYTES: usize = 2 * 1024;

/// DDoS-Guard interstitial markers: the redirect target's query token
/// (`/?chek=1`, sic) and a lowercase snippet of the 403 guard page.
const DDOS_GUARD_CHEK: &str = "chek=1";
const DDOS_GUARD_BODY_SNIPPET: &str = "ddos-guard";

/// Whether this (status, final URL query, Location header, body) tuple is the
/// DDoS-Guard challenge: a redirect toward `/?chek=1`, or a 403 that either
/// landed on the chek URL or carries the guard page body. Plain redirects and
/// non-guard 403s (membership gate, record re-auth gate) return false.
fn is_ddos_guard_challenge(
    status: u16,
    url_query: Option<&str>,
    location: Option<&str>,
    body: &str,
) -> bool {
    if matches!(status, 301 | 302 | 303 | 307 | 308) {
        return location.is_some_and(|loc| loc.contains(DDOS_GUARD_CHEK));
    }
    if status != 403 {
        return false;
    }
    url_query.is_some_and(|q| q.contains(DDOS_GUARD_CHEK))
        || body.to_ascii_lowercase().contains(DDOS_GUARD_BODY_SNIPPET)
}

/// Async hook where the challenge-solver lab plugs its flow: invoked once
/// with the challenged URL; success triggers a single retry of the request.
type SolverFn = dyn Fn(String) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
    + Send
    + Sync;

#[derive(Clone)]
pub struct ChallengeSolver(Arc<SolverFn>);

/// A cookie store that mirrors reqwest's `Jar` but persists unexpired
/// cookies to a JSON file after every write, so a session survives process
/// restarts. Uses the `cookie_store` crate already in the dependency tree
/// (reqwest's own jar backend) — no new dependencies. Domain scoping is
/// RFC 6265: a cookie earned on `.annas-archive.gd` never matches
/// `.annas-archive.gl` or `.annas-archive.pk`.
pub struct FileJar {
    pub(crate) store: Mutex<cookie_store::CookieStore>,
    path: PathBuf,
}

impl FileJar {
    /// Load a jar from `path` (missing or corrupt file ⇒ empty store).
    pub fn load(path: PathBuf) -> Self {
        let store = std::fs::File::open(&path)
            .map(|file| {
                cookie_store::serde::json::load(std::io::BufReader::new(file)).unwrap_or_default()
            })
            .unwrap_or_default();
        Self {
            store: Mutex::new(store),
            path,
        }
    }

    pub(crate) fn persist(&self, store: &cookie_store::CookieStore) {
        if let Ok(mut file) = std::fs::File::create(&self.path) {
            // Session cookies (no Expires/Max-Age — exactly what DDoS-Guard mints)
            // must survive restarts, so persist everything; expired entries are
            // filtered out on match, not on save.
            let _ =
                cookie_store::serde::json::save_incl_expired_and_nonpersistent(store, &mut file);
        }
    }
}

impl reqwest::cookie::CookieStore for FileJar {
    fn set_cookies(
        &self,
        cookie_headers: &mut dyn Iterator<Item = &reqwest::header::HeaderValue>,
        url: &url::Url,
    ) {
        let mut store = self.store.lock().expect("file-jar lock poisoned");
        let owned: Vec<cookie_store::RawCookie<'static>> = cookie_headers
            .filter_map(|value| {
                cookie_store::RawCookie::parse(value.to_str().ok()?.to_string()).ok()
            })
            .collect();
        store.store_response_cookies(owned.into_iter(), url);
        self.persist(&store);
    }

    fn cookies(&self, url: &url::Url) -> Option<reqwest::header::HeaderValue> {
        let store = self.store.lock().expect("file-jar lock poisoned");
        let pairs = store
            .get_request_values(url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        if pairs.is_empty() {
            return None;
        }
        reqwest::header::HeaderValue::from_maybe_shared(pairs).ok()
    }
}

/// Local-Elasticsearch search backend (fix2): request construction and hit
/// parsing for `SearchBackend::LocalEs`. The query DSL mirrors the upstream
/// `search_page` msearch body (LilyLoops/annas-archive
/// allthethings/page/views.py:7832-7917); hit fields are the
/// `search_only_fields` schema written in views.py:6479-6516.
mod es {
    use serde_json::{Value, json};

    use crate::error::{Error, ParseKind, api_error, parse_error};
    use crate::types::{SearchResponse, SearchResult};

    /// Upstream page size: `max_display_results = 100` (views.py:7900).
    const PAGE_SIZE: u32 = 100;

    /// ES request body mirroring the primary search in `search_page`
    /// (views.py:7902-7917), minus aggregations (this client exposes only
    /// hits). Sort `_score` when no explicit sort was requested upstream;
    /// the MCP layer exposes no sort param, matching the site default.
    pub fn build_request(query: &str, page: u32) -> Value {
        let from = (page - 1).saturating_mul(PAGE_SIZE);
        let mut body = json!({
            "size": PAGE_SIZE,
            "from": from,
            "sort": ["_score"],
        });
        if query.is_empty() {
            // views.py:7832-7836: no input ⇒ match_all, and the site default
            // sort switches to newest-added.
            body["query"] = json!({ "match_all": {} });
            body["sort"] = json!([{ "search_only_fields.search_added_date": "desc" }, "_score"]);
        } else {
            // views.py:7838-7898: two-branch bool query — phrase match
            // (boosted by rank_feature + UI-language constant_score) and a
            // simple_query_string term branch with default_operator "and".
            let phrase = json!({
                "bool": {
                    "should": [
                        { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0 } },
                        {
                            "constant_score": {
                                "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": "en" } } },
                                "boost": 3.0*50000.0,
                            },
                        },
                    ],
                    "must": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "bool": {
                                            "should": [
                                                { "match_phrase": { "search_only_fields.search_text": { "query": query } } },
                                            ],
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                }
            });
            let terms = json!({
                "bool": {
                    "should": [
                        { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0/100000.0 } },
                        {
                            "constant_score": {
                                "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": "en" } } },
                                "boost": 3.0*50000.0/100000.0,
                            },
                        },
                    ],
                    "must": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "bool": {
                                            "should": [
                                                { "simple_query_string": { "query": query, "fields": ["search_only_fields.search_text"], "default_operator": "and" } },
                                            ],
                                        },
                                        "boost": 1.0/100000.0,
                                    },
                                ],
                            },
                        },
                    ],
                }
            });
            body["query"] = json!({ "bool": { "should": [phrase, terms] } });
        }
        body
    }

    /// Parse the ES `_search` response into the shared [`SearchResponse`].
    /// `_id` is `md5:<md5>` (aarecord id); `_source.search_only_fields`
    /// carries the display fields. Missing/empty fields degrade to None the
    /// same way the HTML scraper leaves unset card slots.
    pub fn parse_response(body: &str, page: u32) -> Result<SearchResponse, Error> {
        let parsed: Value = serde_json::from_str(body)
            .map_err(|e| parse_error(format!("ES response: {e}"), ParseKind::MalformedJson))?;
        let hits = parsed
            .get("hits")
            .and_then(Value::as_object)
            .ok_or_else(|| parse_error("ES response: missing hits", ParseKind::MalformedJson))?;
        let total = hits
            .get("total")
            .map(|t| match t {
                Value::Number(n) => n.as_u64().unwrap_or(0),
                Value::Object(obj) => obj.get("value").and_then(Value::as_u64).unwrap_or(0),
                _ => 0,
            })
            .unwrap_or(0);
        let mut results = Vec::new();
        if let Some(list) = hits.get("hits").and_then(Value::as_array) {
            for hit in list {
                let Some(id) = hit.get("_id").and_then(Value::as_str) else {
                    continue;
                };
                let md5 = id.strip_prefix("md5:").unwrap_or(id);
                let Some(source) = hit.get("_source").and_then(|s| s.get("search_only_fields"))
                else {
                    continue;
                };
                let nonempty = |v: &Value| v.as_str().filter(|s| !s.is_empty()).map(str::to_string);
                let title = match source.get("search_title").and_then(nonempty) {
                    Some(t) => t,
                    None => continue, // no title ⇒ no displayable card
                };
                let filesize = source.get("search_filesize").and_then(Value::as_u64);
                let year = source.get("search_year").and_then(Value::as_u64);
                results.push(SearchResult {
                    md5: md5.to_string(),
                    title,
                    author: source.get("search_author").and_then(nonempty),
                    format: source
                        .get("search_extension")
                        .and_then(nonempty)
                        .map(|e| e.to_uppercase()),
                    size: filesize.map(format_filesize),
                    language: source
                        .get("search_most_likely_language_code")
                        .and_then(Value::as_array)
                        .and_then(|codes| {
                            codes.iter().find_map(|c| {
                                c.as_str().filter(|s| !s.is_empty()).map(str::to_string)
                            })
                        }),
                });
                let _ = year;
            }
        }
        let has_more = (total as u64) > (page as u64) * (PAGE_SIZE as u64);
        Ok(SearchResponse {
            results,
            page,
            has_more,
        })
    }

    /// Same human size rounding as the HTML page's metadata line (e.g. "54.2MB", "512B").
    fn format_filesize(bytes: u64) -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if bytes >= 1024 * 1024 {
            format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.1}KB", bytes as f64 / 1024.0)
        } else {
            format!("{bytes}B")
        }
    }

    /// POST the query to `{url}/{index}/_search` and return the body.
    pub async fn search(
        client: &crate::client::Client,
        url: &str,
        query: &str,
        page: u32,
    ) -> Result<SearchResponse, Error> {
        let endpoint = format!("{}/aarecords__main__*/_search", url.trim_end_matches('/'));
        let body = build_request(query, page);
        let response = client
            .post(&endpoint)
            .json(&body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
            });
        }
        let text = response.text().await?;
        if text.len() < 2 {
            return Err(api_error("Local ES response too small"));
        }
        parse_response(&text, page)
    }
}

impl AnnasArchiveClient {
    /// Build against an explicit mirror list with a file-backed cookie jar:
    /// the session (challenge cookies, `aa_account_id2`) survives restarts.
    /// Not part of the documented surface; mock-lab + DDoS-Guard lab hook.
    #[allow(dead_code)] // V3 lab surface: wired by the MCP layer / dg-solve consumer
    pub(crate) fn build_with_jar(
        api_key: Option<String>,
        domains: Vec<String>,
        jar_path: PathBuf,
    ) -> Self {
        Self::build_with_shared_jar(api_key, domains, Arc::new(FileJar::load(jar_path)))
    }

    /// Same, but the caller supplies the jar instance itself: imports made
    /// through that handle are visible to every request (fix1-cookies).
    pub(crate) fn build_with_shared_jar(
        api_key: Option<String>,
        domains: Vec<String>,
        jar: Arc<FileJar>,
    ) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .cookie_provider(jar)
            .timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(20))
            .tcp_keepalive(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let keepalive = KeepAliveCtl::new(client.clone(), format!("https://{}/", DOMAINS[0]));

        Self {
            lenient_records: false,
            client,
            api_key,
            authenticated: AtomicBool::new(false),
            domains,
            request_coalescing: false,
            search_backend: SearchBackend::Remote,
            inflight_details: Mutex::new(HashMap::new()),
            inflight_downloads: Mutex::new(HashMap::new()),
            keepalive,
            challenge_solver: None,
        }
    }
}

enum Request<'a> {
    Document(String, u64, bool),
    FastDownload(&'a str, u32, u32, &'a str),
}

#[derive(Deserialize)]
struct FastDownloadResponse {
    download_url: Option<String>,
    error: Option<String>,
}

/// Typed result of a gated fast_download probe.
enum FastVerdict {
    Url(String),
    /// `error` field present in the body.
    ApiError(String),
    /// 2xx body parsed fine but carried no URL.
    NoUrl,
}

impl FastVerdict {
    fn into_result(self) -> Result<String, Error> {
        match self {
            FastVerdict::Url(u) => Ok(u),
            FastVerdict::ApiError(e) => Err(Error::Api { message: e }),
            FastVerdict::NoUrl => Err(api_error("No download URL in response")),
        }
    }
}

/// Reference DDoS-Guard challenge flow, testable end-to-end against a local
/// mock: GET `{origin}/check.js` (solver script placement), GET the
/// `id-beacon` path the script response points at (the guard's cookie
/// minter), then GET `{origin}/?chek=1` to confirm the cookie unblocks the
/// origin. Any non-success terminal response fails the flow.
pub async fn warm_cookies(client: &Client, origin_url: &str) -> Result<(), Error> {
    let check_js = format!("{origin_url}/check.js");
    let script = client
        .get(&check_js)
        .send()
        .await
        .map_err(Error::Network)?
        .error_for_status()
        .map_err(|e| blocked(format!("check.js fetch failed: {e}")))?;
    let script_body = read_body_capped(script, MAX_CHALLENGE_BODY).await?;

    // The script body names its beacon endpoint; grab the first path it
    // references (the real script would be evaluated by the solver lab).
    let beacon_path = script_body
        .split(['"', '\''])
        .find(|token| token.starts_with('/'))
        .ok_or_else(|| blocked("check.js carries no beacon path"))?;
    let beacon_url = format!("{origin_url}{beacon_path}");
    client
        .get(&beacon_url)
        .send()
        .await
        .map_err(Error::Network)?
        .error_for_status()
        .map_err(|e| blocked(format!("beacon fetch failed: {e}")))?;

    let chek_url = format!("{origin_url}/?chek=1");
    client
        .get(&chek_url)
        .send()
        .await
        .map_err(Error::Network)?
        .error_for_status()
        .map_err(|e| blocked(format!("challenge verification failed: {e}")))?;
    Ok(())
}

/// Bench-shim view of the fast_download wire struct (examples/bench.rs compiles
/// src modules standalone via #[path]; dead in the lib build itself).
#[doc(hidden)]
#[allow(dead_code)]
pub mod __bench {
    #[doc(hidden)]
    #[derive(serde::Deserialize)]
    pub struct FastDownloadResponse {
        pub download_url: Option<String>,
        pub error: Option<String>,
    }
}

/// Borrowed entry of the /dyn/torrents.json index; strings are zero-copy unless source escapes force an owned copy.
#[derive(Deserialize)]
pub struct TorrentEntryRaw<'a> {
    #[serde(borrow)]
    pub url: Cow<'a, str>,
    #[serde(borrow)]
    pub top_level_group_name: Cow<'a, str>,
    #[serde(borrow)]
    pub group_name: Cow<'a, str>,
    #[serde(borrow)]
    pub display_name: Cow<'a, str>,
    #[serde(borrow)]
    pub added_to_torrents_list_at: Cow<'a, str>,
    pub is_metadata: bool,
    #[serde(borrow)]
    pub btih: Cow<'a, str>,
    #[serde(borrow)]
    pub magnet_link: Cow<'a, str>,
    pub torrent_size: u64,
    pub num_files: u64,
    pub data_size: u64,
    pub aa_currently_seeding: bool,
    pub obsolete: bool,
    pub embargo: bool,
    pub seeders: u64,
    pub leechers: u64,
    pub completed: u64,
    #[serde(borrow)]
    pub stats_scraped_at: Cow<'a, str>,
    pub partially_broken: bool,
    #[serde(borrow)]
    pub random: Cow<'a, str>,
}

/// Parse the torrents index into borrowed entries, tolerantly: homogeneous slice parse first, then per-entry salvage skipping malformed ones; an error only when *every* entry fails.
pub fn parse_torrents(json: &str) -> Result<Vec<TorrentEntryRaw<'_>>, Error> {
    if let Ok(entries) = serde_json::from_str(json) {
        return Ok(entries);
    }

    let mut state = TorrentSalvageState {
        entries: Vec::new(),
        skipped: 0,
        array: false,
    };
    let walk = serde_json::Deserializer::from_str(json).deserialize_any(&mut state);
    match walk {
        Err(e) => {
            return Err(malformed_json(format!(
                "Failed to parse torrents JSON: {e}"
            )));
        }
        Ok(()) if state.array && state.entries.is_empty() && state.skipped > 0 => {
            return Err(malformed_json(format!(
                "Failed to parse torrents JSON: all {} entries invalid",
                state.skipped
            )));
        }
        Ok(()) => {}
    }
    Ok(state.entries)
}

struct TorrentSalvageState<'de> {
    entries: Vec<TorrentEntryRaw<'de>>,
    skipped: usize,
    array: bool,
}

impl<'de> serde::de::Visitor<'de> for &'_ mut TorrentSalvageState<'de> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON array of torrent entries")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        self.array = true;
        while let Some(raw) = seq.next_element::<&serde_json::value::RawValue>()? {
            match serde_json::from_str::<TorrentEntryRaw>(raw.get()) {
                Ok(entry) => self.entries.push(entry),
                Err(_) => self.skipped += 1,
            }
        }
        Ok(())
    }
}

/// Response byte caps: abort oversized or drip-fed bodies before they can OOM.
/// Read cap for challenge artifacts (check.js script body).
const MAX_CHALLENGE_BODY: u64 = 256 * 1024;

const MAX_SEARCH_BODY: u64 = 4 * 1024 * 1024;
pub(crate) const MAX_RECORD_BODY: u64 = 16 * 1024 * 1024;

/// Read a response body as text under a byte cap.
pub(crate) async fn read_body_capped(
    mut response: reqwest::Response,
    cap: u64,
) -> Result<String, Error> {
    let declared = response.content_length();
    let mut body: Vec<u8> = Vec::with_capacity(declared.map_or(0, |len| len.min(cap)) as usize);
    while let Some(chunk) = response.chunk().await.map_err(Error::Network)? {
        if body.len() as u64 + chunk.len() as u64 > cap {
            return Err(parse_error(
                format!("Response exceeded {cap}-byte cap"),
                ParseKind::BodyTooLarge,
            ));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body)
        .map_err(|_| parse_error("Response was not valid UTF-8", ParseKind::EncodingInvalid))
}

/// Assemble the fast_download.json URL without fmt machinery (push_str + itoa).
pub(crate) fn build_fast_download_url(
    out: &mut String,
    domain: &str,
    md5: &str,
    path_index: u32,
    domain_index: u32,
    key: &str,
) {
    fast_download_query_prefix(out, domain, md5);
    let mut buf = itoa::Buffer::new();
    out.push_str("&path_index=");
    out.push_str(buf.format(path_index));
    out.push_str("&domain_index=");
    out.push_str(buf.format(domain_index));
    out.push_str("&key=");
    out.push_str(key);
}

/// `{origin}/dyn/api/fast_download.json?md5={md5}` query stem.
/// Inline scheme push: origin() would malloc a throwaway `https://{domain}` String per call.
fn fast_download_query_prefix(out: &mut String, domain: &str, md5: &str) {
    out.clear();
    if !domain.contains("://") {
        out.push_str("https://");
    }
    out.push_str(domain);
    out.push_str("/dyn/api/fast_download.json?md5=");
    out.push_str(md5);
}

/// Std-only percent-encoding (replaces urlencoding::encode): RFC 3986 unreserved bytes raw, uppercase %XX-escapes every other UTF-8 byte.
fn pct_encode(s: &str) -> Cow<'_, str> {
    fn keep(b: u8) -> bool {
        b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
    }
    if s.bytes().all(keep) {
        return s.into();
    }
    let mut out = String::with_capacity(s.len() * 3);
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in s.as_bytes() {
        if keep(b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out.into()
}

/// `parse_error(.., ParseKind::MalformedJson)` for a preformatted message.
fn malformed_json(message: impl Into<String>) -> Error {
    parse_error(message, ParseKind::MalformedJson)
}

/// Origin URL prefix for a mirror entry: bare hostnames get https, mock-lab entries carry their explicit scheme.
fn origin(domain: &str) -> Cow<'_, str> {
    if domain.contains("://") {
        Cow::Borrowed(domain)
    } else {
        Cow::Owned(format!("https://{domain}"))
    }
}

/// Classify a fast_download response in-loop: 2xx bodies parse once into a typed `FastVerdict`; non-2xx bodies keep the substring sniff (no_membership / invalid). `Err` = walk mirrors; `Ok` = terminal.
fn classify_fast_body(status: reqwest::StatusCode, body: &str) -> Result<FastVerdict, Error> {
    if status.is_success() {
        let api: FastDownloadResponse = serde_json::from_str(body)
            .map_err(|e| malformed_json(format!("Failed to parse fast-download JSON: {e}")))?;
        if let Some(error) = api.error {
            return Ok(FastVerdict::ApiError(error));
        }
        return Ok(api
            .download_url
            .map(FastVerdict::Url)
            .unwrap_or(FastVerdict::NoUrl));
    }
    if body.contains("no_membership") {
        return Err(api_error("No active membership for this API key"));
    }
    if body.contains("invalid") {
        return Err(api_error("Invalid API key"));
    }
    Err(Error::Http {
        status: status.as_u16(),
    })
}

/// Membership tier implied by the configured API key, classified through the typed gate on fast_download.json.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipTier {
    Active,
    NoMemberShip,
    InvalidKey,
}

impl MembershipTier {
    /// Canonical tier string for tool output (mcp `get_membership_status`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::NoMemberShip => "no_membership",
            Self::InvalidKey => "invalid_key",
        }
    }
}

const MEMBERSHIP_PROBE_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";

/// Search transport selection. `Remote` (default) scrapes the mirror HTML;
/// `LocalEs { url }` queries a local Elasticsearch instance carrying the
/// `aa_derived_mirror_metadata` index (e.g. from the Anna's Archive
/// torrents), bypassing the mirrors and DDoS-Guard entirely.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchBackend {
    Remote,
    /// Base URL of the local ES node, e.g. `http://localhost:9200`.
    LocalEs {
        url: String,
    },
}

pub struct AnnasArchiveClient {
    client: Client,
    api_key: Option<String>,
    authenticated: AtomicBool,
    /// Mirrors in try order: bare hostnames (https implied) or explicit-scheme origins for mock-server labs.
    pub(crate) domains: Vec<String>,
    request_coalescing: bool,
    lenient_records: bool,
    /// Search transport selection (fix2): Remote (default) or LocalEs { url }.
    search_backend: SearchBackend,
    inflight_details: Mutex<HashMap<Arc<str>, Arc<OnceCell<ItemDetails>>>>,
    inflight_downloads: Mutex<HashMap<Arc<str>, Arc<OnceCell<DownloadInfo>>>>,
    keepalive: Arc<KeepAliveCtl>,
    challenge_solver: Option<ChallengeSolver>,
}

impl AnnasArchiveClient {
    pub fn new(api_key: Option<String>) -> Self {
        Self::build(api_key, DOMAINS.iter().map(|d| (*d).to_string()).collect())
    }

    /// Replace the mirror list (mock-lab injection; not part of the documented surface). Callers must pass explicit-scheme origins or bare hostnames as in `new`.
    #[doc(hidden)]
    pub fn set_mirror_domains(&mut self, domains: Vec<String>) {
        self.domains = domains;
    }

    /// Same construction against an explicit mirror list (mock labs); not part of the documented surface.
    pub(crate) fn build(api_key: Option<String>, domains: Vec<String>) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .cookie_provider(Arc::new(Jar::default()))
            .timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(20))
            .tcp_keepalive(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let keepalive = KeepAliveCtl::new(client.clone(), format!("https://{}/", DOMAINS[0]));

        Self {
            lenient_records: false,
            client,
            api_key,
            authenticated: AtomicBool::new(false),
            domains,
            request_coalescing: false,
            search_backend: SearchBackend::Remote,
            inflight_details: Mutex::new(HashMap::new()),
            inflight_downloads: Mutex::new(HashMap::new()),
            keepalive,
            challenge_solver: None,
        }
    }

    /// Opt into the local-Elasticsearch search backend: `search()` queries
    /// `{url}/aarecords__main__*/_search` with the upstream query DSL
    /// instead of scraping the mirror HTML. Default off (Remote).
    pub fn with_search_backend(mut self, backend: SearchBackend) -> Self {
        self.search_backend = backend;
        self
    }

    /// Install the DDoS-Guard challenge solver (parallel-lab hook): on a
    /// detected challenge it runs once with the original URL, then the
    /// request is retried exactly once.
    pub fn with_challenge_solver<F, Fut>(mut self, solver: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.challenge_solver = Some(ChallengeSolver(Arc::new(move |url| Box::pin(solver(url)))));
        self
    }

    /// Swap in a caller-owned cookie jar (fix1-cookies): imports made
    /// through the same handle are visible to every subsequent request.
    /// Consumes self so it composes with the other `with_*` builders.
    pub fn with_shared_jar(self, jar: Arc<FileJar>) -> Self {
        let Self {
            client: _,
            api_key,
            authenticated,
            domains,
            request_coalescing,
            lenient_records,
            inflight_details,
            inflight_downloads,
            keepalive: _,
            challenge_solver,
            search_backend,
        } = self;
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .cookie_provider(jar)
            .timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(20))
            .tcp_keepalive(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        let keepalive = KeepAliveCtl::new(client.clone(), format!("https://{}/", DOMAINS[0]));
        Self {
            lenient_records,
            client,
            api_key,
            authenticated,
            domains,
            search_backend,
            request_coalescing,
            inflight_details,
            inflight_downloads,
            keepalive,
            challenge_solver,
        }
    }

    /// Opt into lenient record parsing (default OFF = fail-fast, byte-equal). ON: field-level deviations degrade per-field; structural failures error identically in both modes.
    pub fn with_lenient_records(mut self, enabled: bool) -> Self {
        self.lenient_records = enabled;
        self
    }

    /// Opt into single-flight coalescing: concurrent identical calls share one upstream fetch; late joiners clone the leader's result. Default off — zero behavior change.
    pub fn with_request_coalescing(mut self, enabled: bool) -> Self {
        self.request_coalescing = enabled;
        self
    }

    /// The active search transport (fix2); lets the MCP layer skip
    /// mirror-side prewarm when the LocalEs backend is selected.
    pub fn search_backend(&self) -> &SearchBackend {
        &self.search_backend
    }

    /// Run `op` under single-flight coalescing for `key` when enabled.
    async fn coalesced<T, F, Fut>(
        &self,
        inflight: &Mutex<HashMap<Arc<str>, Arc<OnceCell<T>>>>,
        key: &str,
        op: F,
    ) -> Result<T, Error>
    where
        T: Clone,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        if !self.request_coalescing {
            return op().await;
        }
        let cell = {
            let mut map = inflight.lock().expect("inflight map poisoned");
            map.entry(Arc::from(key))
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let value = cell.get_or_try_init(op).await?.clone();
        let mut map = inflight.lock().expect("inflight map poisoned");
        if map
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            map.remove(key);
        }
        Ok(value)
    }

    /// Authenticate with Anna's Archive: sets the aa_account_id2 cookie.
    async fn authenticate(&self) -> Result<(), Error> {
        let api_key = self.api_key.as_ref().ok_or(Error::MissingApiKey)?;

        for domain in &self.domains {
            let url = format!("{}/account/", origin(domain));

            match self
                .client
                .post(&url)
                .form(&[("key", api_key.as_str())])
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() || resp.status().is_redirection() => {
                    self.authenticated.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Ok(resp) if resp.status().is_client_error() => {
                    return Err(api_error("Invalid secret key"));
                }
                _ => continue,
            }
        }

        Err(all_domains_error("Failed to authenticate with any domain"))
    }

    /// Classify a response as the DDoS-Guard challenge from wire metadata
    /// alone (final status, final URL query, Location header). The body is
    /// never read here, so plain redirects can never match.
    fn challenge_from_wire(response: &Response) -> Option<String> {
        let status = response.status().as_u16();
        let query = response.url().query().map(str::to_string);
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        is_ddos_guard_challenge(status, query.as_deref(), location.as_deref(), "")
            .then(|| response.url().to_string())
    }

    /// Run the injected solver once for a detected challenge. Without a
    /// solver the challenge surfaces as [`Error::Blocked`] ("site is
    /// challenging us") instead of a raw 403.
    async fn solve_challenge(&self, challenged: String) -> Result<(), Error> {
        let Some(solver) = self.challenge_solver.as_ref() else {
            return Err(blocked("DDoS-Guard challenge (no solver installed)"));
        };
        (solver.0)(challenged)
            .await
            .map_err(|message| blocked(format!("DDoS-Guard challenge: {message}")))
    }

    /// GET each domain in turn until one satisfies the request's policy. Documents abort on 4xx (403 may re-authenticate once); fast_download sniffs error bodies and falls through on any failure.
    async fn failover(&self, request: Request<'_>) -> Result<String, Error> {
        self.keepalive.ensure_pinger();
        let document = matches!(request, Request::Document(..));
        let (mut cap, mut reauthenticate) = (MAX_RECORD_BODY, false);
        let mut last_error = None;

        for domain in &self.domains {
            let mut url = String::with_capacity(160);
            match &request {
                Request::Document(path, doc_cap, doc_reauth) => {
                    url.push_str(&origin(domain));
                    url.push_str(path);
                    (cap, reauthenticate) = (*doc_cap, *doc_reauth);
                }
                Request::FastDownload(md5, pi, di, key) => {
                    build_fast_download_url(&mut url, domain, md5, *pi, *di, key);
                }
            }

            let started = Instant::now();
            let mut response = match self.client.get(&url).send().await {
                Ok(response) => response,
                Err(e) => {
                    last_error = Some(Error::Network(e));
                    continue;
                }
            };
            if response.status().is_success() {
                self.keepalive.observe_request(started.elapsed());
            }

            // DDoS-Guard checkpoint: redirect-to-chek and guard-403-with-chek-
            // URL are detected from wire metadata alone (plain redirects in
            // existing mocks and live flows never match); the challenge retry
            // budget is one solver pass per domain. Without a solver the
            // challenge surfaces as Error::Blocked right here.
            if let Some(challenged) = Self::challenge_from_wire(&response) {
                self.solve_challenge(challenged).await?;
                response = match self.client.get(&url).send().await {
                    Ok(response) => response,
                    Err(e) => {
                        last_error = Some(Error::Network(e));
                        continue;
                    }
                };
                if response.status().is_success() {
                    self.keepalive.observe_request(started.elapsed());
                }
            }

            // Body is read before the document 4xx gate so a 403 guard page
            // (direct, no redirect chain) can be told apart from the
            // membership/re-auth 403s by its body snippet.
            let status = response.status();
            let challenged_landing = status.as_u16() == 403
                && response
                    .url()
                    .query()
                    .is_some_and(|q| q.contains(DDOS_GUARD_CHEK));
            let mut body = match read_body_capped(response, cap).await {
                Ok(body) => body,
                Err(_) if !status.is_success() => String::new(),
                Err(e) if !document => {
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            if !document {
                match classify_fast_body(status, &body) {
                    Err(last) => {
                        last_error = Some(last);
                        continue;
                    }
                    Ok(v) => return v.into_result(),
                }
            }

            if document && status.is_client_error() {
                let status = status.as_u16();
                // Guard-page 403 (chek URL or body marker): blocked client,
                // not an authorization failure.
                if status == 403
                    && (challenged_landing
                        || body.to_ascii_lowercase().contains(DDOS_GUARD_BODY_SNIPPET))
                {
                    return Err(blocked(format!(
                        "still challenged after solver pass: HTTP 403 at {domain}"
                    )));
                }
                if status != 403 || !reauthenticate {
                    return Err(Error::Http { status });
                }
                self.authenticated.store(false, Ordering::SeqCst);
                self.authenticate().await?;
                let response = match self.client.get(&url).send().await {
                    Ok(resp) => resp,
                    _ => return Err(Error::Http { status }),
                };
                if !response.status().is_success() {
                    return Err(Error::Http { status });
                }
                self.keepalive.observe_request(started.elapsed());
                body = read_body_capped(response, cap).await?;
            }

            if document && body.len() < MIN_DOCUMENT_BODY_BYTES {
                last_error = Some(parse_error(
                    format!(
                        "Suspiciously small success body ({}/{} bytes) from {domain}",
                        body.len(),
                        MIN_DOCUMENT_BODY_BYTES
                    ),
                    ParseKind::GarbagePage,
                ));
                continue;
            }

            return Ok(body);
        }

        Err(last_error.unwrap_or_else(|| all_domains_error("No domains available")))
    }

    /// Warm the primary mirror before user-visible calls. Purely optional.
    pub async fn prewarm(&self) -> Result<(), Error> {
        let url = format!("{}/", origin(&self.domains[0]));
        let head = self
            .client
            .head(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await;
        match head.ok().map(|resp| resp.status()) {
            Some(status) if !matches!(status.as_u16(), 405 | 501) => return Ok(()),
            _ => {}
        }
        self.client
            .get(&url)
            .send()
            .await
            .map(|_| ())
            .map_err(Error::Network)
    }

    /// Warm-start: run the DDoS-Guard challenge flow once so later calls are
    /// already authenticated. Resolves the challenge for the primary mirror
    /// via the installed solver, then verifies the retry with a HEAD against
    /// the homepage. Errors as [`Error::Blocked`] when no solver is present
    /// and the site is challenging plain requests.
    pub async fn prewarm_session(&self) -> Result<(), Error> {
        let url = format!("{}/", origin(&self.domains[0]));
        let response = self
            .client
            .head(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(Error::Network)?;
        let status = response.status().as_u16();
        if status != 403 {
            return Ok(());
        }
        let challenged = format!("{url}?chek=1");
        self.solve_challenge(challenged).await?;
        Ok(())
    }

    /// Membership tier from fast_download.json's typed gate: 200/204 ⇒ Active, 403 ⇒ NoMemberShip, 401 ⇒ InvalidKey. Response dropped unread (quota untouched).
    pub async fn membership_status(&self) -> Result<MembershipTier, Error> {
        let key = self.api_key.as_ref().ok_or(Error::MissingApiKey)?;
        let mut last_error = None;

        for domain in &self.domains {
            let mut url = String::with_capacity(160);
            fast_download_query_prefix(&mut url, domain, MEMBERSHIP_PROBE_MD5);
            url.push_str("&key=");
            url.push_str(key);

            match self.client.get(&url).send().await {
                Ok(resp) => match resp.status().as_u16() {
                    401 => return Ok(MembershipTier::InvalidKey),
                    403 => return Ok(MembershipTier::NoMemberShip),
                    200 | 204 => return Ok(MembershipTier::Active),
                    status => last_error = Some(Error::Http { status }),
                },
                Err(e) => last_error = Some(Error::Network(e)),
            }
        }

        Err(last_error.unwrap_or(all_domains_error("Membership probe failed on all domains")))
    }

    pub async fn search(&self, options: SearchOptions) -> Result<SearchResponse, Error> {
        let page = options.page.unwrap_or(1);
        if let SearchBackend::LocalEs { url } = &self.search_backend {
            return es::search(&self.client, url, &options.query, page).await;
        }
        let query = pct_encode(&options.query);
        let path = format!("/search?q={query}&page={page}");
        let request = Request::Document(path, MAX_SEARCH_BODY, false);
        let html = self.failover(request).await?;
        let (results, has_more) = parse_search_results(&html)?;

        Ok(SearchResponse {
            results,
            page,
            has_more,
        })
    }

    /// Get detailed metadata for an item.
    pub async fn get_details(&self, md5: &str) -> Result<ItemDetails, Error> {
        if self.api_key.is_some() && !self.authenticated.load(Ordering::SeqCst) {
            self.authenticate().await?;
        }

        // Keyed: /dyn/api/aarecord_elasticsearch/md5:<md5>.json?key= — DDoS-Guard-exempt.
        // Fallback: /db/aarecord_elasticsearch/... (guard-exempt; membership-gated with a
        // key, open cookie-less otherwise — the live site serves the full record to
        // anonymous GETs on the current mirrors).
        let request = match self.api_key.as_ref() {
            Some(key) => Request::Document(
                format!("/dyn/api/aarecord_elasticsearch/md5:{md5}.json?key={key}"),
                MAX_RECORD_BODY,
                false,
            ),
            None => Request::Document(
                format!("/db/aarecord_elasticsearch/md5:{md5}.json"),
                MAX_RECORD_BODY,
                false,
            ),
        };
        self.coalesced(&self.inflight_details, md5, || async {
            match self.failover(request).await {
                Ok(json_str) => parse_json_details_mode(&json_str, md5, self.lenient_records),
                Err(_) if self.api_key.is_some() => {
                    let path = format!("/db/aarecord_elasticsearch/md5:{md5}.json");
                    let request = Request::Document(path, MAX_RECORD_BODY, true);
                    let json_str = self.failover(request).await?;
                    parse_json_details_mode(&json_str, md5, self.lenient_records)
                }
                Err(e) => Err(e),
            }
        })
        .await
    }

    pub async fn get_download_url(
        &self,
        md5: &str,
        path_index: Option<u32>,
        domain_index: Option<u32>,
    ) -> Result<DownloadInfo, Error> {
        let key = self.api_key.as_ref().ok_or(Error::MissingApiKey)?;
        let (pi, di) = (path_index.unwrap_or(0), domain_index.unwrap_or(0));
        let request = Request::FastDownload(md5, pi, di, key);
        let flight_key = format!("{md5}/{pi}/{di}");
        self.coalesced(&self.inflight_downloads, &flight_key, || async {
            let download_url = self.failover(request).await?;
            Ok(DownloadInfo { download_url })
        })
        .await
    }

    /// Builder-style dynamic mirror discovery: `AnnasArchiveClient::new(key)?.with_dynamic_mirrors(true).await?`. See [`enable_dynamic_mirrors`].
    pub async fn with_dynamic_mirrors(self, enabled: bool) -> Result<Self, Error> {
        enable_dynamic_mirrors(self, enabled).await
    }
}

/// Borrowed string field: zero-copy unless escapes force an owned copy.
type Str<'a> = Cow<'a, str>;

fn non_empty(s: Option<Str>) -> Option<String> {
    s.filter(|s| !s.is_empty()).map(|s| s.into_owned())
}

fn non_empty_list(v: Option<Vec<Str>>) -> Option<Vec<String>> {
    v.map(|v| v.iter().map(|s| s.to_string()).collect())
}

fn first_str(v: Option<Vec<Str>>) -> Option<String> {
    v.and_then(|v| v.first().map(|s| s.to_string()))
}

fn str_or(v: Option<Str>, default: &str) -> String {
    v.map_or_else(|| default.to_string(), |s| s.into_owned())
}

/// Top-level shape of the aarecord JSON response.
#[derive(Deserialize)]
struct Record<'a> {
    error: Option<Str<'a>>,
    file_unified_data: Option<FileData<'a>>,
    additional: Option<Additional>,
}

/// file_unified_data: all fields optional; absent/null map to None exactly like the previous `Value` navigation.
#[derive(Deserialize)]
struct FileData<'a> {
    title_best: Option<Str<'a>>,
    author_best: Option<Str<'a>>,
    extension_best: Option<Str<'a>>,
    filesize_best: Option<u64>,
    language_codes: Option<Vec<Str<'a>>>,
    publisher_best: Option<Str<'a>>,
    year_best: Option<Str<'a>>,
    stripped_description_best: Option<Str<'a>>,
    cover_url_best: Option<Str<'a>>,
    content_type_best: Option<Str<'a>>,
    original_filename_best: Option<Str<'a>>,
    added_date_best: Option<Str<'a>>,
    pages_best: Option<Str<'a>>,
    edition_varia_best: Option<Str<'a>>,
    series_best: Option<Str<'a>>,
    identifiers_unified: Option<IdentifiersRaw<'a>>,
    classifications_unified: Option<BTreeMap<Str<'a>, Vec<Str<'a>>>>,
    ipfs_infos: Option<Vec<IpfsCidRaw<'a>>>,
}

#[derive(Deserialize)]
struct IpfsCidRaw<'a> {
    ipfs_cid: Str<'a>,
    from: Option<Str<'a>>,
}

/// additional: only the three list-shaped keys are needed, each captured via `Value` navigation.
#[derive(Deserialize)]
struct Additional {
    download_urls: Option<StrArray>,
    ipfs_urls: Option<StrArray>,
    torrent_paths: Option<StrArray>,
}

/// Strings of a top-level JSON array; any non-array value yields empty.
#[derive(Default)]
struct StrArray(Vec<String>);
impl<'de> Deserialize<'de> for StrArray {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&serde_json::value::RawValue>::deserialize(deserializer)?;
        let elements = serde_json::from_str::<Vec<&serde_json::value::RawValue>>(raw.get())
            .unwrap_or_default();
        Ok(Self(
            elements
                .iter()
                .filter(|e| e.get().starts_with('"'))
                .filter_map(|e| serde_json::from_str::<String>(e.get()).ok())
                .collect(),
        ))
    }
}

/// Parse item details from the JSON API response (strict mode, unchanged).
pub fn parse_json_details(json_str: &str, md5: &str) -> Result<ItemDetails, Error> {
    parse_json_details_mode(json_str, md5, false)
}

/// Parse item details with lenient-records policy: strict typed parse first; on field-level failure, sanitized re-parse drops wrong-typed fields. Structural failures error in both modes.
pub fn parse_json_details_mode(
    json_str: &str,
    md5: &str,
    lenient: bool,
) -> Result<ItemDetails, Error> {
    let trimmed = json_str.trim();
    let owned;
    let json_str = if trimmed.starts_with('"') && trimmed.ends_with('"') {
        owned = serde_json::from_str::<String>(trimmed)
            .map_err(|e| malformed_json(format!("Failed to parse outer JSON: {e}")))?;
        owned.as_str()
    } else {
        trimmed
    };

    let record: Record = match serde_json::from_str(json_str) {
        Ok(record) => record,
        Err(e) if lenient => {
            return salvage_lenient(json_str, md5, e);
        }
        Err(e) => {
            return Err(malformed_json(format!("Failed to parse JSON: {e}")));
        }
    };

    if let Some(error) = record.error {
        return Err(api_error(error));
    }

    let parts = StrictParts::from_record(record)?;
    finish_details(md5, parts)
}

/// Assemble `ItemDetails` from the parsed record parts; shared by the strict parse and the lenient salvage.
fn finish_details(md5: &str, parts: StrictParts<'_>) -> Result<ItemDetails, Error> {
    let StrictParts {
        fud,
        size_bytes,
        ref classifications,
        download_sources,
        torrent_paths,
    } = parts;
    Ok(ItemDetails {
        md5: md5.to_string(),
        title: str_or(fud.title_best, "Unknown"),
        author: non_empty(fud.author_best),
        format: fud
            .extension_best
            .filter(|s| !s.is_empty())
            .map(|s| s.to_uppercase()),
        size: size_bytes.map(format_filesize),
        size_bytes,
        language: first_str(fud.language_codes),
        publisher: non_empty(fud.publisher_best),
        year: non_empty(fud.year_best),
        description: non_empty(fud.stripped_description_best),
        cover_url: non_empty(fud.cover_url_best),
        content_type: non_empty(fud.content_type_best),
        original_filename: non_empty(fud.original_filename_best),
        added_date: non_empty(fud.added_date_best),
        pages: non_empty(fud.pages_best),
        edition: non_empty(fud.edition_varia_best),
        series: non_empty(fud.series_best),
        identifiers: fud.identifiers_unified.and_then(parse_identifiers),
        categories: classifications.as_ref().and_then(categories),
        subjects: classifications.as_ref().and_then(subjects),
        ipfs_cids: fud.ipfs_infos.map(|infos| {
            infos
                .into_iter()
                .map(|c| IpfsInfo {
                    cid: c.ipfs_cid.into_owned(),
                    from: str_or(c.from, "unknown"),
                })
                .collect()
        }),
        download_sources,
        torrent_paths,
    })
}

/// Lenient fallback: re-navigate the body as `Value`, dropping fields whose shape deviates from the typed schema before re-running the strict parse; the structural gates stay strict.
fn salvage_lenient(
    json_str: &str,
    md5: &str,
    strict_error: serde_json::Error,
) -> Result<ItemDetails, Error> {
    let mut value: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| malformed_json(format!("Failed to parse JSON: {e}")))?;

    if let Some(error) = value.get("error").and_then(|e| e.as_str()) {
        return Err(api_error(error));
    }
    let Some(fud) = value.get_mut("file_unified_data").filter(|f| f.is_object()) else {
        return Err(Error::Parse {
            message: "Missing file_unified_data".to_string(),
            kind: ParseKind::MalformedJson,
        });
    };

    // Wrong-typed fields drop whole; null survives (typed Option). One table: key → its JSON shape predicate, 18 entries covering the 4 former loops.
    type ShapeFn = fn(&serde_json::Value) -> bool;
    let shape_ok: [(&str, ShapeFn); 18] = [
        ("title_best", serde_json::Value::is_string),
        ("author_best", serde_json::Value::is_string),
        ("extension_best", serde_json::Value::is_string),
        ("publisher_best", serde_json::Value::is_string),
        ("year_best", serde_json::Value::is_string),
        ("stripped_description_best", serde_json::Value::is_string),
        ("cover_url_best", serde_json::Value::is_string),
        ("content_type_best", serde_json::Value::is_string),
        ("original_filename_best", serde_json::Value::is_string),
        ("added_date_best", serde_json::Value::is_string),
        ("pages_best", serde_json::Value::is_string),
        ("edition_varia_best", serde_json::Value::is_string),
        ("series_best", serde_json::Value::is_string),
        ("language_codes", serde_json::Value::is_array),
        ("filesize_best", serde_json::Value::is_u64),
        ("identifiers_unified", serde_json::Value::is_object),
        ("classifications_unified", serde_json::Value::is_object),
        ("ipfs_infos", serde_json::Value::is_array),
    ];
    let obj = fud.as_object_mut().expect("checked object");
    for (key, ok) in shape_ok {
        if obj.get(key).is_some_and(|v| !v.is_null() && !ok(v)) {
            obj.remove(key);
        }
    }

    // Residual errors after re-parse are schema breaks outside the degradation classes.
    serde_json::from_value::<Record>(value)
        .map_err(|e| {
            malformed_json(format!(
                "Failed to parse JSON: {strict_error} (lenient salvage: {e})"
            ))
        })
        .and_then(|record| {
            let parts = StrictParts::from_record(record)?;
            finish_details(md5, parts)
        })
}

/// Record → `finish_details` inputs, shared by the strict parse and the lenient salvage.
struct StrictParts<'a> {
    fud: FileData<'a>,
    size_bytes: Option<u64>,
    classifications: Option<BTreeMap<Str<'a>, Vec<Str<'a>>>>,
    download_sources: Option<Vec<DownloadSource>>,
    torrent_paths: Option<Vec<String>>,
}

impl<'a> StrictParts<'a> {
    fn from_record(record: Record<'a>) -> Result<Self, Error> {
        let fud = record.file_unified_data.ok_or_else(|| Error::Parse {
            message: "Missing file_unified_data".to_string(),
            kind: ParseKind::MalformedJson,
        })?;
        let size_bytes = fud.filesize_best;
        let classifications = fud.classifications_unified.clone();
        let (download_sources, torrent_paths) = match record.additional {
            Some(Additional {
                download_urls,
                ipfs_urls,
                torrent_paths: paths,
            }) => (
                download_sources(download_urls, ipfs_urls),
                torrent_paths(paths),
            ),
            None => (None, None),
        };
        Ok(Self {
            fud,
            size_bytes,
            classifications,
            download_sources,
            torrent_paths,
        })
    }
}

/// Declares the borrowed identifier wire shape and its owned conversion: each field is an optional string list converted by `non_empty_list` or `first_str`; `primary` fields alone qualify the record for an `Identifiers` value.
macro_rules! raw_identifiers {
    (
        $( $(#[$m:meta])* $field:ident : $conv:path ),+ $(,)?; primary: $($p:ident),+ $(,)?
    ) => {
        #[derive(Deserialize)]
        struct IdentifiersRaw<'a> {
            $( $(#[$m])* $field: Option<Vec<Str<'a>>>, )+
        }

        fn parse_identifiers(raw: IdentifiersRaw<'_>) -> Option<Identifiers> {
            let id = Identifiers {
                $( $field: $conv(raw.$field), )+
            };
            ($(id.$p.is_some())||+).then_some(id)
        }
    };
}

raw_identifiers! {
    isbn10: non_empty_list, isbn13: non_empty_list, doi: non_empty_list, asin: non_empty_list,
    sha1: first_str, sha256: first_str, crc32: first_str, blake2b: first_str,
    #[serde(rename = "ol")] open_library: non_empty_list,
    #[serde(rename = "googlebookid")] google_books: non_empty_list,
    goodreads: non_empty_list, amazon: non_empty_list;
    primary: isbn10, isbn13, doi, asin, sha1, sha256, open_library, google_books,
}

/// Collect non-empty, first-occurrence-ordered strings.
fn push_strings(out: &mut Vec<String>, values: &[Str]) {
    for s in values {
        if !s.is_empty() && !out.iter().any(|e| e == s) {
            out.push(s.to_string());
        }
    }
}

/// Flatten classification lists (skipping "collection" and "_"-prefixed keys).
fn categories(m: &BTreeMap<Str, Vec<Str>>) -> Option<Vec<String>> {
    let mut out = Vec::new();
    for (key, values) in m {
        if key.as_ref() == "collection" || key.starts_with('_') {
            continue;
        }
        push_strings(&mut out, values);
    }
    (!out.is_empty()).then_some(out)
}

/// Subjects come from the first "*subject*" classification key.
fn subjects(m: &BTreeMap<Str, Vec<Str>>) -> Option<Vec<String>> {
    let (_, values) = m.iter().find(|(key, _)| key.contains("subject"))?;
    let mut out = Vec::new();
    push_strings(&mut out, values);
    (!out.is_empty()).then_some(out)
}

/// Owned strings captured during the parse pass.
fn download_sources(urls: Option<StrArray>, ipfs: Option<StrArray>) -> Option<Vec<DownloadSource>> {
    let mut sources = Vec::new();
    for (name, values) in [("direct", urls), ("ipfs", ipfs)] {
        for url in values.map(|v| v.0.into_iter()).unwrap_or_default() {
            sources.push(DownloadSource {
                name: name.to_string(),
                url,
            });
        }
    }
    (!sources.is_empty()).then_some(sources)
}

fn torrent_paths(paths: Option<StrArray>) -> Option<Vec<String>> {
    let paths = paths.map(|v| v.0).unwrap_or_default();
    (!paths.is_empty()).then_some(paths)
}

fn format_filesize(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes}B")
    }
}

/// Opt-in dynamic mirror discovery (default OFF). Hardcoded mirrors always first; up to 2 live-probed discoveries appended (deduped, capped). Err on discovery failure.
pub async fn enable_dynamic_mirrors(
    mut client: AnnasArchiveClient,
    enabled: bool,
) -> Result<AnnasArchiveClient, Error> {
    if enabled {
        let mut domains = std::mem::take(&mut client.domains);
        for host in discover_with(DOMAINS, &MirrorPolicy::default()).await? {
            if !domains.iter().any(|d| d.eq_ignore_ascii_case(&host)) {
                domains.push(host);
            }
        }
        domains.truncate(MAX_DOMAINS);
        client.domains = domains;
    }
    Ok(client)
}

// Mirror-fleet engine: one const-driven policy, one mutex-guarded `Warm`, discovery default-OFF.

/// Mirror+keep-alive policy: every tunable of both halves in one deserializable document.
#[rustfmt::skip]
pub struct MirrorPolicy {    pub ep: String,  pub path: std::path::PathBuf,  pub quota: u32,
    pub prior: f64,  pub hi0: f64,  pub lo_min: f64,  pub hi_max: f64,  pub tau: f64,
    pub hys: f64,  pub pct: f64,  pub frac: f64,  pub floor: f64,
    pub cap: f64,  pub burst: f64,  pub ratio: f64,  pub med_n: u16,
}
pub type DiscoveryOptions = MirrorPolicy;

impl Default for MirrorPolicy {
    #[rustfmt::skip]
    fn default() -> Self {
        Self { ep: "https://crt.name/v1/search".into(),
               path: std::env::temp_dir().join("aa-mirror-cache.json"), quota: 6,
               prior: 45.0, hi0: 90.0, lo_min: 5.0, hi_max: 120.0, tau: 300.0, hys: 0.25,
               pct: 0.35, frac: 0.7, floor: 2.0, cap: 60.0, burst: 1.0,
               ratio: 4.0, med_n: 16 }
    }
}

/// [lo, hi] keep-alive window estimate (decays toward priors); scaled time via `scale`.
#[rustfmt::skip]
struct Warm {
    lo: f64, hi: f64, applied: f64, observations: u32, last_t: Instant,
    last_act: Instant, medians: Vec<u128>, scale: f64,
}

impl Warm {
    #[rustfmt::skip]
    fn observe(&mut self, gap: f64, died: bool, c: &MirrorPolicy) {
        let k = (-self.last_t.elapsed().as_secs_f64() / (c.tau * self.scale)).exp();
        self.lo *= k; self.hi += (c.hi0 - self.hi) * (1.0 - k); self.last_t = Instant::now();
        if died { // regime change: history invalid
            self.lo = if gap <= self.lo || self.lo >= self.hi { 0.0 } else { self.lo };
            self.hi = self.hi.min(gap).max(1.0);
            if self.lo >= self.hi { (self.lo, self.hi) = (0.0, (gap * 2.0).min(c.hi_max)); }
        } else { // survived past death bound: raise it
            self.hi = if gap >= self.hi { (gap * 1.25).min(c.hi_max) } else { self.hi };
            self.lo = self.lo.max(gap).min(self.hi);
        }
        self.observations += 1;
    }
}

pub struct KeepAliveCtl {
    pending_client: Mutex<Option<(Client, String)>>, // take() = spawn-once guard
    w: Mutex<Warm>,
    cfg: MirrorPolicy,
    pings: AtomicU64,
    deaths: AtomicU64,
}

impl KeepAliveCtl {
    pub(crate) fn new(client: Client, ping_url: String) -> Arc<Self> {
        Self::with_config(client, ping_url, 1.0, MirrorPolicy::default())
    }
    /// `scale` compresses wall time (observations fed as wall-seconds ÷ scale).
    #[rustfmt::skip]
    pub(crate) fn with_config(client: Client, ping_url: String, scale: f64, cfg: MirrorPolicy) -> Arc<Self> {        Arc::new(Self {
            w: Mutex::new(Warm { lo: 0.0, hi: cfg.hi0, applied: cfg.prior, observations: 0, scale,
                                 last_t: Instant::now(), last_act: Instant::now(), medians: Vec::new() }),
            pending_client: Mutex::new(Some((client, ping_url))), cfg, pings: AtomicU64::new(0), deaths: AtomicU64::new(0),
        })
    }
    /// Spawn the background HEAD-ping task on first use; exits when the owner drops.
    #[rustfmt::skip]
    pub(crate) fn ensure_pinger(self: &Arc<Self>) {        let Some((client, url)) = self.pending_client.lock().unwrap().take() else { return };
        tokio::spawn({ let weak = Arc::downgrade(self); async move {
            while let Some(ctl) = weak.upgrade() {
                let iv = { let mut w = ctl.w.lock().unwrap();
                    let e = (w.lo + ctl.cfg.pct * (w.hi - w.lo)).clamp(ctl.cfg.lo_min, ctl.cfg.hi_max);
                    if w.observations > 0 && ((e - w.applied) / w.applied).abs() > ctl.cfg.hys { w.applied = e; }
                    Duration::from_secs_f64((ctl.cfg.frac * w.applied).clamp(ctl.cfg.floor, ctl.cfg.cap) * w.scale) };
                let deadline = ctl.w.lock().unwrap().last_act + iv;
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                if ctl.w.lock().unwrap().last_act + iv > Instant::now() { continue; }
                let sent = Instant::now();
                if client.head(&url).send().await.is_ok() {
                    ctl.pings.fetch_add(1, Ordering::Relaxed); ctl.observe_request(sent.elapsed());
                }
            }
        }});
    }
    #[rustfmt::skip]
    pub(crate) fn observe_request(&self, ttfb: Duration) {
        let (ttfb_us, now) = (ttfb.as_micros(), Instant::now());
        let mut w = self.w.lock().unwrap();
        let gap = w.last_act.elapsed().as_secs_f64() / w.scale;
        let med = { let mut m = w.medians.clone(); m.sort_unstable(); m };
        let reconnect = med.get(med.len() / 2).is_some_and(|&m| ttfb_us as f64 > self.cfg.ratio * m as f64);
        w.last_act = now;
        match reconnect.then_some(gap >= self.cfg.burst) {
            Some(true) => { self.deaths.fetch_add(1, Ordering::Relaxed); w.observe(gap, true, &self.cfg); }
            Some(false) => w.observations += 1,
            None => { w.medians.push(ttfb_us);
                      if w.medians.len() > self.cfg.med_n as usize { w.medians.remove(0); }
                      w.observe(gap, false, &self.cfg); }
        }
    }
    /// (pings, deaths) — consumed by tests/keepalive_tests.rs.
    #[allow(dead_code)]
    #[rustfmt::skip]
    pub(crate) fn stats(&self) -> (u64, u64) { (self.pings.load(Ordering::Relaxed), self.deaths.load(Ordering::Relaxed)) }    #[allow(dead_code)]
    #[rustfmt::skip]
    pub(crate) fn snapshot(&self) -> (f64, f64, f64) { let w = self.w.lock().unwrap(); (w.lo, w.hi, w.applied) }
}

// CT discovery via crt.name.

/// Bulk same-day cert-burst labels across these apexes; never mirrors.
#[rustfmt::skip]
const NOISE_LABELS: &[&str] = &["auth login oauth signin signup account keycloak portal panel admin",
    "webmail mail smtp imap autoconfig autodiscover grafana jenkins gitlab vault registry",
    "staging dev sandbox demo beta shop billing forum blog wiki chat docs status dashboard",
    "cdn api static assets"];

/// Flattened noise labels, built once.
#[rustfmt::skip]
fn noise_labels() -> &'static Vec<&'static str> {
    static NOISE: OnceLock<Vec<&'static str>> = OnceLock::new();
    NOISE.get_or_init(|| NOISE_LABELS.iter().flat_map(|g| g.split(' ')).collect())
}

#[rustfmt::skip]
#[derive(Deserialize)] struct CtEntry { sub: String }

/// Persisted daily budget + TTL cache (`temp_dir()/aa-mirror-cache.json`).
#[rustfmt::skip]
#[derive(Serialize, Deserialize, Default)]
struct CacheFile { day: u64, used: u32, apexes: BTreeMap<String, (u64, Vec<String>)> }

/// Ranked single-label candidates for `apex`; multi-level entries are wildcard-DNS noise.
#[rustfmt::skip]
pub fn rank_candidates(apex: &str, subs: &[String]) -> Vec<String> {
    let noise: &[&str] = noise_labels();
    let score = |l: &str| match l {
        l if noise.contains(&l) || (l.len() > 20 && l.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')) => 0,
        "www" => 100,
        l if matches!(l.len(), 2..=3) && l.bytes().all(|b| b.is_ascii_alphabetic()) => 50,
        _ => 10,
    };
    let suffix = format!(".{apex}");
    let (mut seen, mut scored) = (BTreeSet::new(), Vec::<(u32, String)>::new());
    for sub in subs {
        let Some(l) = sub.strip_suffix(&suffix).map(str::to_ascii_lowercase) else { continue };
        let s = score(&l);
        if s > 0 && !l.is_empty() && !l.contains('.') && seen.insert(l.clone()) { scored.push((s, format!("{l}.{apex}"))); }
    }
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.into_iter().map(|(_, host)| host).collect()
}

/// Live mirrors for `apexes` with injectable endpoint/cache/budget (mock labs, CI).
#[rustfmt::skip]
pub async fn discover_with(apexes: &[&str], o: &MirrorPolicy) -> Result<Vec<String>, Error> {
    let http = Client::builder().user_agent("annas-archive-api/0.2 (mirror-discovery)").build()?;
    let unix = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let mut cache: CacheFile = std::fs::read_to_string(&o.path).ok()
        .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
    if cache.day != unix / 86_400 { cache.day = unix / 86_400; cache.used = 0; }
    let persist = |c: &CacheFile| { let _ = std::fs::write(&o.path, serde_json::to_string(c).unwrap_or_default()); };
    for apex in apexes {
        if cache.apexes.get(*apex).is_some_and(|&(t, _)| unix - t < 24 * 3600) { continue; }
        if cache.used >= o.quota {
            return Err(api_error(format!("crt.name daily query budget exhausted ({}/{})", cache.used, o.quota))); }
        cache.used += 1; persist(&cache);
        let resp = http.get(format!("{}?apex={apex}&format=json", o.ep)).timeout(Duration::from_secs(30)).send().await?;
        if resp.headers().get("x-ratelimit-remaining").and_then(|v| v.to_str().ok()) == Some("0") {
            cache.used = o.quota; persist(&cache);
            return Err(api_error("crt.name daily quota exhausted")); }
        let subs: Vec<String> = resp.json::<Vec<CtEntry>>().await
            .map_err(|e| malformed_json(format!("crt.name response unusable: {e}")))?
            .into_iter().map(|e| e.sub).collect();
        cache.apexes.insert(apex.to_string(), (unix, rank_candidates(apex, &subs))); persist(&cache);
    }
    let (mut out, mut probes) = (Vec::new(), 0usize);
    'fill: for apex in apexes {
        let Some((_, cands)) = cache.apexes.get(*apex) else { continue };
        for host in cands {
            if out.len() >= 2 || probes >= 6 { break 'fill; }
            probes += 1;
            let alive = http.head(format!("https://{host}/")).timeout(Duration::from_secs(5))
                .send().await.is_ok_and(|r| !r.status().is_server_error());
            if alive { out.push(host.clone()); }
        }
    }
    Ok(out)
}

/// Payload profile applied to `ItemDetails` before serialization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PayloadProfile {
    /// Current wire shape, serialized compact.
    Full,
    /// Default: aliased keys, empty/None dropped, description capped at 300 chars, identifiers reduced to doi+isbn13, one IPFS URL, download info as counts + fast flag.
    #[default]
    Compact,
    /// 8-field whitelist for search-result triage.
    Mini,
}

impl PayloadProfile {
    /// Parse a profile name as sent in the tool arg; unknown names fall back to Compact.
    #[rustfmt::skip]
    pub fn from_arg(name: Option<&str>) -> Self {
        match name.map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("full") => Self::Full, Some("mini") => Self::Mini, _ => Self::Compact,
        }
    }
}

/// Description cap in characters (Compact rule).
const COMPACT_DESCR_MAX_CHARS: usize = 300;

#[rustfmt::skip]
fn cap_description(s: &str) -> String {
    if s.chars().count() <= COMPACT_DESCR_MAX_CHARS { return s.to_string(); }
    let mut out = String::with_capacity(COMPACT_DESCR_MAX_CHARS * 4);
    for c in s.chars().take(COMPACT_DESCR_MAX_CHARS) { out.push(c); }
    out
}

#[rustfmt::skip]
fn first_ipfs_url(infos: &[IpfsInfo]) -> Option<String> { infos.first().map(|i| format!("https://ipfs.io/ipfs/{}", i.cid)) }

#[rustfmt::skip]
#[derive(Serialize)] struct DlSummary { paths: usize, fast: bool }

/// Serialize `details` under the profile (compact JSON).
#[rustfmt::skip]
pub fn shape_details(details: &ItemDetails, profile: PayloadProfile) -> String {
    match profile {
        PayloadProfile::Full => serde_json::to_string(details).expect("ItemDetails serializes"),
        PayloadProfile::Compact => serde_json::to_string(&DetailsCompact::of(details)).expect("DetailsCompact serializes"),
        PayloadProfile::Mini => serde_json::to_string(&DetailsMini::of(details)).expect("DetailsMini serializes"),
    }
}

/// Compact wire shape: only `md5`/`title` always present; every other field skips when None/empty.
#[rustfmt::skip]
#[derive(Serialize)]
struct DetailsCompact<'a> {    md5: &'a str, title: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")] author: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] fmt: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] bytes: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")] lang: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] year: Option<&'a str>,
    #[serde(rename = "pub", skip_serializing_if = "Option::is_none")] publisher: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] descr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] cover: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] r#type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] added: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] ids: Option<BTreeMap<&'a str, &'a [String]>>,
    #[serde(skip_serializing_if = "Option::is_none")] ipfs: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] dl: Option<DlSummary>,
}

impl<'a> DetailsCompact<'a> {
    #[rustfmt::skip]
    fn of(d: &'a ItemDetails) -> Self {
        let ids = d.identifiers.as_ref().and_then(|ids| {
            let mut map = BTreeMap::new();
            for (k, v) in [("doi", &ids.doi), ("isbn13", &ids.isbn13)] {
                if let Some(v) = v.as_ref().filter(|v| !v.is_empty()) { map.insert(k, v.as_slice()); }
            }
            (!map.is_empty()).then_some(map)
        });
        let dl = (d.download_sources.is_some() || d.torrent_paths.is_some() || d.size_bytes.is_some()).then(|| DlSummary { paths: d.download_sources.as_ref().map_or(0, Vec::len).max(d.torrent_paths.as_ref().map_or(0, Vec::len)), fast: d.size_bytes.is_some() });
        Self { md5: &d.md5, title: &d.title, author: d.author.as_deref(), fmt: d.format.as_deref(), bytes: d.size_bytes,
               lang: d.language.as_deref().map_or_else(Vec::new, |l| vec![l]), year: d.year.as_deref(), publisher: d.publisher.as_deref(),
               descr: d.description.as_deref().map(cap_description), cover: d.cover_url.as_deref(), r#type: d.content_type.as_deref(),
               file: d.original_filename.as_deref(), added: d.added_date.as_deref(), ids,
               ipfs: d.ipfs_cids.as_ref().and_then(|c| first_ipfs_url(c)), dl }
    }
}

/// Mini wire shape: 8-field whitelist, aliased keys — Compact's first 8 mapped fields.
#[rustfmt::skip]
#[derive(Serialize)]
struct DetailsMini<'a> {    md5: &'a str, title: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")] author: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] fmt: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] bytes: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")] lang: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")] year: Option<&'a str>,
    #[serde(rename = "pub", skip_serializing_if = "Option::is_none")] pub_: Option<&'a str>,
}

impl<'a> DetailsMini<'a> {
    #[rustfmt::skip]
    fn of(d: &'a ItemDetails) -> Self {
        let c = DetailsCompact::of(d);
        Self { md5: c.md5, title: c.title, author: c.author, fmt: c.fmt, bytes: c.bytes, lang: c.lang, year: c.year, pub_: c.publisher }
    }
}
