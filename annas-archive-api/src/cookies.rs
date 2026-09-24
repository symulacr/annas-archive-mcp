//! Cookie import for the browser-minted DDoS-Guard session (fix1-cookies
//! prototype): the real challenge is fingerprint-scored, so the user runs
//! the site once in a real browser, exports the cookies, and the client
//! consumes them. Sources: Netscape cookies.txt, Chrome DevTools JSON, or a
//! `name=value; ...` env string. Only Anna's Archive domains are accepted
//! (security: a shared export must never inject foreign cookies into
//! requests), and entries are normalized onto every official mirror so a
//! DDoS-Guard session works under failover.

use std::path::Path;
use std::sync::Arc;

use cookie_store::{CookieStore, RawCookie};
use time::OffsetDateTime;

use crate::client::{DOMAINS, FileJar};
use crate::error::{Error, api_error, blocked};

/// Domains an imported cookie may target: official mirrors plus the
/// `annas-archive` suffix so every mirror variant is covered.
fn is_annas_archive_domain(domain: &str) -> bool {
    let domain = domain.trim_start_matches('.');
    DOMAINS.contains(&domain) || domain.ends_with(".annas-archive")
}

/// Cookie import source.
#[derive(Debug, Clone)]
pub enum CookieImport {
    /// Netscape cookies.txt file (browser export format).
    NetscapeFile(std::path::PathBuf),
    /// Chrome DevTools "Copy all cookies" JSON: `[{name, value, domain, path, expires, ...}]`.
    JsonFile(std::path::PathBuf),
    /// `AA_DDGS_COOKIES="name=val; name=val"` style string.
    EnvString(String),
    /// Raw pairs scoped to an explicit origin (mock-lab hook): pairs are
    /// inserted host-only against `origin` instead of the official mirrors.
    MockOrigin {
        origin: String,
        pairs: Vec<(String, String)>,
    },
}

impl CookieImport {
    /// Sniff the source format from the file's first non-blank bytes: a
    /// leading `{`/`[` is DevTools JSON, anything else is Netscape format.
    /// Lets the CLI accept either export style behind one flag.
    pub fn detect(path: &Path) -> Self {
        let path = path.to_path_buf();
        let looks_json = std::fs::read_to_string(&path)
            .map(|text| {
                text.lines()
                    .find(|line| !line.trim().is_empty())
                    .is_some_and(|first| {
                        let first = first.trim_start();
                        first.starts_with('{') || first.starts_with('[')
                    })
            })
            .unwrap_or(false);
        if looks_json {
            CookieImport::JsonFile(path)
        } else {
            CookieImport::NetscapeFile(path)
        }
    }
}

/// Parse a Netscape cookies.txt line: 7 tab/space fields
/// (`domain \t flag \t path \t secure \t expiry \t name \t value`), the
/// `#HttpOnly_` prefix variant, and expiry 0 = session cookie.
/// Returns `None` for comments, blanks, and foreign-domain lines.
fn parse_netscape_line(line: &str) -> Option<(RawCookie<'static>, i64)> {
    let line = line.strip_prefix("#HttpOnly_").unwrap_or(line);
    if line.trim().is_empty() || line.starts_with('#') {
        return None;
    }
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 7 {
        return None;
    }
    let domain = fields[0];
    if !is_annas_archive_domain(domain) {
        return None;
    }
    let (name, value) = (fields[5].to_string(), fields[6].to_string());
    if name.is_empty() {
        return None;
    }
    let mut cookie = RawCookie::new(name, value);
    // A leading dot means the cookie is domain-scoped (not host-only).
    if domain.starts_with('.') {
        cookie.set_domain(domain.to_string());
    }
    if fields[3] == "TRUE" {
        cookie.set_secure(true);
    }
    if fields[1] == "TRUE" {
        cookie.set_http_only(true);
    }
    let expiry: i64 = fields[4].parse().unwrap_or(0);
    Some((cookie, expiry))
}

/// Parse a Chrome DevTools JSON cookie entry into a raw cookie + epoch-seconds expiry.
fn parse_json_entry(entry: &serde_json::Value) -> Option<(RawCookie<'static>, i64)> {
    let name = entry.get("name")?.as_str()?;
    let value = entry.get("value")?.as_str()?;
    let domain = entry.get("domain")?.as_str()?;
    if !is_annas_archive_domain(domain) {
        return None;
    }
    if name.is_empty() {
        return None;
    }
    let mut cookie = RawCookie::new(name.to_string(), value.to_string());
    if let Some(path) = entry.get("path").and_then(serde_json::Value::as_str) {
        cookie.set_path(path.to_string());
    }
    if domain.starts_with('.') {
        cookie.set_domain(domain.to_string());
    }
    if entry.get("secure").and_then(serde_json::Value::as_bool) == Some(true) {
        cookie.set_secure(true);
    }
    if entry.get("httpOnly").and_then(serde_json::Value::as_bool) == Some(true) {
        cookie.set_http_only(true);
    }
    // DevTools exports use floating-point epoch seconds; -1 = session cookie.
    let expiry = entry
        .get("expires")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(-1.0) as i64;
    Some((cookie, expiry))
}

/// Turn a raw cookie + epoch-seconds expiry (0/-1 = session) into a
/// `cookie_store::Cookie` scoped to `origin`, then insert it into `store`.
/// Expiry is attached as an absolute time so `matches()` drops stale
/// imports instead of replaying dead guard sessions.
fn insert_raw(
    store: &mut CookieStore,
    cookie: RawCookie<'static>,
    expiry_epoch: i64,
    origin: &url::Url,
) -> Result<(), Error> {
    let domain = cookie_store::CookieDomain::host_only(origin)
        .map_err(|e| api_error(format!("cookie domain invalid: {e}")))?;
    let path = cookie_store::CookiePath::default_path(origin);
    let expires = if expiry_epoch > 0 {
        // Clamp to year 9999; imports beyond that are treated as far-future.
        let secs = expiry_epoch.min(253_402_300_799);
        let datetime = OffsetDateTime::from_unix_timestamp(secs)
            .map_err(|_| api_error("cookie expiry out of range"))?;
        cookie_store::CookieExpiration::AtUtc(datetime)
    } else {
        cookie_store::CookieExpiration::SessionEnd
    };
    let _ = (domain, path);
    let stored = cookie_store::Cookie::try_from_raw_cookie(&cookie, origin)
        .map_err(|e| api_error(format!("cookie rejected: {e}")))?;
    // try_from_raw_cookie derives expiry from the raw cookie; override with
    // the imported absolute time (0/-1 = session).
    let mut stored = stored;
    stored.expires = expires;
    store
        .insert(stored, origin)
        .map_err(|e| api_error(format!("cookie insert failed: {e}")))?;
    Ok(())
}

/// Import `cookies` into the file-backed jar so every subsequent request
/// (search, details, downloads) attaches them via reqwest's cookie_provider.
/// Returns the number of cookies actually loaded (foreign domains and
/// unparseable lines are skipped, not fatal). An import yielding zero
/// cookies is an error: a stale export should fail loudly, not silently
/// fall back to the challenge path.
pub fn import_cookies_into(jar: &FileJar, cookies: CookieImport) -> Result<usize, Error> {
    let mock_origin = match &cookies {
        CookieImport::MockOrigin { origin, .. } => Some(origin.clone()),
        _ => None,
    };
    let parsed: Vec<(RawCookie<'static>, i64)> = match cookies {
        CookieImport::NetscapeFile(path) => {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| api_error(format!("cookie file unreadable: {e}")))?;
            text.lines().filter_map(parse_netscape_line).collect()
        }
        CookieImport::JsonFile(path) => {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| api_error(format!("cookie file unreadable: {e}")))?;
            let entries: Vec<serde_json::Value> = serde_json::from_str(&text)
                .map_err(|e| api_error(format!("cookie JSON malformed: {e}")))?;
            entries.iter().filter_map(parse_json_entry).collect()
        }
        CookieImport::EnvString(raw) => raw
            .split(';')
            .filter_map(|pair| {
                let pair = pair.trim();
                let (name, value) = pair.split_once('=')?;
                let (name, value) = (name.trim(), value.trim());
                if name.is_empty() || value.is_empty() {
                    return None;
                }
                Some((RawCookie::new(name.to_string(), value.to_string()), 0))
            })
            .collect(),
        CookieImport::MockOrigin { origin: _, pairs } => pairs
            .into_iter()
            .map(|(name, value)| (RawCookie::new(name, value), 0))
            .collect(),
    };
    if parsed.is_empty() {
        return Err(blocked("no Anna's Archive cookies found in import source"));
    }
    let mut loaded = 0;
    {
        let mut store = jar.store.lock().expect("file-jar lock poisoned");
        for (cookie, expiry) in parsed {
            if let Some(origin) = &mock_origin {
                // Mock-lab hook: host-only insert against the explicit origin.
                let origin_url: url::Url = origin.parse().expect("mock origin");
                if insert_raw(&mut store, cookie, 0, &origin_url).is_ok() {
                    loaded += 1;
                }
                continue;
            }
            // Normalize onto every official mirror: a browser session is
            // minted per-origin, so failover across .gd/.gl/.pk needs the
            // cookie replayed on each. The source domain attribute must be
            // stripped first: a `.gd`-scoped cookie would be rejected
            // (DomainMismatch) on the .gl/.pk insertions.
            let mut per_mirror = cookie.clone();
            per_mirror.set_domain("");
            for domain in DOMAINS {
                let origin: url::Url = format!("https://{domain}/").parse().expect("mirror origin");
                if insert_raw(&mut store, per_mirror.clone(), expiry, &origin).is_ok() {
                    loaded += 1;
                }
            }
        }
        jar.persist(&store);
    }
    Ok(loaded)
}

/// Convenience wrapper: load the jar at `jar_path`, import, return (jar, count).
/// Callers hand the jar to `AnnasArchiveClient::build_with_jar`.
pub fn import_cookies(
    jar_path: impl AsRef<Path>,
    cookies: CookieImport,
) -> Result<(Arc<FileJar>, usize), Error> {
    let jar = Arc::new(FileJar::load(jar_path.as_ref().to_path_buf()));
    let count = import_cookies_into(&jar, cookies)?;
    Ok((jar, count))
}
