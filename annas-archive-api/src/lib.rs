mod client;
mod cookies;
mod error;
mod scraper;
mod types;

pub use client::{
    AnnasArchiveClient, DiscoveryOptions, FileJar, KeepAliveCtl, MembershipTier, MirrorPolicy,
    PayloadProfile, SearchBackend, TorrentEntryRaw, discover_with, enable_dynamic_mirrors,
    parse_json_details, parse_torrents, rank_candidates, shape_details, warm_cookies,
};
pub use cookies::{CookieImport, import_cookies, import_cookies_into};
pub use error::{Error, ParseKind};
pub use types::{DownloadInfo, ItemDetails, SearchOptions, SearchResponse, SearchResult};
