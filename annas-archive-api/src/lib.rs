mod client;
mod error;
mod scraper;
mod types;

pub use client::{
    AnnasArchiveClient, DiscoveryOptions, KeepAliveCtl, MembershipTier, MirrorPolicy,
    PayloadProfile, TorrentEntryRaw, discover_with, enable_dynamic_mirrors, parse_json_details,
    parse_torrents, rank_candidates, shape_details,
};
pub use error::{Error, ParseKind};
pub use types::{DownloadInfo, ItemDetails, SearchOptions, SearchResponse, SearchResult};
