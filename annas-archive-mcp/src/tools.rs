use rmcp::schemars;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SearchParams {
    #[schemars(description = "Search query for books, papers, magazines, comics, etc.")]
    pub query: String,
    #[schemars(description = "Page number (starts at 1)")]
    pub page: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DetailsParams {
    #[schemars(description = "MD5 hash of the item to get details for")]
    pub md5: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DownloadParams {
    #[schemars(description = "MD5 hash of the item to download")]
    pub md5: String,
    #[schemars(description = "Path index for download source selection")]
    pub path_index: Option<u32>,
    #[schemars(description = "Domain index for download source selection")]
    pub domain_index: Option<u32>,
}
