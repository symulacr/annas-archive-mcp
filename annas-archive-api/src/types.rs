use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub md5: String,
    pub title: String,
    pub author: Option<String>,
    pub format: Option<String>,
    pub size: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub query: String,
    pub page: Option<u32>,
}

impl SearchOptions {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            page: None,
        }
    }

    pub fn with_page(mut self, page: u32) -> Self {
        self.page = Some(page);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub page: u32,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemDetails {
    pub md5: String,
    pub title: String,
    pub author: Option<String>,
    pub format: Option<String>,
    pub size: Option<String>,
    pub language: Option<String>,
    pub publisher: Option<String>,
    pub year: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadInfo {
    pub download_url: String,
}
