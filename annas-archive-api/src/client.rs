use std::sync::Arc;

use reqwest::{Client, cookie::Jar};

use crate::error::Error;
use crate::scraper::parse_search_results;
use crate::types::{
    DownloadInfo, DownloadSource, Identifiers, IpfsInfo, ItemDetails, SearchOptions, SearchResponse,
};

const DOMAINS: &[&str] = &["annas-archive.org", "annas-archive.se", "annas-archive.li"];

pub struct AnnasArchiveClient {
    client: Client,
    api_key: Option<String>,
    #[allow(dead_code)] // Used by cookie_provider, but not directly accessed
    cookie_jar: Arc<Jar>,
    authenticated: std::sync::atomic::AtomicBool,
}

impl AnnasArchiveClient {
    pub fn new(api_key: Option<String>) -> Self {
        let cookie_jar = Arc::new(Jar::default());

        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .cookie_provider(cookie_jar.clone())
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            api_key,
            cookie_jar,
            authenticated: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Authenticate with Anna's Archive using the secret key.
    /// This sets the aa_account_id2 cookie needed for API access.
    async fn authenticate(&self) -> Result<(), Error> {
        let api_key = self.api_key.as_ref().ok_or(Error::MissingApiKey)?;

        // Try each domain for authentication
        for domain in DOMAINS {
            let url = format!("https://{domain}/account/");

            let response = self
                .client
                .post(&url)
                .form(&[("key", api_key.as_str())])
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() || resp.status().is_redirection() => {
                    self.authenticated
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return Ok(());
                }
                Ok(resp) if resp.status().is_client_error() => {
                    return Err(Error::Api {
                        message: "Invalid secret key".to_string(),
                    });
                }
                _ => continue, // Try next domain
            }
        }

        Err(Error::AllDomainsFailed {
            message: "Failed to authenticate with any domain".to_string(),
        })
    }

    async fn ensure_authenticated(&self) -> Result<(), Error> {
        if !self.authenticated.load(std::sync::atomic::Ordering::SeqCst) {
            self.authenticate().await?;
        }
        Ok(())
    }

    async fn fetch_with_failover(&self, path: &str) -> Result<String, Error> {
        let mut last_error = None;

        for domain in DOMAINS {
            let url = format!("https://{domain}{path}");

            match self.client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return response.text().await.map_err(Error::Network);
                    } else if response.status().is_client_error() {
                        // Client errors (4xx) won't be fixed by trying another domain
                        return Err(Error::Http {
                            status: response.status().as_u16(),
                        });
                    }
                    // Server error - try next domain
                    last_error = Some(Error::Http {
                        status: response.status().as_u16(),
                    });
                }
                Err(e) => {
                    // Connection error - try next domain
                    last_error = Some(Error::Network(e));
                }
            }
        }

        Err(last_error.unwrap_or(Error::AllDomainsFailed {
            message: "No domains available".to_string(),
        }))
    }

    pub async fn search(&self, options: SearchOptions) -> Result<SearchResponse, Error> {
        let page = options.page.unwrap_or(1);
        let query = urlencoding::encode(&options.query);
        let path = format!("/search?q={query}&page={page}");

        let html = self.fetch_with_failover(&path).await?;
        let (results, has_more) = parse_search_results(&html)?;

        Ok(SearchResponse {
            results,
            page,
            has_more,
        })
    }

    /// Get detailed metadata for an item. Requires API key (secret key).
    pub async fn get_details(&self, md5: &str) -> Result<ItemDetails, Error> {
        self.ensure_authenticated().await?;

        let path = format!("/db/aarecord_elasticsearch/md5:{md5}.json");

        let mut last_error = None;

        for domain in DOMAINS {
            let url = format!("https://{domain}{path}");

            match self.client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        let json_str = response.text().await.map_err(Error::Network)?;
                        return parse_json_details(&json_str, md5);
                    } else if response.status().is_client_error() {
                        let status = response.status().as_u16();
                        if status == 403 {
                            // Re-authenticate and retry once
                            self.authenticated
                                .store(false, std::sync::atomic::Ordering::SeqCst);
                            self.authenticate().await?;

                            // Retry request
                            if let Ok(resp) = self.client.get(&url).send().await
                                && resp.status().is_success()
                            {
                                let json_str = resp.text().await.map_err(Error::Network)?;
                                return parse_json_details(&json_str, md5);
                            }
                        }
                        return Err(Error::Http { status });
                    }
                    last_error = Some(Error::Http {
                        status: response.status().as_u16(),
                    });
                }
                Err(e) => {
                    last_error = Some(Error::Network(e));
                }
            }
        }

        Err(last_error.unwrap_or(Error::AllDomainsFailed {
            message: "Failed to get details from any domain".to_string(),
        }))
    }

    pub async fn get_download_url(
        &self,
        md5: &str,
        path_index: Option<u32>,
        domain_index: Option<u32>,
    ) -> Result<DownloadInfo, Error> {
        let api_key = self.api_key.as_ref().ok_or(Error::MissingApiKey)?;

        let path_idx = path_index.unwrap_or(0);
        let domain_idx = domain_index.unwrap_or(0);

        // Try each domain for the fast download API
        let mut last_error = None;

        for domain in DOMAINS {
            let url = format!(
                "https://{domain}/dyn/api/fast_download.json?md5={md5}&path_index={path_idx}&domain_index={domain_idx}&key={api_key}"
            );

            let response = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(Error::Network(e));
                    continue;
                }
            };

            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();

                // Check for common API errors
                if body.contains("no_membership") {
                    return Err(Error::Api {
                        message: "No active membership for this API key".to_string(),
                    });
                }
                if body.contains("invalid") {
                    return Err(Error::Api {
                        message: "Invalid API key".to_string(),
                    });
                }

                last_error = Some(Error::Http { status });
                continue;
            }

            #[derive(serde::Deserialize)]
            struct ApiResponse {
                download_url: Option<String>,
                error: Option<String>,
            }

            let api_response: ApiResponse = match response.json().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(Error::Network(e));
                    continue;
                }
            };

            if let Some(error) = api_response.error {
                return Err(Error::Api { message: error });
            }

            let download_url = api_response.download_url.ok_or(Error::Api {
                message: "No download URL in response".to_string(),
            })?;

            return Ok(DownloadInfo { download_url });
        }

        Err(last_error.unwrap_or(Error::AllDomainsFailed {
            message: "Failed to get download URL from any domain".to_string(),
        }))
    }
}

/// Parse item details from the JSON API response
fn parse_json_details(json_str: &str, md5: &str) -> Result<ItemDetails, Error> {
    // The response is a JSON string that might be double-encoded
    let json_str = json_str.trim();
    let json_str = if json_str.starts_with('"') && json_str.ends_with('"') {
        // Double-encoded JSON string, parse first to get the inner JSON
        serde_json::from_str::<String>(json_str).map_err(|e| Error::Parse {
            message: format!("Failed to parse outer JSON: {e}"),
        })?
    } else {
        json_str.to_string()
    };

    let data: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| Error::Parse {
        message: format!("Failed to parse JSON: {e}"),
    })?;

    // Check for error response
    if let Some(error) = data.get("error").and_then(|v| v.as_str()) {
        return Err(Error::Api {
            message: error.to_string(),
        });
    }

    // Extract file_unified_data which contains the main metadata
    let file_data = data.get("file_unified_data").ok_or_else(|| Error::Parse {
        message: "Missing file_unified_data".to_string(),
    })?;

    let title = file_data
        .get("title_best")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();

    let author = file_data
        .get("author_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let format = file_data
        .get("extension_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase());

    let size_bytes = file_data.get("filesize_best").and_then(|v| v.as_u64());

    let size = size_bytes.map(format_filesize);

    let language = file_data
        .get("language_codes")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let publisher = file_data
        .get("publisher_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let year = file_data
        .get("year_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let description = file_data
        .get("stripped_description_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let cover_url = file_data
        .get("cover_url_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let content_type = file_data
        .get("content_type_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let original_filename = file_data
        .get("original_filename_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let added_date = file_data
        .get("added_date_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let pages = file_data
        .get("pages_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let edition = file_data
        .get("edition_varia_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let series = file_data
        .get("series_best")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // Parse identifiers from identifiers_unified
    let identifiers = parse_identifiers(file_data.get("identifiers_unified"));

    // Parse categories from classifications_unified
    let categories = parse_string_list_from_object(file_data.get("classifications_unified"));

    // Parse subjects (openlib_subject, etc.)
    let subjects = parse_string_list_from_object(
        file_data
            .get("classifications_unified")
            .and_then(|c| c.get("collection")),
    )
    .or_else(|| {
        // Fallback to any subject-like classification
        file_data
            .get("classifications_unified")
            .and_then(|c| c.as_object())
            .and_then(|obj| {
                obj.iter()
                    .find(|(k, _)| k.contains("subject"))
                    .and_then(|(_, v)| {
                        v.as_array().map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                    })
            })
    });

    // Parse IPFS CIDs
    let ipfs_cids = parse_ipfs_infos(file_data.get("ipfs_infos"));

    // Parse additional data for download sources and torrent paths
    let additional = data.get("additional");

    let download_sources = parse_download_sources(additional);
    let torrent_paths = parse_torrent_paths(additional);

    Ok(ItemDetails {
        md5: md5.to_string(),
        title,
        author,
        format,
        size,
        size_bytes,
        language,
        publisher,
        year,
        description,
        cover_url,
        content_type,
        original_filename,
        added_date,
        pages,
        edition,
        series,
        identifiers,
        categories,
        subjects,
        ipfs_cids,
        download_sources,
        torrent_paths,
    })
}

/// Parse identifiers from identifiers_unified object
fn parse_identifiers(value: Option<&serde_json::Value>) -> Option<Identifiers> {
    let obj = value?.as_object()?;

    let get_string_array = |key: &str| -> Option<Vec<String>> {
        obj.get(key).and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
        })
    };

    let get_first_string = |key: &str| -> Option<String> {
        obj.get(key)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    let identifiers = Identifiers {
        isbn10: get_string_array("isbn10"),
        isbn13: get_string_array("isbn13"),
        doi: get_string_array("doi"),
        asin: get_string_array("asin"),
        sha1: get_first_string("sha1"),
        sha256: get_first_string("sha256"),
        crc32: get_first_string("crc32"),
        blake2b: get_first_string("blake2b"),
        open_library: get_string_array("ol"),
        google_books: get_string_array("googlebookid"),
        goodreads: get_string_array("goodreads"),
        amazon: get_string_array("amazon"),
    };

    // Only return Some if at least one field is set
    if identifiers.isbn10.is_some()
        || identifiers.isbn13.is_some()
        || identifiers.doi.is_some()
        || identifiers.asin.is_some()
        || identifiers.sha1.is_some()
        || identifiers.sha256.is_some()
        || identifiers.open_library.is_some()
        || identifiers.google_books.is_some()
    {
        Some(identifiers)
    } else {
        None
    }
}

/// Parse a list of strings from an object's values
fn parse_string_list_from_object(value: Option<&serde_json::Value>) -> Option<Vec<String>> {
    let obj = value?.as_object()?;
    let mut result = Vec::new();

    for (key, val) in obj {
        // Skip certain keys that aren't useful categories
        if key == "collection" || key.starts_with('_') {
            continue;
        }
        if let Some(arr) = val.as_array() {
            for item in arr {
                if let Some(s) = item.as_str()
                    && !s.is_empty()
                    && !result.contains(&s.to_string())
                {
                    result.push(s.to_string());
                }
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Parse IPFS info from ipfs_infos array
fn parse_ipfs_infos(value: Option<&serde_json::Value>) -> Option<Vec<IpfsInfo>> {
    let arr = value?.as_array()?;
    let infos: Vec<IpfsInfo> = arr
        .iter()
        .filter_map(|v| {
            let obj = v.as_object()?;
            let cid = obj.get("ipfs_cid")?.as_str()?.to_string();
            let from = obj
                .get("from")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            Some(IpfsInfo { cid, from })
        })
        .collect();

    if infos.is_empty() { None } else { Some(infos) }
}

/// Parse download sources from additional data
fn parse_download_sources(additional: Option<&serde_json::Value>) -> Option<Vec<DownloadSource>> {
    let obj = additional?.as_object()?;
    let mut sources = Vec::new();

    // Check for direct download URLs
    if let Some(urls) = obj.get("download_urls").and_then(|v| v.as_array()) {
        for url in urls {
            if let Some(url_str) = url.as_str() {
                sources.push(DownloadSource {
                    name: "direct".to_string(),
                    url: url_str.to_string(),
                });
            }
        }
    }

    // Check for IPFS URLs
    if let Some(urls) = obj.get("ipfs_urls").and_then(|v| v.as_array()) {
        for url in urls {
            if let Some(url_str) = url.as_str() {
                sources.push(DownloadSource {
                    name: "ipfs".to_string(),
                    url: url_str.to_string(),
                });
            }
        }
    }

    if sources.is_empty() {
        None
    } else {
        Some(sources)
    }
}

/// Parse torrent paths from additional data
fn parse_torrent_paths(additional: Option<&serde_json::Value>) -> Option<Vec<String>> {
    let arr = additional?.as_object()?.get("torrent_paths")?.as_array()?;

    let paths: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    if paths.is_empty() { None } else { Some(paths) }
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
