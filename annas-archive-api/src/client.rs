use std::sync::Arc;

use reqwest::{Client, cookie::Jar};

use crate::error::Error;
use crate::scraper::parse_search_results;
use crate::types::{DownloadInfo, ItemDetails, SearchOptions, SearchResponse};

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

    let size = file_data
        .get("filesize_best")
        .and_then(|v| v.as_u64())
        .map(format_filesize);

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

    Ok(ItemDetails {
        md5: md5.to_string(),
        title,
        author,
        format,
        size,
        language,
        publisher,
        year,
        description,
    })
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
