use reqwest::Client;

use crate::error::Error;
use crate::scraper::{parse_item_details, parse_search_results};
use crate::types::{DownloadInfo, ItemDetails, SearchOptions, SearchResponse};

const DOMAINS: &[&str] = &["annas-archive.org", "annas-archive.se", "annas-archive.li"];

pub struct AnnasArchiveClient {
    client: Client,
    api_key: Option<String>,
}

impl AnnasArchiveClient {
    pub fn new(api_key: Option<String>) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .build()
            .expect("Failed to create HTTP client");

        Self { client, api_key }
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

    pub async fn get_details(&self, md5: &str) -> Result<ItemDetails, Error> {
        let path = format!("/md5/{md5}");
        let html = self.fetch_with_failover(&path).await?;
        parse_item_details(&html, md5)
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

        // The fast download API endpoint
        let url = format!(
            "https://annas-archive.org/dyn/api/fast_download.json?md5={md5}&path_index={path_idx}&domain_index={domain_idx}&key={api_key}"
        );

        let response = self.client.get(&url).send().await.map_err(Error::Network)?;

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

            return Err(Error::Http { status });
        }

        #[derive(serde::Deserialize)]
        struct ApiResponse {
            download_url: Option<String>,
            error: Option<String>,
        }

        let api_response: ApiResponse = response.json().await.map_err(Error::Network)?;

        if let Some(error) = api_response.error {
            return Err(Error::Api { message: error });
        }

        let download_url = api_response.download_url.ok_or(Error::Api {
            message: "No download URL in response".to_string(),
        })?;

        Ok(DownloadInfo { download_url })
    }
}
