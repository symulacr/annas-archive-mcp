mod server;
mod tools;

use std::env;

use rmcp::{ServiceExt, transport::io::stdio};
use server::AnnasArchiveServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let api_key = env::var("ANNAS_ARCHIVE_API_KEY").ok();
    let server = AnnasArchiveServer::new(api_key);

    let service = server.serve(stdio()).await?;
    service.waiting().await?;

    Ok(())
}
