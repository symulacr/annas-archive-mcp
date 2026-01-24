# Anna's Archive MCP

A Model Context Protocol (MCP) server for [Anna's Archive](https://annas-archive.org), providing access to search and retrieve information about books, papers, magazines, comics, and other documents.

## Installation

```bash
cargo install annas-archive-mcp
```

Or build from source:

```bash
git clone https://github.com/RemiKalbe/annas-archive-mcp
cd annas-archive-mcp
cargo install --path annas-archive-mcp
```

## Usage

### Claude Desktop

Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "annas-archive": {
      "command": "annas-archive-mcp"
    }
  }
}
```

### With API Key (optional)

For fast download URLs, set your API key:

```json
{
  "mcpServers": {
    "annas-archive": {
      "command": "annas-archive-mcp",
      "env": {
        "ANNAS_ARCHIVE_API_KEY": "your-api-key"
      }
    }
  }
}
```

## Available Tools

| Tool | Description |
|------|-------------|
| `search` | Search Anna's Archive for books, papers, magazines, comics, and other documents |
| `get_details` | Get detailed metadata for an item by its MD5 hash |
| `get_download_url` | Get a fast download URL for an item (requires API key) |

## Library Usage

The `annas-archive-api` crate can be used independently:

```rust
use annas_archive_api::{AnnasArchiveClient, SearchOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = AnnasArchiveClient::new(None);

    let options = SearchOptions::new("rust programming");
    let results = client.search(options).await?;

    for item in results.results {
        println!("{} - {}", item.title, item.format.unwrap_or_default());
    }

    Ok(())
}
```

## License

MIT
