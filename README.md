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
      "command": "annas-archive-mcp",
      "env": {
        "PATH": "${HOME}/.cargo/bin:${PATH}"
      }
    }
  }
}
```

### With API Key

The `get_download_url` tool requires an API key with fast download access. Without it, only `search` and `get_details` are functional.

To get an API key:
1. Create an account on [annas-archive.li](https://annas-archive.li)
2. Your API key is your "Secret key" (the key you use to log in)
3. Go to the [donate page](https://annas-archive.li/donate) to get access to fast downloads

```json
{
  "mcpServers": {
    "annas-archive": {
      "command": "annas-archive-mcp",
      "env": {
        "PATH": "${HOME}/.cargo/bin:${PATH}",
        "ANNAS_ARCHIVE_API_KEY": "your-api-key"
      }
    }
  }
}
```

## Available Tools

| Tool | Description | Requires API Key |
|------|-------------|------------------|
| `search` | Search for books, papers, magazines, comics, and other documents | No |
| `get_details` | Get detailed metadata for an item by its MD5 hash | No |
| `get_download_url` | Get a fast download URL for an item | Yes |

## License

MIT
