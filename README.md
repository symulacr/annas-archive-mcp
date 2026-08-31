# Anna's Archive MCP

MCP server for [Anna's Archive](https://annas-archive.gd): search books, papers, journals; get metadata; mint fast-download URLs. Rust, zero-copy parsing.

Fork of [RemiKalbe/annas-archive-mcp](https://github.com/RemiKalbe/annas-archive-mcp), rewritten:

- [`annas-archive-api`](annas-archive-api/README.md) — library crate (parsing, HTTP, mirrors)
- [`annas-archive-mcp`](annas-archive-mcp/README.md) — server crate (same 5 tools, wire-compatible)

| Metric | Upstream | Here | Δ |
|---|---:|---:|---:|
| Search parse | 28,169 ops/s | 218,125 ops/s | 7.7× |
| Record parse | 2,613 ops/s | 9,499 ops/s | 3.6× |
| Torrents parse | 43 ops/s | 560 ops/s | 13× |
| URL build | 6.81M ops/s | 63.90M ops/s | 9.4× |
| MCP init storm | ~10k rps | 158k rps | 15.8× |
| Peak RSS | 23.4 MB | 7.9 MB | −66% |
| Binary | 15.0 MB | 9.0 MB | −38% |
| Live latency | +1.75 s dead-mirror tax | 0 | −1.75 s/call |
| Wire bytes | uncompressed | gzip+brotli | 7.8–11.7× less |

Byte-equal outputs, 146 tests, 615-input fuzz clean.

## Architecture — before (upstream)

```mermaid
flowchart TB
    C([caller<br/>Claude / MCP client])
    subgraph server ["mcp server — rmcp 0.14"]
        direction TB
        S["serve_inner<br/>spawn-per-request"] --> Q["mpsc<br/>sink-proxy"]
        S --> E["Extensions<br/>RequestContext"]
        S --> J["JoinSet<br/>per session"]
    end
    subgraph client ["api client"]
        K["scraper DOM<br/>html5ever tree · 185 allocs/page<br/>serde Value walk · 3,661 allocs/op"]
        F["failover: try mirrors<br/>in order, no cache"]
    end
    subgraph mirrors ["hardcoded mirror list"]
        direction LR
        M1["annas-archive.org<br/>✗ DNS dead"]
        M2["annas-archive.se<br/>✗ DNS dead"]
        M3["annas-archive.li<br/>✗ parked page → HTTP 200"]
    end
    C --> |stdio| S
    S --> |"typed calls"| K
    K --> F
    F --> M1
    F --> M2
    F --> M3
    K -. "whole-body buffer<br/>no caps · no timeout<br/>no compression" .-> M3
    M3 -. "parses garbage as success" .-> X[/"wrong data or error<br/>+1.75 s dead-domain tax per call"/]
    style M1 fill:#c93c37,color:#fff
    style M2 fill:#c93c37,color:#fff
    style M3 fill:#d4a72c,color:#000
    style S fill:#8b949e,color:#000
    style Q fill:#8b949e,color:#000
    style E fill:#8b949e,color:#000
    style J fill:#8b949e,color:#000
    style K fill:#8b949e,color:#000
    style X fill:#c93c37,color:#fff
    style server fill:#f6f8fa,stroke:#8b949e
    style client fill:#f6f8fa,stroke:#8b949e
    style mirrors fill:#ffebe9,stroke:#c93c37
```

## Architecture — after (this fork)

```mermaid
flowchart TB
    C([caller<br/>Claude / MCP client])
    subgraph server ["mcp server — raw ndjson"]
        direction TB
        L["serve_session<br/>one read/dispatch/write loop<br/>sequential ordering"]
        W["TimeoutAsyncRead<br/>30 s frame watchdog<br/>single re-arm site"]
        O["SyncStdout<br/>direct write(2) · 178k rps"]
        L --> W
        L --> O
    end
    subgraph client ["api client"]
        K["astral-tl arena · 42 allocs/page<br/>borrowed serde · 618 allocs/op<br/>TorrentEntryRaw&lt;'a&gt; · dhat-verified zero-copy"]
        D["failover: live-first ranking<br/>keep-alive pinned pool"]
    end
    subgraph net ["network"]
        direction LR
        P[("mirror pool<br/>annas-archive.gd · .gl · .pk")]
        B["body caps 16/4 MB<br/>20 s read timeout<br/>gzip/brotli · keep-alive 15 s"]
    end
    C --> |"ndjson stdio"| L
    L --> |"typed calls"| K
    K --> D
    D --> B
    B --> P
    K -. "CT mirror discovery<br/>request coalescing (opt-in)" .-> P
    O -. "init-storm 15.8× · sequential replies" .-> C
    K --> |"−66% RSS · 7.7× parse"| OK[/"typed result<br/>Error{kind, retryable}"/]
    style L fill:#1a7f37,color:#fff
    style W fill:#1a7f37,color:#fff
    style O fill:#1a7f37,color:#fff
    style K fill:#1a7f37,color:#fff
    style D fill:#1a7f37,color:#fff
    style P fill:#1a7f37,color:#fff
    style B fill:#1a7f37,color:#fff
    style OK fill:#1a7f37,color:#fff
    style server fill:#e6f4ea,stroke:#1a7f37
    style client fill:#e6f4ea,stroke:#1a7f37
    style net fill:#e6f4ea,stroke:#1a7f37
```

## Install

```bash
cargo install annas-archive-mcp
```

Claude Desktop:

```json
{
  "mcpServers": {
    "annas-archive": {
      "command": "annas-archive-mcp",
      "env": { "ANNAS_ARCHIVE_API_KEY": "your-secret-key" }
    }
  }
}
```

Key = account secret key; members get fast downloads, others search/metadata only.

## Tools

`search` (no key) · `get_details` (key, `profile` arg) · `get_download_url` (key) · `prewarm` (no key) · `get_membership_status` (key)

## Env flags (off by default)

`AA_LENIENT_RECORDS` · `AA_REQUEST_COALESCING` · `AA_DYNAMIC_MIRRORS` · `MCP_STDIO_READ_TIMEOUT_MS` (default 30000)

## License

MIT
