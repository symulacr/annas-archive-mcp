# annas-archive-mcp

MCP server for [Anna's Archive](https://annas-archive.gd) over stdio: search, metadata, fast-download URLs. One binary, 4 deps, raw ndjson dispatch.

Rewritten from [RemiKalbe/annas-archive-mcp](https://github.com/RemiKalbe/annas-archive-mcp)'s server crate. Same 5 tools, wire-byte-compatible. Wraps [`annas-archive-api`](../annas-archive-api/README.md); overview in [root README](../README.md).

| Metric | Upstream | Here | Δ |
|---|---:|---:|---:|
| MCP init storm | ~10k rps | 158k rps | 15.8× |
| Stdout ping storm | 10.5k rps | 178k rps | 17× |
| tools/list p50 | 0.133 ms | 0.081 ms | 1.6× |
| rmcp framework | in tree | removed | −48 lockfile packages |
| Binary | 15.0 MB | 9.0 MB | −38% |

146 tests, byte-identical stdout vs golden transcripts, 1000-frame adversarial probe clean.

## Architecture — before (upstream, rmcp 0.14)

```mermaid
flowchart TB
    C([mcp client])
    subgraph rmcp ["rmcp 0.14 runtime"]
        direction TB
        S["serve_inner<br/>spawn-per-request"]
        Q["mpsc sink-proxy<br/>response re-routed"]
        E["Extensions<br/>RequestContext allocs"]
        J["JoinSet<br/>per-session task table"]
        O["tokio blocking-pool stdout"]
        S --> Q
        S --> E
        S --> J
        Q --> O
    end
    subgraph api2 ["api crate"]
        A["scraper DOM · Value trees"]
    end
    C --> |stdio| S
    S --> |"typed calls"| A
    O -. "out-of-order replies<br/>possible" .-> C
    S -. "~84% of hot-path cost<br/>is this machinery<br/>init ~10k rps" .-> X[/"slow"/]
    style S fill:#c93c37,color:#fff
    style Q fill:#8b949e,color:#000
    style E fill:#8b949e,color:#000
    style J fill:#8b949e,color:#000
    style O fill:#8b949e,color:#000
    style A fill:#8b949e,color:#000
    style X fill:#c93c37,color:#fff
    style rmcp fill:#ffebe9,stroke:#c93c37
    style api2 fill:#f6f8fa,stroke:#8b949e
```

## Architecture — after (this crate)

```mermaid
flowchart TB
    C([mcp client])
    subgraph server ["raw ndjson dispatch"]
        direction TB
        L["serve_session<br/>one read/dispatch/write loop<br/>generic over AsyncRead/Write"]
        W["TimeoutAsyncRead<br/>30 s open-frame deadline<br/>single re-arm site (per-chunk refresh)"]
        D["dispatch<br/>initialize · tools/list · tools/call<br/>byte-pinned envelopes"]
        O2["SyncStdout<br/>direct write(2) syscall<br/>no blocking pool"]
        L --> W
        L --> D
        L --> O2
    end
    subgraph api2 ["api crate"]
        A2["zero-copy parse<br/>astral-tl + borrowed serde"]
    end
    C --> |"ndjson stdio<br/>30 s deadline"| L
    D --> |"typed calls"| A2
    O2 -. "158k rps init · 17× ping storm<br/>strict sequential replies" .-> C
    style L fill:#1a7f37,color:#fff
    style W fill:#1a7f37,color:#fff
    style D fill:#1a7f37,color:#fff
    style O2 fill:#1a7f37,color:#fff
    style A2 fill:#1a7f37,color:#fff
    style server fill:#e6f4ea,stroke:#1a7f37
    style api2 fill:#e6f4ea,stroke:#1a7f37
```

## Tools

`search` (no key) · `get_details` (key, `profile` arg) · `get_download_url` (key) · `prewarm` (no key) · `get_membership_status` (key)

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

## Notes

Fixed `V_2024-11-05` echo; tools/list pinned byte-exact by test. Sequential replies; batching skipped (matches upstream). HTTP transport: NO-GO, measured +3.0 MB for no consumer.

LOC 214 → 671: the dispatch loop, watchdog, envelopes — price of rmcp removal at byte-parity.

## License

MIT
