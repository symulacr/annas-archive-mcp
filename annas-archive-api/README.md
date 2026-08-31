# annas-archive-api

Rust client library for [Anna's Archive](https://annas-archive.gd): search, metadata, fast-download URLs. Zero-copy parsing, 6 deps.

Rewritten from [RemiKalbe/annas-archive-mcp](https://github.com/RemiKalbe/annas-archive-mcp)'s library crate. Consumed by [`annas-archive-mcp`](../annas-archive-mcp/README.md); overview in [root README](../README.md).

| Metric | Upstream | Here | Δ |
|---|---:|---:|---:|
| Search parse | 28,169 ops/s | 218,125 ops/s | 7.7× |
| Record parse | 2,613 ops/s | 9,499 ops/s | 3.6× |
| Torrents parse | 43 ops/s | 560 ops/s | 13× |
| URL build | 6.81M ops/s | 63.90M ops/s | 9.4× |
| Peak RSS | 23.4 MB | 7.9 MB | −66% |
| Live latency | +1.75 s dead-mirror tax | 0 | −1.75 s/call |

Byte-equal outputs, fuzz clean (615 inputs).

## Architecture — before (upstream)

```mermaid
flowchart TB
    C([caller])
    subgraph parse ["parse layer"]
        direction LR
        P1["scraper DOM<br/>html5ever tree<br/>185 allocs/page"]
        P2["serde Value walk<br/>3,661 allocs/op<br/>14 MB torrent tree"]
    end
    subgraph http ["http layer"]
        H["reqwest, defaults<br/>whole-body buffer<br/>no caps · no timeout"]
    end
    subgraph mirrors ["mirror failover — hardcoded"]
        direction LR
        M1["annas-archive.org<br/>✗ DNS dead"]
        M2["annas-archive.se<br/>✗ DNS dead"]
        M3["annas-archive.li<br/>✗ parked → HTTP 200"]
    end
    C --> P1
    C --> P2
    P1 --> H
    P2 --> H
    H --> M1
    H --> M2
    H --> M3
    M3 -. "garbage parsed as data" .-> X[/"wrong data or error<br/>+1.75 s per call"/]
    style P1 fill:#8b949e,color:#000
    style P2 fill:#8b949e,color:#000
    style H fill:#8b949e,color:#000
    style M1 fill:#c93c37,color:#fff
    style M2 fill:#c93c37,color:#fff
    style M3 fill:#d4a72c,color:#000
    style X fill:#c93c37,color:#fff
    style parse fill:#f6f8fa,stroke:#8b949e
    style http fill:#f6f8fa,stroke:#8b949e
    style mirrors fill:#ffebe9,stroke:#c93c37
```

## Architecture — after (this crate)

```mermaid
flowchart TB
    C([caller])
    subgraph parse ["parse layer — zero-copy"]
        direction LR
        Q1["astral-tl arena<br/>42 allocs/page<br/>no DOM, no text copy"]
        Q2["borrowed serde<br/>618 allocs/op<br/>TorrentEntryRaw&lt;'a&gt;"]
        Q3["dhat-verified:<br/>zero Cow::Owned<br/>zero Value materialization"]
    end
    subgraph http ["http layer"]
        H2["caps 16/4 MB · 20 s timeout<br/>gzip/brotli · keep-alive 15 s<br/>request coalescing (opt-in)"]
    end
    subgraph net ["mirror pool — live"]
        direction LR
        N1[("annas-archive.gd<br/>primary")]
        N2[(".gl · .pk<br/>failover")]
    end
    C --> Q1
    C --> Q2
    Q1 --> H2
    Q2 --> H2
    H2 --> N1
    H2 --> N2
    Q3 -. "salvage path (opt-in)<br/>strict by default" .-> Q2
    H2 -. "CT mirror discovery<br/>(opt-in)" .-> N2
    Q1 --> |"7.7×"| OK[/"typed result<br/>Error{kind, retryable}"/]
    style Q1 fill:#1a7f37,color:#fff
    style Q2 fill:#1a7f37,color:#fff
    style Q3 fill:#1a7f37,color:#fff
    style H2 fill:#1a7f37,color:#fff
    style N1 fill:#1a7f37,color:#fff
    style N2 fill:#1a7f37,color:#fff
    style OK fill:#1a7f37,color:#fff
    style parse fill:#e6f4ea,stroke:#1a7f37
    style http fill:#e6f4ea,stroke:#1a7f37
    style net fill:#e6f4ea,stroke:#1a7f37
```

## Usage

```rust
let client = AnnasArchiveClient::new(key);
let results = client.search("rust", 1).await?;        // SearchResults
let details = client.get_details("md5").await?;       // ItemDetails
let url = client.fast_download_url("md5").await?;     // member URL
```

## Surface

`AnnasArchiveClient` (`with_page`, `with_lenient_records`, `with_request_coalescing`) · parsers `parse_search_results`, `parse_json_details[_mode]`, `parse_torrents` · types `SearchResults`, `ItemDetails`, `DownloadInfo` · errors `Parse{...}`, `Api`, `Http`, `Network`, `MissingApiKey`, `AllDomainsFailed` (+ `is_retryable`)

LOC 781 → 1,843: buys borrowed parsing, salvage, keep-alive engine, profiles.

## License

MIT
