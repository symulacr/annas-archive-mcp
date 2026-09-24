/// Classification of parse-path failures by failure mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseKind {
    MalformedJson,
    BodyTooLarge,
    EncodingInvalid,
    /// HTTP 200 with an implausibly tiny document body (parked domain).
    GarbagePage,
}

impl ParseKind {
    /// Whether a retry at another origin can plausibly succeed.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::BodyTooLarge | Self::GarbagePage)
    }

    /// Class string for wire payloads (mcp `error_payload`).
    pub fn class_name(self) -> &'static str {
        match self {
            Self::MalformedJson => "MalformedJson",
            Self::BodyTooLarge => "BodyTooLarge",
            Self::EncodingInvalid => "EncodingInvalid",
            Self::GarbagePage => "GarbagePage",
        }
    }
}

/// One-line constructors for the message-carrying variants.
#[rustfmt::skip]
pub(crate) fn parse_error(message: impl Into<String>, kind: ParseKind) -> Error {
    Error::Parse { message: message.into(), kind }
}

#[rustfmt::skip]
pub(crate) fn api_error(message: impl Into<String>) -> Error {
    Error::Api { message: message.into() }
}

#[rustfmt::skip]
pub(crate) fn all_domains_error(message: impl Into<String>) -> Error {
    Error::AllDomainsFailed { message: message.into() }
}

#[rustfmt::skip]
pub(crate) fn blocked(message: impl Into<String>) -> Error {
    Error::Blocked { message: message.into() }
}

/// Display strings are load-bearing: tests/error_taxonomy.rs asserts them
/// verbatim and mcp `error_payload` embeds them.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("HTTP error: status {status}")]
    Http { status: u16 },

    #[error("Parse error: {message}")]
    Parse { message: String, kind: ParseKind },

    #[error("API error: {message}")]
    Api { message: String },

    #[error("Missing API key - required for download URLs")]
    MissingApiKey,

    #[error("Site is challenging us (DDoS-Guard): {message}")]
    Blocked { message: String },

    #[error("All domains failed: {message}")]
    AllDomainsFailed { message: String },
}

impl Error {
    /// Failure class when this error originated on the parse path.
    pub fn kind(&self) -> Option<ParseKind> {
        match self {
            Error::Parse { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// Variant class name for wire payloads (mcp `error_payload`).
    #[rustfmt::skip]
    pub fn name(&self) -> &'static str {
        match self {
            Error::Parse { kind, .. } => kind.class_name(),
            Error::Network(_) => "Network",
            Error::Http { .. } => "Http",
            Error::Api { .. } => "Api",
            Error::MissingApiKey => "MissingApiKey",
            Error::Blocked { .. } => "Blocked",
            Error::AllDomainsFailed { .. } => "AllDomainsFailed",
        }
    }

    /// Whether trying the next domain/mirror can plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Network(e) => e.is_timeout() || e.is_connect(),
            Error::Parse { kind, .. } => kind.is_retryable(),
            _ => false,
        }
    }
}
