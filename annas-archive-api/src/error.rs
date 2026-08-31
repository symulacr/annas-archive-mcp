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

#[derive(Debug)]
pub enum Error {
    Network(reqwest::Error),

    Http { status: u16 },

    Parse { message: String, kind: ParseKind },

    Api { message: String },

    MissingApiKey,

    AllDomainsFailed { message: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Network(e) => write!(f, "Network error: {e}"),
            Error::Http { status } => write!(f, "HTTP error: status {status}"),
            Error::Parse { message, .. } => write!(f, "Parse error: {message}"),
            Error::Api { message } => write!(f, "API error: {message}"),
            Error::MissingApiKey => write!(f, "Missing API key - required for download URLs"),
            Error::AllDomainsFailed { message } => write!(f, "All domains failed: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Network(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Network(e)
    }
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
