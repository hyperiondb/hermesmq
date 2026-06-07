use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("not leader; current leader: {0:?}")]
    NotLeader(Option<String>),

    #[error("unknown topic: {0}")]
    UnknownTopic(String),

    #[error("rate limited; retry after {retry_after_ms}ms")]
    RateLimited { retry_after_ms: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;
