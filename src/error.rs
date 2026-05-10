use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Metadata error: {0}")]
    Metadata(#[from] crate::metadata::Error),

    #[error("Blob storage error: {0}")]
    Blob(#[from] crate::blob::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
