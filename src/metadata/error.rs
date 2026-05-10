use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // --- Redb Specific Conversions ---
    #[error("Database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("Transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("Commit error: {0}")]
    Commit(#[from] redb::CommitError),

    // --- Search Errors ---
    #[error("File {0} not found in metadata")]
    FileNotFound(Uuid),

    #[error("Object {0} not found in metadata")]
    ObjectNotFound(Uuid),

    #[error("Internal metadata error: {0}")]
    Internal(String),
}
