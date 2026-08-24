use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::metadata::MetadataError;

pub struct SqliteMetadataStore {
    pool: SqlitePool,
}

impl SqliteMetadataStore {
    /// Connects using the given options and applies any pending migrations.
    /// Callers choose the options — e.g. `SqliteConnectOptions::new().filename(path).create_if_missing(true)`
    /// for a real file-backed database.
    pub async fn connect(options: SqliteConnectOptions) -> Result<Self, MetadataError> {
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(MetadataError::Backend)?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    /// An in-memory database for tests. `max_connections(1)` keeps every
    /// pooled connection pointed at the same in-memory database — without
    /// it, each connection sqlx opens gets its own separate, empty one.
    #[cfg(test)]
    async fn connect_in_memory() -> Self {
        let options = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("in-memory sqlite pool should connect");
        Self::migrate(&pool)
            .await
            .expect("migrations should apply to an in-memory database");
        Self { pool }
    }

    async fn migrate(pool: &SqlitePool) -> Result<(), MetadataError> {
        sqlx::migrate!("src/metadata/sqlite/migrations")
            .run(pool)
            .await
            .map_err(|err| MetadataError::Backend(sqlx::Error::from(err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_in_memory_creates_the_object_metadata_table() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let row: (String,) = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'object_metadata'",
        )
        .fetch_one(&store.pool)
        .await
        .expect("object_metadata table should exist after migrations run");
        assert_eq!(row.0, "object_metadata");
    }
}
