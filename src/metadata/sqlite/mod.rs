use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

use crate::metadata::{
    CacheControl, ContentType, DataEncryptionContext, Etag, Metadata, MetadataError,
    ObjectStorageClass, ObjectVersion,
};

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

fn system_time_to_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .expect("system time should be after the unix epoch")
        .as_millis() as i64
}

fn millis_to_system_time(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis as u64)
}

fn row_to_metadata(row: &SqliteRow) -> Result<Metadata, sqlx::Error> {
    let storage_class_text: String = row.try_get("storage_class")?;
    let storage_class = ObjectStorageClass::parse(&storage_class_text).ok_or_else(|| {
        sqlx::Error::ColumnDecode {
            index: "storage_class".to_string(),
            source: format!("unrecognized storage class {storage_class_text:?}").into(),
        }
    })?;

    let user_metadata_json: String = row.try_get("user_metadata")?;
    let user_metadata: HashMap<String, Bytes> =
        serde_json::from_str(&user_metadata_json).map_err(|err| sqlx::Error::ColumnDecode {
            index: "user_metadata".to_string(),
            source: Box::new(err),
        })?;

    let encryption_context_json: Option<String> = row.try_get("encryption_context")?;
    let encryption_context = encryption_context_json
        .map(|json| serde_json::from_str::<DataEncryptionContext>(&json))
        .transpose()
        .map_err(|err| sqlx::Error::ColumnDecode {
            index: "encryption_context".to_string(),
            source: Box::new(err),
        })?;

    let upload_id: Option<Vec<u8>> = row.try_get("upload_id")?;

    Ok(Metadata {
        etag: Etag(Bytes::from(row.try_get::<Vec<u8>, _>("etag")?)),
        last_modified: millis_to_system_time(row.try_get("last_modified")?),
        size: row.try_get::<i64, _>("size")? as usize,
        cache_control: CacheControl(row.try_get("cache_control")?),
        backend_id: row.try_get::<i64, _>("backend_id")? as usize,
        bucket: row.try_get("bucket")?,
        key: row.try_get("key")?,
        content_type: row
            .try_get::<Option<String>, _>("content_type")?
            .map(ContentType),
        content_disposition: row.try_get("content_disposition")?,
        content_language: row.try_get("content_language")?,
        version: ObjectVersion(row.try_get("version")?),
        cloned_at: row
            .try_get::<Option<i64>, _>("cloned_at")?
            .map(millis_to_system_time),
        upload_id: upload_id.map(Bytes::from),
        is_latest: row.try_get::<i64, _>("is_latest")? != 0,
        delete_marker: row.try_get::<i64, _>("delete_marker")? != 0,
        user_metadata,
        storage_class,
        encryption_context,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{CacheControl, ContentType, Etag, ObjectStorageClass, ObjectVersion};
    use bytes::Bytes;

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

    #[tokio::test]
    async fn row_to_metadata_decodes_all_columns() {
        let store = SqliteMetadataStore::connect_in_memory().await;

        sqlx::query(
            "INSERT INTO object_metadata
             (bucket, key, version, etag, last_modified, size, cache_control, backend_id,
              content_type, content_disposition, content_language, cloned_at, upload_id,
              is_latest, delete_marker, user_metadata, storage_class, encryption_context)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("my-bucket")
        .bind("my-key")
        .bind("v1")
        .bind(b"\"abc123\"".to_vec())
        .bind(1_700_000_000_000i64)
        .bind(42i64)
        .bind("no-cache")
        .bind(1i64)
        .bind(Some("text/plain"))
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(Some(1_700_000_001_000i64))
        .bind(Some(vec![1u8, 2, 3]))
        .bind(1i64)
        .bind(0i64)
        .bind("{}")
        .bind("STANDARD")
        .bind(Option::<String>::None)
        .execute(&store.pool)
        .await
        .expect("insert should succeed");

        let row = sqlx::query("SELECT * FROM object_metadata WHERE bucket = 'my-bucket'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");

        let metadata = row_to_metadata(&row).expect("row should decode");

        assert_eq!(metadata.bucket, "my-bucket");
        assert_eq!(metadata.key, "my-key");
        assert_eq!(metadata.version, ObjectVersion("v1".to_string()));
        assert_eq!(metadata.etag, Etag(Bytes::from_static(b"\"abc123\"")));
        assert_eq!(metadata.size, 42);
        assert_eq!(
            metadata.content_type,
            Some(ContentType("text/plain".to_string()))
        );
        assert_eq!(metadata.content_disposition, None);
        assert_eq!(
            metadata.cloned_at,
            Some(millis_to_system_time(1_700_000_001_000))
        );
        assert_eq!(metadata.upload_id, Some(Bytes::from(vec![1u8, 2, 3])));
        assert!(metadata.is_latest);
        assert!(!metadata.delete_marker);
        assert!(metadata.user_metadata.is_empty());
        assert_eq!(metadata.storage_class, ObjectStorageClass::Standard);
        assert_eq!(metadata.encryption_context, None);
    }

    #[tokio::test]
    async fn row_to_metadata_rejects_unrecognized_storage_class() {
        let store = SqliteMetadataStore::connect_in_memory().await;

        sqlx::query(
            "INSERT INTO object_metadata
             (bucket, key, version, etag, last_modified, size, cache_control, backend_id,
              content_type, content_disposition, content_language, cloned_at, upload_id,
              is_latest, delete_marker, user_metadata, storage_class, encryption_context)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("b")
        .bind("k")
        .bind("v1")
        .bind(b"etag".to_vec())
        .bind(0i64)
        .bind(0i64)
        .bind("")
        .bind(0i64)
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(Option::<i64>::None)
        .bind(Option::<Vec<u8>>::None)
        .bind(1i64)
        .bind(0i64)
        .bind("{}")
        .bind("NOT_A_REAL_CLASS")
        .bind(Option::<String>::None)
        .execute(&store.pool)
        .await
        .expect("insert should succeed");

        let row = sqlx::query("SELECT * FROM object_metadata WHERE bucket = 'b'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");

        assert!(row_to_metadata(&row).is_err());
    }
}
