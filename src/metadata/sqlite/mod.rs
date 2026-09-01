use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

use crate::metadata::{
    CacheControl, ContentType, DataEncryptionContext, Etag, Metadata, MetadataError, MetadataStore,
    ObjectStorageClass, ObjectVersion,
};

pub struct SqliteMetadataStore {
    pool: SqlitePool,
}

impl SqliteMetadataStore {
    /// Connects using the given options and applies any pending migrations.
    ///
    /// For a real file-backed database: `SqliteConnectOptions::new().filename(path).create_if_missing(true)`.
    ///
    /// If you pass in-memory options (`SqliteConnectOptions::new().in_memory(true)`),
    /// be aware this constructor does not set `max_connections` — sqlx's pool default
    /// (10) means each pooled connection would open its own separate, empty in-memory
    /// database. For in-memory use, prefer a pool built with `max_connections(1)`
    /// (see this module's test-only `connect_in_memory()` for the pattern).
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

/// Encodes a caller-supplied `SystemTime` as epoch milliseconds. Pre-epoch
/// times are not representable, and callers hand us times that ultimately come
/// from request data, so this reports an error rather than panicking.
fn system_time_to_millis(time: SystemTime, field: &'static str) -> Result<i64, MetadataError> {
    time.duration_since(UNIX_EPOCH)
        .map_err(|_| MetadataError::Corrupt {
            field,
            detail: "SystemTime is before the unix epoch".to_string(),
        })
        .map(|duration| duration.as_millis() as i64)
}

fn millis_to_system_time(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis as u64)
}

fn row_to_metadata(row: &SqliteRow) -> Result<Metadata, MetadataError> {
    let storage_class_text: String = row
        .try_get("storage_class")
        .map_err(MetadataError::Backend)?;
    let storage_class =
        ObjectStorageClass::parse(&storage_class_text).ok_or_else(|| MetadataError::Corrupt {
            field: "storage_class",
            detail: format!("unrecognized storage class {storage_class_text:?}"),
        })?;

    let user_metadata_json: String = row.try_get("user_metadata").map_err(MetadataError::Backend)?;
    let user_metadata: HashMap<String, Bytes> = serde_json::from_str(&user_metadata_json)
        .map_err(|err| MetadataError::Corrupt {
            field: "user_metadata",
            detail: err.to_string(),
        })?;

    let encryption_context_json: Option<String> = row
        .try_get("encryption_context")
        .map_err(MetadataError::Backend)?;
    let encryption_context = encryption_context_json
        .map(|json| serde_json::from_str::<DataEncryptionContext>(&json))
        .transpose()
        .map_err(|err| MetadataError::Corrupt {
            field: "encryption_context",
            detail: err.to_string(),
        })?;

    let upload_id: Option<Vec<u8>> = row.try_get("upload_id").map_err(MetadataError::Backend)?;

    let size: usize = row
        .try_get::<i64, _>("size")
        .map_err(MetadataError::Backend)?
        .try_into()
        .map_err(|_| MetadataError::Corrupt {
            field: "size",
            detail: "stored size is negative".to_string(),
        })?;
    let backend_id: usize = row
        .try_get::<i64, _>("backend_id")
        .map_err(MetadataError::Backend)?
        .try_into()
        .map_err(|_| MetadataError::Corrupt {
            field: "backend_id",
            detail: "stored backend_id is negative".to_string(),
        })?;

    Ok(Metadata {
        etag: Etag(Bytes::from(
            row.try_get::<Vec<u8>, _>("etag")
                .map_err(MetadataError::Backend)?,
        )),
        last_modified: millis_to_system_time(
            row.try_get("last_modified").map_err(MetadataError::Backend)?,
        ),
        size,
        cache_control: CacheControl(row.try_get("cache_control").map_err(MetadataError::Backend)?),
        backend_id,
        bucket: row.try_get("bucket").map_err(MetadataError::Backend)?,
        key: row.try_get("key").map_err(MetadataError::Backend)?,
        content_type: row
            .try_get::<Option<String>, _>("content_type")
            .map_err(MetadataError::Backend)?
            .map(ContentType),
        content_disposition: row
            .try_get("content_disposition")
            .map_err(MetadataError::Backend)?,
        content_language: row
            .try_get("content_language")
            .map_err(MetadataError::Backend)?,
        version: ObjectVersion(row.try_get("version").map_err(MetadataError::Backend)?),
        cloned_at: row
            .try_get::<Option<i64>, _>("cloned_at")
            .map_err(MetadataError::Backend)?
            .map(millis_to_system_time),
        upload_id: upload_id.map(Bytes::from),
        is_latest: row
            .try_get::<i64, _>("is_latest")
            .map_err(MetadataError::Backend)?
            != 0,
        delete_marker: row
            .try_get::<i64, _>("delete_marker")
            .map_err(MetadataError::Backend)?
            != 0,
        user_metadata,
        storage_class,
        encryption_context,
    })
}

async fn upsert_row<'e, E>(executor: E, metadata: &Metadata) -> Result<(), MetadataError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let user_metadata_json = serde_json::to_string(&metadata.user_metadata)
        .expect("HashMap<String, Bytes> should always serialize");
    let encryption_context_json = metadata.encryption_context.as_ref().map(|ctx| {
        serde_json::to_string(ctx).expect("DataEncryptionContext should always serialize")
    });

    let last_modified_millis = system_time_to_millis(metadata.last_modified, "last_modified")?;
    let cloned_at_millis = metadata
        .cloned_at
        .map(|time| system_time_to_millis(time, "cloned_at"))
        .transpose()?;

    sqlx::query(
        "INSERT INTO object_metadata
         (bucket, key, version, etag, last_modified, size, cache_control, backend_id,
          content_type, content_disposition, content_language, cloned_at, upload_id,
          is_latest, delete_marker, user_metadata, storage_class, encryption_context)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (bucket, key, version) DO UPDATE SET
            etag = excluded.etag,
            last_modified = excluded.last_modified,
            size = excluded.size,
            cache_control = excluded.cache_control,
            backend_id = excluded.backend_id,
            content_type = excluded.content_type,
            content_disposition = excluded.content_disposition,
            content_language = excluded.content_language,
            cloned_at = excluded.cloned_at,
            upload_id = excluded.upload_id,
            is_latest = excluded.is_latest,
            delete_marker = excluded.delete_marker,
            user_metadata = excluded.user_metadata,
            storage_class = excluded.storage_class,
            encryption_context = excluded.encryption_context",
    )
    .bind(&metadata.bucket)
    .bind(&metadata.key)
    .bind(&metadata.version.0)
    .bind(metadata.etag.0.to_vec())
    .bind(last_modified_millis)
    .bind(metadata.size as i64)
    .bind(&metadata.cache_control.0)
    .bind(metadata.backend_id as i64)
    .bind(metadata.content_type.as_ref().map(|c| c.0.as_str()))
    .bind(metadata.content_disposition.as_deref())
    .bind(metadata.content_language.as_deref())
    .bind(cloned_at_millis)
    .bind(metadata.upload_id.as_ref().map(|b| b.to_vec()))
    .bind(metadata.is_latest as i64)
    .bind(metadata.delete_marker as i64)
    .bind(user_metadata_json)
    .bind(metadata.storage_class.as_str())
    .bind(encryption_context_json)
    .execute(executor)
    .await
    .map_err(MetadataError::Backend)?;

    Ok(())
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn put_versioned(&self, mut metadata: Metadata) -> Result<(), MetadataError> {
        metadata.is_latest = true;

        let mut tx = self.pool.begin().await.map_err(MetadataError::Backend)?;

        sqlx::query(
            "UPDATE object_metadata SET is_latest = 0 WHERE bucket = ? AND key = ? AND is_latest = 1",
        )
        .bind(&metadata.bucket)
        .bind(&metadata.key)
        .execute(&mut *tx)
        .await
        .map_err(MetadataError::Backend)?;

        upsert_row(&mut *tx, &metadata).await?;

        tx.commit().await.map_err(MetadataError::Backend)
    }

    async fn put_unversioned(&self, mut metadata: Metadata) -> Result<(), MetadataError> {
        metadata.version = ObjectVersion::unversioned();
        metadata.is_latest = true;

        let mut tx = self.pool.begin().await.map_err(MetadataError::Backend)?;

        // Versioned rows may already exist for this key (a bucket whose
        // versioning was suspended). Demote whichever of them is currently
        // latest — but never the sentinel row itself, since the upsert below
        // re-marks it latest anyway.
        sqlx::query(
            "UPDATE object_metadata SET is_latest = 0
             WHERE bucket = ? AND key = ? AND is_latest = 1 AND version <> ?",
        )
        .bind(&metadata.bucket)
        .bind(&metadata.key)
        .bind(&metadata.version.0)
        .execute(&mut *tx)
        .await
        .map_err(MetadataError::Backend)?;

        upsert_row(&mut *tx, &metadata).await?;

        tx.commit().await.map_err(MetadataError::Backend)
    }

    async fn get(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&ObjectVersion>,
    ) -> Result<Option<Metadata>, MetadataError> {
        let row = match version {
            Some(version) => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND key = ? AND version = ?",
                )
                .bind(bucket)
                .bind(key)
                .bind(&version.0)
                .fetch_optional(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND key = ? AND is_latest = 1",
                )
                .bind(bucket)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
            }
        }
        .map_err(MetadataError::Backend)?;

        row.map(|row| row_to_metadata(&row)).transpose()
    }

    async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        new_version: ObjectVersion,
    ) -> Result<(), MetadataError> {
        let marker = Metadata {
            etag: Etag(Bytes::new()),
            last_modified: SystemTime::now(),
            size: 0,
            cache_control: CacheControl(String::new()),
            backend_id: 0,
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: None,
            content_disposition: None,
            content_language: None,
            version: new_version,
            cloned_at: None,
            upload_id: None,
            is_latest: true,
            delete_marker: true,
            user_metadata: HashMap::new(),
            storage_class: ObjectStorageClass::Standard,
            encryption_context: None,
        };

        self.put_versioned(marker).await
    }

    async fn delete_specific_version(
        &self,
        bucket: &str,
        key: &str,
        version: &ObjectVersion,
    ) -> Result<(), MetadataError> {
        let mut tx = self.pool.begin().await.map_err(MetadataError::Backend)?;

        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ? AND version = ?")
            .bind(bucket)
            .bind(key)
            .bind(&version.0)
            .execute(&mut *tx)
            .await
            .map_err(MetadataError::Backend)?;

        // If that was the latest row, promote the next-most-recent remaining
        // row (by insertion order) — a no-op if some other row is already
        // latest, since only the deleted row could have held that flag.
        sqlx::query(
            "UPDATE object_metadata SET is_latest = 1
             WHERE id = (
                 SELECT id FROM object_metadata WHERE bucket = ? AND key = ? ORDER BY id DESC LIMIT 1
             )
             AND NOT EXISTS (
                 SELECT 1 FROM object_metadata WHERE bucket = ? AND key = ? AND is_latest = 1
             )",
        )
        .bind(bucket)
        .bind(key)
        .bind(bucket)
        .bind(key)
        .execute(&mut *tx)
        .await
        .map_err(MetadataError::Backend)?;

        tx.commit().await.map_err(MetadataError::Backend)
    }

    async fn delete_unversioned(&self, bucket: &str, key: &str) -> Result<(), MetadataError> {
        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ? AND version = ?")
            .bind(bucket)
            .bind(key)
            .bind(&ObjectVersion::unversioned().0)
            .execute(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;

        Ok(())
    }

    async fn list(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<Metadata>, MetadataError> {
        let rows = match prefix {
            Some(prefix) => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND is_latest = 1 AND key LIKE ? ESCAPE '\\' ORDER BY key",
                )
                .bind(bucket)
                .bind(like_prefix_pattern(prefix))
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND is_latest = 1 ORDER BY key",
                )
                .bind(bucket)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(MetadataError::Backend)?;

        rows.iter().map(row_to_metadata).collect()
    }

    async fn list_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Metadata>, MetadataError> {
        let rows = match prefix {
            Some(prefix) => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND key LIKE ? ESCAPE '\\' ORDER BY key, id DESC",
                )
                .bind(bucket)
                .bind(like_prefix_pattern(prefix))
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query("SELECT * FROM object_metadata WHERE bucket = ? ORDER BY key, id DESC")
                    .bind(bucket)
                    .fetch_all(&self.pool)
                    .await
            }
        }
        .map_err(MetadataError::Backend)?;

        rows.iter().map(row_to_metadata).collect()
    }

    async fn list_buckets(&self) -> Result<Vec<String>, MetadataError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT bucket FROM object_metadata ORDER BY bucket")
                .fetch_all(&self.pool)
                .await
                .map_err(MetadataError::Backend)?;

        Ok(rows.into_iter().map(|(bucket,)| bucket).collect())
    }
}

/// Escapes `%`/`_`/`\` in a caller-supplied prefix so `LIKE ... ESCAPE '\'`
/// matches only literal text — without this, a key that happens to contain
/// `%` or `_` would have those characters misread as SQL wildcards.
fn like_prefix_pattern(prefix: &str) -> String {
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("{escaped}%")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // The behavioral contract is exercised once, through the trait, by the
    // shared conformance suite. Only tests that need SQLite internals — the
    // schema, on-disk column decoding, planting a corrupt stored value —
    // belong here.
    crate::metadata::conformance::metadata_store_conformance!(conformance, || async {
        SqliteMetadataStore::connect_in_memory().await
    });

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
