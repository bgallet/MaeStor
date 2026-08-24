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

async fn upsert_row<'e, E>(executor: E, metadata: &Metadata) -> Result<(), MetadataError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let user_metadata_json = serde_json::to_string(&metadata.user_metadata)
        .expect("HashMap<String, Bytes> should always serialize");
    let encryption_context_json = metadata.encryption_context.as_ref().map(|ctx| {
        serde_json::to_string(ctx).expect("DataEncryptionContext should always serialize")
    });

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
    .bind(system_time_to_millis(metadata.last_modified))
    .bind(metadata.size as i64)
    .bind(&metadata.cache_control.0)
    .bind(metadata.backend_id as i64)
    .bind(metadata.content_type.as_ref().map(|c| c.0.as_str()))
    .bind(metadata.content_disposition.as_deref())
    .bind(metadata.content_language.as_deref())
    .bind(metadata.cloned_at.map(system_time_to_millis))
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

        upsert_row(&self.pool, &metadata).await
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

        row.map(|row| row_to_metadata(&row))
            .transpose()
            .map_err(MetadataError::Backend)
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

        rows.iter()
            .map(row_to_metadata)
            .collect::<Result<Vec<_>, _>>()
            .map_err(MetadataError::Backend)
    }

    async fn list_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Metadata>, MetadataError> {
        let rows = match prefix {
            Some(prefix) => {
                sqlx::query(
                    "SELECT * FROM object_metadata WHERE bucket = ? AND key LIKE ? ESCAPE '\\' ORDER BY key, id",
                )
                .bind(bucket)
                .bind(like_prefix_pattern(prefix))
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query("SELECT * FROM object_metadata WHERE bucket = ? ORDER BY key, id")
                    .bind(bucket)
                    .fetch_all(&self.pool)
                    .await
            }
        }
        .map_err(MetadataError::Backend)?;

        rows.iter()
            .map(row_to_metadata)
            .collect::<Result<Vec<_>, _>>()
            .map_err(MetadataError::Backend)
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
    use crate::metadata::Metadata;
    use crate::metadata::{CacheControl, ContentType, Etag, ObjectStorageClass, ObjectVersion};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::time::SystemTime;

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

    fn sample_metadata(bucket: &str, key: &str, version: &str) -> Metadata {
        Metadata {
            etag: Etag(Bytes::from_static(b"\"etag\"")),
            last_modified: SystemTime::now(),
            size: 10,
            cache_control: CacheControl("no-cache".to_string()),
            backend_id: 1,
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: Some(ContentType("text/plain".to_string())),
            content_disposition: None,
            content_language: None,
            version: ObjectVersion(version.to_string()),
            cloned_at: None,
            upload_id: None,
            is_latest: false,
            delete_marker: false,
            user_metadata: HashMap::new(),
            storage_class: ObjectStorageClass::Standard,
            encryption_context: None,
        }
    }

    async fn row_count(store: &SqliteMetadataStore, bucket: &str, key: &str) -> i64 {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM object_metadata WHERE bucket = ? AND key = ?")
                .bind(bucket)
                .bind(key)
                .fetch_one(&store.pool)
                .await
                .expect("count query should succeed");
        count
    }

    #[tokio::test]
    async fn put_versioned_inserts_a_new_latest_and_demotes_the_old_one() {
        let store = SqliteMetadataStore::connect_in_memory().await;

        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("first put should succeed");
        store
            .put_versioned(sample_metadata("b", "k", "v2"))
            .await
            .expect("second put should succeed");

        assert_eq!(row_count(&store, "b", "k").await, 2);

        let row = sqlx::query(
            "SELECT * FROM object_metadata WHERE bucket = 'b' AND key = 'k' AND is_latest = 1",
        )
        .fetch_one(&store.pool)
        .await
        .expect("exactly one latest row should exist");
        let latest = row_to_metadata(&row).expect("row should decode");
        assert_eq!(latest.version, ObjectVersion("v2".to_string()));
    }

    #[tokio::test]
    async fn put_unversioned_upserts_a_single_row() {
        let store = SqliteMetadataStore::connect_in_memory().await;

        let mut first = sample_metadata("b", "k", "ignored");
        first.size = 10;
        store
            .put_unversioned(first)
            .await
            .expect("first put should succeed");

        let mut second = sample_metadata("b", "k", "also-ignored");
        second.size = 20;
        store
            .put_unversioned(second)
            .await
            .expect("second put should succeed");

        assert_eq!(row_count(&store, "b", "k").await, 1);

        let row = sqlx::query("SELECT * FROM object_metadata WHERE bucket = 'b' AND key = 'k'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");
        let metadata = row_to_metadata(&row).expect("row should decode");
        assert_eq!(metadata.version, ObjectVersion::unversioned());
        assert_eq!(metadata.size, 20);
        assert!(metadata.is_latest);
    }

    #[tokio::test]
    async fn get_with_no_version_returns_the_latest_row() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "k", "v2"))
            .await
            .expect("put should succeed");

        let found = store
            .get("b", "k", None)
            .await
            .expect("get should succeed")
            .expect("a row should be found");
        assert_eq!(found.version, ObjectVersion("v2".to_string()));
    }

    #[tokio::test]
    async fn get_with_a_specific_version_returns_that_version_even_if_not_latest() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "k", "v2"))
            .await
            .expect("put should succeed");

        let found = store
            .get("b", "k", Some(&ObjectVersion("v1".to_string())))
            .await
            .expect("get should succeed")
            .expect("a row should be found");
        assert_eq!(found.version, ObjectVersion("v1".to_string()));
        assert!(!found.is_latest);
    }

    #[tokio::test]
    async fn get_returns_none_for_a_key_that_was_never_written() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let found = store
            .get("no-such-bucket", "no-such-key", None)
            .await
            .expect("get should succeed");
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn delete_versioned_creates_a_marker_as_the_new_latest() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");

        store
            .delete_versioned("b", "k", ObjectVersion("marker1".to_string()))
            .await
            .expect("delete_versioned should succeed");

        let latest = store
            .get("b", "k", None)
            .await
            .expect("get should succeed")
            .expect("a row should be found");
        assert_eq!(latest.version, ObjectVersion("marker1".to_string()));
        assert!(latest.delete_marker);

        let original = store
            .get("b", "k", Some(&ObjectVersion("v1".to_string())))
            .await
            .expect("get should succeed")
            .expect("original version should still exist");
        assert!(!original.delete_marker);
    }

    #[tokio::test]
    async fn delete_specific_version_removes_only_that_row() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "k", "v2"))
            .await
            .expect("put should succeed");

        store
            .delete_specific_version("b", "k", &ObjectVersion("v1".to_string()))
            .await
            .expect("delete should succeed");

        assert_eq!(row_count(&store, "b", "k").await, 1);
        let latest = store
            .get("b", "k", None)
            .await
            .expect("get should succeed")
            .expect("a row should be found");
        assert_eq!(latest.version, ObjectVersion("v2".to_string()));
    }

    #[tokio::test]
    async fn delete_specific_version_promotes_the_next_latest_when_the_latest_is_removed() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "k", "v2"))
            .await
            .expect("put should succeed");

        store
            .delete_specific_version("b", "k", &ObjectVersion("v2".to_string()))
            .await
            .expect("delete should succeed");

        let latest = store
            .get("b", "k", None)
            .await
            .expect("get should succeed")
            .expect("v1 should have been promoted to latest");
        assert_eq!(latest.version, ObjectVersion("v1".to_string()));
        assert!(latest.is_latest);
    }

    #[tokio::test]
    async fn delete_unversioned_removes_the_sentinel_row() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_unversioned(sample_metadata("b", "k", "ignored"))
            .await
            .expect("put should succeed");

        store
            .delete_unversioned("b", "k")
            .await
            .expect("delete should succeed");

        let found = store
            .get("b", "k", None)
            .await
            .expect("get should succeed");
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn list_returns_latest_rows_for_a_bucket() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store.put_versioned(sample_metadata("b", "a", "v1")).await.expect("put should succeed");
        store.put_versioned(sample_metadata("b", "a", "v2")).await.expect("put should succeed");
        store.put_versioned(sample_metadata("b", "c", "v1")).await.expect("put should succeed");
        store
            .put_versioned(sample_metadata("other-bucket", "a", "v1"))
            .await
            .expect("put should succeed");

        let listed = store.list("b", None).await.expect("list should succeed");
        assert_eq!(listed.len(), 2);
        let versions: Vec<_> = listed.iter().map(|m| m.version.0.as_str()).collect();
        assert_eq!(versions, vec!["v2", "v1"]);
    }

    #[tokio::test]
    async fn list_filters_by_prefix() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "docs/a", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "docs/b", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "images/c", "v1"))
            .await
            .expect("put should succeed");

        let listed = store.list("b", Some("docs/")).await.expect("list should succeed");
        let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["docs/a", "docs/b"]);
    }

    #[tokio::test]
    async fn list_prefix_does_not_treat_percent_or_underscore_as_wildcards() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "100%_off", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("b", "100X_off", "v1"))
            .await
            .expect("put should succeed");

        let listed = store.list("b", Some("100%")).await.expect("list should succeed");
        let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["100%_off"]);
    }

    #[tokio::test]
    async fn list_versions_returns_every_version_including_markers() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .delete_versioned("b", "k", ObjectVersion("marker1".to_string()))
            .await
            .expect("delete should succeed");

        let versions = store
            .list_versions("b", None)
            .await
            .expect("list_versions should succeed");
        let version_ids: Vec<_> = versions.iter().map(|m| m.version.0.as_str()).collect();
        assert_eq!(version_ids, vec!["v1", "marker1"]);
        assert!(versions.iter().any(|m| m.delete_marker));
    }

    #[tokio::test]
    async fn list_buckets_returns_distinct_bucket_names() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store
            .put_versioned(sample_metadata("bucket-b", "k", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("bucket-a", "k1", "v1"))
            .await
            .expect("put should succeed");
        store
            .put_versioned(sample_metadata("bucket-a", "k2", "v1"))
            .await
            .expect("put should succeed");

        let buckets = store.list_buckets().await.expect("list_buckets should succeed");
        assert_eq!(buckets, vec!["bucket-a".to_string(), "bucket-b".to_string()]);
    }
}
