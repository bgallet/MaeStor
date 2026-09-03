use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

use crate::metadata::{
    Bucket, BucketVersioning, CacheControl, ContentType, DataEncryptionContext, Etag,
    KnownContentType, ListPage, ListParams, Metadata, MetadataError, MetadataStore,
    ObjectStorageClass, ObjectVersion,
};

pub struct SqliteMetadataStore {
    pool: SqlitePool,
}

/// A batch never fetches more than this many rows at once, regardless of
/// `max_keys` — a delimiter-heavy scan re-queries as it skips groups.
const LIST_BATCH_CAP: usize = 1000;

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

    /// One batch of the paged scan: rows for `bucket` with `key` in
    /// `[lower, upper)` (lower from `prefix`/`pos`, `upper` from
    /// `prefix_successor`), optionally `is_latest = 1`, ordered for the
    /// operation. Returns `(key, rowid, metadata)` triples.
    async fn list_batch(
        &self,
        bucket: &str,
        prefix: &str,
        pos: Option<&Pos>,
        upper: Option<&str>,
        latest_only: bool,
        limit: i64,
    ) -> Result<Vec<(String, i64, Metadata)>, MetadataError> {
        // Lower bound: the greater of the prefix and any cursor key.
        let lower = match pos {
            None => prefix.to_string(),
            Some(Pos::AtKey(k)) | Some(Pos::AfterRow { key: k, .. }) => {
                if k.as_str() > prefix { k.clone() } else { prefix.to_string() }
            }
        };

        let sql = list_batch_sql(upper.is_some(), pos, latest_only);

        let mut query = sqlx::query(&sql).bind(bucket).bind(lower);
        if let Some(upper) = upper {
            query = query.bind(upper.to_string());
        }
        match pos {
            Some(Pos::AfterRow { key, .. }) if latest_only => {
                query = query.bind(key.clone());
            }
            Some(Pos::AfterRow { key, id }) => {
                query = query.bind(key.clone()).bind(key.clone()).bind(*id);
            }
            _ => {}
        }
        let rows = query
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;

        rows.iter()
            .map(|row| {
                let key: String = row.try_get("key").map_err(MetadataError::Backend)?;
                let id: i64 = row.try_get("id").map_err(MetadataError::Backend)?;
                Ok((key, id, row_to_metadata(row)?))
            })
            .collect()
    }

    /// The shared paging loop behind `list` and `list_versions`.
    async fn list_page(
        &self,
        bucket: &str,
        params: ListParams<'_>,
        latest_only: bool,
    ) -> Result<ListPage, MetadataError> {
        let prefix = params.prefix.unwrap_or("");
        let upper = prefix_successor(prefix);
        let mut pos = params.cursor.map(Pos::decode).transpose()?;
        // `max_keys` of 0 is a caller-contract violation (documented on
        // `ListParams`); treat it as 1 rather than panic on the empty `pos`.
        let max_keys = params.max_keys.max(1);
        let limit = max_keys.min(LIST_BATCH_CAP) as i64;

        let mut items: Vec<Metadata> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();

        loop {
            let batch = self
                .list_batch(bucket, prefix, pos.as_ref(), upper.as_deref(), latest_only, limit)
                .await?;
            let batch_len = batch.len();
            let mut rows = batch.into_iter().peekable();

            while let Some((key, id, metadata)) = rows.next() {
                if items.len() + common_prefixes.len() == max_keys {
                    // `key` is a real row that would extend the result, so the
                    // page is truncated. `pos` already points just past the
                    // last emitted output.
                    let cursor = pos.as_ref().expect("pos is set once an output exists");
                    return Ok(ListPage {
                        items,
                        common_prefixes,
                        next_cursor: Some(cursor.encode()),
                    });
                }

                // `list_batch`'s range bounds guarantee `key.starts_with(prefix)`,
                // so the `&key[prefix.len()..]` slice inside `delimiter_group`
                // cannot panic on a char boundary or a short key.
                match params.delimiter.and_then(|d| delimiter_group(&key, prefix, d)) {
                    Some(cp) => {
                        // Compute the group's successor before moving `cp` into
                        // the output — no clone needed.
                        let succ = prefix_successor(&cp);
                        common_prefixes.push(cp);
                        match succ {
                            Some(succ) => {
                                // Skip the rest of this group still in the batch.
                                while rows.peek().is_some_and(|(k, _, _)| k < &succ) {
                                    rows.next();
                                }
                                pos = Some(Pos::AtKey(succ));
                            }
                            None => {
                                // Nothing can sort after this group.
                                return Ok(ListPage {
                                    items,
                                    common_prefixes,
                                    next_cursor: None,
                                });
                            }
                        }
                    }
                    None => {
                        items.push(metadata);
                        pos = Some(Pos::AfterRow { key, id });
                    }
                }
            }

            // A short batch means the range is exhausted.
            if (batch_len as i64) < limit {
                return Ok(ListPage { items, common_prefixes, next_cursor: None });
            }
        }
    }

    /// Shared body for the blob-valued bucket setters. `column` is a static
    /// literal (`"acl"` / `"cors"` / `"lifecycle"`), never caller data.
    async fn set_bucket_blob(
        &self,
        name: &str,
        column: &str,
        value: Option<Bytes>,
    ) -> Result<(), MetadataError> {
        let modified = system_time_to_millis(SystemTime::now(), "modified_at")?;
        let sql = format!("UPDATE buckets SET {column} = ?, modified_at = ? WHERE name = ?");
        let affected = sqlx::query(&sql)
            .bind(value.map(|b| b.to_vec()))
            .bind(modified)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(MetadataError::Backend)?
            .rows_affected();
        if affected == 0 {
            return Err(MetadataError::NoSuchBucket { name: name.to_string() });
        }
        Ok(())
    }
}

/// Builds the `list_batch` query string for a given batch shape, with `?`
/// placeholders. Kept separate from [`SqliteMetadataStore::list_batch`] so the
/// query-plan tests can EXPLAIN the exact SQL every branch produces — the
/// cursor-resume terms included. The bind sequence in `list_batch` must stay in
/// lockstep with the placeholders here: `bucket`, `lower`, then `upper` (if
/// `has_upper`), then the resume params (`key` for latest-only `AfterRow`; or
/// `key`, `key`, `id` otherwise), then `limit`.
fn list_batch_sql(has_upper: bool, pos: Option<&Pos>, latest_only: bool) -> String {
    let mut sql = String::from("SELECT * FROM object_metadata WHERE bucket = ? AND key >= ?");
    if has_upper {
        sql.push_str(" AND key < ?");
    }
    match pos {
        Some(Pos::AfterRow { .. }) if latest_only => sql.push_str(" AND key > ?"),
        Some(Pos::AfterRow { .. }) => sql.push_str(" AND (key > ? OR (key = ? AND id < ?))"),
        _ => {}
    }
    if latest_only {
        sql.push_str(" AND is_latest = 1 ORDER BY key LIMIT ?");
    } else {
        sql.push_str(" ORDER BY key, id DESC LIMIT ?");
    }
    sql
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

/// Encodes a content type for the `content_type` BLOB column: a known type is
/// its one-byte code, anything else is its raw UTF-8. An `Other` shorter than
/// two bytes is rejected — it could not be told apart from a known-type code
/// on the way back out.
fn encode_content_type(content_type: &ContentType) -> Result<Vec<u8>, MetadataError> {
    match content_type {
        ContentType::Known(known) => Ok(vec![known.code()]),
        ContentType::Other(value) if value.len() >= 2 => Ok(value.clone().into_bytes()),
        ContentType::Other(value) => Err(MetadataError::Corrupt {
            field: "content_type",
            detail: format!("content type {value:?} is too short to store unambiguously"),
        }),
    }
}

/// Inverse of [`encode_content_type`]. The caller maps a NULL column to `None`
/// before calling this.
fn decode_content_type(bytes: Vec<u8>) -> Result<ContentType, MetadataError> {
    match bytes.as_slice() {
        [code] => KnownContentType::from_code(*code)
            .map(ContentType::Known)
            .ok_or_else(|| MetadataError::Corrupt {
                field: "content_type",
                detail: format!("unassigned known content-type code {code}"),
            }),
        [] => Err(MetadataError::Corrupt {
            field: "content_type",
            detail: "stored content type is empty".to_string(),
        }),
        _ => String::from_utf8(bytes)
            .map(ContentType::Other)
            .map_err(|err| MetadataError::Corrupt {
                field: "content_type",
                detail: err.to_string(),
            }),
    }
}

/// The shortest string strictly greater than every string beginning with
/// `prefix`, in SQLite `TEXT` order (bytewise, which for UTF-8 is codepoint
/// order). Operates on `char`s, so the result is always valid UTF-8 and can be
/// bound as a `TEXT` parameter. `None` when there is no such string: `prefix`
/// is empty or entirely `char::MAX`.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        if next == 0xD800 {
            next = 0xE000; // step over the UTF-16 surrogate range
        }
        if let Some(next) = char::from_u32(next) {
            let mut out: String = chars.iter().collect();
            out.push(next);
            return Some(out);
        }
        // `last` was char::MAX (U+10FFFF): drop it and carry to the previous char.
    }
    None
}

/// A scan position for the paged list queries — the decoded form of a
/// `ListParams::cursor` and the value re-encoded into `ListPage::next_cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pos {
    /// Resume at `key >= key`. Produced by a bare-key cursor and by skipping
    /// past a common-prefix group.
    AtKey(String),
    /// Resume strictly after the row `(key, id)`.
    AfterRow { key: String, id: i64 },
}

impl Pos {
    fn encode(&self) -> String {
        let mut frame = Vec::new();
        match self {
            Pos::AtKey(key) => {
                frame.push(0x00);
                frame.extend_from_slice(key.as_bytes());
            }
            Pos::AfterRow { key, id } => {
                frame.push(0x01);
                frame.extend_from_slice(&id.to_be_bytes());
                frame.extend_from_slice(key.as_bytes());
            }
        }
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(frame)
    }

    fn decode(cursor: &str) -> Result<Self, MetadataError> {
        let bad = |detail: &str| MetadataError::InvalidCursor {
            detail: detail.to_string(),
        };
        let frame = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|_| bad("not valid base64"))?;
        let (&tag, rest) = frame.split_first().ok_or_else(|| bad("empty cursor"))?;
        match tag {
            0x00 => Ok(Pos::AtKey(decode_cursor_key(rest)?)),
            0x01 => {
                let id_bytes: [u8; 8] = rest
                    .get(..8)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| bad("cursor row id is truncated"))?;
                Ok(Pos::AfterRow {
                    id: i64::from_be_bytes(id_bytes),
                    key: decode_cursor_key(&rest[8..])?,
                })
            }
            other => Err(bad(&format!("unknown cursor tag {other:#04x}"))),
        }
    }
}

/// If `key` (already known to start with `prefix`) contains `delimiter`
/// somewhere after `prefix`, returns the common prefix it rolls up into:
/// `prefix` plus everything through that first delimiter. `None` means `key`
/// is a plain item.
fn delimiter_group(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    let rest = &key[prefix.len()..];
    let idx = rest.find(delimiter)?;
    Some(format!("{prefix}{}", &rest[..idx + delimiter.len()]))
}

fn decode_cursor_key(bytes: &[u8]) -> Result<String, MetadataError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| MetadataError::InvalidCursor {
        detail: "cursor key is not valid UTF-8".to_string(),
    })
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
            .try_get::<Option<Vec<u8>>, _>("content_type")
            .map_err(MetadataError::Backend)?
            .map(decode_content_type)
            .transpose()?,
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

fn row_to_bucket(row: &SqliteRow) -> Result<Bucket, MetadataError> {
    let versioning_text: String = row.try_get("versioning").map_err(MetadataError::Backend)?;
    let versioning =
        BucketVersioning::parse(&versioning_text).ok_or_else(|| MetadataError::Corrupt {
            field: "versioning",
            detail: format!("unrecognized bucket versioning state {versioning_text:?}"),
        })?;

    let blob = |name: &str| -> Result<Option<Bytes>, MetadataError> {
        Ok(row
            .try_get::<Option<Vec<u8>>, _>(name)
            .map_err(MetadataError::Backend)?
            .map(Bytes::from))
    };

    Ok(Bucket {
        name: row.try_get("name").map_err(MetadataError::Backend)?,
        owner: row.try_get("owner").map_err(MetadataError::Backend)?,
        created_at: millis_to_system_time(
            row.try_get("created_at").map_err(MetadataError::Backend)?,
        ),
        modified_at: millis_to_system_time(
            row.try_get("modified_at").map_err(MetadataError::Backend)?,
        ),
        versioning,
        acl: blob("acl")?,
        cors: blob("cors")?,
        lifecycle: blob("lifecycle")?,
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

    let content_type_blob = metadata
        .content_type
        .as_ref()
        .map(encode_content_type)
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
    .bind(content_type_blob)
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

    async fn list(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError> {
        self.list_page(bucket, params, true).await
    }

    async fn list_versions(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError> {
        self.list_page(bucket, params, false).await
    }

    async fn list_buckets(&self, owner: &str) -> Result<Vec<Bucket>, MetadataError> {
        let rows = sqlx::query("SELECT * FROM buckets WHERE owner = ? ORDER BY name")
            .bind(owner)
            .fetch_all(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;
        rows.iter().map(row_to_bucket).collect()
    }

    async fn create_bucket(&self, name: &str, owner: &str) -> Result<Bucket, MetadataError> {
        let now_millis = system_time_to_millis(SystemTime::now(), "created_at")?;
        let versioning = BucketVersioning::default();

        let result = sqlx::query(
            "INSERT INTO buckets (name, owner, created_at, modified_at, versioning)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(name)
        .bind(owner)
        .bind(now_millis)
        .bind(now_millis)
        .bind(versioning.as_str())
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(Bucket {
                name: name.to_string(),
                owner: owner.to_string(),
                created_at: millis_to_system_time(now_millis),
                modified_at: millis_to_system_time(now_millis),
                versioning,
                acl: None,
                cors: None,
                lifecycle: None,
            }),
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                Err(MetadataError::BucketAlreadyExists { name: name.to_string() })
            }
            Err(err) => Err(MetadataError::Backend(err)),
        }
    }

    async fn get_bucket(&self, name: &str) -> Result<Option<Bucket>, MetadataError> {
        let row = sqlx::query("SELECT * FROM buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;
        row.map(|row| row_to_bucket(&row)).transpose()
    }

    async fn delete_bucket(&self, name: &str) -> Result<(), MetadataError> {
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;
        Ok(())
    }

    async fn set_bucket_versioning(
        &self,
        name: &str,
        state: BucketVersioning,
    ) -> Result<(), MetadataError> {
        let modified = system_time_to_millis(SystemTime::now(), "modified_at")?;
        let affected = sqlx::query(
            "UPDATE buckets SET versioning = ?, modified_at = ? WHERE name = ?",
        )
        .bind(state.as_str())
        .bind(modified)
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(MetadataError::Backend)?
        .rows_affected();
        if affected == 0 {
            return Err(MetadataError::NoSuchBucket { name: name.to_string() });
        }
        Ok(())
    }

    async fn set_bucket_acl(&self, name: &str, acl: Option<Bytes>) -> Result<(), MetadataError> {
        self.set_bucket_blob(name, "acl", acl).await
    }

    async fn set_bucket_cors(&self, name: &str, cors: Option<Bytes>) -> Result<(), MetadataError> {
        self.set_bucket_blob(name, "cors", cors).await
    }

    async fn set_bucket_lifecycle(
        &self,
        name: &str,
        lifecycle: Option<Bytes>,
    ) -> Result<(), MetadataError> {
        self.set_bucket_blob(name, "lifecycle", lifecycle).await
    }
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
        .bind(Some(vec![KnownContentType::TextPlain.code()]))
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
            Some(ContentType::Known(KnownContentType::TextPlain))
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

    #[tokio::test]
    async fn row_to_metadata_rejects_a_single_byte_content_type_with_no_assigned_code() {
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
        .bind(Some(vec![250u8]))
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(Option::<i64>::None)
        .bind(Option::<Vec<u8>>::None)
        .bind(1i64)
        .bind(0i64)
        .bind("{}")
        .bind("STANDARD")
        .bind(Option::<String>::None)
        .execute(&store.pool)
        .await
        .expect("insert should succeed");

        let row = sqlx::query("SELECT * FROM object_metadata WHERE bucket = 'b'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");

        let err = row_to_metadata(&row).expect_err("an unassigned code should not decode");
        assert!(
            matches!(err, MetadataError::Corrupt { field: "content_type", .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn prefix_successor_increments_the_last_character() {
        assert_eq!(prefix_successor("abc").as_deref(), Some("abd"));
        // '/' (0x2F) -> '0' (0x30): the delimiter-group skip case.
        assert_eq!(prefix_successor("photos/").as_deref(), Some("photos0"));
    }

    #[test]
    fn prefix_successor_carries_over_a_char_max_tail() {
        assert_eq!(prefix_successor("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_successor("a\u{10FFFF}\u{10FFFF}").as_deref(), Some("b"));
    }

    #[test]
    fn prefix_successor_steps_over_the_surrogate_gap() {
        assert_eq!(prefix_successor("x\u{D7FF}").as_deref(), Some("x\u{E000}"));
    }

    #[test]
    fn prefix_successor_is_none_for_empty_or_all_char_max() {
        assert_eq!(prefix_successor(""), None);
        assert_eq!(prefix_successor("\u{10FFFF}"), None);
        assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
    }

    fn b64(frame: impl AsRef<[u8]>) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(frame)
    }

    #[test]
    fn cursor_round_trips_an_at_key_position() {
        // Keys are UTF-8 but may contain control bytes such as NUL.
        let pos = Pos::AtKey("photos/2024/\u{0}odd".to_string());
        assert_eq!(Pos::decode(&pos.encode()).unwrap(), pos);
    }

    #[test]
    fn cursor_round_trips_an_after_row_position() {
        let pos = Pos::AfterRow { key: "a/b/c".to_string(), id: 123_456 };
        assert_eq!(Pos::decode(&pos.encode()).unwrap(), pos);
    }

    #[test]
    fn cursor_decode_rejects_bad_input() {
        for bad in [
            "!!! not base64 !!!".to_string(),
            b64([]),                       // empty frame
            b64([0x09, b'k']),             // unknown tag
            b64([0x01, 0, 0, 0]),          // tag 0x01, id truncated
            b64([0x00, 0xFF, 0xFE]),       // tag 0x00, non-UTF-8 key
        ] {
            assert!(
                matches!(Pos::decode(&bad), Err(MetadataError::InvalidCursor { .. })),
                "expected InvalidCursor for {bad:?}",
            );
        }
    }

    #[test]
    fn delimiter_group_rolls_up_a_key_containing_the_delimiter() {
        assert_eq!(delimiter_group("photos/jan/a", "", "/").as_deref(), Some("photos/"));
        assert_eq!(delimiter_group("p/sub/a", "p/", "/").as_deref(), Some("p/sub/"));
    }

    #[test]
    fn delimiter_group_is_none_for_a_plain_key() {
        assert_eq!(delimiter_group("photos", "", "/"), None);
        assert_eq!(delimiter_group("p/x", "p/", "/"), None);
    }

    #[test]
    fn delimiter_group_supports_a_multi_char_delimiter() {
        assert_eq!(delimiter_group("aXXbXXc", "", "XX").as_deref(), Some("aXX"));
    }

    #[test]
    fn encode_content_type_rejects_an_other_too_short_to_disambiguate() {
        let err = encode_content_type(&ContentType::Other("x".to_string()))
            .expect_err("a one-byte Other should be rejected");
        assert!(
            matches!(err, MetadataError::Corrupt { field: "content_type", .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn content_type_blob_round_trips_both_variants() {
        for content_type in [
            ContentType::Known(KnownContentType::ImagePng),
            ContentType::Other("application/vnd.acme+xml".to_string()),
        ] {
            let encoded = encode_content_type(&content_type).expect("encode should succeed");
            assert_eq!(
                decode_content_type(encoded).expect("decode should succeed"),
                content_type
            );
        }
    }

    async fn query_plan(store: &SqliteMetadataStore, sql: &str) -> String {
        let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
            .fetch_all(&store.pool)
            .await
            .expect("EXPLAIN QUERY PLAN should run");
        rows.iter()
            .map(|r| r.try_get::<String, _>("detail").expect("detail column"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    // Every page after the first takes a cursor-resume path, so the plan tests
    // EXPLAIN the exact SQL `list_batch` builds — placeholders and all — for the
    // four shapes that reach the database, not a hand-written approximation.
    #[tokio::test]
    async fn list_batch_query_plans_use_the_right_index_for_every_shape() {
        let store = SqliteMetadataStore::connect_in_memory().await;

        let after_row = Pos::AfterRow { key: "p/x".to_string(), id: 42 };
        let at_key = Pos::AtKey("p/x".to_string());

        // (has_upper, pos, latest_only) -> the index the plan must use.
        let cases: [(bool, &Pos, bool, &str); 4] = [
            (true, &after_row, true, "idx_object_metadata_one_latest"),
            (true, &after_row, false, "idx_object_metadata_bucket_key_is_latest"),
            (false, &at_key, true, "idx_object_metadata_one_latest"),
            (false, &after_row, false, "idx_object_metadata_bucket_key_is_latest"),
        ];

        for (has_upper, pos, latest_only, index) in cases {
            let sql = list_batch_sql(has_upper, Some(pos), latest_only);
            let plan = query_plan(&store, &sql).await;

            // SEARCH = a bounded index probe; a bare full scan reads
            // "SCAN object_metadata" on its own line with no "USING INDEX".
            assert!(
                plan.contains("SEARCH"),
                "{sql}\nexpected a bounded index SEARCH, got:\n{plan}",
            );
            assert!(
                plan.contains(&format!("USING INDEX {index}")),
                "{sql}\nexpected USING INDEX {index}, got:\n{plan}",
            );
            for line in plan.lines() {
                assert!(
                    !line.contains("SCAN object_metadata") || line.contains("USING INDEX"),
                    "{sql}\nunexpected bare full scan:\n{plan}",
                );
            }
        }
    }
}
