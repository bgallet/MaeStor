# Metadata Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `MetadataStore` abstraction for `open-conductor`: the `Metadata` record and its supporting types, the `MetadataStore` trait (put/get/delete/list, versioned and unversioned), and a SQLite-backed implementation via `sqlx`.

**Architecture:** New `src/metadata/` module. `mod.rs` holds the data model (`Metadata`, error type, trait). `types.rs` holds the small value types. `sqlite/` holds the SQLite implementation: connection setup, migrations, row↔`Metadata` mapping, and the trait impl itself, built up incrementally by SQL operation.

**Tech Stack:** Rust, sqlx (SQLite, tokio runtime, migrations), async-trait, serde/serde_json (for two JSON columns), bytes (with its `serde` feature, for `Bytes` JSON round-tripping).

**Spec:** `docs/superpowers/specs/2026-08-22-metadata-store-design.md`

## Global Constraints

- Trait must be object-safe (`Arc<dyn MetadataStore>`) — use `async-trait`, not native async-fn-in-trait.
- `put_versioned`/`put_unversioned` and the three delete methods are separate methods, not one method with optional arguments — see spec for why.
- `get`/`list`/`list_versions` return raw stored data as-is, including delete-marker rows — no S3-semantic interpretation in the store.
- `ObjectVersion::unversioned()` returns the literal string `"null"` (AWS's own convention).
- `MetadataError` has no dependency on `http`/`hyper`.
- Migrations live under `src/metadata/sqlite/migrations/`, not a crate-root `migrations/` directory.
- Schema: one table `object_metadata`, `UNIQUE(bucket, key, version)`, index on `(bucket, key, is_latest)` only (no separate `(bucket, key)` index — redundant with that index's leftmost prefix).
- No pagination for `list`/`list_versions` in this cut.

## Column/field order (used consistently in schema, INSERT statements, and row mapping)

`id, bucket, key, version, etag, last_modified, size, cache_control, backend_id, content_type, content_disposition, content_language, cloned_at, upload_id, is_latest, delete_marker, user_metadata, storage_class, encryption_context`

---

### Task 1: Dependencies

**Files:**
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: nothing
- Produces: the crate dependencies later tasks need. No code changes yet.

- [ ] **Step 1: Add/modify dependencies**

Modify `Cargo.toml`'s `[dependencies]` section: add `bytes`'s `serde` feature (it's currently a bare `bytes = "1"`), and add four new dependencies. Add `serde`/`serde_json` as real dependencies (currently `serde_json` is a dev-dependency only; keep it there too isn't necessary — move it up). Resulting `[dependencies]` and `[dev-dependencies]` sections:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
hyper = { version = "1", features = ["server", "http1", "http2"] }
hyper-util = { version = "0.1", features = ["full"] }
http = "1"
http-body = "1"
http-body-util = "0.1"
bytes = { version = "1", features = ["serde"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
percent-encoding = "2"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite", "migrate"] }

[dev-dependencies]
tracing-test = "0.2"
```

- [ ] **Step 2: Verify it builds**

Run: `cargo build`
Expected: succeeds (no new code references the new dependencies yet, so this just confirms they resolve and compile).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add metadata store dependencies (sqlx, async-trait, serde)"
```

---

### Task 2: Supporting value types

**Files:**
- Create: `src/metadata/types.rs`

**Interfaces:**
- Consumes: nothing beyond `std`/`serde`/`bytes`
- Produces: `Etag(pub Bytes)`, `CacheControl(pub String)`, `ContentType(pub String)`, `ObjectVersion(pub String)` (with `ObjectVersion::unversioned() -> Self`), `ObjectStorageClass` enum (with `as_str(&self) -> &'static str`, `parse(value: &str) -> Option<Self>`, `Display`, `Default` → `Standard`), `DataEncryptionContext` (empty struct, `Serialize`/`Deserialize`). All derive at least `Debug, Clone, PartialEq, Eq`. These are consumed by `Metadata` (Task 3) and the SQLite row mapping (Task 5).

- [ ] **Step 1: Write the failing tests**

`src/metadata/types.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_version_unversioned_is_the_null_sentinel() {
        assert_eq!(ObjectVersion::unversioned(), ObjectVersion("null".to_string()));
    }

    #[test]
    fn storage_class_round_trips_through_its_wire_string() {
        let classes = [
            ObjectStorageClass::Standard,
            ObjectStorageClass::ReducedRedundancy,
            ObjectStorageClass::StandardIa,
            ObjectStorageClass::OnezoneIa,
            ObjectStorageClass::IntelligentTiering,
            ObjectStorageClass::Glacier,
            ObjectStorageClass::DeepArchive,
            ObjectStorageClass::Outposts,
            ObjectStorageClass::GlacierIr,
            ObjectStorageClass::ExpressOnezone,
        ];
        for class in classes {
            let wire = class.as_str();
            assert_eq!(ObjectStorageClass::parse(wire), Some(class), "round trip for {wire}");
        }
    }

    #[test]
    fn storage_class_parse_rejects_unknown_strings() {
        assert_eq!(ObjectStorageClass::parse("NOT_A_REAL_CLASS"), None);
    }

    #[test]
    fn storage_class_default_is_standard() {
        assert_eq!(ObjectStorageClass::default(), ObjectStorageClass::Standard);
    }

    #[test]
    fn storage_class_display_matches_as_str() {
        assert_eq!(format!("{}", ObjectStorageClass::Glacier), "GLACIER");
    }

    #[test]
    fn data_encryption_context_serializes_to_json() {
        let json = serde_json::to_string(&DataEncryptionContext).expect("serialize");
        let decoded: DataEncryptionContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, DataEncryptionContext);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib metadata::types`
Expected: FAIL to compile — none of these types exist yet.

- [ ] **Step 3: Implement the types**

Prepend this above the `#[cfg(test)]` block in `src/metadata/types.rs`:

```rust
use std::fmt;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheControl(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentType(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion(pub String);

impl ObjectVersion {
    /// AWS's own sentinel: `GetObject` on an object from a bucket that was
    /// never version-enabled reports this literal string as its `VersionId`.
    pub fn unversioned() -> Self {
        ObjectVersion("null".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObjectStorageClass {
    #[default]
    Standard,
    ReducedRedundancy,
    StandardIa,
    OnezoneIa,
    IntelligentTiering,
    Glacier,
    DeepArchive,
    Outposts,
    GlacierIr,
    ExpressOnezone,
}

impl ObjectStorageClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectStorageClass::Standard => "STANDARD",
            ObjectStorageClass::ReducedRedundancy => "REDUCED_REDUNDANCY",
            ObjectStorageClass::StandardIa => "STANDARD_IA",
            ObjectStorageClass::OnezoneIa => "ONEZONE_IA",
            ObjectStorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
            ObjectStorageClass::Glacier => "GLACIER",
            ObjectStorageClass::DeepArchive => "DEEP_ARCHIVE",
            ObjectStorageClass::Outposts => "OUTPOSTS",
            ObjectStorageClass::GlacierIr => "GLACIER_IR",
            ObjectStorageClass::ExpressOnezone => "EXPRESS_ONEZONE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "STANDARD" => Some(ObjectStorageClass::Standard),
            "REDUCED_REDUNDANCY" => Some(ObjectStorageClass::ReducedRedundancy),
            "STANDARD_IA" => Some(ObjectStorageClass::StandardIa),
            "ONEZONE_IA" => Some(ObjectStorageClass::OnezoneIa),
            "INTELLIGENT_TIERING" => Some(ObjectStorageClass::IntelligentTiering),
            "GLACIER" => Some(ObjectStorageClass::Glacier),
            "DEEP_ARCHIVE" => Some(ObjectStorageClass::DeepArchive),
            "OUTPOSTS" => Some(ObjectStorageClass::Outposts),
            "GLACIER_IR" => Some(ObjectStorageClass::GlacierIr),
            "EXPRESS_ONEZONE" => Some(ObjectStorageClass::ExpressOnezone),
            _ => None,
        }
    }
}

impl fmt::Display for ObjectStorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct DataEncryptionContext;
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to add the line:

```rust
pub mod metadata;
```

Create `src/metadata/mod.rs` with exactly this content (just enough to make `types` reachable; `Metadata`/`MetadataStore`/`MetadataError` are added in Task 3):

```rust
pub mod types;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib metadata::types`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/metadata/types.rs src/metadata/mod.rs src/lib.rs
git commit -m "feat: add metadata value types (Etag, ObjectStorageClass, etc.)"
```

---

### Task 3: `Metadata`, `MetadataError`, and the `MetadataStore` trait

**Files:**
- Modify: `src/metadata/mod.rs`

**Interfaces:**
- Consumes: the types from Task 2
- Produces: `pub struct Metadata { .. }` (exact field list below), `pub enum MetadataError { Backend(sqlx::Error) }` (`Debug + Display + std::error::Error`), and the `#[async_trait] pub trait MetadataStore` with all nine methods from the spec. Every later task implements or exercises this trait; its method signatures are load-bearing — copy them exactly.

- [ ] **Step 1: Write the failing test**

Append this to `src/metadata/mod.rs` (a `#[cfg(test)] mod tests` block — this test just proves the types compile and construct correctly; the trait itself is exercised for real starting in Task 6):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::time::SystemTime;

    #[test]
    fn metadata_constructs_with_all_fields() {
        let metadata = Metadata {
            etag: Etag(Bytes::from_static(b"\"abc123\"")),
            last_modified: SystemTime::now(),
            size: 42,
            cache_control: CacheControl("no-cache".to_string()),
            backend_id: 1,
            bucket: "my-bucket".to_string(),
            key: "my-key".to_string(),
            content_type: Some(ContentType("text/plain".to_string())),
            content_disposition: None,
            content_language: None,
            version: ObjectVersion("v1".to_string()),
            cloned_at: None,
            upload_id: None,
            is_latest: true,
            delete_marker: false,
            user_metadata: HashMap::new(),
            storage_class: ObjectStorageClass::Standard,
            encryption_context: None,
        };
        assert_eq!(metadata.bucket, "my-bucket");
        assert!(metadata.is_latest);
    }

    #[test]
    fn metadata_error_displays_the_backend_error() {
        let err = MetadataError::Backend(sqlx::Error::RowNotFound);
        assert!(format!("{err}").contains("metadata backend error"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib metadata::tests`
Expected: FAIL to compile — `Metadata`, `MetadataError`, and the trait don't exist yet.

- [ ] **Step 3: Implement `Metadata`, `MetadataError`, and `MetadataStore`**

Prepend this above the `#[cfg(test)]` block in `src/metadata/mod.rs` (keep the existing `pub mod types;` line at the top):

```rust
pub mod types;

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

pub use types::{CacheControl, ContentType, DataEncryptionContext, Etag, ObjectStorageClass, ObjectVersion};

#[derive(Debug, Clone, PartialEq)]
pub struct Metadata {
    pub etag: Etag,
    pub last_modified: SystemTime,
    pub size: usize,
    pub cache_control: CacheControl,
    pub backend_id: usize,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<ContentType>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub version: ObjectVersion,
    pub cloned_at: Option<SystemTime>,
    pub upload_id: Option<Bytes>,
    pub is_latest: bool,
    pub delete_marker: bool,
    pub user_metadata: HashMap<String, Bytes>,
    pub storage_class: ObjectStorageClass,
    pub encryption_context: Option<DataEncryptionContext>,
}

#[derive(Debug)]
pub enum MetadataError {
    Backend(sqlx::Error),
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataError::Backend(err) => write!(f, "metadata backend error: {err}"),
        }
    }
}

impl std::error::Error for MetadataError {}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn put_versioned(&self, metadata: Metadata) -> Result<(), MetadataError>;
    async fn put_unversioned(&self, metadata: Metadata) -> Result<(), MetadataError>;
    async fn get(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&ObjectVersion>,
    ) -> Result<Option<Metadata>, MetadataError>;
    async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        new_version: ObjectVersion,
    ) -> Result<(), MetadataError>;
    async fn delete_specific_version(
        &self,
        bucket: &str,
        key: &str,
        version: &ObjectVersion,
    ) -> Result<(), MetadataError>;
    async fn delete_unversioned(&self, bucket: &str, key: &str) -> Result<(), MetadataError>;
    async fn list(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<Metadata>, MetadataError>;
    async fn list_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Metadata>, MetadataError>;
    async fn list_buckets(&self) -> Result<Vec<String>, MetadataError>;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/mod.rs
git commit -m "feat: add Metadata, MetadataError, and the MetadataStore trait"
```

---

### Task 4: SQLite connection, migration, and schema

**Files:**
- Create: `src/metadata/sqlite/mod.rs`
- Create: `src/metadata/sqlite/migrations/0001_create_object_metadata.sql`
- Modify: `src/metadata/mod.rs` (add `pub mod sqlite;`)

**Interfaces:**
- Consumes: nothing beyond `sqlx`
- Produces: `pub struct SqliteMetadataStore { pool: sqlx::SqlitePool }` (fields private), `pub async fn SqliteMetadataStore::connect(options: sqlx::sqlite::SqliteConnectOptions) -> Result<Self, MetadataError>`, and a test-only `async fn connect_in_memory() -> Self` used by every later task's tests in this module. The `object_metadata` table (columns per the Global Constraints' column order). `MetadataStore` is not implemented yet — that starts in Task 6.

- [ ] **Step 1: Write the migration file**

`src/metadata/sqlite/migrations/0001_create_object_metadata.sql`:

```sql
CREATE TABLE object_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version TEXT NOT NULL,
    etag BLOB NOT NULL,
    last_modified INTEGER NOT NULL,
    size INTEGER NOT NULL,
    cache_control TEXT NOT NULL,
    backend_id INTEGER NOT NULL,
    content_type TEXT,
    content_disposition TEXT,
    content_language TEXT,
    cloned_at INTEGER,
    upload_id BLOB,
    is_latest INTEGER NOT NULL,
    delete_marker INTEGER NOT NULL,
    user_metadata TEXT NOT NULL,
    storage_class TEXT NOT NULL,
    encryption_context TEXT,
    UNIQUE (bucket, key, version)
);

CREATE INDEX idx_object_metadata_bucket_key_is_latest
    ON object_metadata (bucket, key, is_latest);
```

- [ ] **Step 2: Write the failing test**

`src/metadata/sqlite/mod.rs`:

```rust
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL to compile — `SqliteMetadataStore` doesn't exist yet.

- [ ] **Step 4: Implement the connection/migration scaffolding**

Prepend this above the `#[cfg(test)]` block in `src/metadata/sqlite/mod.rs`:

```rust
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
```

- [ ] **Step 5: Add the module to the crate root**

Modify `src/metadata/mod.rs` to add the line (alongside the existing `pub mod types;`):

```rust
pub mod sqlite;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (1 test).

- [ ] **Step 7: Commit**

```bash
git add src/metadata/sqlite/mod.rs src/metadata/sqlite/migrations/0001_create_object_metadata.sql src/metadata/mod.rs
git commit -m "feat: add SQLite connection, migration, and object_metadata schema"
```

---

### Task 5: Row → `Metadata` decoding

**Files:**
- Modify: `src/metadata/sqlite/mod.rs`

**Interfaces:**
- Consumes: `SqliteMetadataStore` (Task 4), `Metadata` and its value types (Tasks 2–3)
- Produces: `fn row_to_metadata(row: &sqlx::sqlite::SqliteRow) -> Result<Metadata, sqlx::Error>` and `fn millis_to_system_time(millis: i64) -> SystemTime` / `fn system_time_to_millis(time: SystemTime) -> i64`, all private to this module. Every later task (put/get/delete/list) uses `row_to_metadata` to turn a fetched row into a `Metadata`, and `system_time_to_millis` to bind a `SystemTime` into a query.

- [ ] **Step 1: Write the failing tests**

Add this to `src/metadata/sqlite/mod.rs`'s existing `#[cfg(test)] mod tests` block (append these two `#[tokio::test]` functions alongside `connect_in_memory_creates_the_object_metadata_table`):

```rust
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
```

Also add these imports to the top of the existing `#[cfg(test)] mod tests` block (alongside the existing `use super::*;`):

```rust
    use crate::metadata::{CacheControl, ContentType, Etag, ObjectStorageClass, ObjectVersion};
    use bytes::Bytes;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL to compile — `row_to_metadata` and `millis_to_system_time` don't exist yet.

- [ ] **Step 3: Implement the decoding helpers**

Add this to `src/metadata/sqlite/mod.rs`, above the `#[cfg(test)]` block (below the existing `SqliteMetadataStore` impl), and extend the existing `use` block at the top of the file:

Extend the top-of-file imports to:

```rust
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

use crate::metadata::{
    CacheControl, ContentType, DataEncryptionContext, Etag, Metadata, MetadataError,
    ObjectStorageClass, ObjectVersion,
};
```

(This replaces the narrower `use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};`, `use sqlx::SqlitePool;`, and `use crate::metadata::MetadataError;` lines from Task 4 with the superset above.)

Append the decoding functions after the `impl SqliteMetadataStore` block:

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat: add SQLite row-to-Metadata decoding"
```

---

### Task 6: `put_versioned` and `put_unversioned`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs`

**Interfaces:**
- Consumes: `SqliteMetadataStore`, `row_to_metadata`, `system_time_to_millis` (Tasks 4–5); `Metadata`, `MetadataStore`, `MetadataError`, `ObjectVersion` (Tasks 2–3)
- Produces: the first `impl MetadataStore for SqliteMetadataStore` block, with `put_versioned` and `put_unversioned` implemented (the other seven trait methods are added in Tasks 7–9 and must compile as `todo!()`-free stubs are never used — see Step 3, which implements only these two and leaves the rest to later tasks via a partial `impl` block extended in place). A private `upsert_row` helper shared by both, and by `delete_versioned` in Task 8.

- [ ] **Step 1: Write the failing tests**

Add these two `#[tokio::test]` functions to the existing `#[cfg(test)] mod tests` block in `src/metadata/sqlite/mod.rs`, and add a `sample_metadata` test helper and a `row_count` helper alongside them:

```rust
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
```

Also extend the test module's imports (alongside the existing `use super::*;` and the `use crate::metadata::{...}` line added in Task 5) to add:

```rust
    use crate::metadata::Metadata;
    use std::collections::HashMap;
    use std::time::SystemTime;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL to compile — `put_versioned`/`put_unversioned` don't exist yet (no `MetadataStore` impl at all yet).

- [ ] **Step 3: Implement `upsert_row`, `put_versioned`, and `put_unversioned`**

Add this to `src/metadata/sqlite/mod.rs`, above the `#[cfg(test)]` block (after the `row_to_metadata` function from Task 5), and add `async_trait::async_trait` and `MetadataStore` to the top-of-file imports:

Extend the top-of-file `use crate::metadata::{...}` line to:

```rust
use crate::metadata::{
    CacheControl, ContentType, DataEncryptionContext, Etag, Metadata, MetadataError,
    MetadataStore, ObjectStorageClass, ObjectVersion,
};
```

And add, alongside the other top-of-file `use` lines:

```rust
use async_trait::async_trait;
```

Append the helper and the trait impl:

```rust
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
}
```

Note: this `impl MetadataStore` block is extended in place by Tasks 7, 8, and 9 — each adds its methods inside these same braces rather than opening a new `impl` block. If the exact `sqlx::Executor` bound above doesn't compile against the resolved sqlx version, this is a well-established sqlx pattern (an executor generic over both pool and transaction references) — consult sqlx's own docs/examples for the exact bound shape rather than working around it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (5 tests). Note this will not compile clean as a full `impl MetadataStore` yet — the trait requires all nine methods. Add temporary stub bodies for the other seven methods so the crate compiles, each returning `unimplemented!()`, to be replaced by real implementations in Tasks 7–9:

```rust
    async fn get(
        &self,
        _bucket: &str,
        _key: &str,
        _version: Option<&ObjectVersion>,
    ) -> Result<Option<Metadata>, MetadataError> {
        unimplemented!("implemented in Task 7")
    }

    async fn delete_versioned(
        &self,
        _bucket: &str,
        _key: &str,
        _new_version: ObjectVersion,
    ) -> Result<(), MetadataError> {
        unimplemented!("implemented in Task 8")
    }

    async fn delete_specific_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version: &ObjectVersion,
    ) -> Result<(), MetadataError> {
        unimplemented!("implemented in Task 8")
    }

    async fn delete_unversioned(&self, _bucket: &str, _key: &str) -> Result<(), MetadataError> {
        unimplemented!("implemented in Task 8")
    }

    async fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> Result<Vec<Metadata>, MetadataError> {
        unimplemented!("implemented in Task 9")
    }

    async fn list_versions(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> Result<Vec<Metadata>, MetadataError> {
        unimplemented!("implemented in Task 9")
    }

    async fn list_buckets(&self) -> Result<Vec<String>, MetadataError> {
        unimplemented!("implemented in Task 9")
    }
```

Add these seven stub methods inside the same `impl MetadataStore for SqliteMetadataStore` block, after `put_unversioned`. Each later task (7, 8, 9) replaces its stub(s) with a real implementation — never leaves an `unimplemented!()` in place after its task completes.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat: implement put_versioned and put_unversioned"
```

---

### Task 7: `get`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs`

**Interfaces:**
- Consumes: `SqliteMetadataStore`, `row_to_metadata`, `upsert_row`/`put_versioned` (Task 6, used to seed test data)
- Produces: a real `get` implementation replacing its `unimplemented!()` stub from Task 6.

- [ ] **Step 1: Write the failing tests**

Add these to the existing `#[cfg(test)] mod tests` block:

```rust
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL — `get`'s stub panics with `unimplemented!("implemented in Task 7")`.

- [ ] **Step 3: Implement `get`**

Replace the `get` stub inside `impl MetadataStore for SqliteMetadataStore` (added in Task 6, Step 4) with:

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat: implement get"
```

---

### Task 8: `delete_versioned`, `delete_specific_version`, `delete_unversioned`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs`

**Interfaces:**
- Consumes: `put_versioned` (Task 6, reused directly by `delete_versioned`), `get`/`row_to_metadata` (used by tests)
- Produces: real implementations replacing the three delete stubs from Task 6.

`delete_versioned` is implemented as a thin wrapper: it synthesizes a `Metadata` marker row (empty/zeroed content fields, `delete_marker: true`, the caller-supplied `new_version`) and calls `put_versioned` on it — `put_versioned` already does exactly the "flip old latest, insert new, force `is_latest = true`" work a marker needs.

- [ ] **Step 1: Write the failing tests**

Add these to the existing `#[cfg(test)] mod tests` block:

```rust
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL — the three stubs panic with `unimplemented!("implemented in Task 8")`.

- [ ] **Step 3: Implement the three delete methods**

Replace `delete_versioned`, `delete_specific_version`, and `delete_unversioned`'s stub bodies inside `impl MetadataStore for SqliteMetadataStore` with:

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (12 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat: implement delete_versioned, delete_specific_version, delete_unversioned"
```

---

### Task 9: `list`, `list_versions`, `list_buckets`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs`

**Interfaces:**
- Consumes: `put_versioned`, `delete_versioned`, `row_to_metadata` (used by tests)
- Produces: real implementations replacing the final three stubs from Task 6, plus a private `like_prefix_pattern` helper. This completes the `MetadataStore` trait — after this task, `SqliteMetadataStore` has no `unimplemented!()` left anywhere.

- [ ] **Step 1: Write the failing tests**

Add these to the existing `#[cfg(test)] mod tests` block:

```rust
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite`
Expected: FAIL — the three stubs panic with `unimplemented!("implemented in Task 9")`.

- [ ] **Step 3: Implement `list`, `list_versions`, `list_buckets`**

Replace the three stubs inside `impl MetadataStore for SqliteMetadataStore` with:

```rust
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
```

Add this private helper below the `impl MetadataStore` block (after the closing `}`), used by both `list` and `list_versions`:

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib metadata::sqlite`
Expected: PASS (17 tests).

- [ ] **Step 5: Run the full workspace test suite**

Run: `cargo test`
Expected: PASS — every test in the crate, including the pre-existing routing-layer tests untouched by this plan.

- [ ] **Step 6: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat: implement list, list_versions, and list_buckets"
```

---

## Self-Review Notes

- **Spec coverage:** `Metadata` record shape with the delete-marker field (Task 3), all six supporting types including `ObjectVersion::unversioned()` (Task 2), the full nine-method `MetadataStore` trait (Task 3), and a SQLite implementation of every method (Tasks 4–9) covering the schema, migrations-scoped-to-sqlite, and the no-separate-`(bucket, key)`-index constraint from the spec — all covered.
- **Type consistency:** `Metadata`'s field names/types (Task 3) match what Task 5's `row_to_metadata`, Task 6's `upsert_row`, and every later task's tests construct via `sample_metadata`. The `MetadataStore` trait's method signatures (Task 3) match exactly what Tasks 6–9 implement — copied verbatim, not re-derived.
- **No placeholders:** every step contains complete, real SQL and Rust code; the `unimplemented!()` stubs introduced in Task 6 are each explicitly required to be replaced by their owning task (7, 8, or 9) before that task is considered done — none survive past Task 9.
