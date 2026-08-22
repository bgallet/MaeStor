# Metadata Store Design

Date: 2026-08-22
Status: Approved

## Context

`open-conductor` needs its own metadata system for tracking objects independently of wherever their bytes actually live — the design that shaped the routing layer explicitly deferred this ("How the metadata abstraction trait is shaped, and what the SQLite implementation looks like"). This document is that sub-project: a `MetadataStore` trait abstracting persistence, with SQLite as the first implementation.

Scope for this cut: the trait, the `Metadata` record shape and its supporting value types, and a SQLite-backed implementation covering create/read/delete/list. Not in scope: wiring this into the routing layer's handlers (a later sub-project), storage backends, caching, and real encryption.

## The `Metadata` record

```rust
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
```

`bucket`/`key` match the naming already used by `S3Operation`/`routing.rs` elsewhere in the crate. `delete_marker` is new — it makes a delete marker a representable row (a version whose only job is to say "the object is hidden as of here"), which is what lets `delete_versioned` create real S3-style tombstones without losing history.

### Supporting types (`src/metadata/types.rs`)

- `Etag(Bytes)` — thin newtype around the raw ETag bytes (not required to be UTF-8). `CacheControl(String)`, `ContentType(String)` — thin newtypes around the raw wire value. No parsing/validation in this cut.
- `ObjectVersion(String)` — opaque version identifier. Carries one named constructor beyond a plain wrapper: `ObjectVersion::unversioned()`, returning the literal string `"null"` — this is AWS's own convention (`GetObject` on an object from a bucket that was never version-enabled reports `VersionId: "null"`), reused here as the sentinel that marks a row as belonging to an unversioned bucket.
- `ObjectStorageClass` — enum matching AWS's full storage class set (`Standard`, `ReducedRedundancy`, `StandardIa`, `OnezoneIa`, `IntelligentTiering`, `Glacier`, `DeepArchive`, `Outposts`, `GlacierIr`, `ExpressOnezone`); `Default` → `Standard`.
- `DataEncryptionContext` — empty placeholder struct (zero fields) until a real encryption backend exists.

## The `MetadataStore` trait

```rust
#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn put_versioned(&self, metadata: Metadata) -> Result<(), MetadataError>;
    async fn put_unversioned(&self, metadata: Metadata) -> Result<(), MetadataError>;
    async fn get(&self, bucket: &str, key: &str, version: Option<&ObjectVersion>) -> Result<Option<Metadata>, MetadataError>;
    async fn delete_versioned(&self, bucket: &str, key: &str, new_version: ObjectVersion) -> Result<(), MetadataError>;
    async fn delete_specific_version(&self, bucket: &str, key: &str, version: &ObjectVersion) -> Result<(), MetadataError>;
    async fn delete_unversioned(&self, bucket: &str, key: &str) -> Result<(), MetadataError>;
    async fn list(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<Metadata>, MetadataError>;
    async fn list_versions(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<Metadata>, MetadataError>;
    async fn list_buckets(&self) -> Result<Vec<String>, MetadataError>;
}
```

`async-trait` is used (rather than native async-fn-in-trait) specifically so `Arc<dyn MetadataStore>` is usable — the whole point of this abstraction is a backend selected at runtime, which needs the trait to be object-safe.

The trait deliberately covers two calling conventions rather than one `put`/`delete` pair with optional arguments, because "versioned" and "unversioned" buckets have genuinely different required inputs, not just different values of the same input:

- **`put_versioned`** — always a fresh insert. The caller supplies a new, unique `version` on the `Metadata` it passes in; the store inserts that row and, in the same transaction, flips whichever row was previously `is_latest` for that `(bucket, key)` to `false`.
- **`put_unversioned`** — always targets the single `ObjectVersion::unversioned()` ("null") row for `(bucket, key)`, forcing `version` and `is_latest` to that sentinel/`true` internally regardless of what the caller set on the `Metadata` — this is what keeps unversioned mode a true single-row-per-key upsert no matter what a caller passes in. No transaction needed beyond the upsert itself, since there's only ever one row to touch.
- **`get(version: None)`** — returns the latest row as-is, delete marker or not. The store doesn't interpret S3 semantics (e.g. "latest is a delete marker → the object doesn't exist"); that judgment belongs to whatever layer calls this trait.
- **`get(version: Some(v))`** — returns that exact version regardless of latest/delete-marker status.
- **`delete_versioned`** — unversioned-style `DELETE` on a *versioned* bucket: inserts a new delete-marker row (`delete_marker = true`, caller-supplied `new_version`) as the new latest, preserving all prior history.
- **`delete_specific_version`** — `DELETE ?versionId=X`: permanently removes exactly that row. If it was latest, promotes the next-most-recent remaining row (by internal insert order) to latest.
- **`delete_unversioned`** — deletes the single `ObjectVersion::unversioned()` row for `(bucket, key)`. No marker, no history — there isn't any to preserve.
- **`list`** — latest row per key in a bucket (optionally prefix-filtered), delete markers included as-is (same "store returns raw truth" principle as `get`). No pagination in this cut; returns a full `Vec`, revisited when it's actually needed.
- **`list_versions`** — every version of every matching key in a bucket (optionally prefix-filtered), delete markers included — matches S3's `ListObjectVersions`. Same `(bucket, prefix)` shape as `list` for consistency.
- **`list_buckets`** — distinct bucket names across all stored metadata.

### Errors

```rust
#[derive(Debug)]
pub enum MetadataError {
    Backend(sqlx::Error),
}
```

Minimal for this cut — not-found is `Ok(None)` from `get`, not an error variant. This type has no dependency on `http`/`hyper`; a later task translates it into `S3Error` (e.g. `MetadataError` + "nothing found" → `S3Error::NoSuchKey`) at the handler layer, keeping this module a pure persistence concern.

## SQLite implementation

### Module layout

- `src/metadata/mod.rs` — `Metadata`, `MetadataStore`, `MetadataError`.
- `src/metadata/types.rs` — the value types above.
- `src/metadata/sqlite/mod.rs` — `SqliteMetadataStore`, implementing `MetadataStore` over a `sqlx::SqlitePool`.
- `src/metadata/sqlite/migrations/` — this backend's schema migrations (see below).

### Schema

One table, `object_metadata`:

| column | type | notes |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | internal only; breaks ties when promoting the next-latest row after `delete_specific_version` |
| `bucket` | `TEXT NOT NULL` | |
| `key` | `TEXT NOT NULL` | |
| `version` | `TEXT NOT NULL` | `"null"` for unversioned rows |
| `etag` | `BLOB NOT NULL` | |
| `last_modified` | `INTEGER NOT NULL` | unix millis |
| `size` | `INTEGER NOT NULL` | |
| `cache_control` | `TEXT NOT NULL` | |
| `backend_id` | `INTEGER NOT NULL` | |
| `content_type` | `TEXT NULL` | |
| `content_disposition` | `TEXT NULL` | |
| `content_language` | `TEXT NULL` | |
| `cloned_at` | `INTEGER NULL` | unix millis |
| `upload_id` | `BLOB NULL` | |
| `is_latest` | `INTEGER NOT NULL` | 0/1 |
| `delete_marker` | `INTEGER NOT NULL` | 0/1 |
| `user_metadata` | `TEXT NOT NULL` | JSON-encoded `HashMap<String, Bytes>` |
| `storage_class` | `TEXT NOT NULL` | enum's string form, e.g. `"STANDARD"` |
| `encryption_context` | `TEXT NULL` | JSON |

Constraints: `UNIQUE(bucket, key, version)` (the natural key both `put_*` methods upsert on) and an index on `(bucket, key, is_latest)` for latest-row lookups. No separate `(bucket, key)` index — it would be redundant with `(bucket, key, is_latest)`, whose leftmost prefix already serves `list_versions`' `(bucket, key)` scans.

### Dependencies

- `sqlx` with `runtime-tokio`, `sqlite`, `migrate` features — async-native, connection pooling built in.
- `async-trait` for the trait.
- `serde` + `serde_json`, promoted from dev-dependencies to real dependencies (needed for the `user_metadata`/`encryption_context` JSON columns).

### Migrations

Schema lives in `src/metadata/sqlite/migrations/`, applied via `sqlx::migrate!` when `SqliteMetadataStore` is constructed. Scoped under the SQLite implementation specifically, not a crate-root `migrations/` directory — a future non-SQLite (or non-SQL) backend will have its own schema-setup story, which shouldn't be implied to share this one.

### Testing

In-memory SQLite: `SqliteConnectOptions::in_memory(true)` with the pool's `max_connections(1)` — the standard workaround for pooled connections otherwise each getting their own separate, empty in-memory database.

## Non-goals (deferred to later sub-projects)

- Wiring `MetadataStore` into the routing layer's handlers.
- Pagination for `list`/`list_versions`.
- Real encryption (`DataEncryptionContext` stays an empty placeholder).
- Bucket-level configuration (e.g. tracking whether a given bucket "is versioned" — that decision is made by whichever caller chooses `put_versioned` vs `put_unversioned`, not stored here).
- Storage backends and caching.
