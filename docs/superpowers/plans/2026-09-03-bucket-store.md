# Bucket Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make buckets first-class in the metadata store — a `buckets` table with owner, timestamps, versioning state, and opaque ACL/CORS/lifecycle blobs, plus seven `MetadataStore` methods to manage them.

**Architecture:** A new `Bucket` record and `BucketVersioning` enum in `src/metadata`. A SQLite migration (`0002`) adds a `buckets` table keyed by name with an `(owner, name)` index. Seven trait methods — `create_bucket` / `get_bucket` / `delete_bucket` / `set_bucket_versioning` / `_acl` / `_cors` / `_lifecycle` — plus `list_buckets` changing from `Vec<String>` (distinct object-bucket names) to an owner-scoped `Vec<Bucket>`. Object methods are untouched in this plan (they migrate to `&Bucket` in the follow-up plan).

**Tech Stack:** Rust 2021, `sqlx` 0.8 (SQLite), `async-trait`, `bytes`, `tokio` test runtime.

**Spec:** `docs/superpowers/specs/2026-09-03-bucket-metadata-design.md`

## Global Constraints

- Rust edition `2021`.
- `cargo test` and `cargo clippy --all-targets` must be clean (zero warnings) after every task.
- Do **not** run `cargo fmt` / `rustfmt` on any file — the repo is not fmt-clean; hand-format new code to match the surrounding wide style.
- `MetadataError` is **not** `PartialEq` (it wraps `sqlx::Error`); tests match with `matches!(...)`, never `assert_eq!` on a `Result<_, MetadataError>`.
- Bucket names are globally unique (`name TEXT PRIMARY KEY`) — a name is taken regardless of owner.
- Timestamps are epoch milliseconds, via the existing private `system_time_to_millis(time, field) -> Result<i64, MetadataError>` and `millis_to_system_time(i64) -> SystemTime` helpers in `src/metadata/sqlite/mod.rs`. A `Bucket` returned from `create_bucket` must carry the **millisecond-truncated** timestamp (`millis_to_system_time(millis)`), not the raw `SystemTime::now()`, so it equals what `get_bucket` reads back.
- `BucketVersioning` wire strings are exactly `"UNVERSIONED"` / `"ENABLED"` / `"SUSPENDED"`.
- New public metadata types are added directly in `src/metadata/mod.rs` or `src/metadata/types.rs`; types defined in `types.rs` are re-exported through the `pub use types::{...}` block in `mod.rs`.
- The conformance suite is `src/metadata/conformance.rs`; each case is a `pub(crate) async fn <name>(store: impl MetadataStore)` registered with a `case!($make_store, <name>);` line in the `metadata_store_conformance!` macro body.

---

## File Structure

- `src/metadata/types.rs` — add `BucketVersioning` enum (`as_str` / `parse`, mirroring `ObjectStorageClass`) + unit tests.
- `src/metadata/mod.rs` — add the `Bucket` struct; add `MetadataError::{BucketAlreadyExists, NoSuchBucket}` + `Display` arms; add the seven trait methods; change `list_buckets`'s signature; re-export `BucketVersioning`.
- `src/metadata/sqlite/migrations/0002_create_buckets.sql` — new migration.
- `src/metadata/sqlite/mod.rs` — add `row_to_bucket`; implement the seven methods + the new `list_buckets`; add SQLite-specific tests.
- `src/metadata/conformance.rs` — rewrite the one `list_buckets` case; add nine new bucket cases; register them.

---

## Task 1: `BucketVersioning` type

**Files:**
- Modify: `src/metadata/types.rs` (add the enum after `ObjectStorageClass`'s `impl` / `Display` block, before `DataEncryptionContext`; add tests in `mod tests`)
- Modify: `src/metadata/mod.rs` (add `BucketVersioning` to the `pub use types::{...}` list)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub enum BucketVersioning { Unversioned, Enabled, Suspended }` — derives `Debug, Clone, Copy, PartialEq, Eq, Default` (`#[default]` on `Unversioned`)
  - `BucketVersioning::as_str(&self) -> &'static str` — `"UNVERSIONED"` / `"ENABLED"` / `"SUSPENDED"`
  - `BucketVersioning::parse(value: &str) -> Option<Self>` — exact match, inverse of `as_str`

- [ ] **Step 1: Write the failing tests**

In `src/metadata/types.rs`, inside `mod tests`, after the storage-class tests:

```rust
    #[test]
    fn bucket_versioning_round_trips_through_its_wire_string() {
        for state in [
            BucketVersioning::Unversioned,
            BucketVersioning::Enabled,
            BucketVersioning::Suspended,
        ] {
            assert_eq!(BucketVersioning::parse(state.as_str()), Some(state), "{state:?}");
        }
    }

    #[test]
    fn bucket_versioning_parse_rejects_unknown_strings() {
        assert_eq!(BucketVersioning::parse("MaybeEnabled"), None);
        assert_eq!(BucketVersioning::parse(""), None);
    }

    #[test]
    fn bucket_versioning_default_is_unversioned() {
        assert_eq!(BucketVersioning::default(), BucketVersioning::Unversioned);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::types::tests::bucket_versioning`
Expected: FAIL to compile — `cannot find type BucketVersioning`.

- [ ] **Step 3: Implement `BucketVersioning`**

In `src/metadata/types.rs`, after the `impl fmt::Display for ObjectStorageClass` block:

```rust
/// A bucket's S3 versioning state. A never-configured bucket is
/// `Unversioned`; once configured it only toggles `Enabled` <-> `Suspended`
/// and never returns to `Unversioned`. The store does not police that
/// transition — see the design doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BucketVersioning {
    #[default]
    Unversioned,
    Enabled,
    Suspended,
}

impl BucketVersioning {
    pub fn as_str(&self) -> &'static str {
        match self {
            BucketVersioning::Unversioned => "UNVERSIONED",
            BucketVersioning::Enabled => "ENABLED",
            BucketVersioning::Suspended => "SUSPENDED",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "UNVERSIONED" => Some(BucketVersioning::Unversioned),
            "ENABLED" => Some(BucketVersioning::Enabled),
            "SUSPENDED" => Some(BucketVersioning::Suspended),
            _ => None,
        }
    }
}
```

- [ ] **Step 4: Re-export it**

In `src/metadata/mod.rs`, add `BucketVersioning` to the `pub use types::{...}` block (keep alphabetical-ish order with the others):

```rust
pub use types::{
    BucketVersioning, CacheControl, ContentType, DataEncryptionContext, Etag, KnownContentType,
    ObjectStorageClass, ObjectVersion,
};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib metadata::types::tests::bucket_versioning`
Expected: PASS (3 tests).

- [ ] **Step 6: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. (`BucketVersioning` is `pub` in a lib crate → no dead-code warning even though nothing uses it yet.)

- [ ] **Step 7: Commit**

```bash
git add src/metadata/types.rs src/metadata/mod.rs
git commit -m "feat(metadata): BucketVersioning type"
```

---

## Task 2: `Bucket` record and `MetadataError` variants

**Files:**
- Modify: `src/metadata/mod.rs` (add `Bucket` after the `Metadata` struct; extend `MetadataError` + its `Display`; add one test)

**Interfaces:**
- Consumes: `BucketVersioning` (Task 1).
- Produces:
  - `pub struct Bucket { name: String, owner: String, created_at: SystemTime, modified_at: SystemTime, versioning: BucketVersioning, acl: Option<Bytes>, cors: Option<Bytes>, lifecycle: Option<Bytes> }` — derives `Debug, Clone, PartialEq`
  - `MetadataError::BucketAlreadyExists { name: String }` — `Display` = `bucket already exists: {name}`
  - `MetadataError::NoSuchBucket { name: String }` — `Display` = `no such bucket: {name}`

- [ ] **Step 1: Write the failing test**

In `src/metadata/mod.rs`, inside `mod tests`, after the existing `MetadataError` display tests:

```rust
    #[test]
    fn metadata_error_displays_the_bucket_variants() {
        let exists = MetadataError::BucketAlreadyExists { name: "b".to_string() };
        assert!(format!("{exists}").contains("bucket already exists: b"), "{exists}");
        let missing = MetadataError::NoSuchBucket { name: "b".to_string() };
        assert!(format!("{missing}").contains("no such bucket: b"), "{missing}");
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib metadata::tests::metadata_error_displays_the_bucket_variants`
Expected: FAIL to compile — `no variant named BucketAlreadyExists`.

- [ ] **Step 3: Add the `Bucket` struct**

In `src/metadata/mod.rs`, immediately after the `Metadata` struct definition:

```rust
/// A bucket's stored metadata. `acl` / `cors` / `lifecycle` are the raw
/// configuration documents as S3 receives them; `None` means unconfigured.
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    pub name: String,
    pub owner: String,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub versioning: BucketVersioning,
    pub acl: Option<Bytes>,
    pub cors: Option<Bytes>,
    pub lifecycle: Option<Bytes>,
}
```

- [ ] **Step 4: Extend `MetadataError` and its `Display`**

Add the two variants to the enum, after `InvalidCursor`:

```rust
    /// `create_bucket` on a name that is already taken.
    BucketAlreadyExists { name: String },
    /// A `set_bucket_*` call against a bucket that does not exist.
    NoSuchBucket { name: String },
```

Add to the `Display` match, after the `InvalidCursor` arm:

```rust
            MetadataError::BucketAlreadyExists { name } => {
                write!(f, "bucket already exists: {name}")
            }
            MetadataError::NoSuchBucket { name } => write!(f, "no such bucket: {name}"),
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --lib metadata::tests::metadata_error_displays_the_bucket_variants`
Expected: PASS.

- [ ] **Step 6: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/metadata/mod.rs
git commit -m "feat(metadata): Bucket record, BucketAlreadyExists / NoSuchBucket errors"
```

---

## Task 3: Migration `0002` + `create_bucket` / `get_bucket` / `delete_bucket`

Adds three trait methods with their SQLite implementations, so the trait stays satisfied and the crate compiles.

**Files:**
- Create: `src/metadata/sqlite/migrations/0002_create_buckets.sql`
- Modify: `src/metadata/mod.rs` (three trait method declarations in `trait MetadataStore`)
- Modify: `src/metadata/sqlite/mod.rs` (imports; `row_to_bucket` free fn; three method impls)

**Interfaces:**
- Consumes: `Bucket`, `BucketVersioning`, `MetadataError::BucketAlreadyExists` (Tasks 1–2); existing `system_time_to_millis` / `millis_to_system_time`.
- Produces:
  - `async fn create_bucket(&self, name: &str, owner: &str) -> Result<Bucket, MetadataError>`
  - `async fn get_bucket(&self, name: &str) -> Result<Option<Bucket>, MetadataError>`
  - `async fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>` (idempotent)
  - `fn row_to_bucket(row: &SqliteRow) -> Result<Bucket, MetadataError>` (private to the sqlite module)

- [ ] **Step 1: Write the migration**

Create `src/metadata/sqlite/migrations/0002_create_buckets.sql`:

```sql
CREATE TABLE buckets (
    name        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    versioning  TEXT NOT NULL,
    acl         BLOB,
    cors        BLOB,
    lifecycle   BLOB
);

CREATE INDEX idx_buckets_owner_name ON buckets (owner, name);
```

- [ ] **Step 2: Add the trait method declarations**

In `src/metadata/mod.rs`, in `trait MetadataStore`, after `list_buckets` (they will sit together; the `list_buckets` change itself is Task 4):

```rust
    async fn create_bucket(&self, name: &str, owner: &str) -> Result<Bucket, MetadataError>;
    async fn get_bucket(&self, name: &str) -> Result<Option<Bucket>, MetadataError>;
    async fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>;
```

- [ ] **Step 3: Add imports and `row_to_bucket` to the sqlite module**

In `src/metadata/sqlite/mod.rs`, extend `use crate::metadata::{...}` to include `Bucket, BucketVersioning`.

After the existing `row_to_metadata` function:

```rust
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
```

- [ ] **Step 4: Implement the three methods**

In `src/metadata/sqlite/mod.rs`, inside `impl MetadataStore for SqliteMetadataStore` (place them next to `list_buckets`):

```rust
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
```

If `err.is_unique_violation()` does not resolve, add `use sqlx::error::DatabaseError;` to the module's imports (the method lives on that trait).

- [ ] **Step 5: Build the library**

Run: `cargo build --lib`
Expected: PASS — the trait has three new methods, all implemented; no conformance case uses them yet.

- [ ] **Step 6: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. `row_to_bucket` / the new methods are exercised by tests only in later tasks; `cargo build` (non-test) has real callers now (the trait impl), so no dead-code warning.

- [ ] **Step 7: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs src/metadata/sqlite/migrations/0002_create_buckets.sql
git commit -m "feat(metadata): buckets table + create_bucket / get_bucket / delete_bucket"
```

---

## Task 4: `set_bucket_*` methods + owner-scoped `list_buckets`

**Files:**
- Modify: `src/metadata/mod.rs` (four trait method declarations; change `list_buckets`'s signature)
- Modify: `src/metadata/sqlite/mod.rs` (a `set_bucket_blob` helper; five method impls; rewrite `list_buckets`)
- Modify: `src/metadata/conformance.rs` (rewrite the one `list_buckets` case; fix the `metadata_store_is_object_safe` call; update the macro)

**Interfaces:**
- Consumes: `Bucket`, `BucketVersioning`, `MetadataError::NoSuchBucket`, `row_to_bucket` (Tasks 1–3).
- Produces:
  - `async fn set_bucket_versioning(&self, name: &str, state: BucketVersioning) -> Result<(), MetadataError>`
  - `async fn set_bucket_acl(&self, name: &str, acl: Option<Bytes>) -> Result<(), MetadataError>`
  - `async fn set_bucket_cors(&self, name: &str, cors: Option<Bytes>) -> Result<(), MetadataError>`
  - `async fn set_bucket_lifecycle(&self, name: &str, lifecycle: Option<Bytes>) -> Result<(), MetadataError>`
  - `async fn list_buckets(&self, owner: &str) -> Result<Vec<Bucket>, MetadataError>` (was `list_buckets(&self) -> Result<Vec<String>, _>`)
  - conformance case `list_buckets_is_scoped_to_owner_and_sorted`

- [ ] **Step 1: Change the trait**

In `src/metadata/mod.rs`, in `trait MetadataStore`:

(a) Replace the existing `list_buckets` line:

```rust
    // was: async fn list_buckets(&self) -> Result<Vec<String>, MetadataError>;
    async fn list_buckets(&self, owner: &str) -> Result<Vec<Bucket>, MetadataError>;
```

(b) Add the four setters after the `create_bucket` / `get_bucket` /
`delete_bucket` declarations added in Task 3 (do **not** re-declare those
three):

```rust
    async fn set_bucket_versioning(&self, name: &str, state: BucketVersioning) -> Result<(), MetadataError>;
    async fn set_bucket_acl(&self, name: &str, acl: Option<Bytes>) -> Result<(), MetadataError>;
    async fn set_bucket_cors(&self, name: &str, cors: Option<Bytes>) -> Result<(), MetadataError>;
    async fn set_bucket_lifecycle(&self, name: &str, lifecycle: Option<Bytes>) -> Result<(), MetadataError>;
```

(`bytes::Bytes` is already imported in `mod.rs` as `Bytes`.)

- [ ] **Step 2: Confirm the build breaks where expected**

Run: `cargo build --lib`
Expected: FAIL — `SqliteMetadataStore` no longer satisfies `MetadataStore` (`list_buckets` signature mismatch, four missing methods).

- [ ] **Step 3: Add the `set_bucket_blob` helper**

In `src/metadata/sqlite/mod.rs`, in the inherent `impl SqliteMetadataStore` block (the one with `connect` / `migrate`):

```rust
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
```

- [ ] **Step 4: Implement the five methods**

In `impl MetadataStore for SqliteMetadataStore`:

```rust
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
```

Replace the existing `list_buckets` body (the `SELECT DISTINCT bucket FROM object_metadata` one):

```rust
    async fn list_buckets(&self, owner: &str) -> Result<Vec<Bucket>, MetadataError> {
        let rows = sqlx::query("SELECT * FROM buckets WHERE owner = ? ORDER BY name")
            .bind(owner)
            .fetch_all(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;
        rows.iter().map(row_to_bucket).collect()
    }
```

- [ ] **Step 5: Fix the conformance call sites**

Run `grep -rn "list_buckets" src/` and fix every call.

In `src/metadata/conformance.rs`, `metadata_store_is_object_safe` calls `store.list_buckets().await` — change to `store.list_buckets("owner-1").await` (result is still discarded).

Replace the whole `list_buckets_returns_distinct_bucket_names` function with:

```rust
pub(crate) async fn list_buckets_is_scoped_to_owner_and_sorted(store: impl MetadataStore) {
    store.create_bucket("b", "owner-1").await.expect("create b");
    store.create_bucket("a", "owner-1").await.expect("create a");
    store.create_bucket("m", "owner-2").await.expect("create m");

    let owned = store.list_buckets("owner-1").await.expect("list_buckets should succeed");
    let names: Vec<_> = owned.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b"]);

    let others = store.list_buckets("owner-2").await.expect("list_buckets should succeed");
    let other_names: Vec<_> = others.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(other_names, vec!["m"]);

    let none = store.list_buckets("owner-3").await.expect("list_buckets should succeed");
    assert!(none.is_empty());
}
```

In the `metadata_store_conformance!` macro body, replace the
`case!($make_store, list_buckets_returns_distinct_bucket_names);` line with
`case!($make_store, list_buckets_is_scoped_to_owner_and_sorted);`.

- [ ] **Step 6: Run the suite**

Run: `cargo test --lib metadata::`
Expected: PASS — including `conformance::list_buckets_is_scoped_to_owner_and_sorted`.

- [ ] **Step 7: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. If clippy flags the `format!` in `set_bucket_blob`, it is intentional (dynamic column name from a static literal) — leave it, or `#[allow(clippy::…)]` with a one-line comment only if clippy actually errors.

- [ ] **Step 8: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs src/metadata/conformance.rs
git commit -m "feat(metadata): set_bucket_* setters; list_buckets is owner-scoped -> Vec<Bucket>"
```

---

## Task 5: Bucket conformance cases

**Files:**
- Modify: `src/metadata/conformance.rs` (nine new `pub(crate) async fn`s + nine `case!` lines)

**Interfaces:**
- Consumes: every bucket method (Tasks 3–4); `sample_metadata` and the object methods (unchanged, still `&str`).
- Produces: nine conformance cases run against every backend.

- [ ] **Step 1: Add the nine cases**

In `src/metadata/conformance.rs`, after `list_buckets_is_scoped_to_owner_and_sorted`:

```rust
const BUCKET_OWNER: &str = "owner-1";

pub(crate) async fn create_bucket_then_get_returns_the_row(store: impl MetadataStore) {
    let created = store
        .create_bucket("b", BUCKET_OWNER)
        .await
        .expect("create_bucket should succeed");
    assert_eq!(created.name, "b");
    assert_eq!(created.owner, BUCKET_OWNER);
    assert_eq!(created.versioning, BucketVersioning::Unversioned);
    assert_eq!(created.acl, None);
    assert_eq!(created.cors, None);
    assert_eq!(created.lifecycle, None);
    assert_eq!(created.created_at, created.modified_at);

    let fetched = store
        .get_bucket("b")
        .await
        .expect("get_bucket should succeed")
        .expect("bucket should exist");
    assert_eq!(fetched, created);
}

pub(crate) async fn create_bucket_rejects_a_duplicate_name(store: impl MetadataStore) {
    store.create_bucket("b", BUCKET_OWNER).await.expect("first create should succeed");
    let err = store
        .create_bucket("b", "a-different-owner")
        .await
        .expect_err("a taken name should be rejected regardless of owner");
    assert!(
        matches!(err, MetadataError::BucketAlreadyExists { ref name } if name == "b"),
        "unexpected error: {err:?}"
    );
}

pub(crate) async fn get_bucket_returns_none_for_a_missing_bucket(store: impl MetadataStore) {
    let found = store.get_bucket("no-such-bucket").await.expect("get_bucket should succeed");
    assert_eq!(found, None);
}

pub(crate) async fn delete_bucket_removes_the_row_and_is_idempotent(store: impl MetadataStore) {
    store.create_bucket("b", BUCKET_OWNER).await.expect("create should succeed");
    store.delete_bucket("b").await.expect("first delete should succeed");
    assert_eq!(store.get_bucket("b").await.expect("get_bucket should succeed"), None);
    store.delete_bucket("b").await.expect("second delete should also succeed");
}

pub(crate) async fn delete_bucket_leaves_its_objects_untouched(store: impl MetadataStore) {
    store.create_bucket("b", BUCKET_OWNER).await.expect("create should succeed");
    store
        .put_versioned(sample_metadata("b", "k", "v1"))
        .await
        .expect("put should succeed");

    store.delete_bucket("b").await.expect("delete_bucket should succeed");

    let object = store
        .get("b", "k", None)
        .await
        .expect("get should succeed");
    assert!(object.is_some(), "the object should survive its bucket's deletion");
}

pub(crate) async fn set_bucket_versioning_updates_state(store: impl MetadataStore) {
    let created = store.create_bucket("b", BUCKET_OWNER).await.expect("create should succeed");
    store
        .set_bucket_versioning("b", BucketVersioning::Enabled)
        .await
        .expect("set_bucket_versioning should succeed");

    let updated = store
        .get_bucket("b")
        .await
        .expect("get_bucket should succeed")
        .expect("bucket should exist");
    assert_eq!(updated.versioning, BucketVersioning::Enabled);
    assert!(updated.modified_at >= created.created_at);
}

pub(crate) async fn set_bucket_config_on_a_missing_bucket_is_an_error(store: impl MetadataStore) {
    let v = store.set_bucket_versioning("ghost", BucketVersioning::Enabled).await;
    assert!(matches!(v, Err(MetadataError::NoSuchBucket { .. })), "{v:?}");

    let a = store.set_bucket_acl("ghost", Some(bytes::Bytes::from_static(b"<acl/>"))).await;
    assert!(matches!(a, Err(MetadataError::NoSuchBucket { .. })), "{a:?}");

    let c = store.set_bucket_cors("ghost", None).await;
    assert!(matches!(c, Err(MetadataError::NoSuchBucket { .. })), "{c:?}");

    let l = store.set_bucket_lifecycle("ghost", Some(bytes::Bytes::from_static(b"<lc/>"))).await;
    assert!(matches!(l, Err(MetadataError::NoSuchBucket { .. })), "{l:?}");
}

pub(crate) async fn bucket_acl_cors_lifecycle_blobs_round_trip(store: impl MetadataStore) {
    store.create_bucket("b", BUCKET_OWNER).await.expect("create should succeed");

    let acl = bytes::Bytes::from_static(&[0xff, 0x00, b'a', b'c', b'l']);
    let cors = bytes::Bytes::from_static(b"<CORSConfiguration/>");
    let lifecycle = bytes::Bytes::from_static(&[0x00, 0x01, b'l', b'c']);
    store.set_bucket_acl("b", Some(acl.clone())).await.expect("set acl");
    store.set_bucket_cors("b", Some(cors.clone())).await.expect("set cors");
    store.set_bucket_lifecycle("b", Some(lifecycle.clone())).await.expect("set lifecycle");

    let with = store.get_bucket("b").await.expect("get").expect("exists");
    assert_eq!(with.acl.as_deref(), Some(acl.as_ref()));
    assert_eq!(with.cors.as_deref(), Some(cors.as_ref()));
    assert_eq!(with.lifecycle.as_deref(), Some(lifecycle.as_ref()));

    store.set_bucket_acl("b", None).await.expect("clear acl");
    store.set_bucket_cors("b", None).await.expect("clear cors");
    store.set_bucket_lifecycle("b", None).await.expect("clear lifecycle");

    let without = store.get_bucket("b").await.expect("get").expect("exists");
    assert_eq!(without.acl, None);
    assert_eq!(without.cors, None);
    assert_eq!(without.lifecycle, None);
}

pub(crate) async fn list_buckets_reads_only_the_bucket_table(store: impl MetadataStore) {
    // An object whose bucket has no `buckets` row.
    store
        .put_versioned(sample_metadata("ghost-bucket", "k", "v1"))
        .await
        .expect("put should succeed");
    store.create_bucket("real", BUCKET_OWNER).await.expect("create should succeed");

    let listed = store.list_buckets(BUCKET_OWNER).await.expect("list_buckets should succeed");
    let names: Vec<_> = listed.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, vec!["real"]);
}
```

- [ ] **Step 2: Register the nine cases**

In the `metadata_store_conformance!` macro body, after the
`list_buckets_is_scoped_to_owner_and_sorted` line:

```rust
            case!($make_store, create_bucket_then_get_returns_the_row);
            case!($make_store, create_bucket_rejects_a_duplicate_name);
            case!($make_store, get_bucket_returns_none_for_a_missing_bucket);
            case!($make_store, delete_bucket_removes_the_row_and_is_idempotent);
            case!($make_store, delete_bucket_leaves_its_objects_untouched);
            case!($make_store, set_bucket_versioning_updates_state);
            case!($make_store, set_bucket_config_on_a_missing_bucket_is_an_error);
            case!($make_store, bucket_acl_cors_lifecycle_blobs_round_trip);
            case!($make_store, list_buckets_reads_only_the_bucket_table);
```

- [ ] **Step 3: Run the new cases**

Run: `cargo test --lib metadata::sqlite::tests::conformance::`
Expected: PASS — nine new cases green with the rest.

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): conformance for bucket CRUD, config blobs, owner scoping"
```

---

## Task 6: SQLite-specific tests

**Files:**
- Modify: `src/metadata/sqlite/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: `SqliteMetadataStore::connect_in_memory`, `row_to_bucket`, the bucket methods.
- Produces: nothing (tests only).

- [ ] **Step 1: Write the tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`:

```rust
    #[tokio::test]
    async fn connect_in_memory_creates_the_buckets_table_and_index() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let names: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master
             WHERE name IN ('buckets', 'idx_buckets_owner_name') ORDER BY name",
        )
        .fetch_all(&store.pool)
        .await
        .expect("query should succeed");
        let names: Vec<String> = names.into_iter().map(|(n,)| n).collect();
        assert_eq!(names, vec!["buckets".to_string(), "idx_buckets_owner_name".to_string()]);
    }

    #[tokio::test]
    async fn row_to_bucket_decodes_all_columns() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        sqlx::query(
            "INSERT INTO buckets (name, owner, created_at, modified_at, versioning, acl, cors, lifecycle)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("b")
        .bind("o")
        .bind(1_700_000_000_000i64)
        .bind(1_700_000_009_000i64)
        .bind("ENABLED")
        .bind(Some(vec![1u8, 2, 3]))
        .bind(Option::<Vec<u8>>::None)
        .bind(Some(b"<lc/>".to_vec()))
        .execute(&store.pool)
        .await
        .expect("insert should succeed");

        let row = sqlx::query("SELECT * FROM buckets WHERE name = 'b'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");
        let bucket = row_to_bucket(&row).expect("row should decode");

        assert_eq!(bucket.name, "b");
        assert_eq!(bucket.owner, "o");
        assert_eq!(bucket.created_at, millis_to_system_time(1_700_000_000_000));
        assert_eq!(bucket.modified_at, millis_to_system_time(1_700_000_009_000));
        assert_eq!(bucket.versioning, BucketVersioning::Enabled);
        assert_eq!(bucket.acl.as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(bucket.cors, None);
        assert_eq!(bucket.lifecycle.as_deref(), Some(&b"<lc/>"[..]));
    }

    #[tokio::test]
    async fn row_to_bucket_rejects_an_unrecognized_versioning_string() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        sqlx::query(
            "INSERT INTO buckets (name, owner, created_at, modified_at, versioning)
             VALUES ('b', 'o', 0, 0, 'WAT')",
        )
        .execute(&store.pool)
        .await
        .expect("insert should succeed");

        let row = sqlx::query("SELECT * FROM buckets WHERE name = 'b'")
            .fetch_one(&store.pool)
            .await
            .expect("row should be found");
        let err = row_to_bucket(&row).expect_err("a bogus versioning value should not decode");
        assert!(
            matches!(err, MetadataError::Corrupt { field: "versioning", .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn list_buckets_query_plan_uses_the_owner_index() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let plan = query_plan(
            &store,
            "SELECT * FROM buckets WHERE owner = 'x' ORDER BY name",
        )
        .await;
        assert!(plan.contains("SEARCH"), "expected an index SEARCH, got:\n{plan}");
        assert!(
            plan.contains("USING INDEX idx_buckets_owner_name"),
            "expected the owner index, got:\n{plan}",
        );
    }

    #[tokio::test]
    async fn create_bucket_timestamp_round_trips() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let created = store.create_bucket("b", "o").await.expect("create should succeed");
        let fetched = store.get_bucket("b").await.expect("get").expect("exists");
        assert_eq!(created.created_at, fetched.created_at);
        assert_eq!(created.modified_at, fetched.modified_at);
    }
```

`query_plan` is the existing test helper in `mod tests` (added by the list-pagination work); it runs `EXPLAIN QUERY PLAN {sql}` and joins the `detail` column.

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib metadata::sqlite::tests::connect_in_memory_creates_the_buckets metadata::sqlite::tests::row_to_bucket metadata::sqlite::tests::list_buckets_query_plan metadata::sqlite::tests::create_bucket_timestamp`
Expected: PASS (5 tests). If the query-plan test fails, print `plan` and compare against the spec — do not weaken the assertion.

- [ ] **Step 3: Full check**

Run: `cargo test && cargo clippy --all-targets && cargo build`
Expected: all pass, zero warnings.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "test(metadata/sqlite): buckets table, row_to_bucket, owner-index query plan"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| `BucketVersioning` type (`as_str` / `parse`) | 1 |
| `Bucket` record | 2 |
| `MetadataError::{BucketAlreadyExists, NoSuchBucket}` + Display | 2 |
| Migration `0002` (`buckets` table + `idx_buckets_owner_name`) | 3 |
| `create_bucket` / `get_bucket` / `delete_bucket` (+ `row_to_bucket`) | 3 |
| `set_bucket_versioning` / `_acl` / `_cors` / `_lifecycle` | 4 |
| `list_buckets` → owner-scoped `Vec<Bucket>` | 4 |
| Duplicate-name → `BucketAlreadyExists` (constraint-violation match) | 3 + 5 (test) |
| `set_bucket_*` on missing → `NoSuchBucket` (0-rows-affected) | 4 + 5 (test) |
| Idempotent `delete_bucket`, objects untouched | 3 + 5 (tests) |
| ms-truncated timestamps on the returned `Bucket` | 3 (constraint) + 6 (test) |
| Migrated `list_buckets_returns_distinct_bucket_names` | 4 |
| `metadata_store_is_object_safe` call fixup | 4 |
| 10 conformance cases | 4 (case 9) + 5 (cases 1–8, 10) |
| SQLite-specific tests (schema, decode, reject, query plan, ts round-trip) | 6 |

Object-method migration (`&Bucket`, method collapse, `Metadata` drops `bucket`, `uuid`) is a **separate plan** (spec "Decomposition" point 2) — not covered here, by design.

**Placeholder scan:** no `TBD` / "handle edge cases" / bare "write tests" — every code step carries the code.

**Type consistency:**
- `Bucket` fields `{ name, owner, created_at, modified_at, versioning, acl, cors, lifecycle }` — identical in Task 2 (definition), Task 3 (`row_to_bucket`, `create_bucket` construction), Task 5/6 (assertions).
- `BUCKET_OWNER` const in Task 5; Task 4's case uses literal `"owner-1"` / `"owner-2"` / `"owner-3"` (multiple owners, so no single const). Consistent — `BUCKET_OWNER == "owner-1"`.
- `row_to_bucket(&SqliteRow) -> Result<Bucket, MetadataError>` — Task 3 definition, Task 4 (`list_buckets` uses `.map(row_to_bucket)`), Task 6 (tests call it directly).
- `set_bucket_blob(name, column, value)` — Task 4 definition and its three call sites.
- `list_buckets(owner: &str) -> Result<Vec<Bucket>, _>` — Task 4 trait + impl + every call site (conformance cases 9, 10, `metadata_store_is_object_safe`).

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-09-03-bucket-store.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
