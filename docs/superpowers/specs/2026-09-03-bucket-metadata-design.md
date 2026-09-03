# Bucket Metadata Design

Date: 2026-09-03
Status: Draft

## Context

`maestor` has an object-metadata store (`MetadataStore` + `SqliteMetadataStore`)
but no first-class notion of a *bucket*. Today `list_buckets` derives bucket
existence from `SELECT DISTINCT bucket FROM object_metadata`, so an empty
bucket does not exist, and there is nowhere to store a bucket's owner,
timestamps, versioning state, or its ACL / CORS / lifecycle configuration.
The original metadata-store design
(`2026-08-22-metadata-store-design.md`) explicitly deferred this
("Bucket-level configuration … not stored here"). It is now needed:
`CreateBucket` must persist a bucket before `PutBucketAcl` /
`PutBucketVersioning` / `PutBucketCors` / `PutBucketLifecycleConfiguration`
have anything to attach to, and the versioning state is what gates how an
object write behaves (insert a new version vs. overwrite the `"null"` one).

The HTTP plumbing already exists: `S3Operation::CreateBucket` / `DeleteBucket`
/ `HeadBucket` / `Get`/`PutBucketVersioning` are routed, with
`NotImplemented` handler stubs.

**Scope for this cut:**
- the `Bucket` record and `BucketVersioning` type;
- seven new `MetadataStore` methods (`create_bucket`, `get_bucket`,
  `delete_bucket`, `set_bucket_versioning` / `_acl` / `_cors` / `_lifecycle`);
- a signature change to `list_buckets`;
- **every object method takes `&Bucket` instead of `bucket: &str`**;
  `Metadata` loses its `bucket` field; and the `*_versioned` / `*_unversioned`
  method pairs collapse into `put` and `delete` that branch on
  `bucket.versioning` (8 object methods → 6). See "Object method signatures".
- two new `MetadataError` variants;
- a new direct dependency, `uuid` (`v7` feature), for store-generated
  delete-marker version ids;
- a SQLite migration (`0002`) creating a `buckets` table + one index;
- the SQLite implementation and conformance coverage.

**Not in scope:** handler bodies and injecting the store into
`handlers::dispatch`; typed modeling of ACL / CORS / lifecycle documents
(kept as opaque bytes); `list_buckets` pagination (bucket counts per owner
are small; consistent with the list-pagination spec leaving `list_buckets`
unpaginated); enforcing the `Enabled`/`Suspended`-only versioning transition.

Because object methods take `&Bucket`, the store is *structurally* incapable
of touching an object in a nonexistent bucket — the `NoSuchBucket` check
becomes a single `get_bucket` at the handler, and no object method needs a
`NoSuchBucket` path.

## Types (`src/metadata/types.rs`)

```rust
/// A bucket's S3 versioning state. A never-configured bucket is
/// `Unversioned`; once configured it only toggles `Enabled` <-> `Suspended`
/// and never returns to `Unversioned`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BucketVersioning {
    #[default]
    Unversioned,
    Enabled,
    Suspended,
}
```

`as_str` / `parse` mirror `ObjectStorageClass`:

| variant | stored / `as_str` |
|---|---|
| `Unversioned` | `"UNVERSIONED"` |
| `Enabled` | `"ENABLED"` |
| `Suspended` | `"SUSPENDED"` |

`Unversioned` has no S3 wire form (S3 signals it by the absence of a
`VersioningConfiguration`); the handler maps between the two. `Default` is
`Unversioned`.

An owner is a bare `String` — the canonical user id, the same string
`auth::Identity.user` carries. No newtype, consistent with `bucket` / `key`
on `Metadata`.

## The `Bucket` record (`src/metadata/mod.rs`)

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    pub name: String,
    pub owner: String,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub versioning: BucketVersioning,
    /// Raw `PutBucketAcl` document, as received. `None` = no explicit ACL.
    pub acl: Option<Bytes>,
    /// Raw `PutBucketCors` document. `None` = no CORS configured.
    pub cors: Option<Bytes>,
    /// Raw `PutBucketLifecycleConfiguration` document. `None` = none.
    pub lifecycle: Option<Bytes>,
}
```

ACL / CORS / lifecycle are stored opaquely — S3 sets and gets each as a
whole document, and typed rule modeling (lifecycle especially) is a
substantial hierarchy that belongs in its own project. This matches how
`Metadata` already handles `user_metadata` (raw bytes) and
`encryption_context` (a placeholder).

## Trait changes (`src/metadata/mod.rs`)

```rust
/// Creates a bucket owned by `owner`, with `created_at == modified_at ==`
/// now, `versioning = Unversioned`, and no acl/cors/lifecycle. Returns the
/// stored row. `Err(MetadataError::BucketAlreadyExists)` if `name` is taken.
async fn create_bucket(&self, name: &str, owner: &str) -> Result<Bucket, MetadataError>;

async fn get_bucket(&self, name: &str) -> Result<Option<Bucket>, MetadataError>;

/// Removes the bucket row. Idempotent — `Ok(())` whether or not it existed.
/// Does **not** touch the bucket's objects; the empty-bucket precondition
/// for S3 `DeleteBucket` is the handler's to enforce.
async fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>;

/// Each setter updates one facet and bumps `modified_at` to now.
/// `Err(MetadataError::NoSuchBucket)` if the bucket does not exist.
/// `Some(_)` configures; `None` clears (S3 `DeleteBucketCors` etc.).
async fn set_bucket_versioning(&self, name: &str, state: BucketVersioning) -> Result<(), MetadataError>;
async fn set_bucket_acl(&self, name: &str, acl: Option<Bytes>) -> Result<(), MetadataError>;
async fn set_bucket_cors(&self, name: &str, cors: Option<Bytes>) -> Result<(), MetadataError>;
async fn set_bucket_lifecycle(&self, name: &str, lifecycle: Option<Bytes>) -> Result<(), MetadataError>;
```

**`list_buckets` signature change:**

```rust
// was: async fn list_buckets(&self) -> Result<Vec<String>, MetadataError>;
async fn list_buckets(&self, owner: &str) -> Result<Vec<Bucket>, MetadataError>;
```

Returns every bucket owned by `owner`, full `Bucket` rows, ascending by
`name`. Owner-scoped from the start so the covering index ships in migration
`0002` rather than a later one. S3's `ListBuckets` is per-caller anyway.

`set_bucket_versioning` does not police the `Enabled`/`Suspended`-only
transition rule — it writes whatever state it is given. A caller moving a
configured bucket back to `Unversioned` is a caller bug; enforcing the S3
state machine is a handler concern (it has the old state from `get_bucket`).

## Object method signatures

Every object method drops `bucket: &str` and takes `bucket: &Bucket`. The
caller obtains the `Bucket` (via `get_bucket` or `create_bucket`) first —
exactly the existence check S3 requires. And because the store now *sees*
`bucket.versioning`, the `*_versioned` / `*_unversioned` method pairs
collapse: `put` and `delete` read the state and branch internally.

```rust
// was put_versioned + put_unversioned
async fn put(&self, bucket: &Bucket, metadata: Metadata) -> Result<(), MetadataError>;

// was delete_versioned + delete_unversioned — the S3 `DELETE` with no version id.
// Returns the delete marker's version id (the store generates it), or `None`
// when the delete was a hard removal (unversioned bucket).
async fn delete(&self, bucket: &Bucket, key: &str) -> Result<Option<ObjectVersion>, MetadataError>;

// was delete_specific_version — the S3 `DELETE` *with* a client-named version id
async fn delete_version(&self, bucket: &Bucket, key: &str, version: &ObjectVersion) -> Result<(), MetadataError>;

async fn get(&self, bucket: &Bucket, key: &str, version: Option<&ObjectVersion>) -> Result<Option<Metadata>, MetadataError>;
async fn list(&self, bucket: &Bucket, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
async fn list_versions(&self, bucket: &Bucket, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
```

Eight object methods become six.

### `put` — branches on `bucket.versioning`

| `bucket.versioning` | behavior (same as the old method it replaces) |
|---|---|
| `Enabled` | fresh insert at `metadata.version` (caller-supplied, unique); flip the prior `is_latest` row for `(name, key)` to `false`, in one transaction. (old `put_versioned`) |
| `Suspended` / `Unversioned` | force the row to `ObjectVersion::unversioned()` ("null") and `is_latest = true`; upsert it; demote any *other* `is_latest` row for the key. Under `Unversioned` there is never any other row; under `Suspended` prior versioned rows are kept but lose `is_latest`. (old `put_unversioned`, whose demote step already handled the suspended case) |

`metadata.version` is read only in the `Enabled` branch; ignored otherwise
(as `put_unversioned` already ignored it).

### `delete` — branches on `bucket.versioning`

| `bucket.versioning` | behavior | returns |
|---|---|---|
| `Enabled` | insert a delete-marker row (`delete_marker = true`) as the new latest with a **store-generated** version id; demote the prior latest. (old `delete_versioned`) | `Some(<generated id>)` |
| `Suspended` | insert/replace a delete-marker row at version `"null"` as the new latest; demote other latest rows. | `Some(ObjectVersion::unversioned())` |
| `Unversioned` | `DELETE` the single `"null"` row. | `None` |

The store generates the `Enabled` marker id — matching real S3, where the
server mints all version ids and returns the marker's in `x-amz-version-id`.

**Generator:** a time-sortable UUID (**UUIDv7**) via the `uuid` crate
(`uuid = { version = "1", features = ["v7"] }`, a new direct dependency):
`ObjectVersion(uuid::Uuid::now_v7().to_string())`. The string form is
lexicographically ordered by creation time (48-bit ms timestamp prefix), so
successive markers minted **within one process** sort in creation order by the
`version` column alone. That ordering is per-process only — two markers created
in the same millisecond by different processes have no defined relative order,
and the prefix orders at millisecond granularity in any case — so
`list_versions` keeps ordering by rowid, not by `version`. It is generated in
Rust, so the marker row's `INSERT` binds it like
any other value — no `RETURNING`, and `delete` returns the id it just
generated. This is a free function `fn new_version_id() -> ObjectVersion` in
`sqlite/mod.rs` (or promoted to `metadata` if a second backend wants it).

`put` on an `Enabled` bucket still takes its content version id from
`metadata.version` (caller-supplied) — an intentional asymmetry: content
version ids stay caller-controlled so tests and callers can assert version
ordering deterministically, while a delete marker carries no client data and
its id is pure server bookkeeping. Extending `new_version_id()` to `put`
(and then ordering `list_versions` by `version` instead of rowid) is the
natural next step, out of scope here.

`delete_version` binds `&bucket.name` and is otherwise the current
`delete_specific_version` — a targeted row delete (of the client-named
version) plus the promote-next-latest fixup.

### The rest

`get` / `list` / `list_versions` / `delete_version` just bind `&bucket.name`
where they bound the `&str`; **no query-logic change**. Only `put` and
`delete` gain the `bucket.versioning` branch.

A handler loads the `Bucket` and then calls `put` / `delete`; if
`set_bucket_versioning` runs in between, the object op uses the slightly
stale state. That is an acceptable race (S3 bucket config is eventually
consistent) and not worth a re-read or lock.

### `Metadata` loses `bucket`

```rust
pub struct Metadata {
    pub etag: Etag,
    // ... unchanged ...
    pub key: String,
    // `bucket` field removed — the container is the `&Bucket` argument.
    // ... unchanged ...
}
```

`key` stays (it is intrinsic to the object); `bucket` was redundant with the
new argument and invited `metadata.bucket != bucket.name` mismatches. The
`object_metadata.bucket` **column** is unchanged — the store writes
`bucket.name` into it and filters on it exactly as before; only the Rust
struct field goes away. `row_to_metadata` stops populating it;
`sample_metadata` in the conformance suite drops the parameter.

### New `MetadataError` variants

```rust
pub enum MetadataError {
    Backend(sqlx::Error),
    Corrupt { field: &'static str, detail: String },
    InvalidCursor { detail: String },
    /// `create_bucket` on a name that already exists.
    BucketAlreadyExists { name: String },
    /// A `set_bucket_*` call against a bucket that does not exist.
    NoSuchBucket { name: String },
}
```

`Display`:
- `BucketAlreadyExists { name }` → `bucket already exists: {name}`
- `NoSuchBucket { name }` → `no such bucket: {name}`

Both are client-facing (map to S3 `BucketAlreadyExists` / `BucketAlreadyOwnedByYou`
and `NoSuchBucket`), distinct from `Backend` (500) and `Corrupt` (bug).

## SQLite implementation (`src/metadata/sqlite/mod.rs`)

### Migration `0002_create_buckets.sql`

```sql
CREATE TABLE buckets (
    name        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    created_at  INTEGER NOT NULL,   -- epoch millis
    modified_at INTEGER NOT NULL,   -- epoch millis
    versioning  TEXT NOT NULL,      -- 'UNVERSIONED' | 'ENABLED' | 'SUSPENDED'
    acl         BLOB,               -- raw config doc, NULL = unconfigured
    cors        BLOB,
    lifecycle   BLOB
);

CREATE INDEX idx_buckets_owner_name ON buckets (owner, name);
```

- Timestamps are epoch millis via the existing `system_time_to_millis` /
  `millis_to_system_time` helpers (already private in `sqlite/mod.rs`).
- `idx_buckets_owner_name` serves `list_buckets`: `WHERE owner = ? ORDER BY
  name` — equality on the leading column, ordering on the second, so no temp
  sort. `EXPLAIN QUERY PLAN` must show `SEARCH … USING INDEX
  idx_buckets_owner_name`, not a full `SCAN buckets`.
- No foreign key from `object_metadata.bucket` to `buckets.name`. Object
  methods stay permissive; the handler enforces bucket existence.

### Method mapping

| method | SQL |
|---|---|
| `create_bucket` | `INSERT INTO buckets (…) VALUES (…)`; a `UNIQUE`/PK violation on `name` → `BucketAlreadyExists { name }` (match on `sqlx::Error` being a constraint violation) |
| `get_bucket` | `SELECT * FROM buckets WHERE name = ?` → `Option<Bucket>` via a `row_to_bucket` helper |
| `delete_bucket` | `DELETE FROM buckets WHERE name = ?`; ignore the affected-row count (idempotent) |
| `set_bucket_versioning` | `UPDATE buckets SET versioning = ?, modified_at = ? WHERE name = ?`; 0 rows affected → `NoSuchBucket { name }` |
| `set_bucket_acl` / `_cors` / `_lifecycle` | `UPDATE buckets SET <col> = ?, modified_at = ? WHERE name = ?`; 0 rows affected → `NoSuchBucket { name }`; the value binds as `Option<Vec<u8>>` (NULL when `None`) |
| `list_buckets` | `SELECT * FROM buckets WHERE owner = ? ORDER BY name` → `Vec<Bucket>` |

`row_to_bucket(&SqliteRow) -> Result<Bucket, MetadataError>`: decodes the
columns; an unrecognized `versioning` string → `MetadataError::Corrupt {
field: "versioning", … }` (same pattern as `storage_class` in
`row_to_metadata`).

## Conformance suite (`src/metadata/conformance.rs`)

### Object-method migration (the bulk of the churn)

Every existing conformance case that calls an object method changes shape:

- `sample_metadata` drops its `bucket` parameter: `sample_metadata(key, version) -> Metadata`.
- Method renames at every call site:
  `put_versioned` / `put_unversioned` → `put`;
  `delete_versioned` / `delete_unversioned` → `delete(&bucket, key)` (now
  returns `Option<ObjectVersion>` — the generated marker id);
  `delete_specific_version` → `delete_version`.
- Cases that asserted a *specific* delete-marker version id (e.g.
  `list_versions_returns_every_version_including_markers` expects
  `["marker1", "v1"]`) now assert on the marker's *presence and position*:
  the returned `Option<ObjectVersion>` is `Some`, `list_versions` has that
  id first, and its row has `delete_marker == true`.
- Each case creates a bucket in setup and threads `&bucket` through the
  object calls. Add a helper:
  ```rust
  const OWNER: &str = "owner-1";
  async fn fresh_bucket(store: &impl MetadataStore, name: &str, versioning: BucketVersioning) -> Bucket {
      let bucket = store.create_bucket(name, OWNER).await.expect("create_bucket should succeed");
      if versioning != BucketVersioning::Unversioned {
          store.set_bucket_versioning(name, versioning).await.expect("set_bucket_versioning should succeed");
      }
      store.get_bucket(name).await.expect("get_bucket").expect("bucket exists")
  }
  ```
  A case that exercised `put_versioned` opens with a
  `BucketVersioning::Enabled` bucket; a `put_unversioned` case with an
  `Unversioned` one. Cases that mixed both (e.g.
  `put_unversioned_demotes_existing_versioned_latest_rows`) use a
  `Suspended` bucket — which is exactly the state that scenario models.
- `versions_for_key` and `collect_all` take `&Bucket`.
- The "no such bucket" / "object-only bucket" scenarios build a `Bucket`
  literal (the struct is public) rather than calling `create_bucket`.

This touches ~25 cases; mechanical but large. Assertion *values* are
unchanged — only plumbing and the bucket's versioning state (which was
previously implicit in the method name).

### New trait-level cases (run against every backend via the macro):

1. `create_bucket_then_get_returns_the_row` — all fields populated;
   `versioning == Unversioned`; `acl`/`cors`/`lifecycle == None`;
   `created_at == modified_at`; `owner` matches.
2. `create_bucket_rejects_a_duplicate_name` — second `create_bucket` with the
   same name (any owner) → `MetadataError::BucketAlreadyExists`.
3. `get_bucket_returns_none_for_a_missing_bucket`.
4. `delete_bucket_removes_the_row_and_is_idempotent` — `get_bucket` after →
   `None`; a second `delete_bucket` → `Ok(())`.
5. `delete_bucket_leaves_its_objects_untouched` — create bucket, put an
   object into it, delete the bucket, the object is still `get`-able (the
   store is permissive).
6. `set_bucket_versioning_updates_state_and_bumps_modified_at` — set
   `Enabled`; `get_bucket` shows `Enabled` and `modified_at >= created_at`
   (assert `!=` only if the test can guarantee a clock tick; otherwise
   assert the state change and that `modified_at >= created_at`).
7. `set_bucket_config_on_a_missing_bucket_is_an_error` — each of
   `set_bucket_versioning` / `_acl` / `_cors` / `_lifecycle` against an
   absent name → `MetadataError::NoSuchBucket`.
8. `bucket_acl_cors_lifecycle_blobs_round_trip` — set each to `Some(bytes)`
   (distinct, non-UTF-8-safe values), `get_bucket` returns them verbatim;
   set each back to `None`, `get_bucket` returns `None`.
9. `list_buckets_is_scoped_to_owner_and_sorted` — create `b`, `a` for
   `owner-1` and `m` for `owner-2`; `list_buckets("owner-1")` returns
   `["a", "b"]` (names), `list_buckets("owner-2")` returns `["m"]`,
   `list_buckets("owner-3")` returns `[]`.
10. `list_buckets_reads_only_the_bucket_table` — build a `Bucket` literal for
    a name that was never `create_bucket`'d, `put` an object under it;
    `list_buckets(OWNER)` still does not list that name (proves
    `list_buckets` reads the `buckets` table, not `object_metadata`).

**Migrated:** `list_buckets_returns_distinct_bucket_names` — this "buckets
are distinct object-bucket names" model is gone; it becomes case 9.
`metadata_store_is_object_safe` calls `store.list_buckets(...)` only to
exercise a `dyn` dispatch — update the call to pass an owner; no assertion
change.

## SQLite-specific tests (`src/metadata/sqlite/mod.rs`)

- Migration `0002` creates the `buckets` table and the
  `idx_buckets_owner_name` index (query `sqlite_master`).
- `row_to_bucket` decodes all columns from a raw-`INSERT`ed row, including
  `Some` and `None` blob columns.
- `row_to_bucket` rejects an unrecognized `versioning` string →
  `MetadataError::Corrupt { field: "versioning", .. }` (raw `INSERT` of a
  bogus value, as with the storage-class test).
- `list_buckets_query_plan_uses_the_owner_index` — `EXPLAIN QUERY PLAN` for
  `SELECT * FROM buckets WHERE owner = 'x' ORDER BY name` contains `SEARCH`
  and `USING INDEX idx_buckets_owner_name`, no bare `SCAN buckets`.
- `create_bucket` timestamp round-trip: the returned `Bucket.created_at`
  equals what `get_bucket` returns (millisecond precision).

## Error handling

- Duplicate `create_bucket` → `BucketAlreadyExists` (never a raw
  `sqlx::Error` leak; match the constraint violation).
- `set_bucket_*` on a missing bucket → `NoSuchBucket` (from the 0-rows-affected
  check, never a silent no-op).
- Unrecognized stored `versioning` → `Corrupt` on read.
- Backend/IO failure → `Backend`, as today.

## Decomposition

This is large enough that the implementation plan may split into two
sequential plans, each independently compiling and testable:

1. **Bucket store** — `BucketVersioning`, `Bucket`, the two error variants,
   migration `0002`, the seven bucket methods, `list_buckets` signature
   change, bucket conformance cases. Object methods stay on `bucket: &str`.
2. **Object-method migration** — object methods take `&Bucket`; the
   `*_versioned` / `*_unversioned` pairs collapse into `put` / `delete` that
   branch on `bucket.versioning`; `Metadata` loses `bucket`; `sample_metadata`
   and ~25 conformance cases rewired. `list_page` / `list_batch` internals
   stay on `&str` (private helpers).

`writing-plans` makes the final call.

## Out of scope / follow-ups

- **Handler layer:** thread a `MetadataStore` into `handlers::dispatch`;
  every object handler now does `get_bucket(name)?` (→ `NoSuchBucket` / 404)
  and passes the `&Bucket` down. Then implement `create_bucket` (409
  `BucketAlreadyExists` vs 200 `BucketAlreadyOwnedByYou` by owner match),
  `delete_bucket` (409 `BucketNotEmpty` when `list` is non-empty, else
  `NoSuchBucket` / 204), `head_bucket`, `get`/`put_bucket_versioning`, and
  the ACL / CORS / lifecycle get/put/delete handlers (each a passthrough of
  the opaque doc).
- Typed ACL / CORS / lifecycle models.
- `ListBuckets` continuation-token pagination.
- Enforcing the `Enabled`/`Suspended`-only versioning transition (handler
  reads the prior state from `get_bucket`).
- Owner filtering *within* an object listing (irrelevant — object ACLs are
  separate) and per-object owner tracking.
