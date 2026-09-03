# Object Methods Take `&Bucket` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `MetadataStore` object method takes `&Bucket` instead of `bucket: &str`; the `*_versioned` / `*_unversioned` pairs collapse into `put` / `delete` that branch on `bucket.versioning`; `Metadata` loses its `bucket` field; delete-marker version ids become store-generated (UUIDv7).

**Architecture:** Plan B of two (Plan A, the bucket store, is merged). The eight object methods become six: `put` (was `put_versioned` + `put_unversioned`), `delete` (was `delete_versioned` + `delete_unversioned`, now returns the marker's `ObjectVersion`), `delete_version` (was `delete_specific_version`), plus `get` / `list` / `list_versions` — all taking `&Bucket`. `put` and `delete` read `bucket.versioning` and dispatch to the behavior the old method encoded in its name. The migration is staged: add the new methods alongside the old (additive, compiles), migrate the conformance suite off the old methods (suite stays green), then delete the old methods and `Metadata.bucket` (final cleanup).

**Tech Stack:** Rust 2021, `sqlx` 0.8 (SQLite), `async-trait`, `uuid` 1 (`v7` — new direct dependency), `tokio` test runtime.

**Spec:** `docs/superpowers/specs/2026-09-03-bucket-metadata-design.md` (sections "Object method signatures", "put — branches on bucket.versioning", "delete — branches on bucket.versioning", "Metadata loses bucket").

## Global Constraints

- Rust edition `2021`.
- `cargo test`, `cargo clippy --all-targets`, and `cargo build` must be clean (zero warnings) after every task.
- Do **not** run `cargo fmt` / `rustfmt` on any file — the repo is not fmt-clean; hand-format to match the surrounding wide style.
- `MetadataError` is **not** `PartialEq`; tests match with `matches!(...)`, never `assert_eq!` on a `Result<_, MetadataError>`.
- `uuid` version is exactly `"1"` with feature `"v7"`.
- Content version ids for `put` on an `Enabled` bucket stay **caller-supplied** (`metadata.version`); only the delete marker's id is store-generated. Do not change how `put` sources the version.
- The `object_metadata.bucket` **column** stays — the store writes `bucket.name` into it and filters on it. Only the Rust `Metadata.bucket` **field** goes away.
- Behavior preserved: `put` on `Enabled` == old `put_versioned`; `put` on `Suspended`/`Unversioned` == old `put_unversioned`; `delete` on `Enabled` == old `delete_versioned` with a generated marker; `delete` on `Suspended` inserts a delete marker at version `"null"`; `delete` on `Unversioned` == old `delete_unversioned`; `delete_version` == old `delete_specific_version`.
- The conformance suite is `src/metadata/conformance.rs`; cases are `pub(crate) async fn <name>(store: impl MetadataStore)` registered via `case!($make_store, <name>);` in the `metadata_store_conformance!` macro.
- `BucketVersioning` (`Unversioned` default / `Enabled` / `Suspended`), `Bucket` (fields `name, owner, created_at, modified_at, versioning, acl, cors, lifecycle`), and the seven bucket methods (`create_bucket` / `get_bucket` / `delete_bucket` / `set_bucket_versioning` / `_acl` / `_cors` / `_lifecycle`) exist and are stable (Plan A).

---

## File Structure

- `Cargo.toml` — add `uuid = { version = "1", features = ["v7"] }`.
- `src/metadata/sqlite/mod.rs` — `new_version_id()` free fn; add `put` / `delete` / `delete_version` impls (Task 2, delegating); rewrite `put` / `delete` to branch directly + drop the old 5 methods (Tasks 5–6); `get` / `list` / `list_versions` bind `&bucket.name` (Task 5); `upsert_row` takes a `bucket_name` param and `row_to_metadata` stops reading `bucket` (Task 6); SQLite-specific tests (Task 7).
- `src/metadata/mod.rs` — trait: add `put` / `delete` / `delete_version` (Task 2), change `get` / `list` / `list_versions` to `&Bucket` and remove the old 5 (Task 5); `Metadata` drops `bucket` (Task 6).
- `src/metadata/conformance.rs` — `fresh_bucket` helper + migrate ~29 object cases (Tasks 3–4); `collect_all` / `versions_for_key` take `&Bucket` (Task 4); `sample_metadata` drops the `bucket` param (Task 6); new versioning-branch cases (Task 7).

---

## Task 1: `uuid` dependency + `new_version_id()`

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/metadata/sqlite/mod.rs` (add the free fn near `system_time_to_millis` / `millis_to_system_time`; add unit tests in `mod tests`)

**Interfaces:**
- Consumes: `crate::metadata::ObjectVersion` (a `String` newtype: `ObjectVersion(pub String)`).
- Produces: `fn new_version_id() -> ObjectVersion` (private to the sqlite module) — a fresh UUIDv7 string; two successive calls compare `a.0 < b.0` as strings (v7 is time-ordered).

- [ ] **Step 1: Add the dependency**

In `Cargo.toml` `[dependencies]`:

```toml
uuid = { version = "1", features = ["v7"] }
```

- [ ] **Step 2: Write the failing tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`, near the other free-fn unit tests (`prefix_successor_*` etc.):

```rust
    #[test]
    fn new_version_id_is_a_distinct_uuid_each_call() {
        let a = new_version_id();
        let b = new_version_id();
        assert_ne!(a, b);
        // UUID string form: 36 chars, and the version nibble (char 14) is '7'.
        assert_eq!(a.0.len(), 36, "{}", a.0);
        assert_eq!(a.0.as_bytes()[14], b'7', "expected a v7 UUID: {}", a.0);
    }

    #[test]
    fn new_version_id_is_time_sortable() {
        // v7 embeds a millisecond timestamp prefix, so a later id sorts after
        // an earlier one lexicographically. A tiny sleep guarantees a tick.
        let a = new_version_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_version_id();
        assert!(a.0 < b.0, "{} should sort before {}", a.0, b.0);
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite::tests::new_version_id`
Expected: FAIL to compile — `cannot find function new_version_id`.

- [ ] **Step 4: Implement it**

In `src/metadata/sqlite/mod.rs`, after `millis_to_system_time`:

```rust
/// A fresh, time-sortable object version id (UUIDv7). Used for
/// store-generated delete-marker ids; the 48-bit millisecond prefix makes
/// successive ids sort in creation order by the `version` column alone.
fn new_version_id() -> ObjectVersion {
    ObjectVersion(uuid::Uuid::now_v7().to_string())
}
```

If `cargo clippy --all-targets` flags it as unused (no non-test caller yet), add `#[allow(dead_code)]` with a `// gains a caller in the next task` comment — it is removed in Task 2.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib metadata::sqlite::tests::new_version_id`
Expected: PASS (2 tests).

- [ ] **Step 6: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/metadata/sqlite/mod.rs
git commit -m "feat(metadata/sqlite): new_version_id (UUIDv7) for store-generated marker ids"
```

---

## Task 2: Add `put` / `delete` / `delete_version` (additive, delegating)

Adds three new trait methods that coexist with the existing eight. `put` and `delete` branch on `bucket.versioning` and delegate to the current method bodies; `Metadata` still has `bucket`, so `put` populates it from `bucket.name` before delegating. Purely additive — the crate compiles and every existing test still passes.

**Files:**
- Modify: `src/metadata/mod.rs` (three trait method declarations)
- Modify: `src/metadata/sqlite/mod.rs` (three method impls in `impl MetadataStore for SqliteMetadataStore`)

**Interfaces:**
- Consumes: `Bucket`, `BucketVersioning`, `new_version_id` (Task 1); the existing `put_versioned` / `put_unversioned` / `delete_versioned` / `delete_unversioned` / `delete_specific_version`.
- Produces:
  - `async fn put(&self, bucket: &Bucket, metadata: Metadata) -> Result<(), MetadataError>`
  - `async fn delete(&self, bucket: &Bucket, key: &str) -> Result<Option<ObjectVersion>, MetadataError>`
  - `async fn delete_version(&self, bucket: &Bucket, key: &str, version: &ObjectVersion) -> Result<(), MetadataError>`

- [ ] **Step 1: Add the trait declarations**

In `src/metadata/mod.rs`, in `trait MetadataStore`, right after the existing `delete_unversioned` declaration:

```rust
    /// The S3 object write. Branches on `bucket.versioning`: an `Enabled`
    /// bucket inserts a new version at `metadata.version`; a `Suspended` or
    /// `Unversioned` bucket overwrites the `"null"` version.
    async fn put(&self, bucket: &Bucket, metadata: Metadata) -> Result<(), MetadataError>;

    /// The S3 `DELETE` with no version id. `Enabled`: inserts a delete marker
    /// with a store-generated id (returned). `Suspended`: a delete marker at
    /// version `"null"` (returns that). `Unversioned`: hard-removes the
    /// `"null"` row (returns `None`).
    async fn delete(&self, bucket: &Bucket, key: &str) -> Result<Option<ObjectVersion>, MetadataError>;

    /// The S3 `DELETE` with a client-named version id — removes exactly that
    /// row and promotes the next-most-recent remaining row to latest if the
    /// removed one held that flag.
    async fn delete_version(&self, bucket: &Bucket, key: &str, version: &ObjectVersion) -> Result<(), MetadataError>;
```

- [ ] **Step 2: Confirm the build breaks where expected**

Run: `cargo build --lib`
Expected: FAIL — `SqliteMetadataStore` is missing `put` / `delete` / `delete_version`.

- [ ] **Step 3: Implement the three methods**

In `src/metadata/sqlite/mod.rs`, in `impl MetadataStore for SqliteMetadataStore`, next to the object methods:

```rust
    async fn put(&self, bucket: &Bucket, mut metadata: Metadata) -> Result<(), MetadataError> {
        metadata.bucket = bucket.name.clone();
        match bucket.versioning {
            BucketVersioning::Enabled => self.put_versioned(metadata).await,
            BucketVersioning::Suspended | BucketVersioning::Unversioned => {
                self.put_unversioned(metadata).await
            }
        }
    }

    async fn delete(
        &self,
        bucket: &Bucket,
        key: &str,
    ) -> Result<Option<ObjectVersion>, MetadataError> {
        match bucket.versioning {
            BucketVersioning::Enabled => {
                let marker = new_version_id();
                self.delete_versioned(&bucket.name, key, marker.clone()).await?;
                Ok(Some(marker))
            }
            BucketVersioning::Suspended => {
                let null = ObjectVersion::unversioned();
                self.delete_versioned(&bucket.name, key, null.clone()).await?;
                Ok(Some(null))
            }
            BucketVersioning::Unversioned => {
                self.delete_unversioned(&bucket.name, key).await?;
                Ok(None)
            }
        }
    }

    async fn delete_version(
        &self,
        bucket: &Bucket,
        key: &str,
        version: &ObjectVersion,
    ) -> Result<(), MetadataError> {
        self.delete_specific_version(&bucket.name, key, version).await
    }
```

`Bucket` / `BucketVersioning` are already imported in `sqlite/mod.rs` (Plan A). The `Suspended` `delete` path delegates to `delete_versioned` with version `"null"`: `delete_versioned` builds a marker `Metadata` and calls `put_versioned`, which flips the prior `is_latest` and upserts. `ON CONFLICT (bucket, key, version)` in `upsert_row` means a re-delete of an already-`"null"`-deleted key updates that marker row in place — which matches "insert/replace a delete-marker row at version `"null"`".

- [ ] **Step 4: Build**

Run: `cargo build --lib`
Expected: PASS.

- [ ] **Step 5: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass — every existing test still green (the old methods are untouched). Remove the `#[allow(dead_code)]` from `new_version_id` (Task 1) if it was added — it has a caller now.

- [ ] **Step 6: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs
git commit -m "feat(metadata): add put / delete / delete_version taking &Bucket (delegating)"
```

---

## Task 3: Migrate conformance — `put` / `get` / delete-marker cases

Migrates the first group of object conformance cases from the old methods to `put` / `delete` / `delete_version` + a `fresh_bucket` helper. The old methods still exist, so the suite stays green throughout.

**Files:**
- Modify: `src/metadata/conformance.rs` (add `fresh_bucket` + `OBJECT_OWNER`; rewrite the listed cases)

**Interfaces:**
- Consumes: `put` / `delete` / `delete_version` (Task 2); `create_bucket` / `set_bucket_versioning` / `get_bucket` (Plan A); `sample_metadata(bucket, key, version)` (unchanged this task).
- Produces: `async fn fresh_bucket(store: &impl MetadataStore, name: &str, versioning: BucketVersioning) -> Bucket`.

### The migration pattern

Old:
```rust
    store.put_versioned(sample_metadata("b", "k", "v1")).await.expect("put should succeed");
    let found = store.get("b", "k", None).await.expect("get").expect("row");
```

New:
```rust
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store.put(&bucket, sample_metadata("b", "k", "v1")).await.expect("put should succeed");
    let found = store.get(&bucket, "k", None).await.expect("get").expect("row");
```

- `put_versioned(m)` → `put(&bucket, m)` with `bucket` = `Enabled`.
- `put_unversioned(m)` → `put(&bucket, m)` with `bucket` = `Unversioned`.
- `delete_versioned("b","k", ObjectVersion("marker1"))` → `let marker = store.delete(&bucket, "k").await.expect("delete").expect("marker for an enabled bucket");` — the id is generated, so assert **presence and position**, not the literal `"marker1"`.
- `delete_unversioned("b","k")` → `store.delete(&bucket, "k").await.expect("delete")` (returns `None`; assert that if the case cares).
- `delete_specific_version("b","k",&v)` → `delete_version(&bucket, "k", &v)`.
- `get("b","k",v)` → `get(&bucket, "k", v)`; same for any `list` / `list_versions` calls in these cases.
- `sample_metadata("b", "k", "v1")` stays as-is (still takes a bucket arg this task) — pass the same name you gave `fresh_bucket`.

Pick the bucket's versioning to match the behavior the old method name asserted: `put_versioned*` / `delete_versioned*` / `delete_specific_version*` / any case exercising multiple versions of one key → `BucketVersioning::Enabled`. `put_unversioned*` / `delete_unversioned*` → `BucketVersioning::Unversioned`.

- [ ] **Step 1: Add the helper**

In `src/metadata/conformance.rs`, near `sample_metadata`:

```rust
const OBJECT_OWNER: &str = "object-owner";

/// Creates a bucket in `versioning` state and returns the stored row.
async fn fresh_bucket(
    store: &impl MetadataStore,
    name: &str,
    versioning: BucketVersioning,
) -> Bucket {
    let bucket = store
        .create_bucket(name, OBJECT_OWNER)
        .await
        .expect("create_bucket should succeed");
    if versioning == BucketVersioning::Unversioned {
        return bucket;
    }
    store
        .set_bucket_versioning(name, versioning)
        .await
        .expect("set_bucket_versioning should succeed");
    store
        .get_bucket(name)
        .await
        .expect("get_bucket should succeed")
        .expect("the bucket exists")
}
```

- [ ] **Step 2: Migrate these cases** (apply the pattern; assertion *values* are unchanged except for generated marker ids):

- `put_versioned_inserts_a_new_latest_and_demotes_the_old_one` → `Enabled`
- `put_unversioned_upserts_a_single_row` → `Unversioned`
- `get_with_no_version_returns_the_latest_row` → `Enabled`
- `get_with_a_specific_version_returns_that_version_even_if_not_latest` → `Enabled`
- `get_returns_none_for_a_key_that_was_never_written` → build a `Bucket` literal for the missing bucket (the struct is public):
  ```rust
  let bucket = Bucket {
      name: "no-such-bucket".to_string(),
      owner: OBJECT_OWNER.to_string(),
      created_at: std::time::SystemTime::now(),
      modified_at: std::time::SystemTime::now(),
      versioning: BucketVersioning::Unversioned,
      acl: None,
      cors: None,
      lifecycle: None,
  };
  ```
- `delete_versioned_creates_a_marker_as_the_new_latest` → `Enabled`; assert the returned `Option<ObjectVersion>` is `Some`, `get(&bucket, "k", None)` is the marker (`delete_marker == true`, `version` equal to the returned id), and `get(&bucket, "k", Some(&original_version))` still returns the original with `delete_marker == false`.
- `delete_specific_version_removes_only_that_row` → `Enabled`; `delete_specific_version` → `delete_version`.
- `delete_specific_version_promotes_the_next_latest_when_the_latest_is_removed` → `Enabled`; `delete_specific_version` → `delete_version`.
- `delete_unversioned_removes_the_sentinel_row` → `Unversioned`; `delete_unversioned` → `delete` (returns `None`).
- `an_unknown_content_type_round_trips_verbatim` → `Enabled`.
- `all_fields_round_trip` → `Enabled`. It builds a `Metadata { .. }` literal — keep its `bucket:` field this task (`"b"`), `put(&bucket, expected.clone())` / `get(&bucket, "deep/key/name", None)`.
- `put_rejects_a_pre_epoch_last_modified` → `Unversioned`; `put_versioned` → `put`.
- `put_rejects_a_pre_epoch_cloned_at` → `Unversioned`; `put_versioned` → `put`.
- `delete_bucket_leaves_its_objects_untouched` (a Plan-A bucket case) — `create_bucket("b", ..)` + `put_versioned(sample_metadata("b","k","v1"))` + `delete_bucket` + `get("b","k",None)`. Rework: `let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;` then `store.put(&bucket, sample_metadata("b","k","v1")).await...`, `store.delete_bucket("b").await...`, `store.get(&bucket, "k", None).await...` still `is_some()`.
- `list_buckets_reads_only_the_bucket_table` — `put_versioned`s into a never-created `"ghost-bucket"`. Build a `Bucket` literal for `"ghost-bucket"` and `store.put(&ghost, sample_metadata("ghost-bucket","k","v1"))`.

- [ ] **Step 3: Run the suite**

Run: `cargo test --lib metadata::`
Expected: PASS — migrated cases green, un-migrated ones (list/pagination) still on the old methods also green.

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): migrate put/get/delete conformance cases to &Bucket"
```

---

## Task 4: Migrate conformance — `list` / pagination / delimiter / versions cases

Second migration batch: everything that lists. Also switches `collect_all` and `versions_for_key` to `&Bucket`.

**Files:**
- Modify: `src/metadata/conformance.rs`

**Interfaces:**
- Consumes: `put` / `delete` (Task 2); `list` / `list_versions` (still `&str` this task — unchanged until Task 5); `fresh_bucket` (Task 3).
- Produces: `collect_all` / `versions_for_key` taking `bucket: &Bucket`.

- [ ] **Step 1: Switch the helpers**

`collect_all`'s `bucket: &str` → `bucket: &Bucket`; inside, `list` / `list_versions` still take `&str` this task, so pass `&bucket.name`:

```rust
async fn collect_all(
    store: &impl MetadataStore,
    bucket: &Bucket,
    versions: bool,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page_size: usize,
) -> (Vec<Metadata>, Vec<String>) {
    ...
        let page: ListPage = if versions {
            store.list_versions(&bucket.name, params).await
        } else {
            store.list(&bucket.name, params).await
        }
    ...
}
```

`versions_for_key(store, bucket: &Bucket, key: &str)` likewise — its `collect_all(store, bucket, ...)` call passes the `&Bucket` through.

(Task 5 flips `list` / `list_versions` to `&Bucket`, and these `&bucket.name` become `bucket`.)

- [ ] **Step 2: Migrate these cases** (`store.list("b", p)` → `store.list(&bucket.name, p)` for now; `collect_all(&store, "b", ..)` → `collect_all(&store, &bucket, ..)`; `put_versioned` → `put(&bucket, ..)`; `Enabled` when the case uses multiple versions of one key, else `Unversioned`):

- `list_returns_latest_rows_for_a_bucket` → `Enabled` (key `a` has `v1`/`v2`); the `"other-bucket"` object needs its own `fresh_bucket(&store, "other-bucket", BucketVersioning::Enabled)`.
- `list_filters_by_prefix` → `Unversioned`
- `list_prefix_does_not_treat_percent_or_underscore_as_wildcards` → `Unversioned`
- `list_versions_returns_every_version_including_markers` → `Enabled`. Currently: `put_versioned(v1)`, `delete_versioned("b","k",ObjectVersion("marker1"))`, asserts `version_ids == ["marker1", "v1"]` + a delete marker exists. Rework: `store.put(&bucket, sample_metadata("b","k","v1"))`, `let marker = store.delete(&bucket, "k").await.expect(..).expect("marker");`, then `collect_all(&store, &bucket, true, None, None, 1000)` → assert two rows, first row's `version` equals `marker` and `delete_marker == true`, second is `"v1"`.
- `list_paginates_and_reports_truncation` → `Unversioned` (5 distinct keys)
- `list_final_exact_page_has_no_next_cursor` → `Unversioned`
- `list_of_an_empty_bucket_is_an_empty_page` → `fresh_bucket` a bucket for `"no-such-bucket"`; `store.list(&bucket.name, ..)`.
- `list_groups_keys_under_a_delimiter` → `Unversioned`
- `list_delimiter_respects_prefix` → `Unversioned`
- `list_delimiter_page_ends_on_a_common_prefix` → `Unversioned`
- `list_rejects_a_malformed_cursor` → `Unversioned` (only needs `list` to reach cursor decoding)
- `list_versions_paginates_across_keys_and_versions` → `Enabled` (multiple versions per key)
- `put_unversioned_demotes_existing_versioned_latest_rows` → **the suspended-bucket scenario**: `fresh_bucket` as `Enabled`, do the two versioned `put`s, then `store.set_bucket_versioning("b", BucketVersioning::Suspended).await.expect("suspend")` + `let bucket = store.get_bucket("b").await.expect("get").expect("exists");`, then `put(&bucket, ..)` that demotes them. Assertions unchanged (`get(&bucket, "k", None)` is `ObjectVersion::unversioned()`; `list` shows one key; all three rows still present — `collect_all(.., true, ..)` length 3).
- `repeated_put_unversioned_keeps_the_sentinel_row_latest` → `Unversioned`
- `versions_for_key` callers migrated in Task 3 (`put_versioned_inserts_...`, `put_unversioned_upserts_...`, `delete_specific_version_removes_...`) plus `put_unversioned_demotes_...` here: their `versions_for_key(&store, "b", "k")` → `versions_for_key(&store, &bucket, "k")` (the helper's new signature breaks the old `&str` call sites — fix all of them in this task).

- [ ] **Step 3: Run the suite**

Run: `cargo test --lib metadata::`
Expected: PASS. Then verify no old-method callers remain:
`grep -n "put_versioned\|put_unversioned\|delete_versioned\|delete_unversioned\|delete_specific_version" src/metadata/conformance.rs` — must be empty.

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. A bare `cargo build` may warn about the 5 trait methods being test-uncalled — expected (they still have delegating callers in `put`/`delete` and the trait requires them); clears in Task 5. If clippy `--all-targets` *errors*, STOP and report.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): migrate list/pagination/versions conformance cases to &Bucket"
```

---

## Task 5: Remove the old five methods; `get` / `list` / `list_versions` take `&Bucket`

No test calls them now. Delete `put_versioned` / `put_unversioned` / `delete_versioned` / `delete_unversioned` / `delete_specific_version`, inlining their bodies into `put` / `delete` / `delete_version`. Change `get` / `list` / `list_versions` to `&Bucket`.

**Files:**
- Modify: `src/metadata/mod.rs` (trait: remove 5 decls; change 3 sigs)
- Modify: `src/metadata/sqlite/mod.rs` (inline the 5 bodies; `get`/`list`/`list_versions` bind `&bucket.name`)
- Modify: `src/metadata/conformance.rs` (`collect_all`/`versions_for_key` pass `bucket` not `&bucket.name`; any remaining `list(&bucket.name, ..)` / `get(&bucket.name, ..)` → drop `.name`)

**Interfaces:**
- Produces: the final object-method surface — `put(&Bucket, Metadata)`, `delete(&Bucket, &str) -> Option<ObjectVersion>`, `delete_version(&Bucket, &str, &ObjectVersion)`, `get(&Bucket, &str, Option<&ObjectVersion>)`, `list(&Bucket, ListParams)`, `list_versions(&Bucket, ListParams)`.

- [ ] **Step 1: Trait — remove and re-sign**

In `src/metadata/mod.rs` `trait MetadataStore`: delete the `put_versioned` / `put_unversioned` / `delete_versioned` / `delete_specific_version` / `delete_unversioned` declarations. Change:

```rust
    async fn get(&self, bucket: &Bucket, key: &str, version: Option<&ObjectVersion>) -> Result<Option<Metadata>, MetadataError>;
    async fn list(&self, bucket: &Bucket, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
    async fn list_versions(&self, bucket: &Bucket, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
```

- [ ] **Step 2: Confirm the break**

Run: `cargo build --lib`
Expected: FAIL — the trait impl still has the 5 removed methods (now trait-orphaned) and `get`/`list`/`list_versions` signatures mismatch.

- [ ] **Step 3: SQLite — inline and re-sign**

In `src/metadata/sqlite/mod.rs`, in `impl MetadataStore for SqliteMetadataStore`:

- **`put`**: replace the delegating body. `Enabled` arm gets `put_versioned`'s body (the `UPDATE ... SET is_latest = 0 WHERE bucket = ? AND key = ? AND is_latest = 1` demote + `upsert_row`, in a transaction). `Suspended | Unversioned` arm gets `put_unversioned`'s body (force `metadata.version = ObjectVersion::unversioned()`, the `... AND version <> ?` demote + `upsert_row`). Both bodies read `metadata.bucket` — keep that; it is still set by `metadata.bucket = bucket.name.clone()` at the top of `put` until Task 6.
- **`delete`**: replace the delegating body. `Enabled` / `Suspended` arms inline what `delete_versioned` did — build a marker `Metadata` (`bucket: bucket.name.clone()`, `version: <marker | null>`, `delete_marker: true`, `is_latest: true`, empty/zero everything else, `storage_class: ObjectStorageClass::Standard`) and run the same demote-UPDATE + `upsert_row` transaction. `Unversioned` arm inlines `delete_unversioned`'s single `DELETE FROM object_metadata WHERE bucket = ? AND key = ? AND version = ?` (bind `ObjectVersion::unversioned().0`).
- **`delete_version`**: inline `delete_specific_version`'s body (the `DELETE` + the promote-next-latest `UPDATE ... WHERE id = (SELECT ... ORDER BY id DESC LIMIT 1) AND NOT EXISTS (...)`), binding `&bucket.name` everywhere it bound the `&str`.
- **`get`**: `bucket: &str` → `bucket: &Bucket`; bind `&bucket.name` in both query arms.
- **`list` / `list_versions`**: `bucket: &str` → `bucket: &Bucket`; `self.list_page(&bucket.name, params, ...)`.

Delete the five now-unused methods entirely.

If `put`'s two arms and `delete`'s marker path end up with a verbatim copy of the demote-UPDATE + `upsert_row` transaction, extract it into a private helper (e.g. `async fn insert_row_as_latest(&self, metadata: &Metadata, keep_null_row: bool) -> Result<(), MetadataError>`) — a reviewer will flag a duplicated transaction block. Use your judgment on the exact shape.

- [ ] **Step 4: Conformance — finish the `&Bucket` switch**

`collect_all` / `versions_for_key`: `store.list(&bucket.name, ..)` / `store.list_versions(&bucket.name, ..)` → `store.list(bucket, ..)` / `store.list_versions(bucket, ..)` (they hold a `&Bucket`). Any case still writing `store.list(&bucket.name, ..)` or `store.get(&bucket.name, ..)` → drop `.name`. Run `grep -n "bucket.name" src/metadata/conformance.rs` and clean up every hit *except* those inside `sample_metadata(...)` arguments and `Bucket { name: ... }` literals.

- [ ] **Step 5: Run the suite**

Run: `cargo test --lib metadata::`
Expected: PASS — behavior identical to before the refactor.

- [ ] **Step 6: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. `metadata_store_is_object_safe` still type-checks the whole trait behind `Arc<dyn MetadataStore>`.

- [ ] **Step 7: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs src/metadata/conformance.rs
git commit -m "refactor(metadata): collapse to 6 object methods; get/list/list_versions take &Bucket"
```

---

## Task 6: `Metadata` drops `bucket`

**Files:**
- Modify: `src/metadata/mod.rs` (remove the field)
- Modify: `src/metadata/sqlite/mod.rs` (`upsert_row` takes `bucket_name: &str`; `row_to_metadata` stops reading `bucket`; `put` / `delete` pass `&bucket.name`; sqlite tests)
- Modify: `src/metadata/conformance.rs` (`sample_metadata` drops the `bucket` param; every call; the `Metadata` literal in `all_fields_round_trip`)

**Interfaces:**
- Produces: `Metadata` with no `bucket` field; `sample_metadata(key: &str, version: &str) -> Metadata`; `upsert_row(executor, bucket_name: &str, metadata: &Metadata)`.

- [ ] **Step 1: Remove the field**

In `src/metadata/mod.rs`, delete `pub bucket: String,` from `struct Metadata`.

- [ ] **Step 2: Confirm the break**

Run: `cargo build --lib`
Expected: FAIL — `row_to_metadata`, `upsert_row`, `put`, `delete` reference `.bucket`.

- [ ] **Step 3: SQLite fixups**

- `upsert_row`: signature gains `bucket_name: &str` (place it before `metadata: &Metadata`). Replace `.bind(&metadata.bucket)` with `.bind(bucket_name)`. Its callers (`put`'s two arms, `delete`'s marker path) pass `&bucket.name`.
- `put`: delete `metadata.bucket = bucket.name.clone();` and drop `mut` on `metadata` if now unused. The inlined demote-UPDATEs that bound `&metadata.bucket` → bind `&bucket.name`.
- `delete`: the marker `Metadata` literal drops its `bucket:` field; the demote-UPDATE + `upsert_row` call use `&bucket.name`.
- `row_to_metadata`: delete `bucket: row.try_get("bucket").map_err(MetadataError::Backend)?,` from the `Ok(Metadata { .. })` block. (`SELECT *` still returns the column; it is simply not read.)

- [ ] **Step 4: Conformance fixups**

- `sample_metadata`: `pub(crate) fn sample_metadata(key: &str, version: &str) -> Metadata`; drop `bucket:` from its `Metadata { .. }` literal. Update every call: `sample_metadata("b", "k", "v1")` → `sample_metadata("k", "v1")` (~42 sites — the second and third args become the only args).
- `all_fields_round_trip`: drop `bucket:` from its `Metadata { .. }` literal.

- [ ] **Step 5: SQLite-test fixups**

`grep -n "bucket" src/metadata/sqlite/mod.rs` within `mod tests`. `row_to_metadata_decodes_all_columns` raw-INSERTs a `bucket` value (leave the INSERT — the column exists) and must drop any `assert_eq!(metadata.bucket, ...)`. Any test building a `Metadata { .. }` literal drops the `bucket:` field.

- [ ] **Step 6: Run the suite**

Run: `cargo test --lib metadata::`
Expected: PASS.

- [ ] **Step 7: Full check**

Run: `cargo test && cargo clippy --all-targets && cargo build`
Expected: all pass, zero warnings. `grep -rn "metadata\.bucket\|m\.bucket\|\.bucket\b" src/metadata/` should only match `bucket.name` and `Bucket`-struct field accesses — never a `Metadata` `.bucket`.

- [ ] **Step 8: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs src/metadata/conformance.rs
git commit -m "refactor(metadata): drop Metadata.bucket; sample_metadata(key, version)"
```

---

## Task 7: Conformance + SQLite tests for the versioning branches

The migration preserved behavior, but the `bucket.versioning` branch points and the generated-marker path deserve dedicated coverage.

**Files:**
- Modify: `src/metadata/conformance.rs` (4 new cases + `case!` lines)
- Modify: `src/metadata/sqlite/mod.rs` (`mod tests`, 1 new test)

**Interfaces:**
- Consumes: `put` / `delete` / `get` / `collect_all` / `set_bucket_versioning` / `get_bucket` / `fresh_bucket` / `sample_metadata(key, version)`.
- Produces: conformance cases + a SQLite test.

- [ ] **Step 1: New conformance cases**

In `src/metadata/conformance.rs`, after the migrated object cases:

```rust
pub(crate) async fn put_on_a_suspended_bucket_overwrites_null_and_keeps_versioned_history(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put v1");
    store.put(&bucket, sample_metadata("k", "v2")).await.expect("put v2");

    store.set_bucket_versioning("b", BucketVersioning::Suspended).await.expect("suspend");
    let bucket = store.get_bucket("b").await.expect("get_bucket").expect("exists");

    let mut null_put = sample_metadata("k", "ignored");
    null_put.size = 999;
    store.put(&bucket, null_put).await.expect("put into suspended bucket");

    let latest = store.get(&bucket, "k", None).await.expect("get").expect("row");
    assert_eq!(latest.version, ObjectVersion::unversioned());
    assert_eq!(latest.size, 999);

    let (versions, _) = collect_all(&store, &bucket, true, None, None, 1000).await;
    let ids: Vec<_> = versions.iter().map(|m| m.version.0.as_str()).collect();
    assert!(
        ids.contains(&"v1") && ids.contains(&"v2") && ids.contains(&"null"),
        "{ids:?}"
    );
}

pub(crate) async fn delete_on_an_enabled_bucket_returns_a_generated_marker_id(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put");

    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete should succeed")
        .expect("an enabled bucket's delete returns a marker id");
    assert_ne!(marker, ObjectVersion::unversioned());

    let latest = store.get(&bucket, "k", None).await.expect("get").expect("row");
    assert_eq!(latest.version, marker);
    assert!(latest.delete_marker);
}

pub(crate) async fn delete_on_a_suspended_bucket_marks_null(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Suspended).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put");

    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete")
        .expect("a suspended bucket's delete returns a marker");
    assert_eq!(marker, ObjectVersion::unversioned());

    let latest = store.get(&bucket, "k", None).await.expect("get").expect("row");
    assert!(latest.delete_marker);
    assert_eq!(latest.version, ObjectVersion::unversioned());
}

pub(crate) async fn delete_on_an_unversioned_bucket_hard_removes_and_returns_none(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put");

    let result = store.delete(&bucket, "k").await.expect("delete should succeed");
    assert_eq!(result, None);
    assert_eq!(store.get(&bucket, "k", None).await.expect("get"), None);
}
```

Register in the macro after the migrated cases:

```rust
            case!($make_store, put_on_a_suspended_bucket_overwrites_null_and_keeps_versioned_history);
            case!($make_store, delete_on_an_enabled_bucket_returns_a_generated_marker_id);
            case!($make_store, delete_on_a_suspended_bucket_marks_null);
            case!($make_store, delete_on_an_unversioned_bucket_hard_removes_and_returns_none);
```

- [ ] **Step 2: SQLite test — successive markers sort ascending**

In `src/metadata/sqlite/mod.rs` `mod tests` (add `use crate::metadata::conformance::sample_metadata;` to the module's `use` lines if not resolvable — it is `pub(crate)`):

```rust
    #[tokio::test]
    async fn successive_delete_markers_get_ascending_version_ids() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        store.create_bucket("b", "o").await.expect("create");
        store.set_bucket_versioning("b", BucketVersioning::Enabled).await.expect("enable");
        let bucket = store.get_bucket("b").await.expect("get").expect("exists");

        store.put(&bucket, sample_metadata("k", "v1")).await.expect("put");
        let m1 = store.delete(&bucket, "k").await.expect("d1").expect("marker");
        store.put(&bucket, sample_metadata("k", "v2")).await.expect("put");
        std::thread::sleep(std::time::Duration::from_millis(2));
        let m2 = store.delete(&bucket, "k").await.expect("d2").expect("marker");

        assert!(m1.0 < m2.0, "{} should sort before {}", m1.0, m2.0);
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --lib metadata::`
Expected: PASS — 4 new conformance cases + 1 new SQLite test.

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets && cargo build`
Expected: all pass, zero warnings.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs src/metadata/sqlite/mod.rs
git commit -m "test(metadata): versioning-branch coverage for put / delete"
```

---

## Self-Review

**Spec coverage:**

| Spec item | Task |
|---|---|
| `uuid` v7 dependency; `new_version_id()` | 1 |
| `put(&Bucket, Metadata)` branches on `bucket.versioning` | 2 (delegating), 5 (inlined) |
| `delete(&Bucket, key) -> Option<ObjectVersion>`; store-generated `Enabled` marker; `"null"` for `Suspended`; `None` for `Unversioned` | 2, 5, 7 (tests) |
| `delete_version(&Bucket, key, &version)` | 2, 5 |
| `get` / `list` / `list_versions` take `&Bucket` | 5 |
| 8 object methods → 6 | 5 |
| `Metadata` loses `bucket`; `object_metadata.bucket` column stays | 6 |
| `sample_metadata(key, version)` | 6 |
| content version id stays caller-supplied for `put` on `Enabled` | 2 constraint (never changed) |
| ~29 conformance cases rewired; `fresh_bucket` helper | 3, 4 |
| markers asserted by presence/position, not literal id | 3, 4 |
| `put_unversioned_demotes_existing_versioned_latest_rows` → Suspended bucket | 4 |
| versioning-branch + generated-marker coverage | 7 |

Handler wiring (`dispatch` gets a store; `CreateBucket` / versioning / ACL / CORS / lifecycle bodies) is out of scope for this plan and the spec.

**Placeholder scan:** Tasks 3 and 4 are pattern-plus-inventory rather than full code for each of ~29 mechanical case rewrites — the pattern block plus per-case versioning-state callouts are the actual content; the compiler (nothing builds until every old-method call is gone) and the `grep` checks in Steps 3–4 are the completeness gate. Tasks 1, 2, 5, 6, 7 carry full code or precise inlining instructions.

**Type consistency:**
- `fresh_bucket(store, name, versioning) -> Bucket` — Task 3 definition; used in Tasks 3, 4, 7.
- `delete(&Bucket, &str) -> Result<Option<ObjectVersion>, MetadataError>` — Task 2 trait + impl, unchanged through Tasks 5–7; `Some` sites use `.expect(..).expect("marker")`, `Unversioned` sites use `assert_eq!(.., None)`.
- `collect_all(store, bucket: &Bucket, versions, prefix, delimiter, page_size)` — Task 4 changes the 2nd param to `&Bucket`; Task 5 changes the internal `list` call from `&bucket.name` to `bucket`.
- `upsert_row(executor, bucket_name: &str, metadata: &Metadata)` — Task 6 signature; callers in `put` / `delete` pass `&bucket.name`.
- `Metadata` field set after Task 6: `etag, last_modified, size, cache_control, backend_id, key, content_type, content_disposition, content_language, version, cloned_at, upload_id, is_latest, delete_marker, user_metadata, storage_class, encryption_context` (no `bucket`).

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-09-03-object-methods-take-bucket.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
