//! Behavioral conformance suite for [`MetadataStore`] implementations.
//!
//! Every implementation is expected to satisfy the same observable contract,
//! so the behavioral tests live here once and run against each implementation
//! through the trait rather than being rewritten per backend. Assertions use
//! only trait methods — no implementation may be inspected through a side
//! channel such as raw SQL.
//!
//! Invoke the suite from an implementation's own test module:
//!
//! ```ignore
//! crate::metadata::conformance::metadata_store_conformance!(
//!     conformance,
//!     || async { SqliteMetadataStore::connect_in_memory().await },
//! );
//! ```
//!
//! Tests that genuinely depend on a backend's internals (schema, on-disk
//! encoding, planting a corrupt stored value) stay in that backend's module.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::metadata::{
    Bucket, BucketVersioning, CacheControl, ContentType, DataEncryptionContext, Etag, ListPage,
    ListParams, Metadata, MetadataError, MetadataStore, ObjectStorageClass, ObjectVersion,
};

/// A metadata record with every optional field left empty, for tests that only
/// care about a couple of fields. `is_latest` starts `false`; `put` sets it
/// itself.
pub(crate) fn sample_metadata(key: &str, version: &str) -> Metadata {
    Metadata {
        etag: Etag(Bytes::from_static(b"\"etag\"")),
        last_modified: SystemTime::now(),
        size: 10,
        cache_control: CacheControl("no-cache".to_string()),
        backend_id: 1,
        key: key.to_string(),
        content_type: Some(ContentType::parse("text/plain")),
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
    set_versioning(store, name, versioning).await
}

/// Moves an existing bucket to `versioning` and returns the refreshed row.
async fn set_versioning(
    store: &impl MetadataStore,
    name: &str,
    versioning: BucketVersioning,
) -> Bucket {
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

/// A `Bucket` value with no backing `buckets` row, for the object methods that
/// read only `name` and `versioning` and never require the bucket to exist.
fn detached_bucket(name: &str, versioning: BucketVersioning) -> Bucket {
    Bucket {
        name: name.to_string(),
        owner: OBJECT_OWNER.to_string(),
        created_at: SystemTime::now(),
        modified_at: SystemTime::now(),
        versioning,
        acl: None,
        cors: None,
        lifecycle: None,
    }
}

/// Drains every page of a list operation into flat vectors. Requires
/// `page_size >= 1`.
async fn collect_all(
    store: &impl MetadataStore,
    bucket: &Bucket,
    versions: bool,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page_size: usize,
) -> (Vec<Metadata>, Vec<String>) {
    assert!(page_size >= 1, "collect_all needs a positive page size");
    let mut items = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;

    loop {
        pages += 1;
        assert!(pages < 10_000, "collect_all: cursor did not terminate");
        let params = ListParams {
            prefix,
            delimiter,
            cursor: cursor.as_deref(),
            max_keys: page_size,
        };
        let page: ListPage = if versions {
            store.list_versions(bucket, params).await
        } else {
            store.list(bucket, params).await
        }
        .expect("list should succeed");

        items.extend(page.items);
        common_prefixes.extend(page.common_prefixes);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    (items, common_prefixes)
}

/// Every stored version for one key, newest-first, via the public API.
async fn versions_for_key(store: &impl MetadataStore, bucket: &Bucket, key: &str) -> Vec<Metadata> {
    let (mut versions, _) = collect_all(store, bucket, true, None, None, 1000).await;
    versions.retain(|m| m.key == key);
    versions
}

pub(crate) async fn put_on_an_enabled_bucket_inserts_a_new_latest_and_demotes_the_old_one(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("first put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("second put should succeed");

    assert_eq!(versions_for_key(&store, &bucket, "k").await.len(), 2);

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a latest row should exist");
    assert_eq!(latest.version, ObjectVersion("v2".to_string()));
    assert!(latest.is_latest);

    let previous = store
        .get(&bucket, "k", Some(&ObjectVersion("v1".to_string())))
        .await
        .expect("get should succeed")
        .expect("v1 should still exist");
    assert!(!previous.is_latest);
}

pub(crate) async fn put_on_an_unversioned_bucket_upserts_a_single_row(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    let mut first = sample_metadata("k", "ignored");
    first.size = 10;
    store
        .put(&bucket, first)
        .await
        .expect("first put should succeed");

    let mut second = sample_metadata("k", "also-ignored");
    second.size = 20;
    store
        .put(&bucket, second)
        .await
        .expect("second put should succeed");

    assert_eq!(versions_for_key(&store, &bucket, "k").await.len(), 1);

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(latest.version, ObjectVersion::unversioned());
    assert_eq!(latest.size, 20);
    assert!(latest.is_latest);
}

pub(crate) async fn get_with_no_version_returns_the_latest_row(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("put should succeed");

    let found = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found.version, ObjectVersion("v2".to_string()));
}

pub(crate) async fn get_with_a_specific_version_returns_that_version_even_if_not_latest(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("put should succeed");

    let found = store
        .get(&bucket, "k", Some(&ObjectVersion("v1".to_string())))
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found.version, ObjectVersion("v1".to_string()));
    assert!(!found.is_latest);
}

pub(crate) async fn get_returns_none_for_a_key_that_was_never_written(store: impl MetadataStore) {
    let bucket = detached_bucket("no-such-bucket", BucketVersioning::Unversioned);
    let found = store
        .get(&bucket, "no-such-key", None)
        .await
        .expect("get should succeed");
    assert_eq!(found, None);
}

pub(crate) async fn delete_on_an_enabled_bucket_creates_a_marker_as_the_new_latest(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");

    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete should succeed")
        .expect("an enabled bucket returns a marker");

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(latest.version, marker);
    assert!(latest.delete_marker);

    let original = store
        .get(&bucket, "k", Some(&ObjectVersion("v1".to_string())))
        .await
        .expect("get should succeed")
        .expect("original version should still exist");
    assert!(!original.delete_marker);
}

pub(crate) async fn delete_version_removes_only_that_row(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("put should succeed");

    store
        .delete_version(&bucket, "k", &ObjectVersion("v1".to_string()))
        .await
        .expect("delete should succeed");

    assert_eq!(versions_for_key(&store, &bucket, "k").await.len(), 1);
    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(latest.version, ObjectVersion("v2".to_string()));
}

pub(crate) async fn delete_version_promotes_the_next_latest_when_the_latest_is_removed(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("put should succeed");

    store
        .delete_version(&bucket, "k", &ObjectVersion("v2".to_string()))
        .await
        .expect("delete should succeed");

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("v1 should have been promoted to latest");
    assert_eq!(latest.version, ObjectVersion("v1".to_string()));
    assert!(latest.is_latest);
}

pub(crate) async fn delete_on_an_unversioned_bucket_removes_the_sentinel_row(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store
        .put(&bucket, sample_metadata("k", "ignored"))
        .await
        .expect("put should succeed");

    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete should succeed");
    assert_eq!(marker, None);

    let found = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed");
    assert_eq!(found, None);
}

pub(crate) async fn list_returns_latest_rows_for_a_bucket(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    let other = fresh_bucket(&store, "other-bucket", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("a", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("a", "v2"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("c", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&other, sample_metadata("a", "v1"))
        .await
        .expect("put should succeed");

    let (listed, _) = collect_all(&store, &bucket, false, None, None, 2).await;
    assert_eq!(listed.len(), 2);
    let versions: Vec<_> = listed.iter().map(|m| m.version.0.as_str()).collect();
    assert_eq!(versions, vec!["v2", "v1"]);
}

pub(crate) async fn list_filters_by_prefix(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store
        .put(&bucket, sample_metadata("docs/a", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("docs/b", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("images/c", "v1"))
        .await
        .expect("put should succeed");

    let (listed, _) = collect_all(&store, &bucket, false, Some("docs/"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["docs/a", "docs/b"]);
}

pub(crate) async fn list_prefix_does_not_treat_percent_or_underscore_as_wildcards(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store
        .put(&bucket, sample_metadata("100%_off", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("100X_off", "v1"))
        .await
        .expect("put should succeed");

    let (listed, _) = collect_all(&store, &bucket, false, Some("100%"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["100%_off"]);
}

pub(crate) async fn list_versions_returns_every_version_including_markers(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete should succeed")
        .expect("an enabled bucket returns a marker");

    let (versions, _) = collect_all(&store, &bucket, true, None, None, 1000).await;
    assert_eq!(versions.len(), 2);
    // Newest-first within a key, matching S3's ListObjectVersions.
    assert_eq!(versions[0].version, marker);
    assert!(versions[0].delete_marker);
    assert_eq!(versions[1].version.0.as_str(), "v1");
}

pub(crate) async fn list_paginates_and_reports_truncation(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    for key in ["k1", "k2", "k3", "k4", "k5"] {
        store
            .put(&bucket, sample_metadata(key, "v1"))
            .await
            .expect("put should succeed");
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = store
            .list(
                &bucket,
                ListParams { prefix: None, delimiter: None, cursor: cursor.as_deref(), max_keys: 2 },
            )
            .await
            .expect("list should succeed");
        pages += 1;
        assert!(page.items.len() <= 2, "page over max_keys");
        seen.extend(page.items.iter().map(|m| m.key.clone()));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }

    assert_eq!(pages, 3, "5 keys at page size 2 is three pages");
    assert_eq!(seen, vec!["k1", "k2", "k3", "k4", "k5"]);
}

pub(crate) async fn list_final_exact_page_has_no_next_cursor(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    for key in ["k1", "k2", "k3", "k4"] {
        store
            .put(&bucket, sample_metadata(key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list(&bucket, ListParams { prefix: None, delimiter: None, cursor: None, max_keys: 2 })
        .await
        .expect("list should succeed");
    let cursor = page1.next_cursor.expect("first page of four is truncated");

    let page2 = store
        .list(
            &bucket,
            ListParams { prefix: None, delimiter: None, cursor: Some(&cursor), max_keys: 2 },
        )
        .await
        .expect("list should succeed");
    let keys: Vec<_> = page2.items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k3", "k4"]);
    assert_eq!(page2.next_cursor, None, "the exact final page is not truncated");
}

pub(crate) async fn list_of_an_empty_bucket_is_an_empty_page(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "empty", BucketVersioning::Unversioned).await;
    let page = store
        .list(
            &bucket,
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 100 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page.items, Vec::new());
    assert_eq!(page.common_prefixes, Vec::<String>::new());
    assert_eq!(page.next_cursor, None);
}

pub(crate) async fn list_groups_keys_under_a_delimiter(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    for key in ["a", "p/1", "p/2", "q/1", "z"] {
        store
            .put(&bucket, sample_metadata(key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, &bucket, false, None, Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "z"]);
    assert_eq!(common_prefixes, vec!["p/".to_string(), "q/".to_string()]);
}

pub(crate) async fn list_delimiter_respects_prefix(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    for key in ["p/x", "p/sub/a", "p/sub/b"] {
        store
            .put(&bucket, sample_metadata(key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, &bucket, false, Some("p/"), Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["p/x"]);
    assert_eq!(common_prefixes, vec!["p/sub/".to_string()]);
}

pub(crate) async fn list_delimiter_page_ends_on_a_common_prefix(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    for key in ["g/1", "g/2", "g/3", "g/4", "z"] {
        store
            .put(&bucket, sample_metadata(key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list(
            &bucket,
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 1 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page1.items, Vec::new());
    assert_eq!(page1.common_prefixes, vec!["g/".to_string()]);
    let cursor = page1.next_cursor.expect("more remains after the group");

    let page2 = store
        .list(
            &bucket,
            ListParams {
                prefix: None,
                delimiter: Some("/"),
                cursor: Some(&cursor),
                max_keys: 10,
            },
        )
        .await
        .expect("list should succeed");
    let keys: Vec<_> = page2.items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["z"], "resumes past the whole group");
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.next_cursor, None);
}

pub(crate) async fn list_rejects_a_malformed_cursor(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    let err = store
        .list(
            &bucket,
            ListParams {
                prefix: None,
                delimiter: None,
                cursor: Some("!!!not-base64!!!"),
                max_keys: 10,
            },
        )
        .await
        .expect_err("a garbled cursor should be rejected");
    assert!(
        matches!(err, MetadataError::InvalidCursor { .. }),
        "unexpected error: {err:?}"
    );
}

pub(crate) async fn list_versions_paginates_across_keys_and_versions(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    // k1 gets three versions, k2 gets two.
    for version in ["v1", "v2", "v3"] {
        store
            .put(&bucket, sample_metadata("k1", version))
            .await
            .expect("put should succeed");
    }
    for version in ["v1", "v2"] {
        store
            .put(&bucket, sample_metadata("k2", version))
            .await
            .expect("put should succeed");
    }

    let (paged, _) = collect_all(&store, &bucket, true, None, None, 2).await;
    let (single, _) = collect_all(&store, &bucket, true, None, None, 1000).await;

    let ids = |rows: &[Metadata]| -> Vec<(String, String)> {
        rows.iter().map(|m| (m.key.clone(), m.version.0.clone())).collect()
    };
    assert_eq!(ids(&paged), ids(&single), "paging must not reorder or drop rows");
    assert_eq!(
        ids(&single),
        vec![
            ("k1".to_string(), "v3".to_string()),
            ("k1".to_string(), "v2".to_string()),
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("k2".to_string(), "v1".to_string()),
        ],
    );
}

const BUCKET_OWNER: &str = "owner-1";

pub(crate) async fn list_buckets_is_scoped_to_owner_and_sorted(store: impl MetadataStore) {
    store.create_bucket("b", BUCKET_OWNER).await.expect("create b");
    store.create_bucket("a", BUCKET_OWNER).await.expect("create a");
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
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");

    store.delete_bucket("b").await.expect("delete_bucket should succeed");

    let object = store
        .get(&bucket, "k", None)
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

    store
        .set_bucket_cors("b", None)
        .await
        .expect("clearing an already-unset config should still succeed");

    let without = store.get_bucket("b").await.expect("get").expect("exists");
    assert_eq!(without.acl, None);
    assert_eq!(without.cors, None);
    assert_eq!(without.lifecycle, None);
}

pub(crate) async fn list_buckets_reads_only_the_bucket_table(store: impl MetadataStore) {
    // This case isolates one property: `list_buckets` reads only the `buckets`
    // table, never `object_metadata`. The ghost bucket below gets object rows
    // but no `buckets` row, so it must not appear in the listing.
    let ghost = detached_bucket("ghost-bucket", BucketVersioning::Enabled);
    store
        .put(&ghost, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store.create_bucket("real", BUCKET_OWNER).await.expect("create should succeed");

    let listed = store.list_buckets(BUCKET_OWNER).await.expect("list_buckets should succeed");
    let names: Vec<_> = listed.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, vec!["real"]);
}

pub(crate) async fn put_on_a_suspended_bucket_demotes_existing_versioned_latest_rows(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store
        .put(&bucket, sample_metadata("k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put(&bucket, sample_metadata("k", "v2"))
        .await
        .expect("put should succeed");

    let bucket = set_versioning(&store, "b", BucketVersioning::Suspended).await;

    store
        .put(&bucket, sample_metadata("k", "ignored"))
        .await
        .expect("put should succeed");

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a latest row should be found");
    assert_eq!(latest.version, ObjectVersion::unversioned());

    let (listed, _) = collect_all(&store, &bucket, false, None, None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k"]);

    // All three versions still exist; only the latest flag moved.
    assert_eq!(versions_for_key(&store, &bucket, "k").await.len(), 3);
}

pub(crate) async fn repeated_put_on_an_unversioned_bucket_keeps_the_sentinel_row_latest(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    store
        .put(&bucket, sample_metadata("k", "ignored"))
        .await
        .expect("first put should succeed");
    store
        .put(&bucket, sample_metadata("k", "ignored"))
        .await
        .expect("second put should succeed");

    let latest = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("the sentinel row should still be latest");
    assert_eq!(latest.version, ObjectVersion::unversioned());
    assert!(latest.is_latest);
}

pub(crate) async fn put_rejects_a_pre_epoch_last_modified(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    let mut metadata = sample_metadata("k", "v1");
    metadata.last_modified = UNIX_EPOCH - Duration::from_secs(60);

    let err = store
        .put(&bucket, metadata)
        .await
        .expect_err("a pre-epoch timestamp should be rejected");
    assert!(
        matches!(
            err,
            MetadataError::Corrupt {
                field: "last_modified",
                ..
            }
        ),
        "unexpected error: {err:?}"
    );
}

pub(crate) async fn put_rejects_a_pre_epoch_cloned_at(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Unversioned).await;
    let mut metadata = sample_metadata("k", "v1");
    metadata.cloned_at = Some(UNIX_EPOCH - Duration::from_secs(60));

    let err = store
        .put(&bucket, metadata)
        .await
        .expect_err("a pre-epoch timestamp should be rejected");
    assert!(
        matches!(
            err,
            MetadataError::Corrupt {
                field: "cloned_at",
                ..
            }
        ),
        "unexpected error: {err:?}"
    );
}

pub(crate) async fn all_fields_round_trip(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    let expected = Metadata {
        etag: Etag(Bytes::from_static(b"\"round-trip-etag\"")),
        // Exact-millisecond instants so no backend loses precision on them.
        last_modified: UNIX_EPOCH + Duration::from_millis(1_712_000_000_123),
        size: 4096,
        cache_control: CacheControl("max-age=3600".to_string()),
        backend_id: 7,
        key: "deep/key/name".to_string(),
        // Exact match — round-trips through the one-byte known-type code.
        content_type: Some(ContentType::parse("application/json")),
        content_disposition: Some("attachment; filename=\"x.json\"".to_string()),
        content_language: Some("en-US".to_string()),
        version: ObjectVersion("v1".to_string()),
        cloned_at: Some(UNIX_EPOCH + Duration::from_millis(1_712_000_009_000)),
        upload_id: Some(Bytes::from_static(&[0x00, 0x01, 0xfe, 0xff])),
        is_latest: true,
        delete_marker: false,
        user_metadata: HashMap::from([
            ("plain".to_string(), Bytes::from_static(b"value")),
            (
                "binary".to_string(),
                Bytes::from_static(&[0xff, 0x00, b'h', b'i']),
            ),
        ]),
        storage_class: ObjectStorageClass::IntelligentTiering,
        encryption_context: Some(DataEncryptionContext),
    };

    store
        .put(&bucket, expected.clone())
        .await
        .expect("put should succeed");

    let found = store
        .get(&bucket, "deep/key/name", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found, expected);
}

pub(crate) async fn an_unknown_content_type_round_trips_verbatim(store: impl MetadataStore) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    let mut metadata = sample_metadata("k", "v1");
    metadata.content_type = Some(ContentType::Other("application/vnd.acme+special".to_string()));

    store
        .put(&bucket, metadata.clone())
        .await
        .expect("put should succeed");

    let found = store
        .get(&bucket, "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found.content_type, metadata.content_type);
}

pub(crate) async fn metadata_store_is_object_safe(store: impl MetadataStore + 'static) {
    let store: std::sync::Arc<dyn MetadataStore> = std::sync::Arc::new(store);
    store
        .list_buckets("owner-1")
        .await
        .expect("list_buckets should succeed");
}

pub(crate) async fn put_on_a_suspended_bucket_overwrites_null_and_keeps_versioned_history(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put v1");
    store.put(&bucket, sample_metadata("k", "v2")).await.expect("put v2");

    let bucket = set_versioning(&store, "b", BucketVersioning::Suspended).await;

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

pub(crate) async fn delete_on_a_suspended_bucket_demotes_other_latest_rows_and_keeps_history(
    store: impl MetadataStore,
) {
    let bucket = fresh_bucket(&store, "b", BucketVersioning::Enabled).await;
    store.put(&bucket, sample_metadata("k", "v1")).await.expect("put v1");
    store.put(&bucket, sample_metadata("k", "v2")).await.expect("put v2");

    let bucket = set_versioning(&store, "b", BucketVersioning::Suspended).await;

    let marker = store
        .delete(&bucket, "k")
        .await
        .expect("delete should succeed")
        .expect("a suspended bucket's delete returns a marker");
    assert_eq!(marker, ObjectVersion::unversioned());

    let latest = store.get(&bucket, "k", None).await.expect("get").expect("row");
    assert!(latest.delete_marker);
    assert_eq!(latest.version, ObjectVersion::unversioned());

    // v1 and v2 are demoted, not removed — the null marker joins them.
    let history = versions_for_key(&store, &bucket, "k").await;
    let ids: Vec<_> = history.iter().map(|m| m.version.0.as_str()).collect();
    assert_eq!(history.len(), 3, "{ids:?}");
    assert!(ids.contains(&"v1") && ids.contains(&"v2") && ids.contains(&"null"), "{ids:?}");
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

/// Emits one `#[tokio::test]` that runs `$name` from this module against a
/// fresh store from `$make_store`.
macro_rules! metadata_store_conformance_case {
    ($make_store:expr, $name:ident) => {
        #[tokio::test]
        async fn $name() {
            let make_store = $make_store;
            $crate::metadata::conformance::$name(make_store().await).await;
        }
    };
}
pub(crate) use metadata_store_conformance_case;

/// Runs the full [`MetadataStore`] behavioral suite against an implementation.
///
/// `$mod_name` is the wrapper module the generated tests land in; `$make_store`
/// is a zero-argument closure returning a future that yields a fresh, empty
/// store, called once per test.
macro_rules! metadata_store_conformance {
    ($mod_name:ident, $make_store:expr) => {
        mod $mod_name {
            use super::*;
            use $crate::metadata::conformance::metadata_store_conformance_case as case;

            case!(
                $make_store,
                put_on_an_enabled_bucket_inserts_a_new_latest_and_demotes_the_old_one
            );
            case!($make_store, put_on_an_unversioned_bucket_upserts_a_single_row);
            case!($make_store, get_with_no_version_returns_the_latest_row);
            case!(
                $make_store,
                get_with_a_specific_version_returns_that_version_even_if_not_latest
            );
            case!(
                $make_store,
                get_returns_none_for_a_key_that_was_never_written
            );
            case!(
                $make_store,
                delete_on_an_enabled_bucket_creates_a_marker_as_the_new_latest
            );
            case!($make_store, delete_version_removes_only_that_row);
            case!(
                $make_store,
                delete_version_promotes_the_next_latest_when_the_latest_is_removed
            );
            case!($make_store, delete_on_an_unversioned_bucket_removes_the_sentinel_row);
            case!($make_store, list_returns_latest_rows_for_a_bucket);
            case!($make_store, list_filters_by_prefix);
            case!(
                $make_store,
                list_prefix_does_not_treat_percent_or_underscore_as_wildcards
            );
            case!(
                $make_store,
                list_versions_returns_every_version_including_markers
            );
            case!($make_store, list_paginates_and_reports_truncation);
            case!($make_store, list_final_exact_page_has_no_next_cursor);
            case!($make_store, list_of_an_empty_bucket_is_an_empty_page);
            case!($make_store, list_groups_keys_under_a_delimiter);
            case!($make_store, list_delimiter_respects_prefix);
            case!($make_store, list_delimiter_page_ends_on_a_common_prefix);
            case!($make_store, list_rejects_a_malformed_cursor);
            case!($make_store, list_versions_paginates_across_keys_and_versions);
            case!($make_store, list_buckets_is_scoped_to_owner_and_sorted);
            case!($make_store, create_bucket_then_get_returns_the_row);
            case!($make_store, create_bucket_rejects_a_duplicate_name);
            case!($make_store, get_bucket_returns_none_for_a_missing_bucket);
            case!($make_store, delete_bucket_removes_the_row_and_is_idempotent);
            case!($make_store, delete_bucket_leaves_its_objects_untouched);
            case!($make_store, set_bucket_versioning_updates_state);
            case!($make_store, set_bucket_config_on_a_missing_bucket_is_an_error);
            case!($make_store, bucket_acl_cors_lifecycle_blobs_round_trip);
            case!($make_store, list_buckets_reads_only_the_bucket_table);
            case!(
                $make_store,
                put_on_a_suspended_bucket_demotes_existing_versioned_latest_rows
            );
            case!(
                $make_store,
                repeated_put_on_an_unversioned_bucket_keeps_the_sentinel_row_latest
            );
            case!($make_store, put_rejects_a_pre_epoch_last_modified);
            case!($make_store, put_rejects_a_pre_epoch_cloned_at);
            case!($make_store, all_fields_round_trip);
            case!($make_store, an_unknown_content_type_round_trips_verbatim);
            case!($make_store, put_on_a_suspended_bucket_overwrites_null_and_keeps_versioned_history);
            case!($make_store, delete_on_an_enabled_bucket_returns_a_generated_marker_id);
            case!($make_store, delete_on_a_suspended_bucket_marks_null);
            case!(
                $make_store,
                delete_on_a_suspended_bucket_demotes_other_latest_rows_and_keeps_history
            );
            case!($make_store, delete_on_an_unversioned_bucket_hard_removes_and_returns_none);
            case!($make_store, metadata_store_is_object_safe);
        }
    };
}
pub(crate) use metadata_store_conformance;
