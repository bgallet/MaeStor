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
    CacheControl, ContentType, DataEncryptionContext, Etag, ListPage, ListParams, Metadata,
    MetadataError, MetadataStore, ObjectStorageClass, ObjectVersion,
};

/// A metadata record with every optional field left empty, for tests that only
/// care about a couple of fields. `is_latest` starts `false`; the `put_*`
/// methods set it themselves.
pub(crate) fn sample_metadata(bucket: &str, key: &str, version: &str) -> Metadata {
    Metadata {
        etag: Etag(Bytes::from_static(b"\"etag\"")),
        last_modified: SystemTime::now(),
        size: 10,
        cache_control: CacheControl("no-cache".to_string()),
        backend_id: 1,
        bucket: bucket.to_string(),
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

/// Drains every page of a list operation into flat vectors. Requires
/// `page_size >= 1`.
async fn collect_all(
    store: &impl MetadataStore,
    bucket: &str,
    versions: bool,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page_size: usize,
) -> (Vec<Metadata>, Vec<String>) {
    assert!(page_size >= 1, "collect_all needs a positive page size");
    let mut items = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
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
async fn versions_for_key(store: &impl MetadataStore, bucket: &str, key: &str) -> Vec<Metadata> {
    let (mut versions, _) = collect_all(store, bucket, true, None, None, 1000).await;
    versions.retain(|m| m.key == key);
    versions
}

pub(crate) async fn put_versioned_inserts_a_new_latest_and_demotes_the_old_one(
    store: impl MetadataStore,
) {
    store
        .put_versioned(sample_metadata("b", "k", "v1"))
        .await
        .expect("first put should succeed");
    store
        .put_versioned(sample_metadata("b", "k", "v2"))
        .await
        .expect("second put should succeed");

    assert_eq!(versions_for_key(&store, "b", "k").await.len(), 2);

    let latest = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("a latest row should exist");
    assert_eq!(latest.version, ObjectVersion("v2".to_string()));
    assert!(latest.is_latest);

    let previous = store
        .get("b", "k", Some(&ObjectVersion("v1".to_string())))
        .await
        .expect("get should succeed")
        .expect("v1 should still exist");
    assert!(!previous.is_latest);
}

pub(crate) async fn put_unversioned_upserts_a_single_row(store: impl MetadataStore) {
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

    assert_eq!(versions_for_key(&store, "b", "k").await.len(), 1);

    let latest = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(latest.version, ObjectVersion::unversioned());
    assert_eq!(latest.size, 20);
    assert!(latest.is_latest);
}

pub(crate) async fn get_with_no_version_returns_the_latest_row(store: impl MetadataStore) {
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

pub(crate) async fn get_with_a_specific_version_returns_that_version_even_if_not_latest(
    store: impl MetadataStore,
) {
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

pub(crate) async fn get_returns_none_for_a_key_that_was_never_written(store: impl MetadataStore) {
    let found = store
        .get("no-such-bucket", "no-such-key", None)
        .await
        .expect("get should succeed");
    assert_eq!(found, None);
}

pub(crate) async fn delete_versioned_creates_a_marker_as_the_new_latest(store: impl MetadataStore) {
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

pub(crate) async fn delete_specific_version_removes_only_that_row(store: impl MetadataStore) {
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

    assert_eq!(versions_for_key(&store, "b", "k").await.len(), 1);
    let latest = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(latest.version, ObjectVersion("v2".to_string()));
}

pub(crate) async fn delete_specific_version_promotes_the_next_latest_when_the_latest_is_removed(
    store: impl MetadataStore,
) {
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

pub(crate) async fn delete_unversioned_removes_the_sentinel_row(store: impl MetadataStore) {
    store
        .put_unversioned(sample_metadata("b", "k", "ignored"))
        .await
        .expect("put should succeed");

    store
        .delete_unversioned("b", "k")
        .await
        .expect("delete should succeed");

    let found = store.get("b", "k", None).await.expect("get should succeed");
    assert_eq!(found, None);
}

pub(crate) async fn list_returns_latest_rows_for_a_bucket(store: impl MetadataStore) {
    store
        .put_versioned(sample_metadata("b", "a", "v1"))
        .await
        .expect("put should succeed");
    store
        .put_versioned(sample_metadata("b", "a", "v2"))
        .await
        .expect("put should succeed");
    store
        .put_versioned(sample_metadata("b", "c", "v1"))
        .await
        .expect("put should succeed");
    store
        .put_versioned(sample_metadata("other-bucket", "a", "v1"))
        .await
        .expect("put should succeed");

    let (listed, _) = collect_all(&store, "b", false, None, None, 2).await;
    assert_eq!(listed.len(), 2);
    let versions: Vec<_> = listed.iter().map(|m| m.version.0.as_str()).collect();
    assert_eq!(versions, vec!["v2", "v1"]);
}

pub(crate) async fn list_filters_by_prefix(store: impl MetadataStore) {
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

    let (listed, _) = collect_all(&store, "b", false, Some("docs/"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["docs/a", "docs/b"]);
}

pub(crate) async fn list_prefix_does_not_treat_percent_or_underscore_as_wildcards(
    store: impl MetadataStore,
) {
    store
        .put_versioned(sample_metadata("b", "100%_off", "v1"))
        .await
        .expect("put should succeed");
    store
        .put_versioned(sample_metadata("b", "100X_off", "v1"))
        .await
        .expect("put should succeed");

    let (listed, _) = collect_all(&store, "b", false, Some("100%"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["100%_off"]);
}

pub(crate) async fn list_versions_returns_every_version_including_markers(
    store: impl MetadataStore,
) {
    store
        .put_versioned(sample_metadata("b", "k", "v1"))
        .await
        .expect("put should succeed");
    store
        .delete_versioned("b", "k", ObjectVersion("marker1".to_string()))
        .await
        .expect("delete should succeed");

    let (versions, _) = collect_all(&store, "b", true, None, None, 1000).await;
    let version_ids: Vec<_> = versions.iter().map(|m| m.version.0.as_str()).collect();
    // Newest-first within a key, matching S3's ListObjectVersions.
    assert_eq!(version_ids, vec!["marker1", "v1"]);
    assert!(versions.iter().any(|m| m.delete_marker));
}

pub(crate) async fn list_paginates_and_reports_truncation(store: impl MetadataStore) {
    for key in ["k1", "k2", "k3", "k4", "k5"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = store
            .list(
                "b",
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
    for key in ["k1", "k2", "k3", "k4"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list("b", ListParams { prefix: None, delimiter: None, cursor: None, max_keys: 2 })
        .await
        .expect("list should succeed");
    let cursor = page1.next_cursor.expect("first page of four is truncated");

    let page2 = store
        .list(
            "b",
            ListParams { prefix: None, delimiter: None, cursor: Some(&cursor), max_keys: 2 },
        )
        .await
        .expect("list should succeed");
    let keys: Vec<_> = page2.items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k3", "k4"]);
    assert_eq!(page2.next_cursor, None, "the exact final page is not truncated");
}

pub(crate) async fn list_of_an_empty_bucket_is_an_empty_page(store: impl MetadataStore) {
    let page = store
        .list(
            "no-such-bucket",
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 100 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page.items, Vec::new());
    assert_eq!(page.common_prefixes, Vec::<String>::new());
    assert_eq!(page.next_cursor, None);
}

pub(crate) async fn list_groups_keys_under_a_delimiter(store: impl MetadataStore) {
    for key in ["a", "p/1", "p/2", "q/1", "z"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, "b", false, None, Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "z"]);
    assert_eq!(common_prefixes, vec!["p/".to_string(), "q/".to_string()]);
}

pub(crate) async fn list_delimiter_respects_prefix(store: impl MetadataStore) {
    for key in ["p/x", "p/sub/a", "p/sub/b"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, "b", false, Some("p/"), Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["p/x"]);
    assert_eq!(common_prefixes, vec!["p/sub/".to_string()]);
}

pub(crate) async fn list_delimiter_page_ends_on_a_common_prefix(store: impl MetadataStore) {
    for key in ["g/1", "g/2", "g/3", "g/4", "z"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list(
            "b",
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 1 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page1.items, Vec::new());
    assert_eq!(page1.common_prefixes, vec!["g/".to_string()]);
    let cursor = page1.next_cursor.expect("more remains after the group");

    let page2 = store
        .list(
            "b",
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

pub(crate) async fn list_buckets_returns_distinct_bucket_names(store: impl MetadataStore) {
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

    let buckets = store
        .list_buckets()
        .await
        .expect("list_buckets should succeed");
    assert_eq!(
        buckets,
        vec!["bucket-a".to_string(), "bucket-b".to_string()]
    );
}

pub(crate) async fn put_unversioned_demotes_existing_versioned_latest_rows(
    store: impl MetadataStore,
) {
    store
        .put_versioned(sample_metadata("b", "k", "v1"))
        .await
        .expect("put should succeed");
    store
        .put_versioned(sample_metadata("b", "k", "v2"))
        .await
        .expect("put should succeed");

    store
        .put_unversioned(sample_metadata("b", "k", "ignored"))
        .await
        .expect("put_unversioned should succeed");

    let latest = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("a latest row should be found");
    assert_eq!(latest.version, ObjectVersion::unversioned());

    let (listed, _) = collect_all(&store, "b", false, None, None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k"]);

    // All three versions still exist; only the latest flag moved.
    assert_eq!(versions_for_key(&store, "b", "k").await.len(), 3);
}

pub(crate) async fn repeated_put_unversioned_keeps_the_sentinel_row_latest(
    store: impl MetadataStore,
) {
    store
        .put_unversioned(sample_metadata("b", "k", "ignored"))
        .await
        .expect("first put should succeed");
    store
        .put_unversioned(sample_metadata("b", "k", "ignored"))
        .await
        .expect("second put should succeed");

    let latest = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("the sentinel row should still be latest");
    assert_eq!(latest.version, ObjectVersion::unversioned());
    assert!(latest.is_latest);
}

pub(crate) async fn put_rejects_a_pre_epoch_last_modified(store: impl MetadataStore) {
    let mut metadata = sample_metadata("b", "k", "v1");
    metadata.last_modified = UNIX_EPOCH - Duration::from_secs(60);

    let err = store
        .put_versioned(metadata)
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
    let mut metadata = sample_metadata("b", "k", "v1");
    metadata.cloned_at = Some(UNIX_EPOCH - Duration::from_secs(60));

    let err = store
        .put_versioned(metadata)
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
    let expected = Metadata {
        etag: Etag(Bytes::from_static(b"\"round-trip-etag\"")),
        // Exact-millisecond instants so no backend loses precision on them.
        last_modified: UNIX_EPOCH + Duration::from_millis(1_712_000_000_123),
        size: 4096,
        cache_control: CacheControl("max-age=3600".to_string()),
        backend_id: 7,
        bucket: "b".to_string(),
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
        .put_versioned(expected.clone())
        .await
        .expect("put should succeed");

    let found = store
        .get("b", "deep/key/name", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found, expected);
}

pub(crate) async fn an_unknown_content_type_round_trips_verbatim(store: impl MetadataStore) {
    let mut metadata = sample_metadata("b", "k", "v1");
    metadata.content_type = Some(ContentType::Other("application/vnd.acme+special".to_string()));

    store
        .put_versioned(metadata.clone())
        .await
        .expect("put should succeed");

    let found = store
        .get("b", "k", None)
        .await
        .expect("get should succeed")
        .expect("a row should be found");
    assert_eq!(found.content_type, metadata.content_type);
}

pub(crate) async fn metadata_store_is_object_safe(store: impl MetadataStore + 'static) {
    let store: std::sync::Arc<dyn MetadataStore> = std::sync::Arc::new(store);
    store
        .list_buckets()
        .await
        .expect("list_buckets should succeed");
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
                put_versioned_inserts_a_new_latest_and_demotes_the_old_one
            );
            case!($make_store, put_unversioned_upserts_a_single_row);
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
                delete_versioned_creates_a_marker_as_the_new_latest
            );
            case!($make_store, delete_specific_version_removes_only_that_row);
            case!(
                $make_store,
                delete_specific_version_promotes_the_next_latest_when_the_latest_is_removed
            );
            case!($make_store, delete_unversioned_removes_the_sentinel_row);
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
            case!($make_store, list_buckets_returns_distinct_bucket_names);
            case!(
                $make_store,
                put_unversioned_demotes_existing_versioned_latest_rows
            );
            case!(
                $make_store,
                repeated_put_unversioned_keeps_the_sentinel_row_latest
            );
            case!($make_store, put_rejects_a_pre_epoch_last_modified);
            case!($make_store, put_rejects_a_pre_epoch_cloned_at);
            case!($make_store, all_fields_round_trip);
            case!($make_store, an_unknown_content_type_round_trips_verbatim);
            case!($make_store, metadata_store_is_object_safe);
        }
    };
}
pub(crate) use metadata_store_conformance;
