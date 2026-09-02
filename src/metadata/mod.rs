#[cfg(test)]
mod conformance;
pub mod sqlite;
pub mod types;

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

pub use types::{
    CacheControl, ContentType, DataEncryptionContext, Etag, KnownContentType, ObjectStorageClass,
    ObjectVersion,
};

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

/// Query parameters for a single page of a list operation.
pub struct ListParams<'a> {
    /// Only keys starting with this string are considered.
    pub prefix: Option<&'a str>,
    /// When set, keys that contain this string after `prefix` are rolled up
    /// into a common prefix instead of being returned individually.
    pub delimiter: Option<&'a str>,
    /// An opaque token from a previous page's `next_cursor`. `None` starts at
    /// the beginning of the prefix-bounded range.
    pub cursor: Option<&'a str>,
    /// Hard cap on `items.len() + common_prefixes.len()` for the page. Must be
    /// at least 1; the caller owns S3's default/clamp policy.
    pub max_keys: usize,
}

/// One page of a list operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ListPage {
    /// Matching objects, ascending by key (and, for `list_versions`,
    /// newest-version-first within a key).
    pub items: Vec<Metadata>,
    /// Rolled-up prefixes, ascending and deduplicated, each ending with the
    /// delimiter. Empty when `delimiter` is `None`.
    pub common_prefixes: Vec<String>,
    /// `Some` iff the page was truncated; pass it back as the next
    /// `ListParams::cursor`.
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub enum MetadataError {
    /// The backend itself failed — unreachable database, I/O error, and so on.
    /// Generally transient and retryable.
    Backend(sqlx::Error),
    /// A stored value could not be interpreted, or a caller-supplied value
    /// cannot be represented in storage. Not retryable — it means a bug or
    /// out-of-band tampering, not a transient fault.
    Corrupt { field: &'static str, detail: String },
    /// A caller-supplied page cursor could not be decoded. Distinct from
    /// `Corrupt` (a stored-data or logic fault): this is a client error and
    /// maps to `InvalidArgument` / 400 once a handler consumes it.
    InvalidCursor { detail: String },
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataError::Backend(err) => write!(f, "metadata backend error: {err}"),
            MetadataError::Corrupt { field, detail } => {
                write!(f, "corrupt metadata field {field}: {detail}")
            }
            MetadataError::InvalidCursor { detail } => {
                write!(f, "invalid page cursor: {detail}")
            }
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
    async fn list(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError>;
    async fn list_versions(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError>;
    async fn list_buckets(&self) -> Result<Vec<String>, MetadataError>;
}

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
            content_type: Some(ContentType::parse("text/plain")),
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

    #[test]
    fn metadata_error_displays_the_corrupt_field_and_detail() {
        let err = MetadataError::Corrupt {
            field: "storage_class",
            detail: "unrecognized storage class \"NOPE\"".to_string(),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("corrupt metadata field storage_class"), "{rendered}");
        assert!(rendered.contains("unrecognized storage class"), "{rendered}");
    }

    #[test]
    fn metadata_error_displays_the_invalid_cursor_detail() {
        let err = MetadataError::InvalidCursor {
            detail: "not valid base64".to_string(),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("invalid page cursor"), "{rendered}");
        assert!(rendered.contains("not valid base64"), "{rendered}");
    }
}
