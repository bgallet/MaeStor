use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    HeadBucket { bucket: String },
    ListObjects { bucket: String },
    GetBucketAcl { bucket: String },
    PutBucketAcl { bucket: String },
    GetBucketTagging { bucket: String },
    PutBucketTagging { bucket: String },
    DeleteBucketTagging { bucket: String },
    GetBucketVersioning { bucket: String },
    PutBucketVersioning { bucket: String },
    ListMultipartUploads { bucket: String },
    GetObject { bucket: String, key: String },
    PutObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
    CopyObject { bucket: String, key: String, source: String },
    GetObjectAcl { bucket: String, key: String },
    PutObjectAcl { bucket: String, key: String },
    GetObjectTagging { bucket: String, key: String },
    PutObjectTagging { bucket: String, key: String },
    DeleteObjectTagging { bucket: String, key: String },
    CreateMultipartUpload { bucket: String, key: String },
    UploadPart {
        bucket: String,
        key: String,
        part_number: u32,
        upload_id: String,
    },
    CompleteMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    AbortMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    ListParts {
        bucket: String,
        key: String,
        upload_id: String,
    },
}

impl S3Operation {
    pub fn name(&self) -> &'static str {
        match self {
            S3Operation::ListBuckets => "ListBuckets",
            S3Operation::CreateBucket { .. } => "CreateBucket",
            S3Operation::DeleteBucket { .. } => "DeleteBucket",
            S3Operation::HeadBucket { .. } => "HeadBucket",
            S3Operation::ListObjects { .. } => "ListObjects",
            S3Operation::GetBucketAcl { .. } => "GetBucketAcl",
            S3Operation::PutBucketAcl { .. } => "PutBucketAcl",
            S3Operation::GetBucketTagging { .. } => "GetBucketTagging",
            S3Operation::PutBucketTagging { .. } => "PutBucketTagging",
            S3Operation::DeleteBucketTagging { .. } => "DeleteBucketTagging",
            S3Operation::GetBucketVersioning { .. } => "GetBucketVersioning",
            S3Operation::PutBucketVersioning { .. } => "PutBucketVersioning",
            S3Operation::ListMultipartUploads { .. } => "ListMultipartUploads",
            S3Operation::GetObject { .. } => "GetObject",
            S3Operation::PutObject { .. } => "PutObject",
            S3Operation::DeleteObject { .. } => "DeleteObject",
            S3Operation::HeadObject { .. } => "HeadObject",
            S3Operation::CopyObject { .. } => "CopyObject",
            S3Operation::GetObjectAcl { .. } => "GetObjectAcl",
            S3Operation::PutObjectAcl { .. } => "PutObjectAcl",
            S3Operation::GetObjectTagging { .. } => "GetObjectTagging",
            S3Operation::PutObjectTagging { .. } => "PutObjectTagging",
            S3Operation::DeleteObjectTagging { .. } => "DeleteObjectTagging",
            S3Operation::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            S3Operation::UploadPart { .. } => "UploadPart",
            S3Operation::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            S3Operation::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            S3Operation::ListParts { .. } => "ListParts",
        }
    }
}

impl fmt::Display for S3Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

use bytes::Bytes;
use http::Response;
use http_body_util::Full;

use crate::error::S3Error;
use crate::handlers;

pub async fn dispatch(op: &S3Operation) -> Result<Response<Full<Bytes>>, S3Error> {
    match op {
        S3Operation::ListBuckets => handlers::bucket::list_buckets().await,
        S3Operation::CreateBucket { bucket } => handlers::bucket::create_bucket(bucket).await,
        S3Operation::DeleteBucket { bucket } => handlers::bucket::delete_bucket(bucket).await,
        S3Operation::HeadBucket { bucket } => handlers::bucket::head_bucket(bucket).await,
        S3Operation::ListObjects { bucket } => handlers::bucket::list_objects(bucket).await,
        S3Operation::GetBucketAcl { bucket } => handlers::bucket::get_bucket_acl(bucket).await,
        S3Operation::PutBucketAcl { bucket } => handlers::bucket::put_bucket_acl(bucket).await,
        S3Operation::GetBucketTagging { bucket } => {
            handlers::bucket::get_bucket_tagging(bucket).await
        }
        S3Operation::PutBucketTagging { bucket } => {
            handlers::bucket::put_bucket_tagging(bucket).await
        }
        S3Operation::DeleteBucketTagging { bucket } => {
            handlers::bucket::delete_bucket_tagging(bucket).await
        }
        S3Operation::GetBucketVersioning { bucket } => {
            handlers::bucket::get_bucket_versioning(bucket).await
        }
        S3Operation::PutBucketVersioning { bucket } => {
            handlers::bucket::put_bucket_versioning(bucket).await
        }
        S3Operation::ListMultipartUploads { bucket } => {
            handlers::bucket::list_multipart_uploads(bucket).await
        }
        S3Operation::GetObject { bucket, key } => handlers::object::get_object(bucket, key).await,
        S3Operation::PutObject { bucket, key } => handlers::object::put_object(bucket, key).await,
        S3Operation::DeleteObject { bucket, key } => {
            handlers::object::delete_object(bucket, key).await
        }
        S3Operation::HeadObject { bucket, key } => {
            handlers::object::head_object(bucket, key).await
        }
        S3Operation::CopyObject { bucket, key, source } => {
            handlers::object::copy_object(bucket, key, source).await
        }
        S3Operation::GetObjectAcl { bucket, key } => {
            handlers::object::get_object_acl(bucket, key).await
        }
        S3Operation::PutObjectAcl { bucket, key } => {
            handlers::object::put_object_acl(bucket, key).await
        }
        S3Operation::GetObjectTagging { bucket, key } => {
            handlers::object::get_object_tagging(bucket, key).await
        }
        S3Operation::PutObjectTagging { bucket, key } => {
            handlers::object::put_object_tagging(bucket, key).await
        }
        S3Operation::DeleteObjectTagging { bucket, key } => {
            handlers::object::delete_object_tagging(bucket, key).await
        }
        S3Operation::CreateMultipartUpload { bucket, key } => {
            handlers::object::create_multipart_upload(bucket, key).await
        }
        S3Operation::UploadPart {
            bucket,
            key,
            part_number,
            upload_id,
        } => handlers::object::upload_part(bucket, key, *part_number, upload_id).await,
        S3Operation::CompleteMultipartUpload {
            bucket,
            key,
            upload_id,
        } => handlers::object::complete_multipart_upload(bucket, key, upload_id).await,
        S3Operation::AbortMultipartUpload {
            bucket,
            key,
            upload_id,
        } => handlers::object::abort_multipart_upload(bucket, key, upload_id).await,
        S3Operation::ListParts {
            bucket,
            key,
            upload_id,
        } => handlers::object::list_parts(bucket, key, upload_id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_returns_pascal_case_operation() {
        assert_eq!(S3Operation::ListBuckets.name(), "ListBuckets");
        assert_eq!(
            S3Operation::GetObject {
                bucket: "b".to_string(),
                key: "k".to_string(),
            }
            .name(),
            "GetObject"
        );
    }

    #[test]
    fn display_matches_name() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        assert_eq!(format!("{op}"), "CreateBucket");
    }

    #[test]
    fn variants_carry_expected_fields() {
        let op = S3Operation::UploadPart {
            bucket: "b".to_string(),
            key: "k".to_string(),
            part_number: 3,
            upload_id: "u1".to_string(),
        };
        assert_eq!(
            op,
            S3Operation::UploadPart {
                bucket: "b".to_string(),
                key: "k".to_string(),
                part_number: 3,
                upload_id: "u1".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn dispatch_list_buckets_succeeds() {
        let response = dispatch(&S3Operation::ListBuckets).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn dispatch_create_bucket_is_not_implemented() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let result = dispatch(&op).await;
        assert_eq!(result.unwrap_err(), crate::error::S3Error::NotImplemented);
    }

    /// Every stub handler currently returns the same `NotImplemented` error, so
    /// this can only prove each arm reaches *a* handler rather than panicking or
    /// failing to compile against the wrong signature. It is still worth having:
    /// it pins the arms across both handler modules, including the multipart
    /// variants whose extra fields are the easiest to mis-wire.
    #[tokio::test]
    async fn dispatch_routes_a_representative_sample_of_operations() {
        let bucket = "b".to_string();
        let key = "k".to_string();
        let upload_id = "u1".to_string();

        let ops = vec![
            S3Operation::DeleteBucket {
                bucket: bucket.clone(),
            },
            S3Operation::ListObjects {
                bucket: bucket.clone(),
            },
            S3Operation::GetBucketVersioning {
                bucket: bucket.clone(),
            },
            S3Operation::GetObject {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            S3Operation::PutObject {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            S3Operation::DeleteObjectTagging {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            S3Operation::CopyObject {
                bucket: bucket.clone(),
                key: key.clone(),
                source: "/src/obj".to_string(),
            },
            S3Operation::CreateMultipartUpload {
                bucket: bucket.clone(),
                key: key.clone(),
            },
            S3Operation::UploadPart {
                bucket: bucket.clone(),
                key: key.clone(),
                part_number: 2,
                upload_id: upload_id.clone(),
            },
            S3Operation::CompleteMultipartUpload {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
            },
            S3Operation::ListParts {
                bucket,
                key,
                upload_id,
            },
        ];

        for op in ops {
            let result = dispatch(&op).await;
            assert_eq!(
                result.unwrap_err(),
                crate::error::S3Error::NotImplemented,
                "operation {} should dispatch to a not-implemented stub",
                op.name()
            );
        }
    }
}
