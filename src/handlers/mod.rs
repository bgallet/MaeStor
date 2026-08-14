pub mod bucket;
pub mod object;

use bytes::Bytes;
use http::Response;
use http_body_util::Full;

use crate::error::S3Error;
use crate::operation::S3Operation;

pub async fn dispatch(op: &S3Operation) -> Result<Response<Full<Bytes>>, S3Error> {
    match op {
        S3Operation::ListBuckets => bucket::list_buckets().await,
        S3Operation::CreateBucket { bucket: name } => bucket::create_bucket(name).await,
        S3Operation::DeleteBucket { bucket: name } => bucket::delete_bucket(name).await,
        S3Operation::HeadBucket { bucket: name } => bucket::head_bucket(name).await,
        S3Operation::ListObjects { bucket: name } => bucket::list_objects(name).await,
        S3Operation::GetBucketAcl { bucket: name } => bucket::get_bucket_acl(name).await,
        S3Operation::PutBucketAcl { bucket: name } => bucket::put_bucket_acl(name).await,
        S3Operation::GetBucketTagging { bucket: name } => bucket::get_bucket_tagging(name).await,
        S3Operation::PutBucketTagging { bucket: name } => bucket::put_bucket_tagging(name).await,
        S3Operation::DeleteBucketTagging { bucket: name } => {
            bucket::delete_bucket_tagging(name).await
        }
        S3Operation::GetBucketVersioning { bucket: name } => {
            bucket::get_bucket_versioning(name).await
        }
        S3Operation::PutBucketVersioning { bucket: name } => {
            bucket::put_bucket_versioning(name).await
        }
        S3Operation::ListMultipartUploads { bucket: name } => {
            bucket::list_multipart_uploads(name).await
        }
        S3Operation::GetObject { bucket, key } => object::get_object(bucket, key).await,
        S3Operation::PutObject { bucket, key } => object::put_object(bucket, key).await,
        S3Operation::DeleteObject { bucket, key } => object::delete_object(bucket, key).await,
        S3Operation::HeadObject { bucket, key } => object::head_object(bucket, key).await,
        S3Operation::CopyObject { bucket, key, source } => {
            object::copy_object(bucket, key, source).await
        }
        S3Operation::GetObjectAcl { bucket, key } => object::get_object_acl(bucket, key).await,
        S3Operation::PutObjectAcl { bucket, key } => object::put_object_acl(bucket, key).await,
        S3Operation::GetObjectTagging { bucket, key } => {
            object::get_object_tagging(bucket, key).await
        }
        S3Operation::PutObjectTagging { bucket, key } => {
            object::put_object_tagging(bucket, key).await
        }
        S3Operation::DeleteObjectTagging { bucket, key } => {
            object::delete_object_tagging(bucket, key).await
        }
        S3Operation::CreateMultipartUpload { bucket, key } => {
            object::create_multipart_upload(bucket, key).await
        }
        S3Operation::UploadPart {
            bucket,
            key,
            part_number,
            upload_id,
        } => object::upload_part(bucket, key, *part_number, upload_id).await,
        S3Operation::CompleteMultipartUpload {
            bucket,
            key,
            upload_id,
        } => object::complete_multipart_upload(bucket, key, upload_id).await,
        S3Operation::AbortMultipartUpload {
            bucket,
            key,
            upload_id,
        } => object::abort_multipart_upload(bucket, key, upload_id).await,
        S3Operation::ListParts {
            bucket,
            key,
            upload_id,
        } => object::list_parts(bucket, key, upload_id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(result.unwrap_err(), S3Error::NotImplemented);
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
                S3Error::NotImplemented,
                "operation {} should dispatch to a not-implemented stub",
                op.name()
            );
        }
    }
}
