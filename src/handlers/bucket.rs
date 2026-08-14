use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;

use crate::error::S3Error;

pub async fn list_buckets() -> Result<Response<Full<Bytes>>, S3Error> {
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Owner><ID>anonymous</ID><DisplayName>anonymous</DisplayName></Owner>\
<Buckets></Buckets>\
</ListAllMyBucketsResult>";

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(body)))
        .expect("building ListBuckets response should never fail"))
}

pub async fn create_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn head_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_objects(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_acl(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_acl(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_versioning(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_versioning(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_multipart_uploads(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_buckets_returns_ok() {
        let response = list_buckets().await.expect("should succeed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_bucket_is_not_yet_implemented() {
        let result = create_bucket("my-bucket").await;
        assert_eq!(result.unwrap_err(), S3Error::NotImplemented);
    }
}
