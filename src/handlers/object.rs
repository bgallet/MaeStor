use bytes::Bytes;
use http::Response;
use http_body_util::Full;

use crate::error::S3Error;

pub async fn get_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn head_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn copy_object(
    _bucket: &str,
    _key: &str,
    _source: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_object_acl(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object_acl(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn create_multipart_upload(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn upload_part(
    _bucket: &str,
    _key: &str,
    _part_number: u32,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn complete_multipart_upload(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn abort_multipart_upload(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_parts(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_object_is_not_yet_implemented() {
        let result = get_object("my-bucket", "my-key").await;
        assert_eq!(result.unwrap_err(), S3Error::NotImplemented);
    }
}
