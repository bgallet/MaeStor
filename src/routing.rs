use std::collections::HashMap;

use http::{HeaderMap, Method};

use crate::error::S3Error;
use crate::operation::S3Operation;

pub fn parse_request(
    method: &Method,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Result<S3Operation, S3Error> {
    let params = parse_query(query);
    let trimmed = path.trim_start_matches('/');

    if trimmed.is_empty() {
        return match *method {
            Method::GET => Ok(S3Operation::ListBuckets),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match trimmed.split_once('/') {
        Some((bucket, key)) if !key.is_empty() => {
            parse_object_operation(method, bucket, key, &params, headers)
        }
        Some((bucket, _)) => parse_bucket_operation(method, bucket, &params),
        None => parse_bucket_operation(method, trimmed, &params),
    }
}

fn parse_query(query: Option<&str>) -> HashMap<String, String> {
    match query {
        None => HashMap::new(),
        Some(q) => url::form_urlencoded::parse(q.as_bytes())
            .into_owned()
            .collect(),
    }
}

fn parse_bucket_operation(
    method: &Method,
    bucket: &str,
    params: &HashMap<String, String>,
) -> Result<S3Operation, S3Error> {
    let bucket = bucket.to_string();

    if params.contains_key("acl") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketAcl { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketAcl { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("tagging") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketTagging { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketTagging { bucket }),
            Method::DELETE => Ok(S3Operation::DeleteBucketTagging { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("versioning") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketVersioning { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketVersioning { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("uploads") {
        return match *method {
            Method::GET => Ok(S3Operation::ListMultipartUploads { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match *method {
        Method::GET => Ok(S3Operation::ListObjects { bucket }),
        Method::PUT => Ok(S3Operation::CreateBucket { bucket }),
        Method::DELETE => Ok(S3Operation::DeleteBucket { bucket }),
        Method::HEAD => Ok(S3Operation::HeadBucket { bucket }),
        _ => Err(S3Error::MethodNotAllowed),
    }
}

fn parse_object_operation(
    method: &Method,
    bucket: &str,
    key: &str,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<S3Operation, S3Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();

    if params.contains_key("acl") {
        return match *method {
            Method::GET => Ok(S3Operation::GetObjectAcl { bucket, key }),
            Method::PUT => Ok(S3Operation::PutObjectAcl { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("tagging") {
        return match *method {
            Method::GET => Ok(S3Operation::GetObjectTagging { bucket, key }),
            Method::PUT => Ok(S3Operation::PutObjectTagging { bucket, key }),
            Method::DELETE => Ok(S3Operation::DeleteObjectTagging { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("uploads") {
        return match *method {
            Method::POST => Ok(S3Operation::CreateMultipartUpload { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if let Some(upload_id) = params.get("uploadId") {
        let upload_id = upload_id.clone();
        return match *method {
            Method::PUT => {
                let part_number = params
                    .get("partNumber")
                    .ok_or(S3Error::InvalidRequest)?
                    .parse::<u32>()
                    .map_err(|_| S3Error::InvalidRequest)?;
                Ok(S3Operation::UploadPart {
                    bucket,
                    key,
                    part_number,
                    upload_id,
                })
            }
            Method::POST => Ok(S3Operation::CompleteMultipartUpload {
                bucket,
                key,
                upload_id,
            }),
            Method::DELETE => Ok(S3Operation::AbortMultipartUpload {
                bucket,
                key,
                upload_id,
            }),
            Method::GET => Ok(S3Operation::ListParts {
                bucket,
                key,
                upload_id,
            }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match *method {
        Method::GET => Ok(S3Operation::GetObject { bucket, key }),
        Method::HEAD => Ok(S3Operation::HeadObject { bucket, key }),
        Method::DELETE => Ok(S3Operation::DeleteObject { bucket, key }),
        Method::PUT => {
            if let Some(source) = headers.get("x-amz-copy-source") {
                let source = source
                    .to_str()
                    .map_err(|_| S3Error::InvalidRequest)?
                    .to_string();
                Ok(S3Operation::CopyObject { bucket, key, source })
            } else {
                Ok(S3Operation::PutObject { bucket, key })
            }
        }
        _ => Err(S3Error::MethodNotAllowed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::S3Error;
    use crate::operation::S3Operation;
    use http::{HeaderMap, HeaderValue, Method};

    fn headers_with_copy_source(source: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-copy-source",
            HeaderValue::from_str(source).unwrap(),
        );
        headers
    }

    #[test]
    fn table_driven_parse_cases() {
        let empty = HeaderMap::new();
        let cases: Vec<(Method, &str, Option<&str>, S3Operation)> = vec![
            (Method::GET, "/", None, S3Operation::ListBuckets),
            (
                Method::PUT,
                "/my-bucket",
                None,
                S3Operation::CreateBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket",
                None,
                S3Operation::DeleteBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::HEAD,
                "/my-bucket",
                None,
                S3Operation::HeadBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                None,
                S3Operation::ListObjects {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                Some("acl"),
                S3Operation::GetBucketAcl {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket",
                Some("acl"),
                S3Operation::PutBucketAcl {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket",
                Some("versioning"),
                S3Operation::PutBucketVersioning {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                Some("uploads"),
                S3Operation::ListMultipartUploads {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                None,
                S3Operation::GetObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::HEAD,
                "/my-bucket/my-key",
                None,
                S3Operation::HeadObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket/my-key",
                None,
                S3Operation::DeleteObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                None,
                S3Operation::PutObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                Some("acl"),
                S3Operation::GetObjectAcl {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                Some("tagging"),
                S3Operation::PutObjectTagging {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::POST,
                "/my-bucket/my-key",
                Some("uploads"),
                S3Operation::CreateMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                Some("partNumber=2&uploadId=up1"),
                S3Operation::UploadPart {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    part_number: 2,
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::POST,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::CompleteMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::AbortMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::ListParts {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
        ];

        for (method, path, query, expected) in cases {
            let result = parse_request(&method, path, query, &empty);
            assert_eq!(
                result,
                Ok(expected.clone()),
                "method={method:?} path={path} query={query:?}"
            );
        }
    }

    #[test]
    fn put_object_with_copy_source_header_is_copy_object() {
        let headers = headers_with_copy_source("/src-bucket/src-key");
        let result = parse_request(&Method::PUT, "/dst-bucket/dst-key", None, &headers);
        assert_eq!(
            result,
            Ok(S3Operation::CopyObject {
                bucket: "dst-bucket".to_string(),
                key: "dst-key".to_string(),
                source: "/src-bucket/src-key".to_string(),
            })
        );
    }

    #[test]
    fn upload_part_without_part_number_is_invalid_request() {
        let empty = HeaderMap::new();
        let result = parse_request(
            &Method::PUT,
            "/my-bucket/my-key",
            Some("uploadId=up1"),
            &empty,
        );
        assert_eq!(result, Err(S3Error::InvalidRequest));
    }

    #[test]
    fn unsupported_bucket_method_is_method_not_allowed() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::POST, "/my-bucket", None, &empty);
        assert_eq!(result, Err(S3Error::MethodNotAllowed));
    }
}
