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
    let params = parse_query(query)?;
    let trimmed = path.trim_start_matches('/');

    if trimmed.is_empty() {
        return match *method {
            Method::GET => Ok(S3Operation::ListBuckets),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    // Split on the raw path so that an encoded slash (`%2F`) inside a key is
    // not mistaken for a path separator, then decode each segment.
    match trimmed.split_once('/') {
        Some((bucket, key)) if !key.is_empty() => parse_object_operation(
            method,
            &percent_decode(bucket)?,
            &percent_decode(key)?,
            &params,
            headers,
        ),
        Some((bucket, _)) => parse_bucket_operation(method, &percent_decode(bucket)?, &params),
        None => parse_bucket_operation(method, &percent_decode(trimmed)?, &params),
    }
}

/// Decode `%XX` escapes. Unlike `application/x-www-form-urlencoded` decoding,
/// a literal `+` is left alone — AWS's canonical encoding treats `+` as a
/// literal character and encodes a space as `%20`.
fn percent_decode(value: &str) -> Result<String, S3Error> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| S3Error::InvalidRequest)
}

fn parse_query(query: Option<&str>) -> Result<HashMap<String, String>, S3Error> {
    let mut params = HashMap::new();
    let Some(query) = query else {
        return Ok(params);
    };

    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        params.insert(percent_decode(key)?, percent_decode(value)?);
    }

    Ok(params)
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
                Some("tagging"),
                S3Operation::GetBucketTagging {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket",
                Some("tagging"),
                S3Operation::PutBucketTagging {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket",
                Some("tagging"),
                S3Operation::DeleteBucketTagging {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                Some("versioning"),
                S3Operation::GetBucketVersioning {
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
                Method::GET,
                "/my-bucket/my-key",
                Some("tagging"),
                S3Operation::GetObjectTagging {
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
                Method::DELETE,
                "/my-bucket/my-key",
                Some("tagging"),
                S3Operation::DeleteObjectTagging {
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
    fn path_segments_are_percent_decoded() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::GET, "/my%20bucket/my%20key", None, &empty);
        assert_eq!(
            result,
            Ok(S3Operation::GetObject {
                bucket: "my bucket".to_string(),
                key: "my key".to_string(),
            })
        );
    }

    #[test]
    fn encoded_slash_in_key_is_decoded_without_splitting_the_bucket() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::GET, "/my-bucket/dir%2Ffile.txt", None, &empty);
        assert_eq!(
            result,
            Ok(S3Operation::GetObject {
                bucket: "my-bucket".to_string(),
                key: "dir/file.txt".to_string(),
            })
        );
    }

    #[test]
    fn plus_in_path_is_a_literal_plus_not_a_space() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::GET, "/my-bucket/a+b", None, &empty);
        assert_eq!(
            result,
            Ok(S3Operation::GetObject {
                bucket: "my-bucket".to_string(),
                key: "a+b".to_string(),
            })
        );
    }

    #[test]
    fn query_value_keeps_literal_plus() {
        let empty = HeaderMap::new();
        let result = parse_request(
            &Method::GET,
            "/my-bucket/my-key",
            Some("uploadId=ab+cd"),
            &empty,
        );
        assert_eq!(
            result,
            Ok(S3Operation::ListParts {
                bucket: "my-bucket".to_string(),
                key: "my-key".to_string(),
                upload_id: "ab+cd".to_string(),
            })
        );
    }

    #[test]
    fn query_value_percent_escapes_are_decoded() {
        let empty = HeaderMap::new();
        let result = parse_request(
            &Method::GET,
            "/my-bucket/my-key",
            Some("uploadId=ab%20cd"),
            &empty,
        );
        assert_eq!(
            result,
            Ok(S3Operation::ListParts {
                bucket: "my-bucket".to_string(),
                key: "my-key".to_string(),
                upload_id: "ab cd".to_string(),
            })
        );
    }

    #[test]
    fn valueless_query_marker_still_registers() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::GET, "/my-bucket", Some("acl"), &empty);
        assert_eq!(
            result,
            Ok(S3Operation::GetBucketAcl {
                bucket: "my-bucket".to_string(),
            })
        );
    }

    #[test]
    fn invalid_percent_encoding_is_invalid_request() {
        let empty = HeaderMap::new();
        // %FF is not valid UTF-8 once decoded.
        let result = parse_request(&Method::GET, "/my-bucket/bad%FFkey", None, &empty);
        assert_eq!(result, Err(S3Error::InvalidRequest));
    }

    #[test]
    fn unsupported_bucket_method_is_method_not_allowed() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::POST, "/my-bucket", None, &empty);
        assert_eq!(result, Err(S3Error::MethodNotAllowed));
    }
}
