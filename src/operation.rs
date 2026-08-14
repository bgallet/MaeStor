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
}
