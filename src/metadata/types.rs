use std::fmt;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheControl(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentType(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion(pub String);

impl ObjectVersion {
    /// AWS's own sentinel: `GetObject` on an object from a bucket that was
    /// never version-enabled reports this literal string as its `VersionId`.
    pub fn unversioned() -> Self {
        ObjectVersion("null".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObjectStorageClass {
    #[default]
    Standard,
    ReducedRedundancy,
    StandardIa,
    OnezoneIa,
    IntelligentTiering,
    Glacier,
    DeepArchive,
    Outposts,
    GlacierIr,
    ExpressOnezone,
}

impl ObjectStorageClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectStorageClass::Standard => "STANDARD",
            ObjectStorageClass::ReducedRedundancy => "REDUCED_REDUNDANCY",
            ObjectStorageClass::StandardIa => "STANDARD_IA",
            ObjectStorageClass::OnezoneIa => "ONEZONE_IA",
            ObjectStorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
            ObjectStorageClass::Glacier => "GLACIER",
            ObjectStorageClass::DeepArchive => "DEEP_ARCHIVE",
            ObjectStorageClass::Outposts => "OUTPOSTS",
            ObjectStorageClass::GlacierIr => "GLACIER_IR",
            ObjectStorageClass::ExpressOnezone => "EXPRESS_ONEZONE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "STANDARD" => Some(ObjectStorageClass::Standard),
            "REDUCED_REDUNDANCY" => Some(ObjectStorageClass::ReducedRedundancy),
            "STANDARD_IA" => Some(ObjectStorageClass::StandardIa),
            "ONEZONE_IA" => Some(ObjectStorageClass::OnezoneIa),
            "INTELLIGENT_TIERING" => Some(ObjectStorageClass::IntelligentTiering),
            "GLACIER" => Some(ObjectStorageClass::Glacier),
            "DEEP_ARCHIVE" => Some(ObjectStorageClass::DeepArchive),
            "OUTPOSTS" => Some(ObjectStorageClass::Outposts),
            "GLACIER_IR" => Some(ObjectStorageClass::GlacierIr),
            "EXPRESS_ONEZONE" => Some(ObjectStorageClass::ExpressOnezone),
            _ => None,
        }
    }
}

impl fmt::Display for ObjectStorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct DataEncryptionContext;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_version_unversioned_is_the_null_sentinel() {
        assert_eq!(ObjectVersion::unversioned(), ObjectVersion("null".to_string()));
    }

    #[test]
    fn storage_class_round_trips_through_its_wire_string() {
        let classes = [
            ObjectStorageClass::Standard,
            ObjectStorageClass::ReducedRedundancy,
            ObjectStorageClass::StandardIa,
            ObjectStorageClass::OnezoneIa,
            ObjectStorageClass::IntelligentTiering,
            ObjectStorageClass::Glacier,
            ObjectStorageClass::DeepArchive,
            ObjectStorageClass::Outposts,
            ObjectStorageClass::GlacierIr,
            ObjectStorageClass::ExpressOnezone,
        ];
        for class in classes {
            let wire = class.as_str();
            assert_eq!(ObjectStorageClass::parse(wire), Some(class), "round trip for {wire}");
        }
    }

    #[test]
    fn storage_class_parse_rejects_unknown_strings() {
        assert_eq!(ObjectStorageClass::parse("NOT_A_REAL_CLASS"), None);
    }

    #[test]
    fn storage_class_default_is_standard() {
        assert_eq!(ObjectStorageClass::default(), ObjectStorageClass::Standard);
    }

    #[test]
    fn storage_class_display_matches_as_str() {
        assert_eq!(format!("{}", ObjectStorageClass::Glacier), "GLACIER");
    }

    #[test]
    fn data_encryption_context_serializes_to_json() {
        let json = serde_json::to_string(&DataEncryptionContext).expect("serialize");
        let decoded: DataEncryptionContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, DataEncryptionContext);
    }
}
