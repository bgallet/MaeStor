use std::fmt;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheControl(pub String);

/// An object's MIME type. Common types are persisted as a single byte (see
/// [`KnownContentType`]); anything else is kept verbatim as a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentType {
    Known(KnownContentType),
    Other(String),
}

impl ContentType {
    /// Classifies a MIME string: an exact, case-sensitive match against a
    /// known type becomes [`ContentType::Known`], everything else is kept as
    /// [`ContentType::Other`]. Parameters (`; charset=…`) are not stripped, so
    /// `application/json; charset=utf-8` is `Other`.
    pub fn parse(value: &str) -> Self {
        match KnownContentType::parse(value) {
            Some(known) => ContentType::Known(known),
            None => ContentType::Other(value.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            ContentType::Known(known) => known.as_str(),
            ContentType::Other(value) => value,
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The MIME types common enough to store as a one-byte code. The byte on the
/// wire is the enum discriminant, so these values are persisted: **never
/// renumber or reuse a discriminant — only append new variants.**
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KnownContentType {
    ApplicationOctetStream = 0,
    ApplicationJson = 1,
    ApplicationXml = 2,
    ApplicationPdf = 3,
    ApplicationZip = 4,
    ApplicationGzip = 5,
    ApplicationFormUrlencoded = 6,
    ApplicationJavascript = 7,
    TextPlain = 8,
    TextHtml = 9,
    TextCss = 10,
    TextCsv = 11,
    TextXml = 12,
    TextMarkdown = 13,
    ImageJpeg = 14,
    ImagePng = 15,
    ImageGif = 16,
    ImageWebp = 17,
    ImageSvgXml = 18,
    ImageAvif = 19,
    AudioMpeg = 20,
    AudioOgg = 21,
    VideoMp4 = 22,
    VideoWebm = 23,
    FontWoff2 = 24,
}

impl KnownContentType {
    /// Every variant, in discriminant order. The single source of truth for
    /// the round-trip tests and for [`from_code`](Self::from_code).
    pub const ALL: [KnownContentType; 25] = [
        KnownContentType::ApplicationOctetStream,
        KnownContentType::ApplicationJson,
        KnownContentType::ApplicationXml,
        KnownContentType::ApplicationPdf,
        KnownContentType::ApplicationZip,
        KnownContentType::ApplicationGzip,
        KnownContentType::ApplicationFormUrlencoded,
        KnownContentType::ApplicationJavascript,
        KnownContentType::TextPlain,
        KnownContentType::TextHtml,
        KnownContentType::TextCss,
        KnownContentType::TextCsv,
        KnownContentType::TextXml,
        KnownContentType::TextMarkdown,
        KnownContentType::ImageJpeg,
        KnownContentType::ImagePng,
        KnownContentType::ImageGif,
        KnownContentType::ImageWebp,
        KnownContentType::ImageSvgXml,
        KnownContentType::ImageAvif,
        KnownContentType::AudioMpeg,
        KnownContentType::AudioOgg,
        KnownContentType::VideoMp4,
        KnownContentType::VideoWebm,
        KnownContentType::FontWoff2,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            KnownContentType::ApplicationOctetStream => "application/octet-stream",
            KnownContentType::ApplicationJson => "application/json",
            KnownContentType::ApplicationXml => "application/xml",
            KnownContentType::ApplicationPdf => "application/pdf",
            KnownContentType::ApplicationZip => "application/zip",
            KnownContentType::ApplicationGzip => "application/gzip",
            KnownContentType::ApplicationFormUrlencoded => "application/x-www-form-urlencoded",
            KnownContentType::ApplicationJavascript => "application/javascript",
            KnownContentType::TextPlain => "text/plain",
            KnownContentType::TextHtml => "text/html",
            KnownContentType::TextCss => "text/css",
            KnownContentType::TextCsv => "text/csv",
            KnownContentType::TextXml => "text/xml",
            KnownContentType::TextMarkdown => "text/markdown",
            KnownContentType::ImageJpeg => "image/jpeg",
            KnownContentType::ImagePng => "image/png",
            KnownContentType::ImageGif => "image/gif",
            KnownContentType::ImageWebp => "image/webp",
            KnownContentType::ImageSvgXml => "image/svg+xml",
            KnownContentType::ImageAvif => "image/avif",
            KnownContentType::AudioMpeg => "audio/mpeg",
            KnownContentType::AudioOgg => "audio/ogg",
            KnownContentType::VideoMp4 => "video/mp4",
            KnownContentType::VideoWebm => "video/webm",
            KnownContentType::FontWoff2 => "font/woff2",
        }
    }

    pub fn code(&self) -> u8 {
        *self as u8
    }

    /// `ALL` is kept in discriminant order with no gaps (enforced by a test),
    /// so the code is a direct index.
    pub fn from_code(code: u8) -> Option<Self> {
        Self::ALL.get(code as usize).copied()
    }

    /// Exact, case-sensitive match against [`as_str`](Self::as_str).
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.as_str() == value)
    }
}

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

    #[test]
    fn known_content_type_all_is_in_discriminant_order_with_no_gaps() {
        for (index, known) in KnownContentType::ALL.into_iter().enumerate() {
            assert_eq!(known.code() as usize, index, "{known:?} is out of order");
        }
    }

    #[test]
    fn known_content_type_round_trips_through_its_code() {
        for known in KnownContentType::ALL {
            assert_eq!(KnownContentType::from_code(known.code()), Some(known), "{known:?}");
        }
    }

    #[test]
    fn known_content_type_round_trips_through_its_wire_string() {
        for known in KnownContentType::ALL {
            let wire = known.as_str();
            assert_eq!(KnownContentType::parse(wire), Some(known), "round trip for {wire}");
        }
    }

    #[test]
    fn known_content_type_from_an_unassigned_code_is_none() {
        assert_eq!(KnownContentType::from_code(200), None);
    }

    #[test]
    fn known_content_type_parse_is_exact_and_case_sensitive() {
        assert_eq!(KnownContentType::parse("application/does-not-exist"), None);
        assert_eq!(KnownContentType::parse("Application/JSON"), None);
        assert_eq!(KnownContentType::parse("application/json; charset=utf-8"), None);
    }

    #[test]
    fn content_type_parse_classifies_known_and_unknown() {
        assert_eq!(
            ContentType::parse("application/json"),
            ContentType::Known(KnownContentType::ApplicationJson)
        );
        assert_eq!(
            ContentType::parse("application/vnd.acme+json"),
            ContentType::Other("application/vnd.acme+json".to_string())
        );
    }

    #[test]
    fn content_type_as_str_is_the_inverse_of_parse() {
        for input in ["text/html", "application/json", "application/x-custom-thing"] {
            assert_eq!(ContentType::parse(input).as_str(), input);
        }
    }
}
