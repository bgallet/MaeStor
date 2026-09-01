CREATE TABLE object_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version TEXT NOT NULL,
    etag BLOB NOT NULL,
    last_modified INTEGER NOT NULL,
    size INTEGER NOT NULL,
    cache_control TEXT NOT NULL,
    backend_id INTEGER NOT NULL,
    -- NULL, or a single byte holding a KnownContentType code, or the UTF-8
    -- bytes of an unrecognized MIME string (always 2+ bytes, since no valid
    -- MIME type is one character).
    content_type BLOB,
    content_disposition TEXT,
    content_language TEXT,
    cloned_at INTEGER,
    upload_id BLOB,
    is_latest INTEGER NOT NULL,
    delete_marker INTEGER NOT NULL,
    user_metadata TEXT NOT NULL,
    storage_class TEXT NOT NULL,
    encryption_context TEXT,
    UNIQUE (bucket, key, version)
);

CREATE INDEX idx_object_metadata_bucket_key_is_latest
    ON object_metadata (bucket, key, is_latest);

-- Defense in depth: at most one row per (bucket, key) may be the latest.
CREATE UNIQUE INDEX idx_object_metadata_one_latest
    ON object_metadata (bucket, key)
    WHERE is_latest = 1;
