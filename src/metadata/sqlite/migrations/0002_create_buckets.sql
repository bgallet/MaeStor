CREATE TABLE buckets (
    name        TEXT PRIMARY KEY NOT NULL,
    owner       TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    versioning  TEXT NOT NULL,
    acl         BLOB,
    cors        BLOB,
    lifecycle   BLOB
);

CREATE INDEX idx_buckets_owner_name ON buckets (owner, name);
