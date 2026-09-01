# Metadata List Pagination Design

Date: 2026-09-01
Status: Draft

## Context

`MetadataStore::list` and `list_versions` currently return an unbounded
`Vec<Metadata>` filtered only by `bucket` and an optional `prefix`. They have no
page size, no cursor, no truncation signal, and no `delimiter`/CommonPrefixes
grouping. S3's `ListObjectsV2` and `ListObjectVersions` need all of that, and
`handlers::bucket::list_objects` is still a `NotImplemented` stub that does not
parse query parameters — so nothing downstream depends on the present shape yet.
This is the moment to reshape the two methods before a consumer locks them in.

Scope for this cut: the `list` / `list_versions` trait signatures, the
`ListParams` / `ListPage` types, a new `MetadataError::InvalidCursor` variant,
the SQLite implementation of both methods (paged, delimiter-aware,
index-backed), and the conformance suite updates. Not in scope: wiring
`list_objects` / `list_object_versions` handlers to the store, `list_buckets`
pagination (S3's is newer and optional; bucket counts are small — left
unpaginated), and any XML rendering.

## Types (`src/metadata/mod.rs`)

```rust
/// Query parameters for a single page of a list operation.
pub struct ListParams<'a> {
    /// Only keys starting with this string are considered.
    pub prefix: Option<&'a str>,
    /// When set, keys that contain this string after `prefix` are rolled up
    /// into a common prefix instead of being returned individually.
    pub delimiter: Option<&'a str>,
    /// An opaque token from a previous page's `next_cursor`. `None` starts at
    /// the beginning of the (prefix-bounded) range.
    pub cursor: Option<&'a str>,
    /// Hard cap on `items.len() + common_prefixes.len()` for the returned page.
    /// The caller is responsible for S3's default/clamp policy (1..=1000).
    pub max_keys: usize,
}

/// One page of a list operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ListPage {
    /// Matching objects, in ascending key order (and, for `list_versions`,
    /// newest-version-first within a key).
    pub items: Vec<Metadata>,
    /// Rolled-up prefixes, ascending, deduplicated, each ending with the
    /// delimiter. Empty when `delimiter` is `None`.
    pub common_prefixes: Vec<String>,
    /// `Some` iff the result was truncated; pass it back as the next
    /// `ListParams::cursor`.
    pub next_cursor: Option<String>,
}
```

`ListParams` is a plain struct constructed with a literal — no builder, no
`Default` (a `max_keys` of `0` is a caller bug, not a sensible default). It
carries a lifetime because `prefix` / `delimiter` / `cursor` borrow.

### `MetadataError::InvalidCursor`

```rust
pub enum MetadataError {
    Backend(sqlx::Error),
    Corrupt { field: &'static str, detail: String },
    /// A caller-supplied page cursor could not be decoded. Distinct from
    /// `Corrupt` (a stored-data or logic fault): this is a client error and
    /// maps to `InvalidArgument` / 400 once a handler consumes it.
    InvalidCursor { detail: String },
}
```

`Display` renders it as `invalid page cursor: {detail}`.

## Trait changes (`src/metadata/mod.rs`)

```rust
async fn list(&self, bucket: &str, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
async fn list_versions(&self, bucket: &str, params: ListParams<'_>) -> Result<ListPage, MetadataError>;
```

`list_buckets` is unchanged. These are the only list methods — there is no
unpaginated convenience method on the trait. Callers that want everything loop
on `next_cursor` themselves (the conformance suite provides a helper; see
below).

## Semantics

All matching is **case-sensitive** and **byte-ordered**, matching S3 (keys are
opaque byte strings; SQLite `BINARY` collation).

### Ordering

- `list`: latest row per key, ascending by key.
- `list_versions`: every row, ascending by key, then newest-version-first
  within a key. "Newest" is by insertion order (`id` descending), preserving
  the current `list_versions` behavior and matching S3's
  `ListObjectVersions` ordering.

### Prefix

A row matches when `prefix` is `None`, or `key` starts with `prefix`.
Implemented as the range `key >= prefix AND key < prefix_successor(prefix)`
(see below) — never `LIKE` (SQLite's default case-insensitive `LIKE` defeats
the index range optimization, and `LIKE` would also need `%`/`_` escaping).

### Delimiter and common prefixes

Given `prefix` `P` (possibly empty) and `delimiter` `D`, for each matching key
`K`:

- Let `rest = K[P.len()..]`. If `D` occurs in `rest` at byte offset `i`, then
  `K` belongs to the common prefix `P + rest[..i + D.len()]` and is **not**
  returned in `items`.
- Otherwise `K` is returned in `items`.

Common prefixes are deduplicated and interleaved with items in ascending order
(a page's `items` and `common_prefixes` are each sorted; together they cover a
contiguous ascending key range).

`max_keys` caps the **combined** count of `items` and `common_prefixes`.

### Cursor

The cursor is opaque and represents an **exclusive lower bound on the scan
position**. It is `base64::engine::general_purpose::URL_SAFE_NO_PAD` of a small
binary frame:

| first byte | rest | meaning |
|---|---|---|
| `0x00` | the UTF-8 `key` | resume at `key >= <key>` (inclusive key, no version) |
| `0x01` | 8-byte big-endian `id`, then the UTF-8 `key` | resume strictly after the row `(<key>, <id>)` |

The `key` portion is bound to the query as a `TEXT` parameter, so decode
rejects a non-UTF-8 key portion as `InvalidCursor`.

`base64` is already a transitive dependency; this promotes it to a direct one
(`base64 = "0.22"` in `Cargo.toml`). The tag byte keeps decoding unambiguous
regardless of what bytes a key contains (S3 keys may contain `\0`). The `id` is
SQLite's `rowid`; it is read off the batch row for cursor construction and is
**not** added to `Metadata`. Leaking it inside an opaque token is acceptable
(another backend encodes whatever its own resume needs).

- `list`: `next_cursor` frames are always tag `0x00` (`key` alone is a
  sufficient sort key for the latest-only scan).
- `list_versions`: after a returned row, tag `0x01` with that row's `(key,
  id)`. After a rolled-up common prefix, tag `0x00` with
  `prefix_successor(common_prefix)`.

"Scan position" — not "last returned item" — is the point. When a page ends on
a common prefix, the cursor points *past the entire group*
(`prefix_successor(common_prefix)`), so the next scan resumes with one index
seek rather than re-walking the group or carrying cross-page dedupe state.

Decoding failures — not valid base64, empty frame, unknown tag byte, tag `0x01`
with fewer than 8 bytes of `id` — are `MetadataError::InvalidCursor`. A
well-formed cursor pointing before the current prefix range is simply clamped
by the prefix lower bound (no error).

### `prefix_successor`

`fn prefix_successor(prefix: &str) -> Option<String>` — the shortest string
strictly greater than every string beginning with `prefix`, in SQLite `TEXT`
order (which for UTF-8 is bytewise, which is codepoint order). Works on
**characters, not bytes**, so the result is always valid UTF-8 and can be bound
as a `TEXT` parameter: pop the last `char`, increment its codepoint (stepping
`U+D7FF → U+E000` over the surrogate gap); if it was `char::MAX`, drop it and
carry to the previous `char`; if `prefix` is empty or all `char::MAX`, return
`None` ("no upper bound — scan to the end of the bucket").

Used for the query's `key <` upper bound and for the post-common-prefix resume
cursor. A free function with unit tests, beside `encode_content_type` in
`src/metadata/sqlite/mod.rs`. (SQLite-agnostic; promote to `metadata` if a
second backend needs it.)

## SQLite implementation (`src/metadata/sqlite/mod.rs`)

### Query

Per batch:

`list`:

```sql
SELECT * FROM object_metadata
WHERE bucket = ?1
  AND key >= ?2                    -- max(prefix, cursor's inclusive key bound); '' if neither
  AND (?3 IS NULL OR key > ?3)     -- cursor's exclusive key bound (tag 0x00 resume / group skip)
  AND (?4 IS NULL OR key < ?4)     -- prefix_successor(prefix), NULL if none
  AND is_latest = 1
ORDER BY key
LIMIT ?5
```

`list_versions` drops `is_latest = 1`, orders `BY key, id DESC`, and replaces
the resume term with the `(key, id)` tiebreak for a tag-`0x01` cursor:

```sql
  AND ( ?3 IS NULL
        OR key > ?3
        OR (key = ?3 AND id < ?6) )   -- ?6 = cursor id; NULL disables this disjunct
```

`EXPLAIN QUERY PLAN` confirms this still drives the index off the `key >=` /
`key <` bounds (`bucket=? AND key>? AND key<?`); the `OR` is applied as a
filter, and the `id DESC` tiebreak is a temp b-tree over the batch only.

The inclusive bound (`?2`) and the exclusive bound (`?3`) are separate so a
first batch (or a post-group-skip resume) can include a key equal to the bound
while a mid-key resume strictly advances.

`EXPLAIN QUERY PLAN` on the real schema (verified against 5000 rows):

- `list`: `SEARCH ... USING INDEX idx_object_metadata_one_latest (bucket=? AND
  key>? AND key<?)` — the partial index (`WHERE is_latest = 1`), one entry per
  live key, is chosen because of the explicit `is_latest = 1` predicate. A
  bounded index range scan, then one rowid lookup per returned row.
- `list_versions`: `SEARCH ... USING INDEX
  idx_object_metadata_bucket_key_is_latest (bucket=? AND key>? AND key<?)` plus
  `USE TEMP B-TREE FOR LAST TERM OF ORDER BY` — the temp sort is only over the
  fetched batch (≤ `LIMIT` rows), for the `id DESC` tiebreak.

Neither is a full `SCAN object_metadata`. No schema change and no new index.

### Paging loop

```
prefix_lower = prefix.unwrap_or("")
upper        = prefix.and_then(prefix_successor)          // Option
pos          = cursor.map(decode_cursor)                  // Option<Pos>; Err -> InvalidCursor
                                                          // Pos = AtKey(key) | AfterRow(key, id)
outputs = 0
items, common_prefixes = [], []
done = false   // a common prefix had no successor -> nothing can follow

loop {
    batch = query(bucket, prefix_lower, pos, upper, is_latest?, LIMIT = BATCH_SIZE)
    if batch empty { break }

    for row in batch {
        if outputs == max_keys {
            return ListPage { items, common_prefixes, next_cursor: Some(encode(pos)) }
        }
        if let Some(cp) = delimiter_group(row.key, prefix, delimiter) {
            if common_prefixes.last() != Some(&cp) {
                common_prefixes.push(cp); outputs += 1;
                match prefix_successor(&cp) {
                    Some(succ) => pos = AtKey(succ),
                    None       => { done = true; break }
                }
                skip remaining batch rows whose key belongs to `cp`;
            } else {
                pos = AfterRow(row.key, row.id);   // still inside the emitted group
            }
        } else {
            items.push(row.metadata); outputs += 1;
            pos = AfterRow(row.key, row.id);       // list: id ignored on encode (tag 0x00)
        }
    }
    if done { break }
    // batch drained below max_keys: re-query from the new `pos`
}
return ListPage { items, common_prefixes, next_cursor: None }
```

- `pos` is both the scan position and, on a truncated page, what
  `next_cursor` encodes. `AtKey` → tag `0x00`; `AfterRow` → tag `0x00` for
  `list` (key alone suffices) and tag `0x01` for `list_versions`.
- `BATCH_SIZE` is `min(max_keys, 1000).max(1)` — a bounded fetch window so a
  delimiter-heavy scan never pulls an unbounded result set; the loop re-queries
  as needed.
- `next_cursor` is `Some` only when the loop exits via `outputs == max_keys`
  **and** a follow-up probe (`SELECT 1 ... WHERE bucket = ? AND <same bounds
  from pos/upper/is_latest> LIMIT 1`) confirms another row exists. Exiting via
  an empty batch or `done` always yields `next_cursor: None`.

### Deletions

`like_prefix_pattern` and its `%`/`_`/`\` escaping are removed — range bounds
replace it. The `list_prefix_does_not_treat_percent_or_underscore_as_wildcards`
conformance case stays meaningful (range comparison is inherently literal) and
is kept.

## Conformance suite (`src/metadata/conformance.rs`)

`list` / `list_versions` are the only trait methods. Add a private helper:

```rust
/// Drains every page. Requires `max_keys >= 1`. Returns items and common
/// prefixes accumulated across pages.
async fn collect_all(
    store: &impl MetadataStore,
    versions: bool,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page_size: usize,
) -> (Vec<Metadata>, Vec<String>)
```

Existing list-related cases (`list_returns_latest_rows_for_a_bucket`,
`list_filters_by_prefix`, `list_prefix_does_not_treat_percent_or_underscore_as_wildcards`,
`list_versions_returns_every_version_including_markers`,
`put_unversioned_demotes_existing_versioned_latest_rows`,
`repeated_put_unversioned_keeps_the_sentinel_row_latest`) switch to
`collect_all` with `delimiter: None`, mostly with a page size large enough to
return everything in one page — except `list_returns_latest_rows_for_a_bucket`,
which uses `page_size = 2` to exercise multi-page accumulation. The
`versions_for_key` helper is reimplemented on `collect_all` (drain all
versions, then filter to the exact key — `prefix` alone would over-match
sibling keys).

New conformance cases (each run against every backend):

1. `list_paginates_and_reports_truncation` — 5 keys, `page_size = 2`: page 1
   has 2 items + `next_cursor`, page 2 has 2 + cursor, page 3 has 1 + no
   cursor; concatenation equals the full sorted list; no key repeated or
   skipped.
2. `list_final_exact_page_has_no_next_cursor` — 4 keys, `page_size = 2`: the
   second page returns 2 items and `next_cursor: None`.
3. `list_groups_keys_under_a_delimiter` — keys `a`, `p/1`, `p/2`, `q/1`, `z`
   with `delimiter = "/"`: items `["a", "z"]`, common prefixes `["p/", "q/"]`.
4. `list_delimiter_respects_prefix` — `prefix = "p/"`, `delimiter = "/"` over
   `p/x`, `p/sub/a`, `p/sub/b`: items `["p/x"]`, common prefixes `["p/sub/"]`.
5. `list_delimiter_page_ends_on_a_common_prefix` — many keys under one common
   prefix, then a trailing plain key, `page_size = 1`: page 1 is just the
   common prefix with a `next_cursor`; page 2 resumes *past the whole group*
   and returns the trailing key; the common prefix is emitted exactly once and
   no grouped key leaks into `items`.
6. `list_rejects_a_malformed_cursor` — `cursor = Some("!!!not-base64!!!")` →
   `MetadataError::InvalidCursor`.
7. `list_versions_paginates_across_keys_and_versions` — multiple versions of
   multiple keys, small page size: pages concatenate to the full
   newest-first-within-key ordering with no repeats.
8. `list_of_an_empty_bucket_is_an_empty_page` — `items` empty,
   `common_prefixes` empty, `next_cursor: None`.

## SQLite-specific tests (`src/metadata/sqlite/mod.rs`)

- `prefix_successor_*` unit tests: normal increment, trailing `0xFF` carry,
  all-`0xFF` and empty → `None`.
- Cursor codec unit tests: round-trip of `AtKey` and `AfterRow(key, id)`
  frames, including a key containing `\0` (keys are UTF-8, so the frame stays
  decodable); empty frame, unknown tag byte, tag `0x01` with a short `id`,
  non-UTF-8 key bytes, and non-base64 input all → `InvalidCursor`.
- `list_query_plan_uses_the_partial_index` — run `EXPLAIN QUERY PLAN` for the
  `list` batch query and assert the output contains
  `idx_object_metadata_one_latest` and no bare `SCAN object_metadata`.
- `list_versions_query_plan_uses_an_index` — same, asserting
  `idx_object_metadata_bucket_key_is_latest` and no bare
  `SCAN object_metadata`.

## Error handling

- Malformed cursor → `MetadataError::InvalidCursor` (never a panic, never a
  silent restart from the beginning).
- Backend/IO failure → `MetadataError::Backend` as today.
- A row that fails to decode mid-page → `MetadataError::Corrupt` propagates,
  as in the current `row_to_metadata` path.

## Out of scope / follow-ups

- Handler wiring (`ListObjectsV2`, `ListObjectVersions`): query-param parsing,
  S3's `max-keys` default (1000) and clamp, translating our opaque
  `next_cursor` to S3's `continuation-token` (opaque, fine as-is) and to
  `ListObjectVersions`' `key-marker` + `version-id-marker` pair, XML rendering
  with `<CommonPrefixes>`.
- `list_buckets` pagination.
- `start-after` (S3 `ListObjectsV2`): can be mapped onto `cursor` by the
  handler (encode it the same way) without a trait change.
