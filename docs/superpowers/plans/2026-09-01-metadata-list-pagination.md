# Metadata List Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape `MetadataStore::list` / `list_versions` into paged,
delimiter-aware queries returning a `ListPage`, backed by an index range scan
in SQLite.

**Architecture:** `list` / `list_versions` take a `ListParams` (prefix,
delimiter, opaque cursor, max_keys) and return a `ListPage` (items,
common_prefixes, next_cursor). The SQLite implementation shares one
`list_page` loop that pulls bounded batches via an index range scan
(`key >= ? AND key < ? [AND is_latest = 1]`), rolls delimiter-grouped keys into
common prefixes, and resumes past a group in one seek via a
`prefix_successor` cursor. Cursors are opaque base64 frames. The behavioral
contract is verified once through the trait by the conformance suite.

**Tech Stack:** Rust 2021, `sqlx` 0.8 (SQLite), `async-trait`, `tokio` test
runtime, `base64` 0.22 (new direct dependency).

**Spec:** `docs/superpowers/specs/2026-09-01-metadata-list-pagination-design.md`

## Global Constraints

- Rust edition: `2021`. Toolchain supports `Option::is_some_and` (1.70+).
- All key matching is case-sensitive and byte-ordered (SQLite `TEXT` /
  `BINARY` collation). Never use `LIKE` for prefix matching.
- `cargo test` and `cargo clippy --all-targets` must be clean after every task.
- Do **not** run `cargo fmt` / `rustfmt` across existing files — the repo is
  not fmt-clean; hand-format new code to match the surrounding (wide) style.
- New dependency version pins are exact values from the spec: `base64 = "0.22"`.
- `MetadataError` is **not** `PartialEq` (it wraps `sqlx::Error`); tests match
  with `matches!(...)`, never `assert_eq!` on a `Result<_, MetadataError>`.
- The crate root re-exports metadata types from `src/metadata/mod.rs` via
  `pub use types::{...}`; new public types added directly in `mod.rs` need no
  re-export line.

---

## File Structure

- `Cargo.toml` — add `base64 = "0.22"` to `[dependencies]`.
- `src/metadata/mod.rs` — add `ListParams`, `ListPage`; add
  `MetadataError::InvalidCursor` + its `Display` arm; change the `list` /
  `list_versions` trait signatures.
- `src/metadata/sqlite/mod.rs` — add `prefix_successor`, the `Pos` cursor
  codec, `delimiter_group`; replace `list` / `list_versions` bodies with a
  shared `list_page` / `list_batch`; delete `like_prefix_pattern`; add
  SQLite-specific unit tests (helpers, cursor codec, query-plan assertions).
- `src/metadata/conformance.rs` — add the `collect_all` page-draining helper;
  reimplement `versions_for_key` on it; migrate the 5 existing list cases;
  add 8 new conformance cases and register them in the
  `metadata_store_conformance!` macro.

---

## Task 1: List parameter and page types

**Files:**
- Modify: `src/metadata/mod.rs` (add types near `Metadata`; extend
  `MetadataError` and its `Display`; add one test)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub struct ListParams<'a> { pub prefix: Option<&'a str>, pub delimiter: Option<&'a str>, pub cursor: Option<&'a str>, pub max_keys: usize }`
  - `pub struct ListPage { pub items: Vec<Metadata>, pub common_prefixes: Vec<String>, pub next_cursor: Option<String> }` — derives `Debug, Clone, PartialEq`
  - `MetadataError::InvalidCursor { detail: String }`, `Display` = `invalid page cursor: {detail}`

- [ ] **Step 1: Add the `InvalidCursor` display test**

In `src/metadata/mod.rs`, inside `mod tests`, after
`metadata_error_displays_the_corrupt_field_and_detail`:

```rust
    #[test]
    fn metadata_error_displays_the_invalid_cursor_detail() {
        let err = MetadataError::InvalidCursor {
            detail: "not valid base64".to_string(),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("invalid page cursor"), "{rendered}");
        assert!(rendered.contains("not valid base64"), "{rendered}");
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib metadata::tests::metadata_error_displays_the_invalid_cursor_detail`
Expected: FAIL to compile — `no variant named InvalidCursor`.

- [ ] **Step 3: Add the enum variant and display arm**

In `src/metadata/mod.rs`, extend the enum:

```rust
pub enum MetadataError {
    /// The backend itself failed — unreachable database, I/O error, and so on.
    /// Generally transient and retryable.
    Backend(sqlx::Error),
    /// A stored value could not be interpreted, or a caller-supplied value
    /// cannot be represented in storage. Not retryable — it means a bug or
    /// out-of-band tampering, not a transient fault.
    Corrupt { field: &'static str, detail: String },
    /// A caller-supplied page cursor could not be decoded. Distinct from
    /// `Corrupt` (a stored-data or logic fault): this is a client error and
    /// maps to `InvalidArgument` / 400 once a handler consumes it.
    InvalidCursor { detail: String },
}
```

Add to the `Display` match, after the `Corrupt` arm:

```rust
            MetadataError::InvalidCursor { detail } => {
                write!(f, "invalid page cursor: {detail}")
            }
```

- [ ] **Step 4: Add the `ListParams` / `ListPage` types**

In `src/metadata/mod.rs`, immediately after the `Metadata` struct definition:

```rust
/// Query parameters for a single page of a list operation.
pub struct ListParams<'a> {
    /// Only keys starting with this string are considered.
    pub prefix: Option<&'a str>,
    /// When set, keys that contain this string after `prefix` are rolled up
    /// into a common prefix instead of being returned individually.
    pub delimiter: Option<&'a str>,
    /// An opaque token from a previous page's `next_cursor`. `None` starts at
    /// the beginning of the prefix-bounded range.
    pub cursor: Option<&'a str>,
    /// Hard cap on `items.len() + common_prefixes.len()` for the page. Must be
    /// at least 1; the caller owns S3's default/clamp policy.
    pub max_keys: usize,
}

/// One page of a list operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ListPage {
    /// Matching objects, ascending by key (and, for `list_versions`,
    /// newest-version-first within a key).
    pub items: Vec<Metadata>,
    /// Rolled-up prefixes, ascending and deduplicated, each ending with the
    /// delimiter. Empty when `delimiter` is `None`.
    pub common_prefixes: Vec<String>,
    /// `Some` iff the page was truncated; pass it back as the next
    /// `ListParams::cursor`.
    pub next_cursor: Option<String>,
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --lib metadata::tests::metadata_error_displays_the_invalid_cursor_detail`
Expected: PASS.

- [ ] **Step 6: Verify the whole crate still builds and is clean**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. (`ListParams` / `ListPage` are unused so far — `pub` items
in a library crate do not trigger dead-code warnings.)

- [ ] **Step 7: Commit**

```bash
git add src/metadata/mod.rs
git commit -m "feat(metadata): ListParams, ListPage, MetadataError::InvalidCursor"
```

---

## Task 2: `prefix_successor`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs` (add the function after
  `decode_content_type`; add unit tests in `mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: `fn prefix_successor(prefix: &str) -> Option<String>` (private to
  the `sqlite` module) — the shortest string strictly greater than every
  string beginning with `prefix`, in SQLite `TEXT` order; `None` when `prefix`
  is empty or all `char::MAX`.

- [ ] **Step 1: Write the failing tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`, after
`row_to_metadata_rejects_unrecognized_storage_class`:

```rust
    #[test]
    fn prefix_successor_increments_the_last_character() {
        assert_eq!(prefix_successor("abc").as_deref(), Some("abd"));
        // '/' (0x2F) -> '0' (0x30): the delimiter-group skip case.
        assert_eq!(prefix_successor("photos/").as_deref(), Some("photos0"));
    }

    #[test]
    fn prefix_successor_carries_over_a_char_max_tail() {
        assert_eq!(prefix_successor("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_successor("a\u{10FFFF}\u{10FFFF}").as_deref(), Some("b"));
    }

    #[test]
    fn prefix_successor_steps_over_the_surrogate_gap() {
        assert_eq!(prefix_successor("x\u{D7FF}").as_deref(), Some("x\u{E000}"));
    }

    #[test]
    fn prefix_successor_is_none_for_empty_or_all_char_max() {
        assert_eq!(prefix_successor(""), None);
        assert_eq!(prefix_successor("\u{10FFFF}"), None);
        assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite::tests::prefix_successor`
Expected: FAIL to compile — `cannot find function prefix_successor`.

- [ ] **Step 3: Implement `prefix_successor`**

In `src/metadata/sqlite/mod.rs`, after the `decode_content_type` function (a
free function alongside the other codec helpers):

```rust
/// The shortest string strictly greater than every string beginning with
/// `prefix`, in SQLite `TEXT` order (bytewise, which for UTF-8 is codepoint
/// order). Operates on `char`s, so the result is always valid UTF-8 and can be
/// bound as a `TEXT` parameter. `None` when there is no such string: `prefix`
/// is empty or entirely `char::MAX`.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        if next == 0xD800 {
            next = 0xE000; // step over the UTF-16 surrogate range
        }
        if let Some(next) = char::from_u32(next) {
            let mut out: String = chars.iter().collect();
            out.push(next);
            return Some(out);
        }
        // `last` was char::MAX (U+10FFFF): drop it and carry to the previous char.
    }
    None
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib metadata::sqlite::tests::prefix_successor`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat(metadata/sqlite): prefix_successor helper for range bounds"
```

---

## Task 3: Cursor codec (`Pos`) and the `base64` dependency

**Files:**
- Modify: `Cargo.toml` (add `base64 = "0.22"`)
- Modify: `src/metadata/sqlite/mod.rs` (add `use base64::Engine as _;`, the
  `Pos` enum + impl + `decode_cursor_key`; add unit tests)

**Interfaces:**
- Consumes: `MetadataError::InvalidCursor` (Task 1).
- Produces (all private to the `sqlite` module):
  - `enum Pos { AtKey(String), AfterRow { key: String, id: i64 } }` — derives `Debug, Clone, PartialEq, Eq`
  - `impl Pos { fn encode(&self) -> String; fn decode(cursor: &str) -> Result<Pos, MetadataError> }`
  - `Pos::AtKey` encodes to a tag-`0x00` frame; `Pos::AfterRow` to a tag-`0x01`
    frame (8-byte big-endian id, then the UTF-8 key). `decode` returns
    `MetadataError::InvalidCursor` for any malformed input.

- [ ] **Step 1: Add the `base64` dependency**

In `Cargo.toml`, add to the `[dependencies]` table (a new line among the
existing entries — ordering is not enforced in this file):

```toml
base64 = "0.22"
```

- [ ] **Step 2: Write the failing tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`, after the
`prefix_successor` tests:

```rust
    fn b64(frame: impl AsRef<[u8]>) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(frame)
    }

    #[test]
    fn cursor_round_trips_an_at_key_position() {
        // Keys are UTF-8 but may contain control bytes such as NUL.
        let pos = Pos::AtKey("photos/2024/\u{0}odd".to_string());
        assert_eq!(Pos::decode(&pos.encode()).unwrap(), pos);
    }

    #[test]
    fn cursor_round_trips_an_after_row_position() {
        let pos = Pos::AfterRow { key: "a/b/c".to_string(), id: 123_456 };
        assert_eq!(Pos::decode(&pos.encode()).unwrap(), pos);
    }

    #[test]
    fn cursor_decode_rejects_bad_input() {
        for bad in [
            "!!! not base64 !!!".to_string(),
            b64([]),                       // empty frame
            b64([0x09, b'k']),             // unknown tag
            b64([0x01, 0, 0, 0]),          // tag 0x01, id truncated
            b64([0x00, 0xFF, 0xFE]),       // tag 0x00, non-UTF-8 key
        ] {
            assert!(
                matches!(Pos::decode(&bad), Err(MetadataError::InvalidCursor { .. })),
                "expected InvalidCursor for {bad:?}",
            );
        }
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite::tests::cursor`
Expected: FAIL to compile — `cannot find type Pos`.

- [ ] **Step 4: Implement the codec**

In `src/metadata/sqlite/mod.rs`, add near the top with the other `use` lines:

```rust
use base64::Engine as _;
```

After `prefix_successor`:

```rust
/// A scan position for the paged list queries — the decoded form of a
/// `ListParams::cursor` and the value re-encoded into `ListPage::next_cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pos {
    /// Resume at `key >= key`. Produced by a bare-key cursor and by skipping
    /// past a common-prefix group.
    AtKey(String),
    /// Resume strictly after the row `(key, id)`.
    AfterRow { key: String, id: i64 },
}

impl Pos {
    fn encode(&self) -> String {
        let mut frame = Vec::new();
        match self {
            Pos::AtKey(key) => {
                frame.push(0x00);
                frame.extend_from_slice(key.as_bytes());
            }
            Pos::AfterRow { key, id } => {
                frame.push(0x01);
                frame.extend_from_slice(&id.to_be_bytes());
                frame.extend_from_slice(key.as_bytes());
            }
        }
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(frame)
    }

    fn decode(cursor: &str) -> Result<Self, MetadataError> {
        let bad = |detail: &str| MetadataError::InvalidCursor {
            detail: detail.to_string(),
        };
        let frame = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|_| bad("not valid base64"))?;
        let (&tag, rest) = frame.split_first().ok_or_else(|| bad("empty cursor"))?;
        match tag {
            0x00 => Ok(Pos::AtKey(decode_cursor_key(rest)?)),
            0x01 => {
                let id_bytes: [u8; 8] = rest
                    .get(..8)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| bad("cursor row id is truncated"))?;
                Ok(Pos::AfterRow {
                    id: i64::from_be_bytes(id_bytes),
                    key: decode_cursor_key(&rest[8..])?,
                })
            }
            other => Err(bad(&format!("unknown cursor tag {other:#04x}"))),
        }
    }
}

fn decode_cursor_key(bytes: &[u8]) -> Result<String, MetadataError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| MetadataError::InvalidCursor {
        detail: "cursor key is not valid UTF-8".to_string(),
    })
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib metadata::sqlite::tests::cursor`
Expected: PASS (3 tests).

- [ ] **Step 6: Verify the crate builds and is clean**

Run: `cargo test && cargo clippy --all-targets`
Expected: both pass (`clippy --all-targets` compiles the test module, where
`Pos` is used). A bare `cargo build` between now and Task 5 may print a
`dead_code` warning for `Pos` / `prefix_successor` / `delimiter_group` — that
is expected (they gain non-test callers in Task 5) and is a warning, not an
error. Do not add `#[allow(dead_code)]`.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/metadata/sqlite/mod.rs
git commit -m "feat(metadata/sqlite): opaque base64 cursor codec"
```

---

## Task 4: `delimiter_group`

**Files:**
- Modify: `src/metadata/sqlite/mod.rs` (add the function after `Pos`; add unit
  tests)

**Interfaces:**
- Consumes: nothing.
- Produces: `fn delimiter_group(key: &str, prefix: &str, delimiter: &str) -> Option<String>`
  (private). The caller guarantees `key.starts_with(prefix)`. Returns the
  common prefix `key` rolls into (`prefix` + everything through the first
  delimiter after `prefix`), or `None` if `key` is a plain item.

- [ ] **Step 1: Write the failing tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`, after the cursor tests:

```rust
    #[test]
    fn delimiter_group_rolls_up_a_key_containing_the_delimiter() {
        assert_eq!(delimiter_group("photos/jan/a", "", "/").as_deref(), Some("photos/"));
        assert_eq!(delimiter_group("p/sub/a", "p/", "/").as_deref(), Some("p/sub/"));
    }

    #[test]
    fn delimiter_group_is_none_for_a_plain_key() {
        assert_eq!(delimiter_group("photos", "", "/"), None);
        assert_eq!(delimiter_group("p/x", "p/", "/"), None);
    }

    #[test]
    fn delimiter_group_supports_a_multi_char_delimiter() {
        assert_eq!(delimiter_group("aXXbXXc", "", "XX").as_deref(), Some("aXX"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib metadata::sqlite::tests::delimiter_group`
Expected: FAIL to compile — `cannot find function delimiter_group`.

- [ ] **Step 3: Implement `delimiter_group`**

In `src/metadata/sqlite/mod.rs`, after the `Pos` impl block:

```rust
/// If `key` (already known to start with `prefix`) contains `delimiter`
/// somewhere after `prefix`, returns the common prefix it rolls up into:
/// `prefix` plus everything through that first delimiter. `None` means `key`
/// is a plain item.
fn delimiter_group(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    let rest = &key[prefix.len()..];
    let idx = rest.find(delimiter)?;
    Some(format!("{prefix}{}", &rest[..idx + delimiter.len()]))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib metadata::sqlite::tests::delimiter_group`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "feat(metadata/sqlite): delimiter_group helper"
```

---

## Task 5: Switch `list` / `list_versions` to the paged interface

This is one atomic commit: the trait signature change forces the SQLite
implementation and the conformance suite to change together. After it, the
full existing conformance suite passes against the new API; new-behavior
tests come in Tasks 7–9.

**Files:**
- Modify: `src/metadata/mod.rs` — `list` / `list_versions` trait signatures in
  `trait MetadataStore`
- Modify: `src/metadata/sqlite/mod.rs` — replace `list` / `list_versions`
  bodies; add `list_page` and `list_batch` to the existing inherent
  `impl SqliteMetadataStore` block (the one holding `connect` / `migrate`);
  delete `like_prefix_pattern` and its doc comment
- Modify: `src/metadata/conformance.rs` — add `collect_all`; reimplement
  `versions_for_key` on it; migrate 5 list cases (lines 264, 288, 310, 330,
  and the `store.list("b", None)` call at line 400)

**Interfaces:**
- Consumes: `ListParams`, `ListPage`, `MetadataError::InvalidCursor` (Task 1);
  `prefix_successor` (Task 2); `Pos` (Task 3); `delimiter_group` (Task 4).
- Produces:
  - `async fn list(&self, bucket: &str, params: ListParams<'_>) -> Result<ListPage, MetadataError>`
  - `async fn list_versions(&self, bucket: &str, params: ListParams<'_>) -> Result<ListPage, MetadataError>`
  - conformance helper `async fn collect_all(store: &impl MetadataStore, bucket: &str, versions: bool, prefix: Option<&str>, delimiter: Option<&str>, page_size: usize) -> (Vec<Metadata>, Vec<String>)`

- [ ] **Step 1: Change the trait signatures**

In `src/metadata/mod.rs`, replace the two `list` lines in
`trait MetadataStore`:

```rust
    async fn list(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError>;
    async fn list_versions(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError>;
```

- [ ] **Step 2: Confirm the build breaks where expected**

Run: `cargo build --lib`
Expected: FAIL — `list` / `list_versions` in `impl MetadataStore for
SqliteMetadataStore` no longer match the trait; `conformance.rs` calls the old
signature. This confirms the blast radius before fixing it.

- [ ] **Step 3: Add the batch query helper**

In `src/metadata/sqlite/mod.rs`, add this module-level `const` near the top
(with the other free items), then add both methods to the **existing**
`impl SqliteMetadataStore` block (the one with `connect` / `connect_in_memory`
/ `migrate`):

```rust
/// A batch never fetches more than this many rows at once, regardless of
/// `max_keys` — a delimiter-heavy scan re-queries as it skips groups.
const LIST_BATCH_CAP: usize = 1000;

impl SqliteMetadataStore {
    // ... existing connect / connect_in_memory / migrate stay ...

    /// One batch of the paged scan: rows for `bucket` with `key` in
    /// `[lower, upper)` (lower from `prefix`/`pos`, `upper` from
    /// `prefix_successor`), optionally `is_latest = 1`, ordered for the
    /// operation. Returns `(key, rowid, metadata)` triples.
    async fn list_batch(
        &self,
        bucket: &str,
        prefix: &str,
        pos: Option<&Pos>,
        upper: Option<&str>,
        latest_only: bool,
        limit: i64,
    ) -> Result<Vec<(String, i64, Metadata)>, MetadataError> {
        // Lower bound: the greater of the prefix and any cursor key.
        let lower = match pos {
            None => prefix.to_string(),
            Some(Pos::AtKey(k)) | Some(Pos::AfterRow { key: k, .. }) => {
                if k.as_str() > prefix { k.clone() } else { prefix.to_string() }
            }
        };

        let mut sql = String::from(
            "SELECT * FROM object_metadata WHERE bucket = ? AND key >= ?",
        );
        if upper.is_some() {
            sql.push_str(" AND key < ?");
        }
        match pos {
            Some(Pos::AfterRow { .. }) if latest_only => sql.push_str(" AND key > ?"),
            Some(Pos::AfterRow { .. }) => {
                sql.push_str(" AND (key > ? OR (key = ? AND id < ?))")
            }
            _ => {}
        }
        if latest_only {
            sql.push_str(" AND is_latest = 1 ORDER BY key LIMIT ?");
        } else {
            sql.push_str(" ORDER BY key, id DESC LIMIT ?");
        }

        let mut query = sqlx::query(&sql).bind(bucket).bind(lower);
        if let Some(upper) = upper {
            query = query.bind(upper.to_string());
        }
        match pos {
            Some(Pos::AfterRow { key, .. }) if latest_only => {
                query = query.bind(key.clone());
            }
            Some(Pos::AfterRow { key, id }) => {
                query = query.bind(key.clone()).bind(key.clone()).bind(*id);
            }
            _ => {}
        }
        let rows = query
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(MetadataError::Backend)?;

        rows.iter()
            .map(|row| {
                let key: String = row.try_get("key").map_err(MetadataError::Backend)?;
                let id: i64 = row.try_get("id").map_err(MetadataError::Backend)?;
                Ok((key, id, row_to_metadata(row)?))
            })
            .collect()
    }

    /// The shared paging loop behind `list` and `list_versions`.
    async fn list_page(
        &self,
        bucket: &str,
        params: ListParams<'_>,
        latest_only: bool,
    ) -> Result<ListPage, MetadataError> {
        let prefix = params.prefix.unwrap_or("");
        let upper = prefix_successor(prefix);
        let mut pos = params.cursor.map(Pos::decode).transpose()?;
        // `max_keys` of 0 is a caller-contract violation (documented on
        // `ListParams`); treat it as 1 rather than panic on the empty `pos`.
        let max_keys = params.max_keys.max(1);
        let limit = max_keys.min(LIST_BATCH_CAP) as i64;

        let mut items: Vec<Metadata> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();

        loop {
            let batch = self
                .list_batch(bucket, prefix, pos.as_ref(), upper.as_deref(), latest_only, limit)
                .await?;
            let batch_len = batch.len();
            let mut rows = batch.into_iter().peekable();

            while let Some((key, id, metadata)) = rows.next() {
                if items.len() + common_prefixes.len() == max_keys {
                    // `key` is a real row that would extend the result, so the
                    // page is truncated. `pos` already points just past the
                    // last emitted output.
                    let cursor = pos.as_ref().expect("pos is set once an output exists");
                    return Ok(ListPage {
                        items,
                        common_prefixes,
                        next_cursor: Some(cursor.encode()),
                    });
                }

                match params.delimiter.and_then(|d| delimiter_group(&key, prefix, d)) {
                    Some(cp) => {
                        common_prefixes.push(cp.clone());
                        match prefix_successor(&cp) {
                            Some(succ) => {
                                // Skip the rest of this group still in the batch.
                                while rows.peek().is_some_and(|(k, _, _)| k < &succ) {
                                    rows.next();
                                }
                                pos = Some(Pos::AtKey(succ));
                            }
                            None => {
                                // Nothing can sort after this group.
                                return Ok(ListPage {
                                    items,
                                    common_prefixes,
                                    next_cursor: None,
                                });
                            }
                        }
                    }
                    None => {
                        items.push(metadata);
                        pos = Some(Pos::AfterRow { key, id });
                    }
                }
            }

            // A short batch means the range is exhausted.
            if (batch_len as i64) < limit {
                return Ok(ListPage { items, common_prefixes, next_cursor: None });
            }
        }
    }
}
```

- [ ] **Step 4: Replace the `list` / `list_versions` trait bodies**

In `src/metadata/sqlite/mod.rs`, in `impl MetadataStore for
SqliteMetadataStore`, replace both method bodies:

```rust
    async fn list(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError> {
        self.list_page(bucket, params, true).await
    }

    async fn list_versions(
        &self,
        bucket: &str,
        params: ListParams<'_>,
    ) -> Result<ListPage, MetadataError> {
        self.list_page(bucket, params, false).await
    }
```

- [ ] **Step 5: Delete `like_prefix_pattern`**

In `src/metadata/sqlite/mod.rs`, delete the `like_prefix_pattern` function and
its `///` doc comment (it has no callers now).

- [ ] **Step 6: Add `ListParams` / `ListPage` to the sqlite imports**

In `src/metadata/sqlite/mod.rs`, extend the `use crate::metadata::{...}` list
to include `ListPage, ListParams`.

- [ ] **Step 7: Build the library**

Run: `cargo build --lib`
Expected: PASS (`conformance.rs` is a test module, not part of `--lib`; it is
fixed in the next steps).

- [ ] **Step 8: Add the `collect_all` helper to the conformance suite**

In `src/metadata/conformance.rs`, replace the `versions_for_key` helper (and
add `collect_all` above it):

```rust
use crate::metadata::{ListPage, ListParams};

/// Drains every page of a list operation into flat vectors. Requires
/// `page_size >= 1`.
async fn collect_all(
    store: &impl MetadataStore,
    bucket: &str,
    versions: bool,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page_size: usize,
) -> (Vec<Metadata>, Vec<String>) {
    assert!(page_size >= 1, "collect_all needs a positive page size");
    let mut items = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let params = ListParams {
            prefix,
            delimiter,
            cursor: cursor.as_deref(),
            max_keys: page_size,
        };
        let page: ListPage = if versions {
            store.list_versions(bucket, params).await
        } else {
            store.list(bucket, params).await
        }
        .expect("list should succeed");

        items.extend(page.items);
        common_prefixes.extend(page.common_prefixes);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    (items, common_prefixes)
}

/// Every stored version for one key, newest-first, via the public API.
async fn versions_for_key(store: &impl MetadataStore, bucket: &str, key: &str) -> Vec<Metadata> {
    let (mut versions, _) = collect_all(store, bucket, true, None, None, 1000).await;
    versions.retain(|m| m.key == key);
    versions
}
```

(Place the `use crate::metadata::{ListPage, ListParams};` line with the other
`use` statements at the top of the file, or merge the names into the existing
`use crate::metadata::{...}` block.)

- [ ] **Step 9: Migrate `list_returns_latest_rows_for_a_bucket`**

In `src/metadata/conformance.rs`, replace the assertion block of that function
(the `let listed = store.list("b", None)...` line and the two asserts after it):

```rust
    let (listed, _) = collect_all(&store, "b", false, None, None, 2).await;
    assert_eq!(listed.len(), 2);
    let versions: Vec<_> = listed.iter().map(|m| m.version.0.as_str()).collect();
    assert_eq!(versions, vec!["v2", "v1"]);
```

- [ ] **Step 10: Migrate `list_filters_by_prefix`**

Replace its `let listed = store.list("b", Some("docs/"))...` block:

```rust
    let (listed, _) = collect_all(&store, "b", false, Some("docs/"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["docs/a", "docs/b"]);
```

- [ ] **Step 11: Migrate `list_prefix_does_not_treat_percent_or_underscore_as_wildcards`**

Replace its `let listed = store.list("b", Some("100%"))...` block:

```rust
    let (listed, _) = collect_all(&store, "b", false, Some("100%"), None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["100%_off"]);
```

- [ ] **Step 12: Migrate `list_versions_returns_every_version_including_markers`**

Replace its `let versions = store.list_versions("b", None)...` block:

```rust
    let (versions, _) = collect_all(&store, "b", true, None, None, 1000).await;
    let version_ids: Vec<_> = versions.iter().map(|m| m.version.0.as_str()).collect();
    // Newest-first within a key, matching S3's ListObjectVersions.
    assert_eq!(version_ids, vec!["marker1", "v1"]);
    assert!(versions.iter().any(|m| m.delete_marker));
```

- [ ] **Step 13: Migrate the `list` call in `put_unversioned_demotes_existing_versioned_latest_rows`**

Replace its `let listed = store.list("b", None)...` block:

```rust
    let (listed, _) = collect_all(&store, "b", false, None, None, 1000).await;
    let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k"]);
```

- [ ] **Step 14: Run the full conformance suite**

Run: `cargo test --lib metadata::`
Expected: PASS — every existing case green under the new API (43 tests).

- [ ] **Step 15: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass. If clippy flags `list_batch`'s dynamic SQL string
(`clippy::useless_format` on the `format!` in `delimiter_group`, or similar),
address it minimally; the `format!("{prefix}{}", ...)` is intentional.

- [ ] **Step 16: Commit**

```bash
git add src/metadata/mod.rs src/metadata/sqlite/mod.rs src/metadata/conformance.rs
git commit -m "feat(metadata): paged, delimiter-aware list / list_versions"
```

---

## Task 6: Query-plan assertions

**Files:**
- Modify: `src/metadata/sqlite/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: `SqliteMetadataStore::connect_in_memory`, `Pos` (Task 3).
- Produces: nothing (tests only).

- [ ] **Step 1: Write the failing tests**

In `src/metadata/sqlite/mod.rs`, inside `mod tests`:

```rust
    async fn query_plan(store: &SqliteMetadataStore, sql: &str) -> String {
        let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
            .fetch_all(&store.pool)
            .await
            .expect("EXPLAIN QUERY PLAN should run");
        rows.iter()
            .map(|r| r.try_get::<String, _>("detail").expect("detail column"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[tokio::test]
    async fn list_batch_query_plan_uses_the_partial_index() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let plan = query_plan(
            &store,
            "SELECT * FROM object_metadata WHERE bucket = 'b' AND key >= 'p/' \
             AND key < 'p0' AND is_latest = 1 ORDER BY key LIMIT 100",
        )
        .await;
        // SEARCH = a bounded index probe; a full table scan reads "SCAN
        // object_metadata" with no "USING INDEX".
        assert!(plan.contains("SEARCH"), "expected an index SEARCH, got:\n{plan}");
        assert!(
            plan.contains("USING INDEX idx_object_metadata_one_latest"),
            "expected the partial index, got:\n{plan}",
        );
    }

    #[tokio::test]
    async fn list_versions_batch_query_plan_uses_an_index() {
        let store = SqliteMetadataStore::connect_in_memory().await;
        let plan = query_plan(
            &store,
            "SELECT * FROM object_metadata WHERE bucket = 'b' AND key >= 'p/' \
             AND key < 'p0' ORDER BY key, id DESC LIMIT 100",
        )
        .await;
        assert!(plan.contains("SEARCH"), "expected an index SEARCH, got:\n{plan}");
        assert!(
            plan.contains("USING INDEX idx_object_metadata_bucket_key_is_latest"),
            "expected the bucket/key index, got:\n{plan}",
        );
    }
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib metadata::sqlite::tests::list_batch_query_plan metadata::sqlite::tests::list_versions_batch_query_plan`
Expected: PASS. (SQLite writes `SEARCH ... USING INDEX <name>` for a bounded
index probe; a full scan is `SCAN object_metadata` with no `USING INDEX`. If a
test fails, the panic prints the plan — compare against the spec's "SQLite
implementation / Query" section and fix the query, do not weaken the
assertion.)

- [ ] **Step 3: Commit**

```bash
git add src/metadata/sqlite/mod.rs
git commit -m "test(metadata/sqlite): lock in the list query plans"
```

---

## Task 7: Conformance — pagination, truncation, empty result

**Files:**
- Modify: `src/metadata/conformance.rs` (3 new `pub(crate) async fn`s + 3
  `case!` lines in the `metadata_store_conformance!` macro)

**Interfaces:**
- Consumes: `collect_all` (Task 5), `sample_metadata`, `ListParams`,
  `ListPage`, `MetadataError` (Task 1).
- Produces: 3 conformance cases run against every backend.

- [ ] **Step 1: Write the three cases**

In `src/metadata/conformance.rs`, after `list_versions_returns_every_version_including_markers`:

```rust
pub(crate) async fn list_paginates_and_reports_truncation(store: impl MetadataStore) {
    for key in ["k1", "k2", "k3", "k4", "k5"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = store
            .list(
                "b",
                ListParams { prefix: None, delimiter: None, cursor: cursor.as_deref(), max_keys: 2 },
            )
            .await
            .expect("list should succeed");
        pages += 1;
        assert!(page.items.len() <= 2, "page over max_keys");
        seen.extend(page.items.iter().map(|m| m.key.clone()));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }

    assert_eq!(pages, 3, "5 keys at page size 2 is three pages");
    assert_eq!(seen, vec!["k1", "k2", "k3", "k4", "k5"]);
}

pub(crate) async fn list_final_exact_page_has_no_next_cursor(store: impl MetadataStore) {
    for key in ["k1", "k2", "k3", "k4"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list("b", ListParams { prefix: None, delimiter: None, cursor: None, max_keys: 2 })
        .await
        .expect("list should succeed");
    let cursor = page1.next_cursor.expect("first page of four is truncated");

    let page2 = store
        .list(
            "b",
            ListParams { prefix: None, delimiter: None, cursor: Some(&cursor), max_keys: 2 },
        )
        .await
        .expect("list should succeed");
    let keys: Vec<_> = page2.items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["k3", "k4"]);
    assert_eq!(page2.next_cursor, None, "the exact final page is not truncated");
}

pub(crate) async fn list_of_an_empty_bucket_is_an_empty_page(store: impl MetadataStore) {
    let page = store
        .list(
            "no-such-bucket",
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 100 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page.items, Vec::new());
    assert_eq!(page.common_prefixes, Vec::<String>::new());
    assert_eq!(page.next_cursor, None);
}
```

- [ ] **Step 2: Register the cases in the macro**

In `src/metadata/conformance.rs`, in the `metadata_store_conformance!` macro
body, after the `list_versions_returns_every_version_including_markers` line:

```rust
            case!($make_store, list_paginates_and_reports_truncation);
            case!($make_store, list_final_exact_page_has_no_next_cursor);
            case!($make_store, list_of_an_empty_bucket_is_an_empty_page);
```

- [ ] **Step 3: Run the new cases**

Run: `cargo test --lib metadata::sqlite::tests::conformance::list_paginates metadata::sqlite::tests::conformance::list_final_exact metadata::sqlite::tests::conformance::list_of_an_empty`
Expected: PASS (3 tests).

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): conformance for list pagination and truncation"
```

---

## Task 8: Conformance — delimiter grouping

**Files:**
- Modify: `src/metadata/conformance.rs` (3 new `pub(crate) async fn`s + 3
  `case!` lines)

**Interfaces:**
- Consumes: `collect_all`, `sample_metadata`, `ListParams`.
- Produces: 3 conformance cases.

- [ ] **Step 1: Write the three cases**

In `src/metadata/conformance.rs`, after `list_of_an_empty_bucket_is_an_empty_page`:

```rust
pub(crate) async fn list_groups_keys_under_a_delimiter(store: impl MetadataStore) {
    for key in ["a", "p/1", "p/2", "q/1", "z"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, "b", false, None, Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "z"]);
    assert_eq!(common_prefixes, vec!["p/".to_string(), "q/".to_string()]);
}

pub(crate) async fn list_delimiter_respects_prefix(store: impl MetadataStore) {
    for key in ["p/x", "p/sub/a", "p/sub/b"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let (items, common_prefixes) =
        collect_all(&store, "b", false, Some("p/"), Some("/"), 1000).await;
    let keys: Vec<_> = items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["p/x"]);
    assert_eq!(common_prefixes, vec!["p/sub/".to_string()]);
}

pub(crate) async fn list_delimiter_page_ends_on_a_common_prefix(store: impl MetadataStore) {
    for key in ["g/1", "g/2", "g/3", "g/4", "z"] {
        store
            .put_versioned(sample_metadata("b", key, "v1"))
            .await
            .expect("put should succeed");
    }

    let page1 = store
        .list(
            "b",
            ListParams { prefix: None, delimiter: Some("/"), cursor: None, max_keys: 1 },
        )
        .await
        .expect("list should succeed");
    assert_eq!(page1.items, Vec::new());
    assert_eq!(page1.common_prefixes, vec!["g/".to_string()]);
    let cursor = page1.next_cursor.expect("more remains after the group");

    let page2 = store
        .list(
            "b",
            ListParams {
                prefix: None,
                delimiter: Some("/"),
                cursor: Some(&cursor),
                max_keys: 10,
            },
        )
        .await
        .expect("list should succeed");
    let keys: Vec<_> = page2.items.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["z"], "resumes past the whole group");
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.next_cursor, None);
}
```

- [ ] **Step 2: Register the cases in the macro**

After the Task 7 lines:

```rust
            case!($make_store, list_groups_keys_under_a_delimiter);
            case!($make_store, list_delimiter_respects_prefix);
            case!($make_store, list_delimiter_page_ends_on_a_common_prefix);
```

- [ ] **Step 3: Run the new cases**

Run: `cargo test --lib metadata::sqlite::tests::conformance::list_groups metadata::sqlite::tests::conformance::list_delimiter`
Expected: PASS (3 tests).

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): conformance for delimiter grouping"
```

---

## Task 9: Conformance — malformed cursor, versioned pagination

**Files:**
- Modify: `src/metadata/conformance.rs` (2 new `pub(crate) async fn`s + 2
  `case!` lines)

**Interfaces:**
- Consumes: `collect_all`, `sample_metadata`, `ListParams`, `MetadataError`.
- Produces: 2 conformance cases.

- [ ] **Step 1: Write the two cases**

In `src/metadata/conformance.rs`, after `list_delimiter_page_ends_on_a_common_prefix`:

```rust
pub(crate) async fn list_rejects_a_malformed_cursor(store: impl MetadataStore) {
    let err = store
        .list(
            "b",
            ListParams {
                prefix: None,
                delimiter: None,
                cursor: Some("!!!not-base64!!!"),
                max_keys: 10,
            },
        )
        .await
        .expect_err("a garbled cursor should be rejected");
    assert!(
        matches!(err, MetadataError::InvalidCursor { .. }),
        "unexpected error: {err:?}"
    );
}

pub(crate) async fn list_versions_paginates_across_keys_and_versions(store: impl MetadataStore) {
    // k1 gets three versions, k2 gets two.
    for version in ["v1", "v2", "v3"] {
        store
            .put_versioned(sample_metadata("b", "k1", version))
            .await
            .expect("put should succeed");
    }
    for version in ["v1", "v2"] {
        store
            .put_versioned(sample_metadata("b", "k2", version))
            .await
            .expect("put should succeed");
    }

    let (paged, _) = collect_all(&store, "b", true, None, None, 2).await;
    let (single, _) = collect_all(&store, "b", true, None, None, 1000).await;

    let ids = |rows: &[Metadata]| -> Vec<(String, String)> {
        rows.iter().map(|m| (m.key.clone(), m.version.0.clone())).collect()
    };
    assert_eq!(ids(&paged), ids(&single), "paging must not reorder or drop rows");
    assert_eq!(
        ids(&single),
        vec![
            ("k1".to_string(), "v3".to_string()),
            ("k1".to_string(), "v2".to_string()),
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("k2".to_string(), "v1".to_string()),
        ],
    );
}
```

- [ ] **Step 2: Register the cases in the macro**

After the Task 8 lines:

```rust
            case!($make_store, list_rejects_a_malformed_cursor);
            case!($make_store, list_versions_paginates_across_keys_and_versions);
```

- [ ] **Step 3: Run the new cases**

Run: `cargo test --lib metadata::sqlite::tests::conformance::list_rejects_a_malformed_cursor metadata::sqlite::tests::conformance::list_versions_paginates`
Expected: PASS (2 tests).

- [ ] **Step 4: Full check**

Run: `cargo test && cargo clippy --all-targets && cargo build`
Expected: all pass. Total metadata test count: 43 (Task 5 baseline) + 8 new
conformance cases + query-plan + helper unit tests.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/conformance.rs
git commit -m "test(metadata): conformance for cursor rejection and versioned paging"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| `ListParams` / `ListPage` types | 1 |
| `MetadataError::InvalidCursor` + Display | 1 |
| `list` / `list_versions` trait signatures | 5 |
| Case-sensitive, byte-ordered matching | 5 (range bounds, no `LIKE`) |
| Ordering (`list` latest-by-key; `list_versions` newest-version-first) | 5 (`list_batch` `ORDER BY`) |
| Prefix as range bounds, not `LIKE`; `like_prefix_pattern` deleted | 5 |
| Delimiter / common-prefix grouping | 4 (`delimiter_group`), 5 (loop), 8 (tests) |
| `max_keys` caps items + common_prefixes combined | 5 (loop check), 7 (test) |
| Opaque base64 tagged-frame cursor | 3 |
| `prefix_successor` (char-based) | 2 |
| SQLite batch query + `EXPLAIN QUERY PLAN` | 5 (`list_batch`), 6 (tests) |
| Paging loop, group-skip, truncation probe | 5 |
| `collect_all` helper; migrate 5 existing cases; `versions_for_key` reimpl | 5 |
| 8 new conformance cases | 7, 8, 9 |
| `prefix_successor` / cursor codec unit tests | 2, 3 |
| Error handling (`InvalidCursor`, `Backend`, `Corrupt` propagation) | 1, 3, 5 |

The spec text mentions migrating `repeated_put_unversioned_keeps_the_sentinel_row_latest`;
that function does not call `list` / `list_versions` (verified — it uses only
`get`), so it needs no change. No other gaps.

**Placeholder scan:** No `TBD` / "handle edge cases" / bare "write tests" —
every code step carries the actual code.

**Type consistency:**
- `Pos::AfterRow { key: String, id: i64 }` — same field names in Task 3
  (definition + tests), Task 5 (`list_batch` match arms, `list_page`
  construction).
- `collect_all(store, bucket, versions, prefix, delimiter, page_size)` — same
  argument order in the Task 5 definition and every call in Tasks 5, 7, 8, 9.
- `list_batch` returns `Vec<(String, i64, Metadata)>`; `list_page` destructures
  `(key, id, metadata)` — consistent.
- `ListParams` fields `{ prefix, delimiter, cursor, max_keys }` — same in Task
  1 definition and all struct literals in Tasks 5, 7, 8, 9.
- `prefix_successor(&str) -> Option<String>` — Task 2 definition, used
  `.as_deref()` / by value in Task 5.

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-09-01-metadata-list-pagination.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
