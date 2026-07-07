/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TagIndex`] is an index that indexes textual tags for documents. The "Disk mode"
//! (`diskSpec`) is forwarded to another implementation.
//!
//! [`TagIndex`] uses the same indexes as the full text but in a simpler manner. In fact:
//!
//! 1. An entire tag index resides in a single redis key, and doesn't have a key per term
//!
//! 2. We do not perform stemming on tags
//!
//! 3. The tokenization is simpler: The user can determine a separator (default to comma `,`),
//!    and we do whitespace trimming at the end of tags. Thus, tags can contain spaces (in the middle),
//!    punctuation marks, accents, etc. The only two transformations we perform are
//!    lower-casing (not unicode sensitive as of now), and whitespace trimming.
//!
//! 4. Tags cannot be found from a general full-text search. i.e. if a document has a field called "tags"
//!    with the values "foo" and "bar", searching for "foo" or "bar" without a special
//!    tag modifier (see below) will not return the document.
//!
//! 5. The index is much simpler and more compressed: We do not store frequencies, offset vectors of field flags.
//!    The index contains only document ids encoded as delta. This means that an entry in a tag index is usually one or two bytes long.
//!    This makes them very memory efficient and fast.
//!
//!
//! ## Creating a tag field
//!
//! Tag fields can be added to the schema in `FT.CREATE` with the following syntax:
//! ```text
//! FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}]
//! ```
//! `SEPARATOR` defaults to a comma (`,`), and can be any printable ascii character.  For example:
//! ```text
//! FT.CREATE idx SCHEMA tags TAG SEPARATOR ";"
//! ```
//!
//! An unlimited number of tag fields can be created per document, as long as the overall number of
//! fields is under 1024.
//!
//! ### With suffix
//!
//! [`TagIndex`] supports also suffix search if enabled during the creation as follow:
//! ```text
//! FT.CREATE idx SCHEMA tags TAG WITHSUFFIXTRIE
//! ```
//! In this case, also the suffix queries use index.
//!
//! NB: the suffix queries work even without `WITHSUFFIXTRIE`, but they will fallback to a brute force search.
//!
//! ## Querying Tag Fields
//!
//! As mentioned above, just searching for a tag without any modifiers will not retrieve documents
//! containing it.
//! The syntax for matching tags in a query is as follows (the curly braces are part of the syntax in
//! this case):
//! ```text
//! @<field_name>:{ <tag> | <tag> | ...}
//! ```
//!  e.g.
//! ```text
//! @tags:{hello world | foo bar}
//! ```
//!  **IMPORTANT**: When specifying multiple tags in the same tag clause, the semantic meaning is a
//!    **UNION** of the documents containing any of the tags (as in an SQL `WHERE IN` clause).
//!    If you need to intersect tags, you should repeat several tag clauses.
//!    For example:
//! ```text
//! FT.SEARCH idx "@tags:{hello | world}"
//! ```
//! Will return documents containing either hello or world (or both). But:
//! ```text
//! FT.SEARCH idx "@tags:{hello} @tags:{world}"
//! ```
//! Will return documents containing **both tags**.
//!
//! Notice that since tags can contain spaces (the separator by default is a comma), so can tags in
//! the query.
//!
//! However, if a tag contains stopwords (for example, the tag `to be or not to be` will cause a
//! syntax error),
//! you can alternatively escape the spaces inside the tags to avoid syntax errors. In redis-cli and
//! some clients, a second escaping is needed:
//!
//! ```text
//! 127.0.0.1:6379> FT.SEARCH idx "@tags:{to\\ be\\ or\\ not\\ to\\ be}"
//! ```
//!

// Temporary
#![expect(dead_code, reason = "read by methods added in the follow-up change")]

mod suffix;

use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly};
pub use suffix::TagSuffixIndex;
use trie_rs::TrieMap;

/// See the [crate documentation](self) for an overview.
pub struct TagIndex {
    /// Unique id generated at creation time.
    unique_id: u32,

    /// tag value → postings. Tag postings only need document ids, so the
    /// inverted indexes always use the [`DocIdsOnly`] encoding.
    values: TrieMap<InvertedIndex<DocIdsOnly>>,

    /// Suffix index, present only for fields created `WITHSUFFIXTRIE`.
    suffix: Option<TagSuffixIndex>,
}
