/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::RSQueryNode`].

use std::{ffi::c_char, ops::Bound, ptr::NonNull};

use inverted_index::NumericFilter;
use query_node_type::{QueryNodeOptions, QueryNodeType};
use rqe_core::DocId;

/// The wildcard expansion mode for a [`QueryNode::Prefix`] node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WildcardMode {
    /// Prefix match only (e.g. `hel*`).
    Prefix,
    /// Suffix match only (e.g. `*llo`).
    Suffix,
    /// Contains match — both prefix and suffix (e.g. `*ell*`).
    Contains,
}

/// Typed view of a query-node's per-type payload.
///
/// Produced by [`QueryNodeRef::as_enum`].  Each variant that carries a
/// type-specific payload exposes the fields of the corresponding C union
/// member directly.  Variants whose C structs contain only a dummy field
/// (`Union`, `Not`, `Optional`, `Wildcard`, `Null`) are unit variants —
/// their semantics come entirely from the shared header and child list.
///
/// Fields that embed an [`ffi::RSToken`] are exposed as references because
/// the token struct contains bitfields that cannot be cleanly inlined.
pub enum QueryNode<'a> {
    /// A list of child nodes with intersection semantics, or a literal phrase
    /// when the children are token nodes.
    Phrase {
        /// Whether the phrase requires an exact match (quoted string).
        exact: bool,
    },
    /// Logical OR over its children — matches any document matched by at
    /// least one child.  Synonym expansions are represented as unions of
    /// token nodes.
    Union,
    /// A terminal, single-term node.
    Token {
        /// The token string, length, and associated metadata (stemming,
        /// phonetic flags, etc.).
        tok: &'a ffi::RSToken,
    },
    /// A numeric range filter.
    Numeric {
        /// The numeric filter describing the range bounds and target field.
        nf: &'a NumericFilter,
    },
    /// Logical negation — excludes documents matched by its single child.
    Not,
    /// Optional match — boosts score when the child matches but does not
    /// exclude non-matching documents.
    Optional,
    /// A geographic radius filter.
    Geo {
        /// The geo filter describing the centre point, radius, and unit.
        gf: *mut ffi::GeoFilter,
    },
    /// A geometry (WKT) containment/intersection filter.
    Geometry {
        /// The geometry query describing the shape and spatial predicate.
        geomq: *mut ffi::GeometryQuery,
    },
    /// A prefix (and/or suffix) wildcard expansion node.
    Prefix {
        /// The base token to expand.
        tok: &'a ffi::RSToken,
        /// The wildcard mode for this prefix node.
        mode: WildcardMode,
    },
    /// A filter by explicit document key names.
    Ids {
        /// SDS key strings to match against.
        keys: &'a [ffi::sds],
        /// Pre-resolved document IDs (resolved on the main thread for
        /// search-on-disk).  `None` when not in disk mode.
        doc_ids: Option<&'a [DocId]>,
    },
    /// Matches every document in the index (the `*` query).
    Wildcard,
    /// A tag-field exact-match node.
    Tag {
        /// The [`ffi::FieldSpec`] of the tag field being queried.
        fs: &'a ffi::FieldSpec,
    },
    /// A fuzzy (Levenshtein distance) match node.
    Fuzzy {
        /// The base token to fuzzy-match.
        tok: &'a ffi::RSToken,
        /// Maximum edit distance (1, 2, or 3).
        max_dist: i32,
    },
    /// A lexicographic range query on a tag or text field.
    LexRange {
        /// Start of the range, or [`Bound::Unbounded`] for no lower limit.
        begin: Bound<*const c_char>,
        /// End of the range, or [`Bound::Unbounded`] for no upper limit.
        end: Bound<*const c_char>,
    },
    /// A vector similarity search node.
    Vector {
        /// The vector query parameters (field, blob, algorithm, K, etc.).
        vq: *mut ffi::VectorQuery,
    },
    /// A verbatim wildcard-pattern query (e.g. `w'hel*o'`).
    WildcardQuery {
        /// The raw pattern token.
        tok: &'a ffi::RSToken,
    },
    /// A null/no-op node produced when the query contains only stopwords.
    Null,
    /// An `ismissing(@field)` predicate — matches documents where the field
    /// has no value.
    Missing {
        /// The [`ffi::FieldSpec`] of the field being tested.
        field: &'a ffi::FieldSpec,
    },
}

/// Safe read-only wrapper around a [`ffi::RSQueryNode`] pointer.
///
/// [`ffi::RSQueryNode`] is a tagged union: a shared header (`type_`, `opts`,
/// `children`, `params`) followed by a per-type payload accessed through the
/// anonymous union.  This wrapper exposes the common header fields directly
/// and provides typed accessors for each union variant, gated by the node
/// type.
///
/// # Ownership
///
/// `QueryNodeRef` does **not** own the node — it is a borrowed view.
/// The underlying [`ffi::RSQueryNode`] (and its children) are owned by the
/// C `QueryAST` and freed when the AST is dropped.
pub struct QueryNodeRef(NonNull<ffi::RSQueryNode>);

impl QueryNodeRef {
    /// Wrap a raw [`NonNull`] pointer to a [`ffi::RSQueryNode`].
    ///
    /// # Safety
    ///
    /// 1. `ptr` must point to a [valid], properly initialised
    ///    [`ffi::RSQueryNode`].
    /// 2. The caller must have shared access to the pointee for the lifetime
    ///    of the returned [`QueryNodeRef`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn new(ptr: NonNull<ffi::RSQueryNode>) -> Self {
        Self(ptr)
    }

    /// Shared reference to the underlying [`ffi::RSQueryNode`].
    const fn as_ref(&self) -> &ffi::RSQueryNode {
        // SAFETY: invariant (1) of `new`.
        unsafe { self.0.as_ref() }
    }

    /// The discriminant that selects which union variant is active.
    pub const fn node_type(&self) -> QueryNodeType {
        self.as_ref().type_
    }

    /// The shared options header (field mask, weight, slop, flags, …).
    pub const fn opts(&self) -> &QueryNodeOptions {
        &self.as_ref().opts
    }

    /// The number of child nodes.
    ///
    /// Returns 0 when the `children` pointer is null (leaf nodes).
    pub fn num_children(&self) -> usize {
        let children = self.as_ref().children;
        if children.is_null() {
            return 0;
        }
        // SAFETY: `children` is a non-null `array_*`-managed pointer.
        unsafe { ffi::array_len_func(children.cast()) as usize }
    }

    /// Return the child at `index` as a new [`QueryNodeRef`].
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.num_children()`.
    pub fn child(&self, index: usize) -> QueryNodeRef {
        let n = self.num_children();
        assert!(index < n, "index {index} out of bounds (num_children: {n})");
        let children = self.as_ref().children;
        // SAFETY: `children` is non-null (num_children > 0) and `index <
        // num_children`, so `add` stays within the `array_*` allocation.
        let slot = unsafe { children.add(index) };
        // SAFETY: the slot is within bounds; dereference yields a valid
        // child pointer.
        let child_ptr = unsafe { *slot };
        // SAFETY: all children in the AST are valid, non-null nodes
        // (invariant 1 of `new`).
        let child_nn = unsafe { NonNull::new_unchecked(child_ptr) };
        // SAFETY: invariant (1) of `new` — child nodes in the AST are
        // properly initialised.
        unsafe { QueryNodeRef::new(child_nn) }
    }

    /// Iterator over all children as [`QueryNodeRef`]s.
    pub fn children(&self) -> impl Iterator<Item = QueryNodeRef> + '_ {
        (0..self.num_children()).map(|i| self.child(i))
    }

    /// Convert the C tagged union into a [`QueryNode`] enum.
    pub fn as_enum(&self) -> QueryNode<'_> {
        let inner = self.as_ref();
        // Each arm has its own `unsafe` block so that every union access
        // is individually justified, satisfying `multiple_unsafe_ops_per_block`.
        //
        // SAFETY (all arms): the caller (transitively, invariant 1 of `new`)
        // guarantees the node is properly initialised, so `type_` matches
        // the active union variant.
        // Obtain a raw pointer to the union once; each arm casts it to the
        // active variant type.  This avoids `__BindgenUnionField::as_ref()`
        // which uses `transmute` from a ZST reference — UB under Stacked
        // Borrows (and flagged by miri).
        let union_ptr = &raw const inner.__bindgen_anon_1;

        // SAFETY (all arms): the caller (transitively, invariant 1 of `new`)
        // guarantees the node is properly initialised, so `type_` matches the
        // active union variant and the cast is sound.
        match inner.type_ {
            QueryNodeType::Phrase => {
                // SAFETY: `type_` is `Phrase`, so the union holds a `QueryPhraseNode`.
                let pn = unsafe { &*union_ptr.cast::<ffi::QueryPhraseNode>() };
                QueryNode::Phrase {
                    exact: pn.exact != 0,
                }
            }
            QueryNodeType::Union => QueryNode::Union,
            QueryNodeType::Token => QueryNode::Token {
                // SAFETY: `type_` is `Token`, so the union holds an `RSToken`.
                tok: unsafe { &*union_ptr.cast::<ffi::RSToken>() },
            },
            QueryNodeType::Numeric => {
                // SAFETY: `type_` is `Numeric`, so the union holds a `QueryNumericNode`.
                // `nn.nf` is always non-null for a well-formed numeric node
                // (set by `NewNumericNode` or the search-options path).
                // `ffi::NumericFilter` and `NumericFilter` have identical
                // `#[repr(C)]` layout (the former is cheadergen output of the
                // latter).
                let nn = unsafe { &*union_ptr.cast::<ffi::QueryNumericNode>() };
                QueryNode::Numeric {
                    // SAFETY: `nn.nf` points to a valid `NumericFilter` with matching `#[repr(C)]` layout.
                    nf: unsafe { &*nn.nf.cast::<NumericFilter>() },
                }
            }
            QueryNodeType::Not => QueryNode::Not,
            QueryNodeType::Optional => QueryNode::Optional,
            QueryNodeType::Geo => {
                // SAFETY: `type_` is `Geo`, so the union holds a `QueryGeofilterNode`.
                let gn = unsafe { &*union_ptr.cast::<ffi::QueryGeofilterNode>() };
                QueryNode::Geo { gf: gn.gf }
            }
            QueryNodeType::Geometry => {
                // SAFETY: `type_` is `Geometry`, so the union holds a `QueryGeometryNode`.
                let gmn = unsafe { &*union_ptr.cast::<ffi::QueryGeometryNode>() };
                QueryNode::Geometry { geomq: gmn.geomq }
            }
            QueryNodeType::Prefix => {
                // SAFETY: `type_` is `Prefix`, so the union holds a `QueryPrefixNode`.
                let pfx = unsafe { &*union_ptr.cast::<ffi::QueryPrefixNode>() };
                QueryNode::Prefix {
                    tok: &pfx.tok,
                    mode: match (pfx.prefix, pfx.suffix) {
                        (true, false) => WildcardMode::Prefix,
                        (false, true) => WildcardMode::Suffix,
                        (true, true) => WildcardMode::Contains,
                        (false, false) => {
                            unreachable!("QN_PREFIX node with neither prefix nor suffix set")
                        }
                    },
                }
            }
            QueryNodeType::Ids => {
                // SAFETY: `type_` is `Ids`, so the union holds a `QueryIdFilterNode`.
                let fn_ = unsafe { &*union_ptr.cast::<ffi::QueryIdFilterNode>() };
                let len = fn_.len;
                QueryNode::Ids {
                    keys: if fn_.keys.is_null() {
                        &[]
                    } else {
                        // SAFETY: invariant (1) of `new` guarantees `keys` is a
                        // valid pointer to `len` SDS strings when non-null.
                        unsafe { std::slice::from_raw_parts(fn_.keys, len) }
                    },
                    doc_ids: if fn_.docIds.is_null() {
                        None
                    } else {
                        // SAFETY: invariant (1) of `new` guarantees `docIds`,
                        // when non-null, points to `len` valid `DocId` values.
                        Some(unsafe { std::slice::from_raw_parts(fn_.docIds, len) })
                    },
                }
            }
            QueryNodeType::Wildcard => QueryNode::Wildcard,
            QueryNodeType::Tag => {
                // SAFETY: `type_` is `Tag`, so the union holds a `QueryTagNode`.
                // Invariant (1) of `new` guarantees `tag.fs` is a valid,
                // non-null pointer.
                let tag = unsafe { &*union_ptr.cast::<ffi::QueryTagNode>() };
                QueryNode::Tag {
                    // SAFETY: Invariant (1) of `new` guarantees `tag.fs` is valid and non-null.
                    fs: unsafe { &*tag.fs },
                }
            }
            QueryNodeType::Fuzzy => {
                // SAFETY: `type_` is `Fuzzy`, so the union holds a `QueryFuzzyNode`.
                let fz = unsafe { &*union_ptr.cast::<ffi::QueryFuzzyNode>() };
                QueryNode::Fuzzy {
                    tok: &fz.tok,
                    max_dist: fz.maxDist,
                }
            }
            QueryNodeType::LexRange => {
                // SAFETY: `type_` is `LexRange`, so the union holds a `QueryLexRangeNode`.
                let lxrng = unsafe { &*union_ptr.cast::<ffi::QueryLexRangeNode>() };
                QueryNode::LexRange {
                    begin: char_ptr_to_bound(lxrng.begin, lxrng.includeBegin),
                    end: char_ptr_to_bound(lxrng.end, lxrng.includeEnd),
                }
            }
            QueryNodeType::Vector => {
                // SAFETY: `type_` is `Vector`, so the union holds a `QueryVectorNode`.
                let vn = unsafe { &*union_ptr.cast::<ffi::QueryVectorNode>() };
                QueryNode::Vector { vq: vn.vq }
            }
            QueryNodeType::WildcardQuery => {
                // SAFETY: `type_` is `WildcardQuery`, so the union holds a `QueryVerbatimNode`.
                let verb = unsafe { &*union_ptr.cast::<ffi::QueryVerbatimNode>() };
                QueryNode::WildcardQuery { tok: &verb.tok }
            }
            QueryNodeType::Null => QueryNode::Null,
            QueryNodeType::Missing => {
                // SAFETY: `type_` is `Missing`, so the union holds a
                // `QueryMissingNode`.  Invariant (1) of `new` guarantees
                // `miss.field` is a valid, non-null pointer.
                let miss = unsafe { &*union_ptr.cast::<ffi::QueryMissingNode>() };
                QueryNode::Missing {
                    // SAFETY: Invariant (1) of `new` guarantees `miss.field` is valid and non-null.
                    field: unsafe { &*miss.field },
                }
            }
            QueryNodeType::Max => {
                unreachable!("Max is a sentinel, not a real node type")
            }
        }
    }
}

const fn char_ptr_to_bound(ptr: *mut c_char, inclusive: bool) -> Bound<*const c_char> {
    if ptr.is_null() {
        Bound::Unbounded
    } else if inclusive {
        Bound::Included(ptr)
    } else {
        Bound::Excluded(ptr)
    }
}
