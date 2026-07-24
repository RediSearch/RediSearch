/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::RSQueryNode`].

use std::{marker::PhantomData, ops::Deref, ptr::NonNull};

use inverted_index::NumericFilter;
use query_types::{QueryNodeOptions, QueryNodeType};
use rqe_core::{DocId, FieldMask};
use rs_token::{RSTokenRef, RSTokenRefNulTerminated};

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

impl WildcardMode {
    /// The human-readable label for this mode, as surfaced in user-facing
    /// messages (e.g. the "query string is too long" error).
    pub const fn type_str(self) -> &'static str {
        match self {
            Self::Prefix => "PREFIX",
            Self::Suffix => "SUFFIX",
            Self::Contains => "INFIX",
        }
    }
}

/// Typed view of a query-node's per-type payload.
///
/// Produced by [`QueryNodeRef::as_enum`].  Each variant that carries a
/// type-specific payload exposes the fields of the corresponding C union
/// member directly.  Variants whose C structs contain only a dummy field
/// (`Union`, `Not`, `Optional`, `Wildcard`, `Null`) are unit variants —
/// their semantics come entirely from the shared header and child list.
///
/// Fields that embed an [`ffi::RSToken`] are exposed as [`Copy`] [`RSTokenRef`]
/// handles because the token struct contains bitfields that cannot be cleanly
/// inlined.
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
        /// phonetic flags, etc.). Not guaranteed to be NUL-terminated: an
        /// expanded token may come from the length-delimited `ExpandToken` API.
        tok: RSTokenRef<'a>,
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
        tok: RSTokenRefNulTerminated<'a>,
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
        tok: RSTokenRefNulTerminated<'a>,
        /// Maximum edit distance (1, 2, or 3).
        max_dist: i32,
    },
    /// A vector similarity search node.
    Vector {
        /// The vector query parameters (field, blob, algorithm, K, etc.).
        vq: *mut ffi::VectorQuery,
    },
    /// A verbatim wildcard-pattern query (e.g. `w'hel*o'`).
    WildcardQuery {
        /// The raw pattern token.
        tok: RSTokenRefNulTerminated<'a>,
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
/// All accessors here are read-only. Mutation goes through an exclusive
/// [`QueryNodeMut`], obtained via [`as_mut`](Self::as_mut).
///
/// # Ownership
///
/// `QueryNodeRef` does **not** own the node — it is a borrowed view.
/// The underlying [`ffi::RSQueryNode`] (and its children) are owned by the
/// C `QueryAST` and freed when the AST is dropped.
///
/// `#[repr(transparent)]` so a [`QueryNodeMut`] (also transparent over the same
/// pointer) can expose itself as a `&QueryNodeRef` via [`AsRef`].
#[repr(transparent)]
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
    const fn as_ffi_ref(&self) -> &ffi::RSQueryNode {
        // SAFETY: invariant (1) of `new`.
        unsafe { self.0.as_ref() }
    }

    /// The raw [`NonNull`] pointer to the underlying [`ffi::RSQueryNode`].
    pub const fn as_non_null(&self) -> NonNull<ffi::RSQueryNode> {
        self.0
    }

    /// The discriminant that selects which union variant is active.
    pub const fn node_type(&self) -> QueryNodeType {
        self.as_ffi_ref().type_
    }

    /// The shared options header (field mask, weight, slop, flags, …).
    pub const fn opts(&self) -> &QueryNodeOptions {
        &self.as_ffi_ref().opts
    }

    /// Upgrade this shared view into an exclusive [`QueryNodeMut`] over the same
    /// node subtree, enabling in-place mutation.
    ///
    /// # Safety
    ///
    /// The caller must have **exclusive** access to this node and all of its
    /// descendants for the lifetime of the returned [`QueryNodeMut`]: while that
    /// wrapper — or any [`QueryNodeMut`] reborrowed from it via
    /// [`child_mut`](QueryNodeMut::child_mut) — is live, no other
    /// [`QueryNodeRef`]/[`QueryNodeMut`] wrapping a node in this subtree may be
    /// used, and no reference obtained from one (e.g. via [`opts`](Self::opts))
    /// may be live. This cannot be enforced by the borrow checker because
    /// wrappers to the same node can be minted from safe code (e.g.
    /// [`child`](Self::child)); upholding it is the caller's responsibility.
    ///
    /// This is the single point where exclusivity is asserted. Given it, every
    /// write through the returned [`QueryNodeMut`] is safe: its `&mut`-based API
    /// and reborrowing [`child_mut`](QueryNodeMut::child_mut) keep each
    /// individual mutation statically free of aliasing.
    pub const unsafe fn as_mut(&self) -> QueryNodeMut<'_> {
        QueryNodeMut(self.0, PhantomData)
    }

    /// The number of child nodes.
    ///
    /// Returns 0 when the `children` pointer is null (leaf nodes).
    pub fn num_children(&self) -> usize {
        children_len(self.as_ffi_ref())
    }

    /// Return the child at `index` as a new [`QueryNodeRef`].
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.num_children()`.
    pub fn child(&self, index: usize) -> QueryNodeRef {
        // SAFETY: `child_ptr` yields a valid, non-null child node pointer, and
        // having (at least) shared access to this node we also have shared
        // access to its children — invariants (1) and (2) of `new`.
        unsafe { QueryNodeRef::new(child_ptr(self.as_ffi_ref(), index)) }
    }

    /// Iterator over all children as [`QueryNodeRef`]s.
    pub fn children(&self) -> impl Iterator<Item = QueryNodeRef> + '_ {
        (0..self.num_children()).map(|i| self.child(i))
    }

    /// Convert the C tagged union into a [`QueryNode`] enum.
    pub fn as_enum(&self) -> QueryNode<'_> {
        let inner = self.as_ffi_ref();
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
            QueryNodeType::Token => {
                // SAFETY: `type_` is `Token`, so the union holds an `RSToken`.
                let tok = unsafe { &*union_ptr.cast::<ffi::RSToken>() };
                QueryNode::Token {
                    // SAFETY: `tok` is a valid query-node token string owned by
                    // the node (its `str_`/`len` describe a valid byte range for
                    // the node's lifetime), satisfying `from_ffi`. Token strings
                    // may come from the length-delimited `ExpandToken` API, so we
                    // do not assume NUL-termination here.
                    tok: unsafe { RSTokenRef::from_ffi(tok) },
                }
            }
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
                    // SAFETY: `pfx.tok` is a valid, NUL-terminated query-node
                    // token string owned by the node.
                    tok: unsafe { RSTokenRefNulTerminated::from_nul_terminated_ffi(&pfx.tok) },
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
                    // SAFETY: `fz.tok` is a valid, NUL-terminated query-node
                    // token string owned by the node.
                    tok: unsafe { RSTokenRefNulTerminated::from_nul_terminated_ffi(&fz.tok) },
                    max_dist: fz.maxDist,
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
                QueryNode::WildcardQuery {
                    // SAFETY: `verb.tok` is a valid, NUL-terminated query-node
                    // token string owned by the node.
                    tok: unsafe { RSTokenRefNulTerminated::from_nul_terminated_ffi(&verb.tok) },
                }
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

/// Exclusive, mutable view of a [`ffi::RSQueryNode`] subtree.
///
/// Obtained from [`QueryNodeRef::as_mut`], whose `unsafe` contract establishes
/// exclusive access to the node and its descendants. Given that invariant, the
/// mutating API here is **safe**: [`and_field_mask`](Self::and_field_mask) takes
/// `&mut self`, and [`child_mut`](Self::child_mut) reborrows `&mut self`, so the
/// borrow checker guarantees that while a child view is live the parent (and any
/// sibling view) cannot be read or mutated — no two live wrappers ever alias the
/// same node. The exclusivity asserted once at [`as_mut`](QueryNodeRef::as_mut)
/// thus propagates down the subtree without any further `unsafe`.
///
/// The lifetime `'node` ties the view to the originating [`QueryNodeRef`] so it
/// cannot outlive the borrowed node.
///
/// Read-only access goes through [`Deref`]: all of [`QueryNodeRef`]'s shared
/// accessors (`opts`, `num_children`, …) are reachable on a [`QueryNodeMut`]
/// without re-implementation.
///
/// `#[repr(transparent)]` over the node pointer (the [`PhantomData`] is a ZST),
/// matching [`QueryNodeRef`]'s layout so the [`Deref`] view can reinterpret a
/// `&QueryNodeMut` as a borrowed `&QueryNodeRef`.
#[repr(transparent)]
pub struct QueryNodeMut<'node>(
    NonNull<ffi::RSQueryNode>,
    PhantomData<&'node mut ffi::RSQueryNode>,
);

impl QueryNodeMut<'_> {
    /// Intersect `mask` into this node's [field mask](QueryNodeOptions::field_mask).
    pub fn and_field_mask(&mut self, mask: FieldMask) {
        // `&mut self` plus the exclusive-access invariant of
        // [`QueryNodeRef::as_mut`] guarantee no other wrapper or reference
        // aliases this node, so the read-modify-write below cannot race or
        // alias a live reference.
        //
        // SAFETY: the node is valid, so a raw pointer to its `opts` field is in
        // bounds; the update goes through the raw pointer without forming an
        // intermediate reference.
        let opts = unsafe { &raw mut (*self.0.as_ptr()).opts };
        // SAFETY: as above — `opts` points to a valid, exclusively-owned
        // `QueryNodeOptions`.
        unsafe { (*opts).field_mask &= mask };
    }

    /// Reborrow the child at `index` as an exclusive [`QueryNodeMut`].
    ///
    /// The returned view borrows `*self`, so the parent cannot be read or
    /// mutated while it is live. Exclusivity therefore propagates down the
    /// subtree by construction — no further `unsafe` assertion is required.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.num_children()`.
    pub fn child_mut(&mut self, index: usize) -> QueryNodeMut<'_> {
        // The returned view's lifetime is tied to `&mut self` by the signature,
        // so the parent stays exclusively borrowed while the child is live even
        // though `child_ptr` itself only reads.
        QueryNodeMut(child_ptr(self.as_ref().as_ffi_ref(), index), PhantomData)
    }
}

impl Deref for QueryNodeMut<'_> {
    type Target = QueryNodeRef;

    /// A shared [`QueryNodeRef`] view of this node, for handing to read-only
    /// evaluation once any mutation through this view is complete. Every
    /// shared-view method of [`QueryNodeRef`] (e.g.
    /// [`opts`](QueryNodeRef::opts), [`num_children`](QueryNodeRef::num_children))
    /// is therefore reachable on a [`QueryNodeMut`] without re-implementation.
    ///
    /// The returned reference borrows `*self`, so a read view (or anything
    /// derived from it, e.g. via [`opts`](QueryNodeRef::opts)) cannot be held
    /// across — and therefore cannot alias — a mutation through this
    /// [`QueryNodeMut`]: while the `&QueryNodeRef` is live, `self` is
    /// shared-borrowed and the `&mut self` methods are unreachable.
    fn deref(&self) -> &QueryNodeRef {
        // SAFETY: `QueryNodeMut` and `QueryNodeRef` are both
        // `#[repr(transparent)]` over `NonNull<ffi::RSQueryNode>`, so they share
        // an identical layout and a `&QueryNodeMut` can be reinterpreted as a
        // `&QueryNodeRef`. The shared borrow of `self` is carried into the
        // returned reference's lifetime.
        unsafe { &*(self as *const Self as *const QueryNodeRef) }
    }
}

impl AsRef<QueryNodeRef> for QueryNodeMut<'_> {
    fn as_ref(&self) -> &QueryNodeRef {
        self
    }
}

/// The number of children of `node` (0 when the `children` pointer is null).
fn children_len(node: &ffi::RSQueryNode) -> usize {
    let children = node.children;
    if children.is_null() {
        return 0;
    }
    // SAFETY: `children` is a non-null `array_*`-managed pointer.
    unsafe { ffi::array_len_func(children.cast()) as usize }
}

/// Pointer to `node`'s child at `index`.
///
/// # Panics
///
/// Panics if `index >= children_len(node)`.
fn child_ptr(node: &ffi::RSQueryNode, index: usize) -> NonNull<ffi::RSQueryNode> {
    let n = children_len(node);
    assert!(index < n, "index {index} out of bounds (num_children: {n})");
    // SAFETY: `children` is non-null (`n > 0`) and `index < n`, so `add` stays
    // within the `array_*` allocation.
    let slot = unsafe { node.children.add(index) };
    // SAFETY: the slot is within bounds; dereference yields a valid child
    // pointer.
    let child = unsafe { *slot };
    // SAFETY: all children in the AST are valid, non-null nodes.
    unsafe { NonNull::new_unchecked(child) }
}
