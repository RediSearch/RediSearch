/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_TAG → the still-C `Query_EvalTagNode`, driven through the Rust
//! `eval_node` entrypoint.
//!
//! `QN_TAG` is the next node type in line to be ported to Rust. These tests
//! characterise its current C behaviour — including the expansion helpers it
//! delegates to (`Query_EvalTagPrefixNode`, `Query_EvalTagWildcardNode`,
//! `tag_strtolower`) — through the same entrypoint the port will keep, so the
//! same file passes unchanged afterwards and any behavioural drift shows up as
//! a failing test rather than a review argument.
//!
//! [`TagOptions`] holds every knob the tests vary; each field says what it
//! controls and why it is worth covering. See `docs/design/query_eval_tag_tests.md`
//! for the full design.
//!
//! Disabled under Miri: [`TestContext`] calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use std::ptr;

use index_result::{RSIndexResult, RawAggregateResult};
use query::mock::{MockQueryNode, TokenNodeType};
use query_eval::{Config, EvalResult, QueryEvalContext, QueryNodeMut, eval_node};
use query_flags::{QEFlag, QEFlags};
use query_types::QueryNodeType;
use rqe_core::DocId;
use rqe_iterators::{IteratorsConfig, RQEIterator};
use rqe_iterators_test_utils::{
    ContractChecker, GlobalGuard, TestContext, assert_current_contract, with_c_globals_locked,
};

/// A tag value and the documents indexed under it. Owned rather than
/// `&'static`, because the cap and timeout fixtures generate their value sets.
type Indexed = (Vec<u8>, Vec<DocId>);

/// The tag values the fixture indexes. Byte strings throughout: a tag value is
/// binary data, and only a value no `str` can hold tests that it is treated so.
const TAG_APPLE: &[u8] = b"apple"; // docs 1, 2
const TAG_APRICOT: &[u8] = b"apricot"; // docs 2, 3 -- doc 2 is the overlap
const TAG_BANANA: &[u8] = b"banana"; // docs 3, 4
const TAG_PHRASE: &[u8] = b"red apple"; // doc 5 -- the `sdsjoin` target
const TAG_BINARY: &[u8] = b"caf\xff"; // doc 6 -- 0xff appears in no UTF-8 sequence
const TAG_NUL: &[u8] = b"ab\0cd"; // doc 7
const TAG_NUL_HEAD: &[u8] = b"ab"; // doc 8 -- what a truncated lookup of TAG_NUL hits
const TAG_NUL_PHRASE_JOINED: &[u8] = b"ab x"; // doc 9 -- what `sdsjoin` builds from TAG_NUL
const TAG_NUL_PHRASE_WHOLE: &[u8] = b"ab\0cd x"; // doc 10 -- what a binary-safe join would build
// doc 11 -- the value production indexes for an empty tag field, and the only
// one the empty wildcard pattern can reach.
const TAG_EMPTY: &[u8] = b"";
// doc 12 -- a literal `*` in a value; matched by the wildcard `b*t`, since `*`
// matches a `*`.
const TAG_STAR: &[u8] = b"b*t";
// doc 13 -- matched by the wildcard `b*t` and by nothing that read the `*` as a
// literal.
const TAG_BAT: &[u8] = b"bat";
// doc 14 -- a second value under the suffix key `na`, so one key's term list
// outlives the cap.
const TAG_SULTANA: &[u8] = b"sultana";
// doc 15 -- lowering `CAFÉ` re-encodes to the same 5 bytes, so the token
// buffer is rewritten in place rather than replaced.
const TAG_CAFE: &[u8] = "café".as_bytes();
// doc 16 -- what `İSTANBUL` lowers to: `İ` (U+0130, 2 bytes) becomes `i` +
// U+0307 (3 bytes), so the result outgrows its buffer and `tag_strtolower`
// replaces it.
const TAG_DOTTED: &[u8] = "i\u{307}stanbul".as_bytes();
// doc 17 -- a binary value (a control byte, not a letter) that stays within
// ASCII, so a case-insensitive lookup takes `unicode_tolower`'s in-place fast
// path rather than the reallocating slow path a non-ASCII byte like
// `TAG_BINARY` would force -- see `eval_tag_case_insensitive_field_matches_a_binary_value`.
const TAG_BINARY_ASCII: &[u8] = b"caf\x01";
// no documents -- shares the `ap` prefix, and is skipped by every expansion
// that finds it.
const TAG_NO_DOCS: &[u8] = b"apogee";

/// How many generated values the timeout tests index. Comfortably more than the
/// 100 traversal steps between two clock probes (see [`Timeout`]), so an
/// expired deadline is certain to cut the scan short and equally certain not
/// to cut it to nothing.
const TIMEOUT_VALUES: u32 = 500;

/// A node weight that is neither the unit weight a per-expansion reader
/// carries nor the zero that selects `quickExit`, so a test reading a weight
/// back can tell all three apart.
const NODE_WEIGHT: f64 = 5.0;

/// A `CLOCK_MONOTONIC` deadline one second after boot: in the past for any
/// process that can run this test, and not the `{0, 0}` that
/// `TrieMapIterator_SetTimeout` reads as *unlimited*.
const EXPIRED_DEADLINE: ffi::timespec = ffi::timespec {
    tv_sec: 1,
    tv_nsec: 0,
};

/// Adapt byte literals to owned [`Indexed`] pairs, so a test can keep writing
/// its value set inline.
fn values(pairs: &[(&[u8], &[DocId])]) -> Vec<Indexed> {
    pairs
        .iter()
        .map(|(value, docs)| (value.to_vec(), docs.to_vec()))
        .collect()
}

/// The default value set: [`TAG_APPLE`] (docs 1, 2), [`TAG_APRICOT`] (docs 2,
/// 3), [`TAG_BANANA`] (docs 3, 4) and [`TAG_PHRASE`] (doc 5) -- and nothing
/// else, so a pattern matching everything yields exactly `[1, 2, 3, 4, 5]`.
/// Every other constant belongs to the tests that name it, which say so in
/// their own value set.
fn fruit() -> Vec<Indexed> {
    values(&[
        (TAG_APPLE, &[1, 2]),
        (TAG_APRICOT, &[2, 3]),
        (TAG_BANANA, &[3, 4]),
        (TAG_PHRASE, &[5]),
    ])
}

/// `count` values `az0000`, `az0001`, ... each holding one document, ids
/// starting at `first_doc_id`. Used where a scan has to be long enough to be
/// capped or to reach a clock probe; see the timeout granularity note on
/// [`Timeout`].
fn generated(count: u32, first_doc_id: DocId) -> Vec<Indexed> {
    (0..count)
        .map(|i| {
            (
                format!("az{i:06}").into_bytes(),
                vec![first_doc_id + DocId::from(i)],
            )
        })
        .collect()
}

/// What kind of tag field the node names -- which decides whether evaluation
/// gets past its first line, and which expansions consult a suffix trie.
enum TagField {
    /// A tag field with a populated index and no suffix trie, as a field
    /// created without `WITHSUFFIXTRIE` has.
    Indexed,
    /// A tag field carrying both halves of `WITHSUFFIXTRIE`: the
    /// `FieldSpec_WithSuffixTrie` option that `Query_EvalTagPrefixNode` reads,
    /// and the `TagIndex.suffix` trie that `Query_EvalTagWildcardNode` reads.
    /// Production always sets them together, so the fixture does too.
    IndexedWithSuffixTrie,
    /// A tag field whose index was never created, as a field no document has
    /// ever been written to leaves it.
    NoIndex,
}

/// A child of the tag node, one variant per node type
/// `query_EvalSingleTagNode` accepts.
enum Child {
    /// A plain `@tag:{value}` term.
    Token(&'static [u8]),
    /// The same, with the token buffer owned by the Redis allocator rather
    /// than the Rust one, so `tag_strtolower` may free and replace it. Used
    /// only by the lengthening-multibyte row; see
    /// [`MockQueryNode::with_redis_token`].
    RedisToken(&'static [u8]),
    /// A `@tag:{pat*}` expansion. `prefix`/`suffix` are the anchoring flags the
    /// parser sets from where the `*`s sit: `suffix` off is `TAG_PREFIX_MODE`
    /// (`pat*`), `suffix` alone is `TAG_SUFFIX_MODE` (`*pat`), and both is
    /// `TAG_CONTAINS_MODE` (`*pat*`). A node with both off is not a state the
    /// parser can produce, so no test builds one. On a
    /// [`TagField::IndexedWithSuffixTrie`] field the two `suffix`-anchored
    /// modes take the suffix-trie walk instead.
    Prefix {
        pattern: &'static [u8],
        prefix: bool,
        suffix: bool,
    },
    /// A `@tag:{w'pat'}` wildcard expansion.
    WildcardQuery(&'static [u8]),
    /// A `@tag:{multi word value}` phrase, whose children are always tokens.
    Phrase(&'static [&'static [u8]]),
}

/// What deadline the search context carries while the node is evaluated.
///
/// `trie_rs`' `IteratorTimeoutState` probes the clock once per 100 traversal
/// advances, so a deadline in the past does not truncate a small scan at all
/// -- the timeout tests therefore index [`TIMEOUT_VALUES`] generated values
/// and assert the expansion is *strictly smaller* than the untimed one and
/// non-empty, rather than an exact doc set.
enum Timeout {
    /// `sctx->time.timeout` left at `{0, 0}`, which the trie iterator reads as
    /// no deadline at all -- the production default in these fixtures.
    Unlimited,
    /// [`EXPIRED_DEADLINE`], with timeout checks enabled: the scan stops at
    /// the first clock probe past it.
    Expired,
    /// [`EXPIRED_DEADLINE`] with `skipTimeoutChecks` set, which stops the
    /// evaluator installing it on the trie iterator at all.
    ExpiredButSkipped,
}

/// The two request flags `query_EvalSingleTagNode` reads as "this is a hybrid
/// subquery". They are alternatives, never both set at once.
enum Hybrid {
    /// `QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY`.
    SearchSubquery,
    /// `QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY`.
    VectorAggregateSubquery,
}

/// How to build a [`TagFixture`]. Each test sets only the knob it exercises
/// and leaves the rest at [`TagOptions::default`].
struct TagOptions {
    values: Vec<Indexed>,
    field: TagField,
    /// Sets `TagField_CaseSensitive` on the field, which decides whether the
    /// query token is lowercased -- and, for a value carrying a NUL, whether
    /// the lookup keeps its full length.
    case_sensitive: bool,
    children: Vec<Child>,
    /// The node's weight, which reaches the child readers, the union above
    /// them, and the `quickExit` decision.
    weight: f64,
    /// Drives `quickExit` through `q->inNotSubTree` rather than a zero weight.
    in_not_sub_tree: bool,
    /// Which hybrid subquery flag to set on the request, if any. Either one
    /// zeroes the weight the child readers are opened with while leaving the
    /// node's own weight -- and so the union's weight and `quickExit` -- alone;
    /// `None` leaves the request non-hybrid.
    hybrid: Option<Hybrid>,
    /// Overrides `QueryEvalCtx.config->maxPrefixExpansions`, the cap on how
    /// many readers one expansion opens. `None` leaves the production
    /// default. This is *not* `Config::max_prefix_expansions`: `q->config` is
    /// the FFI `IteratorsConfig` `TestContext` allocates, never the `Config`
    /// threaded into `eval_node`.
    max_prefix_expansions: Option<u32>,
    /// Overrides `QueryEvalCtx.config->minTermPrefix`. `None` leaves the
    /// production default of 2.
    min_term_prefix: Option<u32>,
    /// What deadline `sctx->time` carries.
    timeout: Timeout,
}

// Hand-written, *not* derived: a derived `Default` would give `weight: 0.0`,
// which is one of `quickExit`'s two disjuncts, so every test that left the
// weight alone would silently take the quick-exit path -- and an empty value
// set, leaving nothing for a lookup to find.
impl Default for TagOptions {
    fn default() -> Self {
        Self {
            values: fruit(), // NOT `Vec::new()`
            field: TagField::Indexed,
            case_sensitive: false,
            children: Vec::new(),
            weight: 1.0, // NOT `0.0` -- see above
            in_not_sub_tree: false,
            hybrid: None,
            max_prefix_expansions: None,
            min_term_prefix: None,
            timeout: Timeout::Unlimited,
        }
    }
}

/// Populate `idx` with `values`, and its suffix trie too when `with_suffix` --
/// mirroring `TagIndex_Commit`'s memory-mode behaviour and the gate against
/// the empty value that `addSuffixTrieMap` itself asserts on.
fn index_values(idx: *mut ffi::TagIndex, values: &[Indexed], with_suffix: bool) {
    for (value, doc_ids) in values {
        let mut sz: usize = 0;
        // SAFETY: `idx` is a valid, non-null `TagIndex` just created by
        // `TagIndex_Ensure`, and `value` is a live byte slice for the
        // duration of the call.
        let ii_ptr = unsafe {
            ffi::TagIndex_OpenIndex(
                idx,
                value.as_ptr().cast(),
                value.len(),
                1, // CREATE_INDEX
                &mut sz,
            )
        };
        assert!(!ii_ptr.is_null(), "TagIndex_OpenIndex returned null");
        // `TagIndex_OpenIndex` internally calls `NewInvertedIndex_Ex`, so the
        // pointer is actually a Rust opaque `InvertedIndex` despite the C
        // type, exactly as `TestContext::tag` relies on.
        let ii_opaque: *mut inverted_index::opaque::InvertedIndex = ii_ptr.cast();
        for &doc_id in doc_ids {
            let record: RSIndexResult = RSIndexResult::build_virt().doc_id(doc_id).build();
            // SAFETY: `ii_opaque` is the valid pointer just obtained above.
            unsafe {
                inverted_index_ffi::InvertedIndex_WriteEntryGeneric(ii_opaque, &record);
            }
        }
        if with_suffix && !value.is_empty() {
            // SAFETY: `idx` is a valid `TagIndex` created with
            // `withSuffix = true`, so `idx.suffix` is a valid suffix
            // `TrieMap`.
            let suffix = unsafe { (*idx).suffix };
            // SAFETY: `suffix` is the valid suffix `TrieMap` just read above.
            unsafe {
                ffi::addSuffixTrieMap(suffix, value.as_ptr().cast(), value.len() as u32);
            }
        }
    }
}

/// Build the [`MockQueryNode`] for one [`Child`], pushing any nested nodes it
/// must keep alive (a [`Child::Phrase`]'s own token children) onto
/// `grandchildren`.
fn build_child(child: &Child, grandchildren: &mut Vec<MockQueryNode>) -> MockQueryNode {
    match *child {
        Child::Token(value) => MockQueryNode::with_token(TokenNodeType::Token, value),
        Child::RedisToken(value) => MockQueryNode::with_redis_token(TokenNodeType::Token, value),
        Child::Prefix {
            pattern,
            prefix,
            suffix,
        } => {
            let mut node = MockQueryNode::with_token(TokenNodeType::Prefix, pattern);
            node.set_prefix_mode(prefix, suffix);
            node
        }
        Child::WildcardQuery(pattern) => {
            MockQueryNode::with_token(TokenNodeType::WildcardQuery, pattern)
        }
        Child::Phrase(tokens) => {
            let mut node = MockQueryNode::new(QueryNodeType::Phrase);
            let mut ptrs = Vec::new();
            for &token in tokens {
                let child = MockQueryNode::with_token(TokenNodeType::Token, token);
                ptrs.push(child.as_ptr());
                grandchildren.push(child);
            }
            node.set_children(&ptrs);
            node
        }
    }
}

/// One document an iterator yielded, with what the result says about it.
#[derive(Debug, PartialEq)]
struct Match {
    doc_id: DocId,
    /// The weight of the result itself. What that means depends on what the
    /// node built, which is exactly the distinction the hybrid tests turn on:
    ///
    /// - a multi-child node's union carries `qn->opts.weight` verbatim, so
    ///   this stays the node weight even under a hybrid request;
    /// - a single-child node returns its child's own iterator, so this is the
    ///   *effective* weight -- a hybrid request zeroes it, and an expansion's
    ///   sub-union carries it while its per-expansion readers carry 1.
    weight: f64,
    /// The weights of the result's child records, in order, if it is an
    /// aggregate -- a union result's are the child iterators' current
    /// results.
    ///
    /// Empty when the result is not an aggregate, which is every bare
    /// `InvIdxTag` reader: a leaf carries no child records, and its own
    /// weight is in [`weight`](Match::weight). A leaf is therefore
    /// distinguishable from a one-record aggregate, which matters because the
    /// union reduction that returns a lone surviving child bare is one of the
    /// behaviours pinned here.
    ///
    /// The length subsumes the arity `quickExit` is observed by: a quick-exit
    /// union stops at the first child holding the document, so this holds
    /// one entry however many children match.
    records: Vec<f64>,
}

/// The weights of `result`'s child records, or the empty vector if it is not
/// an aggregate. See [`Match::records`].
fn record_weights(result: &RSIndexResult<'_>) -> Vec<f64> {
    match result.as_aggregate() {
        None => Vec::new(),
        Some(RawAggregateResult::Borrowed(borrowed)) => {
            borrowed.records().iter().map(|r| r.get().weight).collect()
        }
        Some(RawAggregateResult::Owned(owned)) => {
            owned.records().iter().map(|r| r.weight).collect()
        }
    }
}

/// Check `it` against the iterator contract, then rewind it and replay it
/// into the documents it yields.
///
/// The contract check comes first because it is what holds the C iterator to
/// the same rules a ported Rust one will have to meet. It rewinds only
/// *between* its two passes and returns with the iterator at EOF, so `drain`
/// rewinds again itself before the replay -- without that the replay reads an
/// exhausted iterator and every result is empty.
fn drain(it: &mut ContractChecker<EvalResult<'_>>) -> Vec<Match> {
    assert_current_contract(it);
    it.rewind();

    let mut matches = Vec::new();
    while let Some(result) = it.read().expect("read must not fail while draining") {
        matches.push(Match {
            doc_id: result.doc_id,
            weight: result.weight,
            records: record_weights(result),
        });
    }
    matches
}

/// The doc ids of `drain(it)`, for the many tests that assert nothing else.
fn drain_doc_ids(it: &mut ContractChecker<EvalResult<'_>>) -> Vec<DocId> {
    drain(it).into_iter().map(|m| m.doc_id).collect()
}

/// Owns everything a `QN_TAG` evaluation borrows or mutates.
struct TagFixture {
    /// Registers the process-exit cleanup of the global spec dictionaries
    /// shared by every [`TestContext`]. Carried purely for that side effect.
    _guard: GlobalGuard,
    /// The evaluation context under test. Also carries the query status,
    /// which is where the warning assertions read the node's effects.
    ///
    /// Declared before the [`TestContext`] it borrows from, so it is the
    /// first of the two to drop.
    ctx: QueryEvalContext,
    /// Owns the `sctx`, the `DocTable` and the `QueryEvalCtx` that `ctx`
    /// wraps. Its own tag field is unused: the node names `field_spec`
    /// instead.
    _context: TestContext,
    /// The `QN_TAG` node under evaluation.
    node: MockQueryNode,
    /// The node's children, and the phrase children's own token nodes,
    /// kept alive because the node holds raw pointers to them.
    _children: Vec<MockQueryNode>,
    /// The field spec the node names. Boxed for a stable address, since the
    /// node points at it. For every [`TagField`] but [`TagField::NoIndex`]
    /// its `tagOpts.tagIndex` is a `TagIndex` this fixture created and frees
    /// -- see the [`Drop`] impl, which must run before `_context` releases
    /// the allocator the index was built with.
    field_spec: Box<ffi::FieldSpec>,
}

impl TagFixture {
    fn new(opts: TagOptions) -> Self {
        let _guard = GlobalGuard::default();

        // sctx, docTable and qctx only -- the context's own tag field and
        // placeholder value are unreachable, since the node points at the
        // fixture's own spec below.
        let mut context = TestContext::tag(std::iter::empty());

        if opts.max_prefix_expansions.is_some() || opts.min_term_prefix.is_some() {
            let mut config = IteratorsConfig::default();
            if let Some(max) = opts.max_prefix_expansions {
                config.max_prefix_expansions = max;
            }
            if let Some(min) = opts.min_term_prefix {
                config.min_term_prefix = min;
            }
            context.set_iterators_config(config);
        }
        match opts.timeout {
            Timeout::Unlimited => {}
            Timeout::Expired => context.set_search_time(EXPIRED_DEADLINE, false),
            Timeout::ExpiredButSkipped => context.set_search_time(EXPIRED_DEADLINE, true),
        }

        // SAFETY: an all-zero bit pattern is a valid (empty) `FieldSpec`.
        let mut field_spec: Box<ffi::FieldSpec> = Box::new(unsafe { std::mem::zeroed() });
        field_spec.set_types(ffi::FieldType_INDEXFLD_T_TAG);
        field_spec.index = 0;
        let with_suffix = matches!(opts.field, TagField::IndexedWithSuffixTrie);
        if with_suffix {
            field_spec.set_options(ffi::FieldSpecOptions_FieldSpec_WithSuffixTrie);
        }
        if opts.case_sensitive {
            // SAFETY: `field_spec` is exclusively owned, and the field is a
            // tag field per `set_types` above, so `tagOpts` is the active
            // union member.
            unsafe {
                field_spec
                    .__bindgen_anon_1
                    .tagOpts
                    .set_tagFlags(ffi::TagFieldFlags_TagField_CaseSensitive);
            }
        }

        if !matches!(opts.field, TagField::NoIndex) {
            with_c_globals_locked(|| {
                // SAFETY: `field_spec` is exclusively owned and outlives the
                // index built from it (freed in `Drop` before `field_spec`
                // itself is dropped).
                let idx =
                    unsafe { ffi::TagIndex_Ensure(&mut *field_spec, ptr::null_mut(), with_suffix) };
                assert!(!idx.is_null(), "TagIndex_Ensure returned null");
                index_values(idx, &opts.values, with_suffix);
            });
        }

        if let Some(hybrid) = opts.hybrid {
            let flag = match hybrid {
                Hybrid::SearchSubquery => QEFlag::IsHybridSearchSubquery,
                Hybrid::VectorAggregateSubquery => QEFlag::IsHybridVectorAggregateSubquery,
            };
            // SAFETY: `context.qctx()` is a valid, exclusively-owned
            // `QueryEvalCtx`.
            unsafe {
                (*context.qctx().as_ptr()).reqFlags |= QEFlags::from(flag).bits();
            }
        }

        // SAFETY: `context.qctx()` returns a valid, exclusively-owned
        // `QueryEvalCtx` (with real `status`, `config` and metric-request
        // head), upholding the `QueryEvalContext::new` invariants.
        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };
        if opts.in_not_sub_tree {
            ctx.set_in_not_sub_tree(true);
        }

        let mut grandchildren = Vec::new();
        let mut children: Vec<MockQueryNode> = opts
            .children
            .iter()
            .map(|child| build_child(child, &mut grandchildren))
            .collect();
        let child_ptrs: Vec<*mut ffi::RSQueryNode> =
            children.iter().map(MockQueryNode::as_ptr).collect();
        children.append(&mut grandchildren);

        let mut node = MockQueryNode::new(QueryNodeType::Tag);
        node.opts_mut().weight = opts.weight;
        node.set_tag_field_spec(&*field_spec as *const ffi::FieldSpec);
        node.set_children(&child_ptrs);

        Self {
            _guard,
            ctx,
            _context: context,
            node,
            _children: children,
            field_spec,
        }
    }

    /// Evaluate the node, wrapping whatever it yields in a contract checker.
    ///
    /// Returns `None` when evaluation yields no iterator. The checker owns
    /// the iterator and frees it on drop, and borrows the fixture for as long
    /// as it lives -- so a test reading the query status does so after
    /// dropping it.
    fn eval(&mut self) -> Option<ContractChecker<EvalResult<'_>>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeMut::new(self.node.as_non_null()) };
        let evaluated = eval_node(&mut self.ctx, node_ref, Config::default())?;
        Some(ContractChecker::new(evaluated.into_boxed()))
    }

    /// Evaluate the node, asserting it yielded no iterator at all -- which is
    /// not the same as yielding an empty one.
    fn eval_yielding_nothing(&mut self) {
        assert!(
            self.eval().is_none(),
            "expected evaluation to yield no iterator at all"
        );
    }

    /// Whether `QueryError_SetReachedMaxPrefixExpansionsWarning` fired during
    /// the last evaluation.
    fn reached_max_prefix_expansions(&mut self) -> bool {
        self.ctx.status().warnings().reached_max_prefix_expansions()
    }
}

impl Drop for TagFixture {
    /// Frees the fixture's `TagIndex`, which takes the per-value inverted
    /// indexes and the suffix trie with it.
    fn drop(&mut self) {
        // SAFETY: `field_spec` is exclusively owned by this fixture, and
        // `tagOpts` is the active union member since the field is always
        // built as a tag field in `new`.
        let idx = unsafe { self.field_spec.__bindgen_anon_1.tagOpts.tagIndex };
        if !idx.is_null() {
            // SAFETY: `idx` is the `TagIndex` this fixture created in `new`
            // and nothing else references it once evaluation has finished.
            unsafe { ffi::TagIndex_Free(idx) };
        }
    }
}

// ---------------------------------------------------------------------------
// Index, children and union shape
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_without_a_tag_index_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::NoIndex,
        children: vec![Child::Token(TAG_APPLE)],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_with_one_child_returns_its_reader_unwrapped() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE)],
        weight: NODE_WEIGHT,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple is indexed");
    let matches = drain(&mut it);
    drop(it);

    assert_eq!(
        matches,
        vec![
            Match {
                doc_id: 1,
                weight: NODE_WEIGHT,
                records: vec![],
            },
            Match {
                doc_id: 2,
                weight: NODE_WEIGHT,
                records: vec![],
            },
        ],
        "a leaf reader, not a one-record aggregate"
    );
}

#[test]
fn eval_tag_with_one_absent_child_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(b"durian")],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_unions_its_children() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_BANANA)],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3, 4]);
}

#[test]
fn eval_tag_union_drops_a_child_value_that_is_not_indexed() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(b"durian")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple is indexed");
    let matches = drain(&mut it);
    drop(it);
    assert!(
        matches.iter().all(|m| m.records.is_empty()),
        "the union must reduce to the bare surviving reader"
    );
    assert_eq!(
        matches.into_iter().map(|m| m.doc_id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn eval_tag_union_of_absent_values_is_empty() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(b"durian"), Child::Token(b"elderberry")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the union reduces to Empty, not to None");
    assert!(drain_doc_ids(&mut it).is_empty());
}

// ---------------------------------------------------------------------------
// `quickExit` and weight
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_full_union_reports_every_matching_child() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: 1.0,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(doc2.records.len(), 2, "quickExit must be off");
}

#[test]
fn eval_tag_zero_weight_takes_quick_exit() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: 0.0,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(doc2.records.len(), 1, "a zero weight must select quickExit");
}

#[test]
fn eval_tag_in_a_not_subtree_takes_quick_exit() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: NODE_WEIGHT,
        in_not_sub_tree: true,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(
        doc2.records.len(),
        1,
        "inNotSubTree must select quickExit too"
    );
}

#[test]
fn eval_tag_union_carries_the_node_weight() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: NODE_WEIGHT,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(doc2.weight, NODE_WEIGHT, "the union's own weight");
    assert_eq!(
        doc2.records,
        vec![NODE_WEIGHT, NODE_WEIGHT],
        "each child reader's weight"
    );
}

#[test]
fn eval_tag_hybrid_search_subquery_opens_its_readers_with_zero_weight() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: NODE_WEIGHT,
        hybrid: Some(Hybrid::SearchSubquery),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(
        doc2.records,
        vec![0.0, 0.0],
        "the zeroing is readable nowhere else"
    );
    assert_eq!(
        doc2.weight, NODE_WEIGHT,
        "NewUnionIterator keeps opts.weight"
    );
    assert_eq!(doc2.records.len(), 2, "quickExit must read opts.weight too");
}

#[test]
fn eval_tag_hybrid_vector_aggregate_subquery_opens_its_readers_with_zero_weight() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE), Child::Token(TAG_APRICOT)],
        weight: NODE_WEIGHT,
        hybrid: Some(Hybrid::VectorAggregateSubquery),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both values are indexed");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(doc2.records, vec![0.0, 0.0]);
    assert_eq!(doc2.weight, NODE_WEIGHT);
    assert_eq!(doc2.records.len(), 2);
}

#[test]
fn eval_tag_hybrid_single_child_reader_carries_zero_weight() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(TAG_APPLE)],
        weight: NODE_WEIGHT,
        hybrid: Some(Hybrid::SearchSubquery),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple is indexed");
    let matches = drain(&mut it);
    drop(it);
    assert!(
        matches
            .iter()
            .all(|m| m.weight == 0.0 && m.records.is_empty())
    );
}

// ---------------------------------------------------------------------------
// Child node types
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_prefix_child_expands_to_every_matching_value() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        weight: NODE_WEIGHT,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple and apricot match");
    let matches = drain(&mut it);
    drop(it);
    assert_eq!(
        matches.iter().map(|m| m.doc_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(doc2.weight, NODE_WEIGHT, "the expansion sub-union's weight");
    assert_eq!(
        doc2.records,
        vec![1.0],
        "per-expansion readers carry unit weight"
    );
}

#[test]
fn eval_tag_prefix_child_expansions_quick_exit() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple and apricot match");
    let matches = drain(&mut it);
    drop(it);
    let doc2 = matches.iter().find(|m| m.doc_id == 2).unwrap();
    assert_eq!(
        doc2.records.len(),
        1,
        "the expansion union is built with quickExit hard-coded true"
    );
}

#[test]
fn eval_tag_prefix_child_in_suffix_mode_matches_the_value_ending() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"na",
            prefix: false,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("banana ends in na");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_prefix_child_in_contains_mode_matches_anywhere() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"an",
            prefix: true,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("banana contains an");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_prefix_child_shorter_than_the_minimum_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"a",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_prefix_child_honours_a_raised_minimum() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        min_term_prefix: Some(3),
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_prefix_child_skips_a_value_with_no_documents() {
    let mut values = fruit();
    values.push((TAG_NO_DOCS.to_vec(), vec![]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple and apricot still match");
    let matches = drain(&mut it);
    drop(it);
    assert_eq!(
        matches.iter().map(|m| m.doc_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        matches
            .iter()
            .find(|m| m.doc_id == 2)
            .unwrap()
            .records
            .len(),
        1,
        "the NULL reader for apogee must be skipped, not admitted"
    );
}

#[test]
fn eval_tag_wildcard_child_matches_by_pattern() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::WildcardQuery(b"ba*na")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("banana matches");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_wildcard_child_skips_a_value_with_no_documents() {
    let mut values = fruit();
    values.push((TAG_NO_DOCS.to_vec(), vec![]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::WildcardQuery(b"a*")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple and apricot still match");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_tag_wildcard_child_with_an_empty_pattern_matches_the_empty_value() {
    let mut values = fruit();
    values.push((TAG_EMPTY.to_vec(), vec![11]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::WildcardQuery(b"")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the empty value is indexed");
    assert_eq!(drain_doc_ids(&mut it), vec![11]);
}

#[test]
fn eval_tag_wildcard_child_with_an_empty_pattern_and_no_empty_value_is_empty() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::WildcardQuery(b"")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the short-circuit reader is NULL, yielding an empty union");
    assert!(drain_doc_ids(&mut it).is_empty());
}

#[test]
fn eval_tag_phrase_child_joins_its_tokens_with_a_space() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Phrase(&[b"red", b"apple"])],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("\"red apple\" is indexed");
    assert_eq!(drain_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_tag_phrase_child_truncates_a_token_at_a_nul() {
    let values = values(&[(TAG_NUL_PHRASE_JOINED, &[9]), (TAG_NUL_PHRASE_WHOLE, &[10])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        case_sensitive: true,
        children: vec![Child::Phrase(&[TAG_NUL, b"x"])],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the truncated join is indexed");
    assert_eq!(
        drain_doc_ids(&mut it),
        vec![9],
        "sdsjoin is strlen-based, even though no lowering step ran"
    );
}

// ---------------------------------------------------------------------------
// Escapes, case and binary values
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_token_child_drops_an_escaping_backslash() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(b"red\\ apple")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the unescaped token matches");
    assert_eq!(drain_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_tag_case_sensitive_field_still_drops_an_escaping_backslash() {
    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::Token(b"red\\ apple")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the escape loop runs ahead of the case-sensitivity check");
    assert_eq!(drain_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_tag_prefix_child_drops_an_escaping_backslash() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"red\\ ap",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the scan is anchored on \"red ap\"");
    assert_eq!(drain_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_tag_wildcard_child_escape_is_eaten_before_remove_escape() {
    let values = values(&[(TAG_STAR, &[12]), (TAG_BAT, &[13])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::WildcardQuery(b"b\\*t")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the pattern matches as the wildcard b*t");
    assert_eq!(drain_doc_ids(&mut it), vec![12, 13]);
}

#[test]
fn eval_tag_wildcard_child_double_escape_is_the_same_wildcard() {
    let values = values(&[(TAG_STAR, &[12]), (TAG_BAT, &[13])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::WildcardQuery(b"b\\\\*t")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the doubly-escaped pattern is byte-identical");
    assert_eq!(drain_doc_ids(&mut it), vec![12, 13]);
}

#[test]
fn eval_tag_wildcard_child_of_a_lone_backslash_matches_the_empty_value() {
    let mut values = fruit();
    values.push((TAG_EMPTY.to_vec(), vec![11]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::WildcardQuery(b"\\")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the backslash is consumed, leaving the empty pattern");
    assert_eq!(drain_doc_ids(&mut it), vec![11]);
}

#[test]
fn eval_tag_lowercases_the_query_on_a_case_insensitive_field() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Token(b"APPLE")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the lowered token matches");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2]);
}

// `tag_strtolower` is called once per branch, not once ahead of the dispatch:
// `Query_EvalTagPrefixNode` and `Query_EvalTagWildcardNode` each call it on
// their own pattern, and `query_EvalSingleTagNode`'s phrase case calls it on
// each child individually before the `sdsjoin`.
// [`eval_tag_lowercases_the_query_on_a_case_insensitive_field`] above only
// exercises the `Token` branch's call, so a port that dropped lowering from
// one of the other three would still pass this suite: every existing
// prefix/wildcard/phrase pattern in it is already lowercase, making the call
// a no-op there. The three tests below give each branch a query that only
// matches if its own lowering runs, and a case-sensitive control confirming
// the same query is rejected once it does not.

#[test]
fn eval_tag_prefix_child_lowercases_the_pattern_on_a_case_insensitive_field() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"AP",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the lowered pattern matches apple and apricot");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_tag_prefix_child_case_sensitive_field_keeps_the_pattern_case() {
    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::Prefix {
            pattern: b"AP",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    // Unlike a token or phrase lookup, the brute-force scan always wraps its
    // matches (however many) in a union, so zero matches is `Some(empty)`,
    // not `None` -- see `eval_tag_wildcard_child_with_an_empty_pattern_and_no_empty_value_is_empty`.
    let mut it = fixture
        .eval()
        .expect("the unlowered pattern still builds an (empty) union");
    assert!(drain_doc_ids(&mut it).is_empty());
}

#[test]
fn eval_tag_wildcard_child_lowercases_the_pattern_on_a_case_insensitive_field() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::WildcardQuery(b"BA*NA")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the lowered pattern matches banana");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_wildcard_child_case_sensitive_field_keeps_the_pattern_case() {
    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::WildcardQuery(b"BA*NA")],
        ..TagOptions::default()
    });
    // Same reasoning as the prefix control above: the wildcard expansion
    // always wraps its matches in a union, so zero matches is `Some(empty)`.
    let mut it = fixture
        .eval()
        .expect("the unlowered pattern still builds an (empty) union");
    assert!(drain_doc_ids(&mut it).is_empty());
}

#[test]
fn eval_tag_phrase_child_lowercases_its_tokens_on_a_case_insensitive_field() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Phrase(&[b"RED", b"APPLE"])],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("each token is lowered before the join, matching \"red apple\"");
    assert_eq!(drain_doc_ids(&mut it), vec![5]);
}

#[test]
fn eval_tag_phrase_child_case_sensitive_field_keeps_its_tokens_case() {
    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::Phrase(&[b"RED", b"APPLE"])],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_multibyte_query_lowered_in_place() {
    let mut values = fruit();
    values.push((TAG_CAFE.to_vec(), vec![15]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::Token("CAFÉ".as_bytes())],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the re-encoded bytes are written back into the token's buffer");
    assert_eq!(drain_doc_ids(&mut it), vec![15]);
}

#[test]
fn eval_tag_multibyte_query_lowered_into_a_longer_buffer() {
    let mut values = fruit();
    values.push((TAG_DOTTED.to_vec(), vec![16]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::RedisToken("İSTANBUL".as_bytes())],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the lookup must use the replacement buffer tag_strtolower installs");
    assert_eq!(drain_doc_ids(&mut it), vec![16]);
}

#[test]
fn eval_tag_case_sensitive_field_keeps_the_query_case() {
    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::Token(b"APPLE")],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();

    let mut fixture = TagFixture::new(TagOptions {
        case_sensitive: true,
        children: vec![Child::Token(TAG_APPLE)],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the matching case matches");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2]);
}

#[test]
fn eval_tag_case_sensitive_field_matches_a_binary_value() {
    let values = values(&[(TAG_BINARY, &[6])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        case_sensitive: true,
        children: vec![Child::Token(TAG_BINARY)],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("no lowering step runs, so 0xff reaches the lookup verbatim");
    assert_eq!(drain_doc_ids(&mut it), vec![6]);
}

#[test]
fn eval_tag_case_insensitive_field_matches_a_binary_value() {
    let mut values = fruit();
    values.push((TAG_BINARY_ASCII.to_vec(), vec![17]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::Token(b"CAF\x01")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the control byte is not a letter, so lowering leaves it untouched");
    assert_eq!(drain_doc_ids(&mut it), vec![17]);
}

#[test]
fn eval_tag_lookup_stops_at_a_nul_on_a_case_insensitive_field() {
    let values = values(&[(TAG_NUL, &[7]), (TAG_NUL_HEAD, &[8])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        children: vec![Child::Token(TAG_NUL)],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the length was cut at the NUL, matching the head value");
    assert_eq!(drain_doc_ids(&mut it), vec![8]);
}

#[test]
fn eval_tag_case_sensitive_field_matches_past_a_nul() {
    let values = values(&[(TAG_NUL, &[7]), (TAG_NUL_HEAD, &[8])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        case_sensitive: true,
        children: vec![Child::Token(TAG_NUL)],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the full length survives");
    assert_eq!(drain_doc_ids(&mut it), vec![7]);
}

// ---------------------------------------------------------------------------
// Suffix trie
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_prefix_child_ignores_the_suffix_trie_when_prefix_anchored() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("!pfx.suffix short-circuits the withSuffixTrie test");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3]);
}

#[test]
fn eval_tag_prefix_child_in_suffix_mode_uses_the_suffix_trie() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"na",
            prefix: false,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("the suffix trie agrees with the brute-force scan");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_prefix_child_in_contains_mode_uses_the_suffix_trie() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"an",
            prefix: true,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("prefix=true walks every suffix starting with an");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_suffix_trie_miss_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"zz",
            prefix: false,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_suffix_trie_contains_miss_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"zz",
            prefix: true,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

#[test]
fn eval_tag_prefix_child_via_suffix_trie_skips_a_value_with_no_documents() {
    let mut values = fruit();
    values.push((TAG_NO_DOCS.to_vec(), vec![]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"ee",
            prefix: false,
            suffix: true,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("apogee is in the suffix trie, but its reader is NULL");
    assert!(drain_doc_ids(&mut it).is_empty());
}

#[test]
fn eval_tag_wildcard_child_uses_the_suffix_trie() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::WildcardQuery(b"ba*na")],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("TagIndex_HasSuffix is enough");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
}

#[test]
fn eval_tag_wildcard_child_with_no_usable_token_falls_back_to_brute_force() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::WildcardQuery(b"*")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("BAD_POINTER falls back to the brute-force scan");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3, 4, 5]);
}

#[test]
fn eval_tag_wildcard_child_via_suffix_trie_skips_a_value_with_no_documents() {
    let mut values = fruit();
    values.push((TAG_NO_DOCS.to_vec(), vec![]));
    let mut fixture = TagFixture::new(TagOptions {
        values,
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::WildcardQuery(b"*ogee")],
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("apogee's reader is NULL, so an empty union comes back");
    assert!(drain_doc_ids(&mut it).is_empty());
}

#[test]
fn eval_tag_wildcard_child_suffix_trie_miss_yields_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::WildcardQuery(b"zz*qq")],
        ..TagOptions::default()
    });
    fixture.eval_yielding_nothing();
}

// ---------------------------------------------------------------------------
// Expansion cap
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_prefix_expansion_stops_at_the_cap() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(1),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("one reader is opened");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2]);
    drop(it);
    assert!(fixture.reached_max_prefix_expansions());
}

#[test]
fn eval_tag_prefix_expansion_at_the_cap_warns_only_if_more_remained() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(2),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("both matches are opened");
    assert_eq!(drain_doc_ids(&mut it), vec![1, 2, 3]);
    drop(it);
    assert!(
        !fixture.reached_max_prefix_expansions(),
        "hasNext must be false once the scan is exactly exhausted at the cap"
    );
}

#[test]
fn eval_tag_prefix_expansion_cap_grows_the_iterator_array() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(20, 100),
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(20),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("all 20 generated values match");
    let ids = drain_doc_ids(&mut it);
    drop(it);
    assert_eq!(ids.len(), 20);
    assert!(!fixture.reached_max_prefix_expansions());
}

#[test]
fn eval_tag_suffix_trie_expansion_stops_at_the_cap() {
    // The cap in `Query_EvalTagPrefixNode`'s suffix-trie branch counts admitted
    // *terms*, not matched doc ids, so a fixture pinning it must not let doc
    // count and term count be conflated. `GetList_SuffixTrieMap` does not pin
    // which of the shared "na" node's two terms (`TAG_BANANA`, `TAG_SULTANA`)
    // the cap admits, so both are given exactly one doc here -- unlike the
    // sibling `..._exits_silently` test below, which only needs one term and
    // so is free to use `TAG_BANANA`'s default two. That keeps `ids.len() ==
    // 1` true however the pick falls, while `TAG_BANANA`'s standalone "nana"
    // node is still left over to make `hasNext` (and so the warning) fire.
    let values = values(&[(TAG_BANANA, &[3]), (TAG_SULTANA, &[14])]);
    let mut fixture = TagFixture::new(TagOptions {
        values,
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"na",
            prefix: true,
            suffix: true,
        }],
        max_prefix_expansions: Some(1),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("one of the two na values matches");
    let ids = drain_doc_ids(&mut it);
    drop(it);
    assert_eq!(ids.len(), 1, "the cap fires from the inner loop");
    assert!(fixture.reached_max_prefix_expansions());
}

#[test]
fn eval_tag_suffix_trie_expansion_at_the_cap_exits_silently() {
    let mut fixture = TagFixture::new(TagOptions {
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"na",
            prefix: true,
            suffix: true,
        }],
        max_prefix_expansions: Some(1),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("banana matches on an");
    assert_eq!(drain_doc_ids(&mut it), vec![3, 4]);
    drop(it);
    assert!(
        !fixture.reached_max_prefix_expansions(),
        "the inner list is exhausted before the cap can be re-tested"
    );
}

#[test]
fn eval_tag_wildcard_expansion_stops_at_the_cap() {
    // The brute-force loop in `Query_EvalTagWildcardNode` iterates the values
    // trie in sorted order and caps the number of admitted *terms*, not
    // matched doc ids. `b"aa"` sorts before [`TAG_BANANA`] in that trie, so it
    // is the one term the cap admits; using the default fruit set here would
    // admit `TAG_APPLE` instead, which carries two docs and would make
    // `ids.len()` 2, not 1.
    let mut fixture = TagFixture::new(TagOptions {
        values: values(&[(b"aa", &[7]), (TAG_BANANA, &[3, 4])]),
        children: vec![Child::WildcardQuery(b"*a*")],
        max_prefix_expansions: Some(1),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("at least one value matches");
    let ids = drain_doc_ids(&mut it);
    drop(it);
    assert_eq!(ids, vec![7], "the cap admits only the first trie match");
    assert!(fixture.reached_max_prefix_expansions());
}

#[test]
fn eval_tag_wildcard_suffix_trie_expansion_stops_at_the_cap() {
    // Both `TAG_SULTANA` and `TAG_BANANA` share the "ana" suffix-trie node
    // matched by `*an*`; `_getWildcardArray`'s own (off-by-one) cap check lets
    // both through into its result array, but `Query_EvalTagWildcardNode`'s
    // own `itsSz >= maxPrefixExpansions` check then admits only the first
    // term of that array as a reader. Which of the two comes first is an
    // insertion-order detail `_getWildcardArray` does not pin, so both are
    // given exactly one doc here: whichever term the cap admits, `ids.len()`
    // stays 1, pinning "one term admitted" rather than an incidental doc
    // count that only held for one specific pick.
    let mut fixture = TagFixture::new(TagOptions {
        values: values(&[(TAG_SULTANA, &[14]), (TAG_BANANA, &[3])]),
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::WildcardQuery(b"*an*")],
        max_prefix_expansions: Some(1),
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("at least one value matches");
    let ids = drain_doc_ids(&mut it);
    drop(it);
    assert_eq!(ids.len(), 1);
    assert!(fixture.reached_max_prefix_expansions());
}

#[test]
fn eval_tag_expansion_under_the_cap_sets_no_warning() {
    let mut fixture = TagFixture::new(TagOptions {
        children: vec![Child::Prefix {
            pattern: b"ap",
            prefix: true,
            suffix: false,
        }],
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("apple and apricot match");
    drain(&mut it);
    drop(it);
    assert!(!fixture.reached_max_prefix_expansions());
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

#[test]
fn eval_tag_prefix_expansion_stops_at_the_deadline() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::Expired,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the scan starts before it times out");
    let ids = drain_doc_ids(&mut it);
    assert!(!ids.is_empty());
    assert!((ids.len() as u32) < TIMEOUT_VALUES);
}

#[test]
fn eval_tag_prefix_expansion_that_times_out_reports_nothing() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::Expired,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the scan starts before it times out");
    drain(&mut it);
    drop(it);
    assert!(
        !fixture.reached_max_prefix_expansions(),
        "a truncated expansion sets no warning"
    );
    assert!(fixture.ctx.status().is_ok());
}

#[test]
fn eval_tag_prefix_expansion_ignores_the_deadline_when_checks_are_skipped() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::ExpiredButSkipped,
        ..TagOptions::default()
    });
    let mut it = fixture
        .eval()
        .expect("skipTimeoutChecks disables the deadline");
    assert_eq!(drain_doc_ids(&mut it).len(), TIMEOUT_VALUES as usize);
}

#[test]
fn eval_tag_wildcard_expansion_stops_at_the_deadline() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        children: vec![Child::WildcardQuery(b"az*")],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::Expired,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the scan starts before it times out");
    let ids = drain_doc_ids(&mut it);
    assert!(!ids.is_empty());
    assert!((ids.len() as u32) < TIMEOUT_VALUES);
}

#[test]
fn eval_tag_suffix_trie_expansion_stops_at_the_deadline() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        field: TagField::IndexedWithSuffixTrie,
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: true,
        }],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::Expired,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("the walk starts before it times out");
    let ids = drain_doc_ids(&mut it);
    assert!(!ids.is_empty());
    assert!((ids.len() as u32) < TIMEOUT_VALUES);
}

#[test]
fn eval_tag_expansion_without_a_deadline_is_complete() {
    let mut fixture = TagFixture::new(TagOptions {
        values: generated(TIMEOUT_VALUES, 100),
        children: vec![Child::Prefix {
            pattern: b"az",
            prefix: true,
            suffix: false,
        }],
        max_prefix_expansions: Some(TIMEOUT_VALUES + 1),
        timeout: Timeout::Unlimited,
        ..TagOptions::default()
    });
    let mut it = fixture.eval().expect("every generated value matches");
    assert_eq!(drain_doc_ids(&mut it).len(), TIMEOUT_VALUES as usize);
}
