/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_VECTOR → vector similarity iterator.
//!
//! The node settles the score (distance) field between the two syntaxes that can
//! name it and reserves a metric request for it, then runs the similarity search
//! — and what the search answers with decides whether that request gets bound to
//! a lookup key or stays as reserved.
//!
//! [`VectorOptions`] holds every knob the tests vary; each field says what it
//! controls and why both of its settings are worth covering.
//!
//! Disabled under Miri: [`TestContext`] calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use std::ffi::{c_char, c_void};

use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use query::mock::MockQueryNode;
use query_error::QueryErrorCode;
use query_eval::{Config, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::{QueryNodeFlags, QueryNodeType};
use rqe_core::DocId;
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{
    ContractChecker, GlobalGuard, TestContext, assert_current_contract,
};

/// The name of the vector field the fixture's query targets.
///
/// Bytes rather than a [`str`], as every name here is: field names are
/// binary-safe, and only a test that can express a name no [`str`] can holds
/// the evaluation to treating them that way.
const VECTOR_FIELD: &[u8] = b"v";

/// The score field a vector query on [`VECTOR_FIELD`] yields under when the user
/// names none. Spelled out rather than formatted from it, so a change to the
/// `__<field>_score` scheme has to be made here deliberately.
const DEFAULT_SCORE_FIELD: &[u8] = b"__v_score";

/// A user-chosen distance field name, distinct from [`DEFAULT_SCORE_FIELD`] so a
/// test can tell "the query kept the default" from "the query took this one".
const USER_SCORE_FIELD: &[u8] = b"myscore";

/// A field name that is not valid UTF-8, and the default score field derived
/// from it.
///
/// `0xFF` appears in no UTF-8 sequence, so a name carrying it survives a lossy
/// decode only as the replacement character — which is what makes these two a
/// test of whether the name is handled as bytes.
const BINARY_FIELD: &[u8] = b"v\xff";
const BINARY_DEFAULT_SCORE_FIELD: &[u8] = b"__v\xff_score";

/// A field name with an interior NUL, and the default score field derived from
/// it.
///
/// The name is stored with its full length, but the default splices it in as a
/// C string, so everything from the NUL on is dropped — leaving the same default
/// [`VECTOR_FIELD`] gets.
const TRUNCATED_FIELD: &[u8] = b"v\0hidden";
const TRUNCATED_DEFAULT_SCORE_FIELD: &[u8] = DEFAULT_SCORE_FIELD;

/// Dimensionality of the vectors the fixture indexes and queries with.
///
/// Arbitrary; what matters is that the query blob and the index agree on it,
/// since the search rejects a blob that is not exactly this many floats wide.
const DIM: usize = 4;

/// How many vectors the tests that need a live index put in it. Comfortably
/// covers what [`Child::Numeric`] can match, so a hybrid search intersects the
/// two to something non-empty.
const INDEXED_VECTORS: usize = 8;

/// How many neighbours the fixture's KNN query asks for. Fewer than
/// [`INDEXED_VECTORS`], so a test can tell the neighbours apart from the whole
/// index.
const KNN_K: usize = 3;

/// What the query asks the index for, once [`VectorOptions::indexed_vectors`]
/// has given it one to ask.
///
/// The two forms differ in the kind of iterator they come back with, which is
/// what the node's binding step has to cope with.
#[derive(Default, Clone, Copy)]
enum Search {
    /// The `[KNN <k> @field $blob]` form, answered by a hybrid iterator whether
    /// or not the node has a child.
    #[default]
    Knn,
    /// The `[VECTOR_RANGE <radius> @field $blob]` form, answered by a metric
    /// iterator that runs the query lazily on its first read.
    Range {
        /// How far from the query vector to match. Generous in the tests that
        /// want an answer, so the index is certain to give one; negative in the
        /// one that wants the search to reject the query instead.
        radius: f64,
        /// The two [`RangeOrder`]s produce *different iterator types*, which is
        /// why this is a knob at all: the node's binding step accepts a family
        /// of metric types, and only naming both halves of it here holds the
        /// family together.
        order: RangeOrder,
    },
}

/// The order a [`Search::Range`] query asks its matches back in.
#[derive(Clone, Copy)]
enum RangeOrder {
    /// Ascending document id, as a range query under a filter is parsed to
    /// because the intersection above it skips.
    ById,
    /// Nearest first, as an unfiltered range query is parsed to.
    ByScore,
}

impl RangeOrder {
    const fn as_ffi(self) -> ffi::VecSimQueryReply_Order {
        match self {
            Self::ById => ffi::VecSimQueryReply_Order_BY_ID,
            Self::ByScore => ffi::VecSimQueryReply_Order_BY_SCORE,
        }
    }
}

/// The child to attach to the node, chosen for the iterator it evaluates to —
/// which is what decides how far the search gets and what becomes of the child.
#[derive(Clone, Copy)]
enum Child {
    /// A numeric node filtering on the inclusive range, as `<filter>=>[KNN ...]`
    /// produces. A range missing every indexed value yields no iterator at all
    /// and short-circuits before the search; one that overlaps them yields a
    /// real iterator for the search to take or decline.
    Numeric(f64, f64),
    /// A `QN_NULL` node — what a query made only of stopwords parses to. Unlike
    /// a numeric filter that matches nothing, it evaluates to an *empty
    /// iterator* rather than to no iterator, which the search hands straight
    /// back in place of a vector one.
    Empty,
}

/// A flat (brute-force) L2 index over `count` vectors, document `i` holding a
/// vector of [`DIM`] copies of `i`.
///
/// That shape is what spares the tests any distance arithmetic: under L2 the
/// distance to the corner the fixture queries falls as the document id rises, so
/// the nearest neighbours are simply the highest ids.
///
/// Flat rather than HNSW because it needs no tuning to be correct at this size,
/// and the node under test only ever sees the iterator that comes back.
fn flat_index(count: usize) -> *mut ffi::VecSimIndex {
    let params = ffi::VecSimParams {
        algo: ffi::VecSimAlgo_VecSimAlgo_BF,
        algoParams: ffi::AlgoParams {
            bfParams: ffi::BFParams {
                type_: ffi::VecSimType_VecSimType_FLOAT32,
                dim: DIM,
                metric: ffi::VecSimMetric_VecSimMetric_L2,
                multi: false,
                initialCapacity: count,
                blockSize: 0,
            },
        },
        logCtx: std::ptr::null_mut(),
    };
    // SAFETY: `params` is fully initialised and describes a flat index, whose
    // `bfParams` is the active union member.
    let index = unsafe { ffi::VecSimIndex_New(&params) };
    assert!(!index.is_null(), "vector index creation failed");

    for i in 1..=count {
        let vector = [i as f32; DIM];
        // SAFETY: `vector` holds exactly `DIM` floats, matching the type and
        // dimensionality `index` was created with, and outlives the call.
        unsafe { ffi::VecSimIndex_AddVector(index, vector.as_ptr().cast(), i) };
    }
    // A rejected add would otherwise surface as a mismatch in some test's
    // expected document ids, which says nothing about where it went wrong.
    //
    // SAFETY: `index` is a live index this function owns.
    assert_eq!(
        unsafe { ffi::VecSimIndex_IndexSize(index) },
        count,
        "every vector must have made it into the index"
    );
    index
}

/// Copy `s` into a NUL-terminated buffer owned by the module allocator.
///
/// The evaluator both takes ownership of these strings and frees the ones it
/// replaces, always through the module allocator — so a test-owned string handed
/// to it must come from there too, not from Rust's allocator.
fn module_string(s: &[u8]) -> *mut c_char {
    let ptr = redis_mock::allocator::alloc_shim(s.len() + 1).cast::<c_char>();
    assert!(!ptr.is_null(), "allocation failed");
    // SAFETY: `ptr` owns `s.len() + 1` writable bytes, so the copy fits and
    // leaves room for the terminator written below; the buffer cannot overlap
    // `s`, which lives in the caller's allocation.
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr().cast::<c_char>(), ptr, s.len());
        *ptr.add(s.len()) = 0;
    }
    ptr
}

/// Free a string obtained from [`module_string`], if any.
///
/// # Safety
///
/// `ptr` must be null or a live allocation from the module allocator that
/// nothing else frees.
unsafe fn free_module_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        redis_mock::allocator::free_shim(ptr.cast::<c_void>());
    }
}

/// Read a NUL-terminated string the evaluator owns, as bytes.
///
/// # Safety
///
/// `ptr` must point to a live, NUL-terminated string.
unsafe fn read_module_string(ptr: *const c_char) -> Vec<u8> {
    assert!(!ptr.is_null(), "expected a string, got NULL");
    // SAFETY: the caller guarantees a live, NUL-terminated string.
    unsafe { std::ffi::CStr::from_ptr(ptr) }.to_bytes().to_vec()
}

/// How to build a [`VectorFixture`] — the two ways a query can name the distance
/// field, plus whether the name is meant to stay hidden from the response.
///
/// [`Default`] describes a query that names no distance field at all, leaving
/// each test to set only the knob it exercises.
#[derive(Default)]
struct VectorOptions {
    /// The name already stored on the vector query, as the `KNN ... AS <name>`
    /// syntax leaves it (and as the parser leaves it, set to
    /// [`DEFAULT_SCORE_FIELD`], for a query that names nothing).
    score_field: Option<&'static [u8]>,
    /// The name carried on the node's options, as the `$YIELD_DISTANCE_AS`
    /// attribute leaves it. Set together with [`score_field`](Self::score_field),
    /// this is the case where the query named the field twice.
    dist_field: Option<&'static [u8]>,
    /// Whether the node is flagged to keep the distance field out of the
    /// response, which the reserved metric request must record.
    hide_distance_field: bool,
    /// The child to attach to the node. [`None`] is the child-less
    /// `*=>[KNN ...]` shape and the common one in production; a [`Child`] builds
    /// the `<filter>=>[KNN ...]` shape, which decides whether evaluation reaches
    /// the search at all and what becomes of the child iterator if it does.
    child: Option<Child>,
    /// The name of the vector field the query targets, which the default score
    /// field is derived from. [`None`] uses [`VECTOR_FIELD`].
    ///
    /// Stored with its full length, so a name may carry an interior NUL.
    field_name: Option<&'static [u8]>,
    /// How many vectors the queried field's index holds. [`None`] leaves the
    /// field without an index at all.
    ///
    /// This decides whether the search answers with an iterator, and so whether
    /// the node gets as far as binding its metric request to one. Without an
    /// index the search declines, and evaluation ends yielding nothing — but
    /// only *after* the score field and the request are settled, which is why
    /// the tests that care about neither use that cheaper shape.
    indexed_vectors: Option<usize>,
    /// What that index is asked for. Ignored when there is no index to ask.
    search: Search,
}

/// Owns everything a `QN_VECTOR` evaluation borrows or mutates.
///
/// The field spec and the field's name are stand-ins, holding only what the
/// paths under test read: the name the default score field is derived from, the
/// type the search checks before opening the index, and — when
/// [`VectorOptions::indexed_vectors`] asks for one — that index itself. The rest
/// is zeroed, which is a valid empty state for every field the search consults.
struct VectorFixture {
    /// Registers the process-exit cleanup of the global spec dictionaries shared
    /// by every [`TestContext`]. Carried purely for that side effect.
    _guard: GlobalGuard,
    /// The evaluation context under test. Also carries the query status and the
    /// metric requests, which is where the assertions read the node's effects.
    ///
    /// Declared before the [`TestContext`] it borrows from, so that it is the
    /// first of the two to drop.
    ctx: QueryEvalContext,
    /// Owns the numeric index backing the child node, and the
    /// [`ffi::QueryEvalCtx`] that [`ctx`](Self::ctx) wraps.
    _context: TestContext,
    /// The `QN_VECTOR` node being evaluated.
    node: MockQueryNode,
    /// The node's child, when it has one. Kept alive because the node holds a
    /// raw pointer to it.
    _child: Option<MockQueryNode>,
    /// The child's filter. Boxed for a stable address, since the child points
    /// at it.
    _filter: Option<Box<NumericFilter>>,
    /// The vector query the node points at, holding the score field the
    /// evaluation resolves. Boxed for a stable address.
    vq: Box<ffi::VectorQuery>,
    /// The blob [`vq`](Self::vq) searches for, when it searches at all. Kept
    /// alive because the query addresses it rather than owning it.
    _query_vector: Box<[f32]>,
    /// The vector field's spec, named by [`field_name`](Self::field_name).
    /// Boxed for a stable address, since [`vq`](Self::vq) points at it.
    _field_spec: Box<ffi::FieldSpec>,
    /// The field's vector index, or null when the field carries none. Owned by
    /// the fixture and freed on drop.
    index: *mut ffi::VecSimIndex,
    /// What the query asks that index for. Kept because [`eval`](Self::eval)
    /// needs the yield order to check the iterator against the right contract.
    search: Search,
    /// The field's name, in the obfuscation-aware form a spec stores. Owned by
    /// the fixture and freed on drop.
    field_name: *mut ffi::HiddenString,
}

impl VectorFixture {
    fn new(opts: VectorOptions) -> Self {
        let _guard = GlobalGuard::default();

        // Documents 1..=3 with numeric values 1.0, 2.0, 3.0 — the index the
        // child node filters over.
        let records = (1u64..=3).map(|i| RSIndexResult::build_numeric(i as f64).doc_id(i).build());
        let context = TestContext::numeric(records, false);

        // SAFETY: `context.qctx()` returns a valid, exclusively-owned
        // `QueryEvalCtx` (with real `status`, `config` and metric-request head),
        // upholding the `QueryEvalContext::new` invariants.
        let ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let (mut child, filter) = match opts.child {
            Some(Child::Numeric(min, max)) => {
                let mut filter = Box::new(NumericFilter {
                    min,
                    max,
                    min_inclusive: true,
                    max_inclusive: true,
                    field_spec: context.field_spec() as *const _,
                    ..Default::default()
                });
                let mut child = MockQueryNode::new(QueryNodeType::Numeric);
                child.opts_mut().weight = 1.0;
                child.set_numeric_filter(&mut *filter as *mut NumericFilter);
                (Some(child), Some(filter))
            }
            Some(Child::Empty) => (Some(MockQueryNode::new(QueryNodeType::Null)), None),
            None => (None, None),
        };

        let name = opts.field_name.unwrap_or(VECTOR_FIELD);
        // SAFETY: `name` is a static byte string, valid for `name.len()` bytes
        // for the duration of the call. `takeOwnership = true` makes the
        // `HiddenString` copy those bytes rather than borrow them; the copy is
        // released by the matching `HiddenString_Free(_, true)` on drop. The
        // length is passed explicitly, so a name holding a NUL keeps it.
        let field_name = unsafe { ffi::NewHiddenString(name.as_ptr().cast(), name.len(), true) };
        assert!(!field_name.is_null());

        // SAFETY: `ffi::FieldSpec` is a `#[repr(C)]` POD struct whose all-zero
        // bit pattern is a valid (empty) instance.
        let mut field_spec: Box<ffi::FieldSpec> = Box::new(unsafe { std::mem::zeroed() });
        field_spec.fieldName = field_name;
        // Opening the index asserts the field is a vector field before reading
        // the index off it, so the type has to be set even when there is none.
        field_spec.set_types(ffi::FieldType_INDEXFLD_T_VECTOR);
        let index = opts
            .indexed_vectors
            .map_or(std::ptr::null_mut(), flat_index);
        field_spec.__bindgen_anon_1.vectorOpts.vecSimIndex = index;

        // SAFETY: `ffi::VectorQuery` is a `#[repr(C)]` POD struct whose all-zero
        // bit pattern is a valid (empty) instance.
        let mut vq: Box<ffi::VectorQuery> = Box::new(unsafe { std::mem::zeroed() });
        vq.field = &*field_spec as *const ffi::FieldSpec;
        vq.scoreField = opts.score_field.map_or(std::ptr::null_mut(), module_string);

        // The corner the index's contents are arranged around; see `flat_index`.
        // Without an index the search stops before it ever reads the query, so
        // the zeroed value the fallback produces is never looked at.
        let mut query_vector: Box<[f32]> =
            vec![opts.indexed_vectors.unwrap_or(0) as f32; DIM].into_boxed_slice();
        if !index.is_null() {
            let vector = query_vector.as_mut_ptr().cast();
            let vec_len = std::mem::size_of_val(&*query_vector);
            match opts.search {
                Search::Knn => {
                    vq.type_ = ffi::VectorQueryType_VECSIM_QT_KNN;
                    vq.__bindgen_anon_1.knn = ffi::KNNVectorQuery {
                        vector,
                        vecLen: vec_len,
                        k: KNN_K,
                        order: ffi::VecSimQueryReply_Order_BY_SCORE,
                        shardWindowRatio: 1.0,
                        k_token_pos: 0,
                        k_token_len: 0,
                    };
                }
                Search::Range { radius, order } => {
                    vq.type_ = ffi::VectorQueryType_VECSIM_QT_RANGE;
                    vq.__bindgen_anon_1.range = ffi::RangeVectorQuery {
                        vector,
                        vecLen: vec_len,
                        radius,
                        order: order.as_ffi(),
                    };
                }
            }
        }

        let mut node = MockQueryNode::new(QueryNodeType::Vector);
        node.opts_mut().weight = 1.0;
        node.opts_mut().dist_field = opts.dist_field.map_or(std::ptr::null_mut(), module_string);
        if opts.hide_distance_field {
            node.opts_mut().flags |= QueryNodeFlags::HideVectorDistanceField;
        }
        node.set_vector_query(&mut *vq as *mut ffi::VectorQuery);
        if let Some(child) = &mut child {
            node.set_children(&[child.as_ptr()]);
        }

        Self {
            _guard,
            ctx,
            _context: context,
            node,
            _child: child,
            _filter: filter,
            vq,
            _query_vector: query_vector,
            _field_spec: field_spec,
            index,
            search: opts.search,
            field_name,
        }
    }

    /// Evaluate the node, releasing any iterator it builds, and report whether
    /// there was one — its kind, and every document it yields.
    ///
    /// The iterator is driven and released here rather than handed back because
    /// it borrows the context exclusively, and every assertion reads the context
    /// afterwards. Releasing it is also what the key-handle assertions need: an
    /// iterator clears its handle's validity flag on its way out, which is only
    /// observable once it has gone.
    ///
    /// Releasing means [`into_boxed`](query_eval::Evaluated::into_boxed) and then
    /// dropping — [`query_eval::Evaluated`] is a raw owning handle with no
    /// destructor, so letting it fall out of scope leaks the iterator.
    fn eval(&mut self) -> Option<(IteratorType, Vec<DocId>)> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeMut::new(self.node.as_non_null()) };
        let evaluated = eval_node(&mut self.ctx, node_ref, Config::default())?;

        // Only a range query asked back by id ascends; a KNN query and a
        // score-ordered range query both yield nearest-first.
        let mut it = match self.search {
            Search::Range {
                order: RangeOrder::ById,
                ..
            } => ContractChecker::new(evaluated.into_boxed()),
            _ => ContractChecker::new_unordered(evaluated.into_boxed()),
        };
        let kind = it.type_();
        Some((kind, assert_current_contract(&mut it)))
    }

    /// Evaluate the node, asserting the search never answered with an iterator.
    fn eval_yielding_nothing(&mut self) {
        assert!(
            self.eval().is_none(),
            "this fixture reaches no iterator: either the child matched nothing \
             to search, or the search declined to build one"
        );
    }

    /// The score field the evaluation settled on, or [`None`] if it left none.
    fn score_field(&self) -> Option<Vec<u8>> {
        (!self.vq.scoreField.is_null())
            // SAFETY: non-null checked, and the evaluator only ever stores a
            // live, NUL-terminated string here.
            .then(|| unsafe { read_module_string(self.vq.scoreField) })
    }

    /// The node's distance field, or [`None`] once the evaluation has moved it
    /// onto the vector query.
    fn dist_field(&mut self) -> Option<Vec<u8>> {
        let ptr = self.node.opts_mut().dist_field;
        // SAFETY: non-null checked, and the node only ever holds a live,
        // NUL-terminated string here.
        (!ptr.is_null()).then(|| unsafe { read_module_string(ptr) })
    }

    /// The metric requests the query has accumulated.
    fn metric_requests(&self) -> &[rlookup::MetricRequest<'_>] {
        self.ctx.metric_requests()
    }
}

impl Drop for VectorFixture {
    fn drop(&mut self) {
        // SAFETY: each pointer below is either null or the fixture's own live
        // allocation, freed exactly once here. The score field is whichever
        // string the evaluation left on the vector query — the one built here or
        // the distance field it moved over — and the node's distance field is
        // null unless the evaluation left it in place.
        unsafe {
            free_module_string(self.vq.scoreField);
            free_module_string(self.node.opts_mut().dist_field);
            ffi::HiddenString_Free(self.field_name, true);
            if !self.index.is_null() {
                // Every iterator the search built over this index was released
                // as `eval` returned, so nothing is still reading it.
                ffi::VecSimIndex_Free(self.index);
            }
        }
    }
}

/// Assert `fixture` reserved exactly one metric request under `name` and bound a
/// lookup-key handle to the iterator that yields it.
///
/// Reserving and binding are two steps: the name is recorded before the search
/// runs, the handle only once the search answers with an iterator to attach it
/// to. Both must have happened, and the iterator must have been pointed back at
/// the handle — which is what clears its validity flag when it is freed, as it
/// has been by the time this reads it.
fn assert_bound_metric_request(fixture: &VectorFixture, name: &[u8]) {
    let requests = fixture.metric_requests();
    assert_eq!(requests.len(), 1);
    // SAFETY: the request names the score field, a live string owned by the
    // vector query.
    assert_eq!(unsafe { read_module_string(requests[0].metric_name) }, name);

    let handle = requests[0].key_handle;
    assert!(
        !handle.is_null(),
        "an iterator that yields a metric must be bound to a lookup-key handle"
    );
    // SAFETY: non-null checked; the handle is owned by the request and outlives
    // the iterator that was pointed at it.
    let is_valid = unsafe { (*handle).is_valid };
    assert!(
        !is_valid,
        "freeing the iterator must invalidate the handle, or the back-reference \
         was never wired up"
    );
}

#[test]
fn eval_vector_moves_the_distance_field_onto_the_query() {
    // The `$YIELD_DISTANCE_AS` attribute alone: with no name on the query yet,
    // the node's is moved over wholesale.
    let mut fixture = VectorFixture::new(VectorOptions {
        dist_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(
        fixture.dist_field(),
        None,
        "the node must give up ownership of the name it moved"
    );
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_distance_field_overrides_the_default_score_field() {
    // Both syntaxes named the field, but the query only carries the name the
    // parser defaults to — so the user named it exactly once, and the explicit
    // name silently replaces the default.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(DEFAULT_SCORE_FIELD),
        dist_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(fixture.dist_field(), None);
    assert_eq!(
        fixture.ctx.status().code(),
        QueryErrorCode::Ok,
        "overriding the default name is not naming the field twice"
    );
}

#[test]
fn eval_vector_default_score_field_is_recognised_case_insensitively() {
    // The same override with the default name in a different case: still the
    // default, so still an override. A case-sensitive comparison would report a
    // duplicate instead.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(b"__V_SCORE"),
        dist_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_distance_field_named_twice_is_an_error() {
    // The query's name is not the default the parser would have left, so the
    // user really did name the field twice — rejected rather than silently
    // resolved in favour of one of them.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(b"score"),
        dist_field: Some(b"score2"),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::DupField);
    let private = fixture
        .ctx
        .status()
        .private_message()
        .expect("a set error must have a private message")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        private.contains("Distance field was specified twice for vector query: score and score2"),
        "the message must name both fields, got {private:?}"
    );
    assert!(
        private.starts_with("SEARCH_FIELD_DUP "),
        "the private message keys the Redis error stat and must keep the \
         error-code prefix, got {private:?}"
    );

    // Neither name is moved and neither is freed, so the AST still owns exactly
    // what it owned before.
    assert_eq!(fixture.score_field().as_deref(), Some(&b"score"[..]));
    assert_eq!(fixture.dist_field().as_deref(), Some(&b"score2"[..]));
    assert!(
        fixture.metric_requests().is_empty(),
        "a rejected query must not reserve a metric request"
    );
}

#[test]
fn eval_vector_reserves_a_metric_request_for_the_score_field() {
    // The request is reserved up front, under the resolved score field. Its
    // lookup-key slot stays empty because no iterator was built to fill it, and
    // that emptiness is what tells the AST teardown there is no handle to free.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    let requests = fixture.metric_requests();
    assert_eq!(requests.len(), 1);
    // SAFETY: the request names the score field, a live string owned by the
    // vector query.
    let name = unsafe { read_module_string(requests[0].metric_name) };
    assert_eq!(name, USER_SCORE_FIELD);
    assert!(
        requests[0].key_handle.is_null(),
        "a request whose iterator was never built must carry no lookup handle"
    );
    assert!(
        !requests[0].is_internal,
        "a user-named distance field is part of the response"
    );
}

#[test]
fn eval_vector_hidden_distance_field_reserves_an_internal_metric_request() {
    // The node is flagged to hide the distance field, as it is for the name the
    // parser generates and the user never asked for, so the request is marked
    // internal: computed, but kept out of the response.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(DEFAULT_SCORE_FIELD),
        hide_distance_field: true,
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    let requests = fixture.metric_requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].is_internal);
}

#[test]
fn eval_vector_without_a_score_field_reserves_no_metric_request() {
    // Nothing to bind to a lookup key, so nothing is reserved.
    let mut fixture = VectorFixture::new(VectorOptions::default());
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field(), None);
    assert!(fixture.metric_requests().is_empty());
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_with_a_child_that_yields_nothing_yields_nothing() {
    // A child that matches nothing leaves nothing to search, so the node yields
    // nothing without reaching the search at all — but the score field and its
    // request are still settled, both happening before the child is evaluated.
    let mut fixture = VectorFixture::new(VectorOptions {
        dist_field: Some(USER_SCORE_FIELD),
        child: Some(Child::Numeric(10.0, 20.0)),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(fixture.dist_field(), None);
    assert_eq!(fixture.metric_requests().len(), 1);
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_frees_the_child_iterator_the_search_declined_to_take() {
    // A child that does match, so the child iterator reaches the search — which
    // declines it for want of a vector index, leaving it owned by nobody but the
    // node. Observably indistinguishable from the test above; what it adds is the
    // leak if the node forgets to free it, which only a sanitized run reports.
    let mut fixture = VectorFixture::new(VectorOptions {
        dist_field: Some(USER_SCORE_FIELD),
        child: Some(Child::Numeric(1.0, 3.0)),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(fixture.dist_field(), None);
    assert_eq!(fixture.metric_requests().len(), 1);
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_binds_the_metric_request_to_the_hybrid_iterator_a_knn_query_yields() {
    // An indexed field, so the search answers with an iterator — which is what
    // the reserved request was waiting for. The node allocates the lookup-key
    // handle, stores it on the request, and points the iterator back at it so the
    // two agree on when the key stops being reachable.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        indexed_vectors: Some(INDEXED_VECTORS),
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must answer with an iterator");
    assert_eq!(kind, IteratorType::Hybrid);
    // The `KNN_K` documents closest to the query corner, closest first.
    assert_eq!(ids, [8, 7, 6]);

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
    assert_bound_metric_request(&fixture, USER_SCORE_FIELD);
}

#[test]
fn eval_vector_binds_the_metric_request_to_the_metric_iterator_a_range_query_yields() {
    // The same binding against the other kind of iterator. A range query keeps
    // its lookup key elsewhere than a hybrid one, so the node reaches it through
    // a different accessor and setter — the arm the KNN tests never take. What
    // ends up on the request has to be indistinguishable.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        indexed_vectors: Some(INDEXED_VECTORS),
        search: Search::Range {
            radius: 1e6,
            order: RangeOrder::ById,
        },
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must answer with an iterator");
    assert_eq!(kind, IteratorType::MetricLazySortedById);
    // A radius that reaches every indexed document, walked in the ascending
    // doc-id order this iterator was asked for.
    assert_eq!(ids, (1..=INDEXED_VECTORS as DocId).collect::<Vec<_>>());

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
    assert_bound_metric_request(&fixture, USER_SCORE_FIELD);
}

#[test]
fn eval_vector_hands_the_child_iterator_to_a_search_that_takes_it() {
    // The indexed counterpart of the declined-child test: the search accepts the
    // child iterator and folds it into the hybrid one it returns, which owns it
    // from then on. The node must not also free it.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        child: Some(Child::Numeric(1.0, 3.0)),
        indexed_vectors: Some(INDEXED_VECTORS),
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must answer with an iterator");
    assert_eq!(kind, IteratorType::Hybrid);
    // Only what the child matched — a different answer than the same query gives
    // without one, so the search really did restrict itself rather than ignore it.
    assert_eq!(ids, [3, 2, 1]);

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
    assert_bound_metric_request(&fixture, USER_SCORE_FIELD);
}

#[test]
fn eval_vector_without_a_score_field_binds_nothing_to_the_iterator() {
    // The same successful search for a query that yields no distance: the node
    // must build the iterator and leave the list empty rather than reserve an
    // unnamed entry for it.
    let mut fixture = VectorFixture::new(VectorOptions {
        indexed_vectors: Some(INDEXED_VECTORS),
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must answer with an iterator");
    assert_eq!(kind, IteratorType::Hybrid);
    // Yielding no distance changes nothing about which documents match.
    assert_eq!(ids, [8, 7, 6]);

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
    assert!(fixture.metric_requests().is_empty());
}

#[test]
fn eval_vector_binds_no_handle_to_an_iterator_that_yields_no_distance() {
    // A child that evaluates to an empty iterator rather than to none at all, so
    // the search hands that very iterator back in place of a vector one. The
    // node, which only sees "an iterator came back", must notice this one yields
    // no distance and leave the request unbound — binding a handle would promise
    // a lookup key nothing will ever fill in.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        child: Some(Child::Empty),
        indexed_vectors: Some(INDEXED_VECTORS),
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must hand the empty child back");
    assert_eq!(kind, IteratorType::Empty);
    assert!(ids.is_empty());

    let requests = fixture.metric_requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].key_handle.is_null(),
        "an iterator that yields no distance must have no handle bound to it"
    );
    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
}

#[test]
fn eval_vector_keeps_the_error_a_declining_search_reported() {
    // The other way the search can decline: it rejects the query, here for a
    // negative radius. Unlike a missing index this leaves an error behind, which
    // the node must pass through untouched rather than overwrite or clear — while
    // still disposing of the child iterator it handed over.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        child: Some(Child::Numeric(1.0, 3.0)),
        indexed_vectors: Some(INDEXED_VECTORS),
        search: Search::Range {
            radius: -1.0,
            order: RangeOrder::ById,
        },
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Inval);
    let private = fixture
        .ctx
        .status()
        .private_message()
        .expect("a set error must have a private message")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        private.contains("negative radius"),
        "the node must not replace the search's own message, got {private:?}"
    );

    // Reserved before the search ran and left unbound because it never answered
    // — the same shape as when there was no index to ask.
    let requests = fixture.metric_requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].key_handle.is_null());
}

#[test]
fn eval_vector_binds_the_metric_request_to_a_score_ordered_range_iterator() {
    // The same range query asked back nearest-first: a different iterator type
    // again. A type dropped from the family the binding step accepts would bind
    // nothing and lose the distance from the response, while every other test
    // here still passed.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(USER_SCORE_FIELD),
        indexed_vectors: Some(INDEXED_VECTORS),
        search: Search::Range {
            radius: 1e6,
            order: RangeOrder::ByScore,
        },
        ..VectorOptions::default()
    });
    let (kind, ids) = fixture
        .eval()
        .expect("the search must answer with an iterator");
    assert_eq!(kind, IteratorType::MetricLazySortedByScore);
    // Nearest-first, which for this data is descending document id — the reverse
    // of the by-id counterpart.
    assert_eq!(
        ids,
        (1..=INDEXED_VECTORS as DocId).rev().collect::<Vec<_>>()
    );

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::Ok);
    assert_bound_metric_request(&fixture, USER_SCORE_FIELD);
}

#[test]
fn eval_vector_derives_the_default_score_field_from_a_binary_field_name() {
    // Deriving the default splices the name in verbatim, so the stored name *is*
    // the default and the explicit one overrides it. Comparing the two after a
    // lossy decode would stop matching, and the query would be rejected as naming
    // the field twice instead.
    let mut fixture = VectorFixture::new(VectorOptions {
        field_name: Some(BINARY_FIELD),
        score_field: Some(BINARY_DEFAULT_SCORE_FIELD),
        dist_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(fixture.dist_field(), None);
    assert_eq!(
        fixture.ctx.status().code(),
        QueryErrorCode::Ok,
        "the stored name is the default derived from this field, so naming the \
         distance field again overrides it rather than duplicating it"
    );
}

#[test]
fn eval_vector_default_score_field_stops_at_a_nul_in_the_field_name() {
    // Splicing the name in as a C string cuts it at the NUL, leaving the default
    // a plain-named field would get.
    let mut fixture = VectorFixture::new(VectorOptions {
        field_name: Some(TRUNCATED_FIELD),
        score_field: Some(TRUNCATED_DEFAULT_SCORE_FIELD),
        dist_field: Some(USER_SCORE_FIELD),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.score_field().as_deref(), Some(USER_SCORE_FIELD));
    assert_eq!(
        fixture.ctx.status().code(),
        QueryErrorCode::Ok,
        "keeping the bytes past the NUL would derive a different default, and \
         the override would be read as a duplicate"
    );
}

#[test]
fn eval_vector_duplicate_error_reports_binary_names_verbatim() {
    // Two names that differ only in a byte no UTF-8 sequence contains. Decoding
    // them would collapse both to the same replacement character, leaving an
    // error that names one field twice, so both must reach the message verbatim.
    let mut fixture = VectorFixture::new(VectorOptions {
        score_field: Some(b"score\xfe"),
        dist_field: Some(b"score\xff"),
        ..VectorOptions::default()
    });
    fixture.eval_yielding_nothing();

    assert_eq!(fixture.ctx.status().code(), QueryErrorCode::DupField);
    let private = fixture
        .ctx
        .status()
        .private_message()
        .expect("a set error must have a private message")
        .to_bytes()
        .to_vec();
    let expected: &[u8] =
        b"Distance field was specified twice for vector query: score\xfe and score\xff";
    assert!(
        private.windows(expected.len()).any(|w| w == expected),
        "the message must carry both names byte for byte, got {:?}",
        String::from_utf8_lossy(&private)
    );
}
