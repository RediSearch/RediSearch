/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`ProfilePrint`] implementations.

use redis_mock::reply::{ReplyValue, capture_replies};
use redis_reply::{RedisModuleCtx, Replier};
use rqe_iterators::{
    RQEIterator, UnionFullFlat,
    profile::ProfileCounters,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

/// Initialize mock and return a [`Replier`] ready for use.
fn init() -> Replier {
    redis_mock::init_redis_module_mock();
    // SAFETY: `init_redis_module_mock` replaces all `RedisModule_Reply*`
    // function pointers with mock implementations that record replies
    // without ever dereferencing the `RedisModuleCtx` pointer, so a
    // dangling non-null address is safe here.
    unsafe { Replier::new(std::ptr::dangling_mut::<RedisModuleCtx>()) }
}

/// Capture a single reply from a closure.
fn capture_single_reply(f: impl FnOnce()) -> ReplyValue {
    let mut replies = capture_replies(f);
    assert_eq!(
        replies.len(),
        1,
        "expected single reply, got {}",
        replies.len()
    );
    replies.pop().unwrap()
}

/// Print an iterator's profile and return the captured [`ReplyValue`].
fn print<I: ProfilePrint>(
    replier: &mut Replier,
    iter: &I,
    print_profile_clock: bool,
) -> ReplyValue {
    capture_single_reply(|| {
        let mut ctx = ProfilePrintCtx::new(false, print_profile_clock);
        let mut map = replier.map();
        iter.print_profile(&mut map, &mut ctx);
    })
}

/// Print an iterator's profile with counters injected.
fn print_with_counters<I: ProfilePrint>(
    replier: &mut Replier,
    iter: &I,
    counters: &ProfileCounters,
    wall_time_ns: u64,
) -> ReplyValue {
    capture_single_reply(|| {
        let base_ctx = ProfilePrintCtx::new(false, false);
        let mut ctx = base_ctx.with_counters(counters, wall_time_ns);
        let mut map = replier.map();
        iter.print_profile(&mut map, &mut ctx);
    })
}

// ── Leaf iterators ──────────────────────────────────────────────

#[test]
fn empty() {
    let mut replier = init();
    let reply = print(&mut replier, &rqe_iterators::Empty, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn wildcard() {
    let mut replier = init();
    let iter = rqe_iterators::Wildcard::new(100, 1.0);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn id_list_sorted() {
    let mut replier = init();
    let iter = rqe_iterators::IdList::<true>::new(vec![1u64, 2, 3]);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn id_list_unsorted() {
    let mut replier = init();
    let iter = rqe_iterators::IdList::<false>::new(vec![3u64, 1, 2]);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn leaf_with_counters() {
    let mut replier = init();
    let counters = ProfileCounters {
        read: 10,
        skip_to: 5,
        eof: true,
    };
    let reply = print_with_counters(&mut replier, &rqe_iterators::Empty, &counters, 0);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn leaf_with_clock() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let base_ctx = ProfilePrintCtx::new(false, true);
        let counters = ProfileCounters {
            read: 3,
            skip_to: 0,
            eof: false,
        };
        let mut ctx = base_ctx.with_counters(&counters, 1_500_000);
        let mut map = replier.map();
        rqe_iterators::Empty.print_profile(&mut map, &mut ctx);
    });
    insta::assert_debug_snapshot!(reply);
}

// ── Metric ──────────────────────────────────────────────────────

#[test]
fn metric_sorted_by_id() {
    let mut replier = init();
    let iter = rqe_iterators::metric::Metric::<true>::new(vec![1u64, 2, 3], vec![0.1, 0.2, 0.3]);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn metric_sorted_by_score() {
    let mut replier = init();
    let iter = rqe_iterators::metric::Metric::<false>::new(vec![1u64, 2, 3], vec![0.1, 0.2, 0.3]);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Profile wrapper ─────────────────────────────────────────────

#[test]
fn profile_wrapping_empty() {
    let mut replier = init();
    let child = rqe_iterators::Empty;
    let iter = rqe_iterators::profile::Profile::new(child);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn profile_wrapping_empty_after_reads() {
    let mut replier = init();
    let child = rqe_iterators::Empty;
    let mut iter = rqe_iterators::profile::Profile::new(child);
    // Read once to bump counters.
    let _ = iter.read();
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!("after_one_read", reply);

    // Second read: read=2, skip_to=0, eof=true → (2 + 0) - 1 = 1
    let _ = iter.read();
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!("after_two_reads", reply);
}

// ── Not ─────────────────────────────────────────────────────────

#[test]
fn not_with_child() {
    let mut replier = init();
    let child = rqe_iterators::Empty;
    let iter = rqe_iterators::not::Not::new(child, 100, 1.0, rqe_iterators::utils::NoTimeout);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn not_optimized_with_child() {
    let mut replier = init();
    let wcii = rqe_iterators::Wildcard::new(100, 1.0);
    let child = rqe_iterators::Empty;
    let iter = rqe_iterators::not_optimized::NotOptimized::new(
        wcii,
        child,
        100,
        1.0,
        rqe_iterators::utils::NoTimeout,
    );
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Optional ────────────────────────────────────────────────────

#[test]
fn optional_with_child() {
    let mut replier = init();
    let child = rqe_iterators::Empty;
    let iter = rqe_iterators::optional::Optional::new(100, 1.0, child);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn optional_optimized_with_child() {
    let mut replier = init();
    let wcii = rqe_iterators::Wildcard::new(100, 1.0);
    let child = rqe_iterators::Empty;
    let iter = rqe_iterators::optional_optimized::OptionalOptimized::new(wcii, child, 100, 1.0);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Intersection ────────────────────────────────────────────────

#[test]
fn intersection_two_children() {
    let mut replier = init();
    let children = vec![rqe_iterators::Empty, rqe_iterators::Empty];
    let iter = rqe_iterators::intersection::Intersection::new(children, 1.0, false);
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Union ───────────────────────────────────────────────────────

#[test]
fn union_full_print() {
    use ffi::QueryNodeType;
    use rqe_iterators::union_opaque::{UnionOpaque, UnionVariant};

    let mut replier = init();
    let children = vec![rqe_iterators::Empty, rqe_iterators::Empty];
    let flat = UnionFullFlat::new(children);
    let iter = UnionOpaque {
        variant: UnionVariant::FlatFull(flat),
        query_node_type: QueryNodeType::Union,
        query_string: std::ptr::null(),
    };
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn union_limited_non_union_type() {
    use ffi::QueryNodeType;
    use rqe_iterators::union_opaque::{UnionOpaque, UnionVariant};

    let mut replier = init();
    let children = vec![
        rqe_iterators::Empty,
        rqe_iterators::Empty,
        rqe_iterators::Empty,
    ];
    let flat = UnionFullFlat::new(children);
    let iter = UnionOpaque {
        variant: UnionVariant::FlatFull(flat),
        query_node_type: QueryNodeType::Tag,
        query_string: std::ptr::null(),
    };
    let reply = capture_single_reply(|| {
        let mut ctx = ProfilePrintCtx::new(true, false);
        let mut map = replier.map();
        iter.print_profile(&mut map, &mut ctx);
    });
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn union_limited_geo_prints_full() {
    use ffi::QueryNodeType;
    use rqe_iterators::union_opaque::{UnionOpaque, UnionVariant};

    let mut replier = init();
    let children = vec![rqe_iterators::Empty, rqe_iterators::Empty];
    let flat = UnionFullFlat::new(children);
    let iter = UnionOpaque {
        variant: UnionVariant::FlatFull(flat),
        query_node_type: QueryNodeType::Geo,
        query_string: std::ptr::null(),
    };
    let reply = capture_single_reply(|| {
        let mut ctx = ProfilePrintCtx::new(true, false);
        let mut map = replier.map();
        iter.print_profile(&mut map, &mut ctx);
    });
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn union_limited_lexrange_prints_full() {
    use ffi::QueryNodeType;
    use rqe_iterators::union_opaque::{UnionOpaque, UnionVariant};

    let mut replier = init();
    let children = vec![rqe_iterators::Empty];
    let flat = UnionFullFlat::new(children);
    let iter = UnionOpaque {
        variant: UnionVariant::FlatFull(flat),
        query_node_type: QueryNodeType::LexRange,
        query_string: std::ptr::null(),
    };
    let reply = capture_single_reply(|| {
        let mut ctx = ProfilePrintCtx::new(true, false);
        let mut map = replier.map();
        iter.print_profile(&mut map, &mut ctx);
    });
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn union_with_query_string() {
    use ffi::QueryNodeType;
    use rqe_iterators::union_opaque::{UnionOpaque, UnionVariant};

    let mut replier = init();
    let children = vec![rqe_iterators::Empty];
    let flat = UnionFullFlat::new(children);
    let query_str = std::ffi::CString::new("hello").unwrap();
    let iter = UnionOpaque {
        variant: UnionVariant::FlatFull(flat),
        query_node_type: QueryNodeType::Prefix,
        query_string: query_str.as_ptr(),
    };
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Term (inverted index) ───────────────────────────────────────

#[test]
fn term_with_query_term() {
    use ffi::{
        IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
        IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets,
    };
    use inverted_index::full::Full;
    use query_term::RSQueryTerm;

    let mut replier = init();
    let flags = IndexFlags_Index_StoreFreqs
        | IndexFlags_Index_StoreTermOffsets
        | IndexFlags_Index_StoreFieldFlags
        | IndexFlags_Index_StoreByteOffsets;
    let mut ii = inverted_index::InvertedIndex::<Full>::new(flags);
    let offsets: &[u8] = &[0, 1, 2, 3];
    let record = index_result::RSIndexResult::build_term()
        .borrowed_record(
            Some(RSQueryTerm::new("hello", 1, 0)),
            index_result::RSOffsetSlice::from_slice(offsets),
        )
        .doc_id(1)
        .field_mask(1)
        .frequency(1)
        .build();
    ii.add_record(&record).expect("add_record");
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(1, 1);
    let reader = ii.reader();
    // SAFETY: mock_ctx provides a valid RedisSearchCtx.
    let mut iter = unsafe {
        rqe_iterators::inverted_index::Term::new(
            reader,
            mock_ctx.sctx(),
            RSQueryTerm::new("hello", 1, 0),
            1.0,
            rqe_iterators::NoOpChecker,
        )
    };
    // Read once to populate the current result with the term.
    let _ = iter.read();
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Tag (inverted index) ────────────────────────────────────────

#[test]
fn tag_with_query_term() {
    use ffi::IndexFlags_Index_DocIdsOnly;
    use inverted_index::doc_ids_only::DocIdsOnly;
    use query_term::RSQueryTerm;
    use rqe_core::RS_FIELDMASK_ALL;

    let mut replier = init();
    let mut ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let record = index_result::RSIndexResult::build_term()
        .doc_id(1)
        .field_mask(RS_FIELDMASK_ALL)
        .build();
    ii.add_record(&record).expect("add_record");
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(1, 1);
    let reader = ii.reader();
    // SAFETY: mock_ctx provides a valid RedisSearchCtx. The TagIndex
    // pointer points to a zeroed struct, fine with NoOpChecker.
    let mut iter = unsafe {
        rqe_iterators::inverted_index::Tag::new(
            reader,
            mock_ctx.sctx(),
            mock_ctx.tag_index(),
            RSQueryTerm::new("my_tag", 0, 0),
            0.0,
            rqe_iterators::NoOpChecker,
        )
    };
    // Read once to populate the current result with the term.
    let _ = iter.read();
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Missing (inverted index) ────────────────────────────────────

#[test]
fn missing_without_field() {
    use ffi::IndexFlags_Index_DocIdsOnly;
    use inverted_index::doc_ids_only::DocIdsOnly;

    let mut replier = init();
    let ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let reader = ii.reader();
    // SAFETY: mock_ctx provides a valid RedisSearchCtx with a zeroed spec
    // (null fields → empty field name). field_index is 0 (unused with NoOpChecker).
    let iter = unsafe {
        rqe_iterators::inverted_index::Missing::new(
            reader,
            mock_ctx.sctx(),
            0,
            rqe_iterators::NoOpChecker,
        )
    };
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[cfg(not(miri))]
#[test]
fn missing_with_field() {
    use inverted_index::{doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding};
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    let _guard = GlobalGuard::default();
    let mut replier = init();
    let ctx = TestContext::missing(1..=3);
    let ii = DocIdsOnly::from_opaque(ctx.missing_inverted_index());
    let field_index = ctx.field_spec().index;
    // SAFETY: ctx provides a valid RedisSearchCtx with a valid spec,
    // missingFieldDict, and field_index.
    let iter = unsafe {
        rqe_iterators::inverted_index::Missing::new(
            ii.reader(),
            ctx.sctx,
            field_index,
            rqe_iterators::NoOpChecker,
        )
    };
    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

// ── Numeric (inverted index) ─────────────────────────────────────

/// Build a [`NumericIteratorVariant`] for profile print tests.
fn make_numeric_variant<'a>(
    idx: &'a numeric_range_tree::NumericIndex,
    ctx: &'a rqe_iterators_test_utils::MockContext,
    filter: Option<&'a inverted_index::NumericFilter>,
    range_min: f64,
    range_max: f64,
) -> rqe_iterators::NumericIteratorVariant<'a> {
    let checker = unsafe {
        rqe_iterators::FieldExpirationChecker::new(
            ctx.sctx(),
            field::FieldFilterContext {
                field: field::FieldMaskOrIndex::Index(0),
                predicate: field::FieldExpirationPredicate::Default,
            },
            0,
        )
    };
    rqe_iterators::NumericIteratorVariant::new(
        idx.reader(),
        filter,
        checker,
        None,
        range_min,
        range_max,
    )
}

#[test]
fn numeric_variant_unfiltered() {
    let mut replier = init();
    let mut idx = numeric_range_tree::NumericIndex::new(false);
    idx.add_record(
        &index_result::RSIndexResult::build_numeric(5.0)
            .doc_id(1)
            .build(),
    );
    let ctx = rqe_iterators_test_utils::MockContext::new(1, 1);
    let iter = make_numeric_variant(&idx, &ctx, None, 1.0, 10.0);

    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}

#[test]
fn numeric_variant_geo() {
    let mut replier = init();
    let mut idx = numeric_range_tree::NumericIndex::new(false);
    idx.add_record(
        &index_result::RSIndexResult::build_numeric(5.0)
            .doc_id(1)
            .build(),
    );
    let ctx = rqe_iterators_test_utils::MockContext::new(1, 1);

    // Encode two known coordinates as geohash scores.
    use decorum::R64;
    let se_coords =
        geo::hash::WGS84Coordinates::new(R64::assert(34.0), R64::assert(31.0)).expect("SE coords");
    let se_hash = geo::hash::encode_wgs84(se_coords, geo::hash::GEO_STEP_MAX);
    let nw_coords =
        geo::hash::WGS84Coordinates::new(R64::assert(35.0), R64::assert(32.0)).expect("NW coords");
    let nw_hash = geo::hash::encode_wgs84(nw_coords, geo::hash::GEO_STEP_MAX);
    let se_score = se_hash.bits as f64;
    let nw_score = nw_hash.bits as f64;

    let geo_filter = ffi::GeoFilter {
        fieldSpec: std::ptr::null(),
        lat: 0.0,
        lon: 0.0,
        radius: 1.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_M,
        numericFilters: std::ptr::null_mut(),
    };
    let filter = inverted_index::NumericFilter {
        geo_filter: &geo_filter as *const _ as *const _,
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        min_inclusive: true,
        max_inclusive: true,
        ..Default::default()
    };
    let iter = make_numeric_variant(&idx, &ctx, Some(&filter), se_score, nw_score);

    let reply = print(&mut replier, &iter, false);
    insta::assert_debug_snapshot!(reply);
}
