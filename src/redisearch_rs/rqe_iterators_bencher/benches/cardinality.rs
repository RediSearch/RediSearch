/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Measure cardinality access separately from iterator construction and traversal.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use query_types::QueryNodeType;
use rqe_iterators::{
    Intersection, RQEIterator, UnionFullFlat, UnionFullHeap,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::{ProfileChildren, RQEIteratorWrapper},
    profile_print::ProfilePrint,
};
use rqe_iterators_bencher as _;

/// Shared document population passed to each estimate in a tree.
const TOTAL_DOCS: usize = 1_000_000;
/// Widths cover small trees and the larger fanout commonly handled by heap unions.
const FANOUTS: [usize; 3] = [2, 8, 32];

/// Vary leaf cardinalities so the compound benchmarks traverse unequal estimates.
fn leaves(fanout: usize) -> Vec<IdListSorted<'static>> {
    (0..fanout)
        .map(|i| IdListSorted::new((1..=1_024 + i as u64 * 64).collect::<Vec<_>>()))
        .collect()
}

/// Compare the two methods on the same already-constructed iterator.
fn measure<I: RQEIterator<'static>>(c: &mut Criterion, label: &str, iterator: &I) {
    let mut group = c.benchmark_group(format!("Iterator - Cardinality - {label}"));
    group.bench_function("upper_bound", |b| {
        b.iter(|| black_box(black_box(iterator).num_estimated()));
    });
    group.bench_function("estimated_matches", |b| {
        b.iter(|| black_box(black_box(iterator).estimated_matches(black_box(TOTAL_DOCS))));
    });
    group.finish();
}

/// Preserve production callback dispatch at every level of the FFI tree.
fn wrap_compound<I>(iterator: I) -> CRQEIterator
where
    I: RQEIterator<'static> + ProfilePrint + ProfileChildren<'static>,
{
    let header = RQEIteratorWrapper::boxed_new_compound(iterator);
    // SAFETY: (1, 3, 4) boxed_new_compound allocated and initialized the header and
    // its callbacks for this concrete iterator. (2) This is its only owning handle.
    unsafe { CRQEIterator::new(header) }
}

/// Benchmark leaves, compounds, nested trees, and the C callback bridge.
fn benchmark_cardinality(c: &mut Criterion) {
    redis_mock::init_redis_module_mock();
    measure(
        c,
        "IdList",
        &IdListSorted::new((1..=1_024).collect::<Vec<_>>()),
    );
    for fanout in FANOUTS {
        let intersection = Intersection::new(leaves(fanout), 1.0, false);
        measure(c, &format!("Intersection/n={fanout}"), &intersection);

        let union_flat = UnionFullFlat::new(leaves(fanout));
        measure(c, &format!("UnionFlat/n={fanout}"), &union_flat);

        let union_heap = UnionFullHeap::new(leaves(fanout));
        measure(c, &format!("UnionHeap/n={fanout}"), &union_heap);

        let and_or = Intersection::new(
            (0..fanout)
                .map(|_| UnionFullFlat::new(leaves(fanout)))
                .collect(),
            1.0,
            false,
        );
        measure(c, &format!("AndOfOr/n={fanout}"), &and_or);

        let or_and = UnionFullHeap::new(
            (0..fanout)
                .map(|_| Intersection::new(leaves(fanout), 1.0, false))
                .collect(),
        );
        measure(c, &format!("OrOfAnd/n={fanout}"), &or_and);

        let ffi_tree = wrap_compound(Intersection::new(
            (0..fanout)
                .map(|_| {
                    let children = leaves(fanout)
                        .into_iter()
                        .map(CRQEIterator::from_rust_leaf)
                        .collect();
                    let header = rqe_iterators::union_opaque::build_union(
                        children,
                        false,
                        usize::MAX,
                        QueryNodeType::Union,
                        1.0,
                    );
                    // SAFETY: (1, 3, 4) build_union initialized a valid owning header
                    // and its callbacks. (2) This transfers its sole owning pointer.
                    unsafe { CRQEIterator::new(header) }
                })
                .collect(),
            1.0,
            false,
        ));
        measure(c, &format!("AndOfOr FFI/n={fanout}"), &ffi_tree);
    }
}

criterion_group!(benches, benchmark_cardinality);
criterion_main!(benches);
