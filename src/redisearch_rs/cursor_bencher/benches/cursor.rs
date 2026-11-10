/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Both arms own cursor-sized allocations without a query or index, use the same
//! random source and monotonic clock, and lock each operation. Redis timers are
//! unavailable in the mock; these measure the registry rather than query teardown.

use criterion::{Criterion, criterion_group, criterion_main};
use cursor::{Collection, Kind, Registry};
use cursor_bencher as _;
use std::{hint::black_box, ptr, sync::Mutex};

fn now() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is writable and CLOCK_MONOTONIC is supported on our target platforms.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn random() -> u32 {
    // SAFETY: Criterion executes these benchmarks serially; no concurrent PRNG access.
    unsafe { libc::lrand48() as u32 }
}

fn benchmarks(c: &mut Criterion) {
    let c_list = &raw mut ffi::g_CursorsList;
    // SAFETY: the process-lifetime global is initialized once before any benchmark uses it.
    unsafe { ffi::CursorList_Init(c_list, false) };
    for count in [16, 1024] {
        for (operation, interval) in [("resume", u32::MAX), ("expire", 0), ("purge", u32::MAX)] {
            let mut group = c.benchmark_group(format!("cursor/{operation}/{count}"));
            group.bench_function("c", |b| {
                b.iter(|| {
                    let cursors: Vec<_> = (0..count)
                        .map(|_| {
                            // SAFETY: the initialized global is accessed serially; a null spec is supported.
                            let cur = unsafe {
                                ffi::Cursors_Reserve(
                                    c_list,
                                    ffi::StrongRef {
                                        rm: ptr::null_mut(),
                                    },
                                    interval,
                                    ptr::null_mut(),
                                )
                            };
                            // SAFETY: reservation succeeded without a capacity-limited spec; this pointer is exclusively owned.
                            let id = unsafe { cur.as_ref() }.expect("reservation succeeds").id;
                            // SAFETY: this newly reserved cursor is executing and owned by this iteration.
                            unsafe { ffi::Cursor_Pause(cur) };
                            id
                        })
                        .collect();
                    match operation {
                        "resume" => {
                            for id in cursors {
                                // SAFETY: the global is initialized and the ID belongs to a nonexpired idle cursor.
                                let cur =
                                    unsafe { ffi::Cursors_TakeForExecution(c_list, black_box(id)) };
                                assert!(!cur.is_null());
                                // SAFETY: successful take transfers execution ownership to this iteration.
                                unsafe { ffi::Cursor_Free(cur) };
                            }
                        }
                        "expire" => {
                            // SAFETY: the global is initialized and no other thread accesses it.
                            black_box(unsafe { ffi::Cursors_CollectIdle(c_list) });
                        }
                        _ => {
                            for id in cursors {
                                // SAFETY: the global is initialized and the ID belongs to an idle cursor.
                                black_box(unsafe { ffi::Cursors_Purge(c_list, black_box(id)) });
                            }
                        }
                    }
                })
            });
            let rust_list = Mutex::new(Registry::new(Kind::User));
            group.bench_function("rust", |b| {
                b.iter(|| {
                    let cursors: Vec<_> = (0..count)
                        .map(|_| {
                            let id = {
                                let mut list =
                                    rust_list.lock().expect("benchmark lock is not poisoned");
                                list.tick(now);
                                list.reserve(
                                    Box::new([0u8; size_of::<ffi::Cursor>()]),
                                    interval,
                                    random,
                                )
                            };
                            let mut list =
                                rust_list.lock().expect("benchmark lock is not poisoned");
                            list.tick(now);
                            black_box(list.pause(id, now()));
                            id
                        })
                        .collect();
                    match operation {
                        "resume" => {
                            for id in cursors {
                                {
                                    let mut list =
                                        rust_list.lock().expect("benchmark lock is not poisoned");
                                    list.tick(now);
                                    assert!(black_box(list.take(black_box(id))).is_some());
                                }
                                let mut list =
                                    rust_list.lock().expect("benchmark lock is not poisoned");
                                list.tick(now);
                                list.remove(id);
                            }
                        }
                        "expire" => {
                            black_box(
                                rust_list
                                    .lock()
                                    .expect("benchmark lock is not poisoned")
                                    .collect(now(), Collection::Forced),
                            );
                        }
                        _ => {
                            for id in cursors {
                                let mut list =
                                    rust_list.lock().expect("benchmark lock is not poisoned");
                                list.tick(now);
                                black_box(list.purge(black_box(id)));
                            }
                        }
                    }
                })
            });
            group.finish();
        }
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
