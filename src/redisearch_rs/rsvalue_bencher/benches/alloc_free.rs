use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rsvalue_bencher::ffi::bindings::{
    RSValue_Free, RSValue_NewWithType, RSValueType_RSValueType_RedisString,
};

/// Allocates then frees `count` RSValues in C.
fn alloc_free_c(count: usize) {
    let mut values = Vec::new();

    for _ in 0..count {
        let value = unsafe { RSValue_NewWithType(RSValueType_RSValueType_RedisString) };
        values.push(value);
    }

    for value in values {
        unsafe { RSValue_Free(value) };
    }
}

fn bench_alloc_free(c: &mut Criterion) {
    c.bench_function("Alloc/Free C 100000", |b| {
        b.iter(|| alloc_free_c(black_box(100000)))
    });

    // TODO:
    // c.bench_function("Alloc/Free Rust 10000", |b| {
    //     b.iter(|| alloc_free_c(black_box(10000)))
    // });
}

criterion_group!(benches, bench_alloc_free);
criterion_main!(benches);
