use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rsvalue_bencher::ffi::bindings::{
    RSValueMap_AllocUninit,RSValue_NewMap
};

/// Inserts and deletes `count` entries into a map in C.
fn map_c(count: u32) {
  // TODO:
  // let map = unsafe { RSValueMap_AllocUninit(count) };
  // let map = unsafe { RSValue_NewMap(map) };
}

fn bench_map(c: &mut Criterion) {
    c.bench_function("Collections/Map C 100000", |b| {
        b.iter(|| map_c(black_box(100000)))
    });
}

criterion_group!(benches, bench_map);
criterion_main!(benches);
