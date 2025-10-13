use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rsvalue_bencher::ffi::bindings::{RSValue_Free, RSValue_NewParsedNumber};
use std::ffi::c_char;

mod test_data {
    use std::ffi::CStr;

    pub fn basic() -> Vec<&'static CStr> {
        vec![
            c"0",
            c"1.0",
            c"-123.456",
            c"3.141592653589793",
            c"1e10",
            c"1e-10",
            c"1.234567890123456e+300",
            c"2.2250738585072014e-308", // smallest normal f64
            c"5e-324",                  // subnormal
            c"NaN",
            c"Infinity",
            c"-Infinity",
            c"0.9999999999999999", // rounding edge
        ]
    }

    pub fn long_mantissa() -> Vec<String> {
        (1..=1000)
            .map(|n| format!("0.{}1", "0".repeat(n)))
            .collect()
    }
}

/// Allocates then frees `count` RSValues in C.
fn float_parse_c(cases: &[(*const c_char, usize)]) {
    let mut values = Vec::new();

    for (cstr, len) in cases {
        let value = unsafe { RSValue_NewParsedNumber(*cstr, *len) };
        values.push(value);
    }

    for value in values {
        unsafe { RSValue_Free(value) };
    }
}

fn bench_float_parse(c: &mut Criterion) {
    let test_cases_c: Vec<(*const c_char, usize)> = test_data::basic()
        .iter()
        .map(|s| (s.as_ptr(), s.count_bytes()))
        .collect();

    c.bench_function("Float Parse C", |b| {
        b.iter(|| float_parse_c(black_box(&test_cases_c)))
    });

    // TODO:
    // c.bench_function("Float Parse Rust", |b| {
    //     b.iter(|| float_parse_c(black_box(10000)))
    // });
}

criterion_group!(benches, bench_float_parse);
criterion_main!(benches);
