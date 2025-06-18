/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::HashMap, ptr::NonNull, time::Duration};

use buffer::Buffer;
use criterion::{
    BenchmarkGroup, Criterion, black_box,
    measurement::{Measurement, WallTime},
};

use crate::ffi::{encode_numeric, read_numeric};

pub struct NumericBencher {
    encoding_test_values: HashMap<String, Vec<f64>>,
    decoding_test_values: HashMap<String, Vec<Vec<u8>>>,
}

impl NumericBencher {
    const MEASUREMENT_TIME: Duration = Duration::from_secs(10);
    const WARMUP_TIME: Duration = Duration::from_secs(5);

    pub fn new() -> Self {
        let encoding_test_values = HashMap::from_iter([
            ("TinyInt".to_string(), vec![0.0, 3.0, 7.0]),
            ("PosInt".to_string(), vec![16.0, 256.0, 100_000.0]),
            ("NegInt".to_string(), vec![-16.0, -256.0, -100_000.0]),
            (
                "Float".to_string(),
                vec![-f64::INFINITY, -3.125, -3.124, 3.124, 3.125, f64::INFINITY],
            ),
        ]);

        let decoding_test_values = HashMap::from_iter([
            (
                "TinyInt".to_string(),
                vec![vec![2, 172, 2], vec![98, 172, 2], vec![226, 172, 2]],
            ),
            (
                "PosInt".to_string(),
                vec![
                    vec![18, 172, 2, 16],
                    vec![50, 172, 2, 0, 1],
                    vec![82, 172, 2, 160, 134, 1],
                ],
            ),
            (
                "NegInt".to_string(),
                vec![
                    vec![26, 172, 2, 16],
                    vec![58, 172, 2, 0, 1],
                    vec![90, 172, 2, 160, 134, 1],
                ],
            ),
            (
                "Float".to_string(),
                vec![
                    vec![106, 172, 2],
                    vec![74, 172, 2, 0, 0, 72, 64],
                    vec![202, 172, 2, 203, 161, 69, 182, 243, 253, 8, 64],
                    vec![138, 172, 2, 203, 161, 69, 182, 243, 253, 8, 64],
                    vec![10, 172, 2, 0, 0, 72, 64],
                    vec![42, 172, 2],
                ],
            ),
        ]);

        Self {
            encoding_test_values,
            decoding_test_values,
        }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(label);
        group.measurement_time(Self::MEASUREMENT_TIME);
        group.warm_up_time(Self::WARMUP_TIME);
        group
    }

    pub fn encoding(&self, c: &mut Criterion) {
        for (group, values) in &self.encoding_test_values {
            let group = format!("Encode - {}", group);
            let mut group = self.benchmark_group(c, &group);
            numeric_c_encode(&mut group, values);
            group.finish();
        }
    }

    pub fn decoding(&self, c: &mut Criterion) {
        for (group, values) in &self.decoding_test_values {
            let group = format!("Decode - {}", group);
            let mut group = self.benchmark_group(c, &group);
            numeric_c_decode(&mut group, values);
            group.finish();
        }
    }
}

fn numeric_c_encode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[f64]) {
    let mut buffer = Buffer::from_array([0; 16]);

    group.bench_function("C", |b| {
        b.iter(|| {
            for &value in values {
                // Reset buffer to prevent it from growing
                // This is fine since we don't care about benchmarking the growth operation anyway
                buffer.clear();
                let mut record = inverted_index::RSIndexResult::numeric(value);
                let grew_size = encode_numeric(&mut buffer, &mut record, 684);

                black_box(grew_size);
            }
        });
    });
}

fn numeric_c_decode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[Vec<u8>]) {
    group.bench_function("C", |b| {
        b.iter(|| {
            for value in values {
                let buffer_ptr = NonNull::new(value.as_ptr() as *mut _).unwrap();
                let mut buffer = unsafe { Buffer::new(buffer_ptr, value.len(), value.len()) };
                let (_filtered, result) = read_numeric(&mut buffer, 100);

                black_box(result);
            }
        });
    });
}
