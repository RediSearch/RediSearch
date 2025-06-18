/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use std::{collections::HashMap, time::Duration};

use buffer::Buffer;
use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};

use crate::ffi::encode_numeric;

pub struct NumericBencher {
    test_values: HashMap<String, Vec<f64>>,
}

impl NumericBencher {
    const MEASUREMENT_TIME: Duration = Duration::from_secs(10);
    const WARMUP_TIME: Duration = Duration::from_secs(5);

    pub fn new() -> Self {
        let test_values = HashMap::from_iter([
            ("TinyInt".to_string(), vec![0.0, 3.0, 7.0]),
            ("PosInt".to_string(), vec![16.0, 256.0, 100_000.0]),
            ("NegInt".to_string(), vec![-16.0, -256.0, -100_000.0]),
            (
                "Float".to_string(),
                vec![-f64::INFINITY, -3.125, -3.124, 3.124, 3.125, f64::INFINITY],
            ),
        ]);

        Self { test_values }
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

    pub fn numeric(&self, c: &mut Criterion) {
        for (group, values) in &self.test_values {
            let group = format!("Encode - {}", group);
            let mut group = self.benchmark_group(c, &group);
            numeric_c_encode(&mut group, values);
            group.finish();
        }
    }
}

fn numeric_c_encode<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[f64]) {
    let mut buffer = Buffer::from_array([0; 9]);

    group.bench_function("C", |b| {
        b.iter(|| {
            for &value in values {
                // Reset buffer to prevent it from growing
                // This is fine since we don't care about benchmarking the growth operation anyway
                buffer.reset();
                let mut record = inverted_index::RSIndexResult::numeric(value);
                encode_numeric(&mut buffer, &mut record, 0);
            }
        });
    });
}
