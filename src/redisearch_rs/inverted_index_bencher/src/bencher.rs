use std::time::Duration;

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};

use crate::ffi::encode_numeric;

pub struct NumericBencher {
    test_values: Vec<f64>,
}

impl NumericBencher {
    pub fn new() -> Self {
        let test_values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        Self { test_values }
    }

    fn benchmark_group<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(label);
        group.measurement_time(Duration::from_secs(10));
        group.warm_up_time(Duration::from_secs(5));
        group
    }

    pub fn numeric(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group(c, "Numeric Bencher");
        numeric_c_benchmark(&mut group, &self.test_values);
        group.finish();
    }
}

fn numeric_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, values: &[f64]) {
    let mut buffer = vec![0u8; 1];

    group.bench_function("C", |b| {
        b.iter(|| {
            for &value in values {
                let mut record = inverted_index::RSIndexResult::numeric(value);
                encode_numeric(&mut buffer, &mut record);
            }
        });
    });
}
