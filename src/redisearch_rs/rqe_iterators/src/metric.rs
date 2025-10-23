/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Metric iterator implementation

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome, id_list::IdList};
use ffi::{RSYieldableMetric, array_ensure_append_n_func, t_docId};
use inverted_index::{RSIndexResult, ResultMetrics_Reset};
use value::{RSValueFFI, RSValueTrait};

pub enum MetricType {
    VectorDistance,
}

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
pub struct Metric<'index> {
    base: IdList<'index>,
    metric_data: Vec<f64>,
    #[allow(dead_code)]
    type_: MetricType,
}

#[inline(always)]
fn set_result_metrics(result: &mut RSIndexResult, val: f64) {
    if let Some(num) = result.as_numeric_mut() {
        *num = val;
    } else {
        // Safety: we created a metric result, which is numeric, in the constructor
        panic!("Result is not numeric");
    }

    // SAFETY: reset the metrics c_array
    unsafe {
        ResultMetrics_Reset(result.metrics);
    }

    let value = RSValueFFI::create_num(val);
    let new_metrics: *const RSYieldableMetric = &RSYieldableMetric {
        key: std::ptr::null_mut(),
        value: value.as_ptr(),
    };
    // SAFETY: calling a C function to append a new metric to the result's metrics array
    unsafe {
        result.metrics = array_ensure_append_n_func(
            result.metrics as *mut std::ffi::c_void,
            new_metrics as *mut std::ffi::c_void,
            1,
            std::mem::size_of::<RSYieldableMetric>() as u16,
        ) as *mut RSYieldableMetric;
    }
}

impl<'index> Metric<'index> {
    pub fn new(ids: Vec<t_docId>, metric_data: Vec<f64>) -> Self {
        debug_assert!(ids.len() == metric_data.len());

        // Metric iterator returns a metric result while IdList returns a virtual one.
        let mut base = IdList::new(ids);
        let res = base.get_mut_result();
        *res = RSIndexResult::metric();

        Metric {
            base,
            metric_data,
            type_: MetricType::VectorDistance,
        }
    }
}

impl<'index> RQEIterator<'index> for Metric<'index> {
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.base.at_eof() {
            return Ok(None);
        }
        self.base.read()?;

        let val = self.metric_data[self.base.offset() - 1];
        let result = self.base.get_mut_result();
        set_result_metrics(result, val);
        Ok(Some(result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let skip_outcome = self.base.skip_to(doc_id)?;
        match skip_outcome {
            Some(SkipToOutcome::Found(_)) => {
                let val = self.metric_data[self.base.offset() - 1];
                let result = self.base.get_mut_result();
                set_result_metrics(result, val);
                Ok(Some(SkipToOutcome::Found(result)))
            }
            Some(SkipToOutcome::NotFound(_)) => {
                let val = self.metric_data[self.base.offset() - 1];
                let result = self.base.get_mut_result();
                set_result_metrics(result, val);
                Ok(Some(SkipToOutcome::NotFound(result)))
            }
            None => Ok(None),
        }
    }

    fn rewind(&mut self) {
        self.base.rewind();
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.base.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.base.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.base.at_eof()
    }
}
