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
use inverted_index::{RSIndexResult, ResultMetrics_Reset_func};
use value::{RSValueFFI, RSValueTrait};

// enum indicating the type of metric. currently only vector distance is supported.
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
        ResultMetrics_Reset_func(result);
    }

    let value = RSValueFFI::create_num(val);
    let new_metrics: *const RSYieldableMetric = &RSYieldableMetric {
        key: std::ptr::null_mut(),
        value: value.as_ptr(),
    };
    // SAFETY: calling a C function to append a new metric to the result's metrics array
    unsafe {
        result.metrics = array_ensure_append_n_func(
            result.metrics as *mut _,
            new_metrics as *mut _,
            1,
            std::mem::size_of::<RSYieldableMetric>() as u16,
        ) as *mut RSYieldableMetric;
    }
}

impl<'index> Metric<'index> {
    pub fn new(ids: Vec<t_docId>, metric_data: Vec<f64>) -> Self {
        debug_assert!(ids.len() == metric_data.len());

        Metric {
            base: IdList::with_result(ids, RSIndexResult::metric()),
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

        let Some((result, offset)) = self.base.read_and_get_offset()? else {
            return Ok(None);
        };
        let val = self.metric_data[offset - 1];

        set_result_metrics(result, val);
        Ok(Some(result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let skip_outcome = self.base.skip_to_and_get_offset(doc_id)?;
        match skip_outcome {
            Some((SkipToOutcome::Found(result), offset)) => {
                let val = self.metric_data[offset - 1];
                set_result_metrics(result, val);
                Ok(Some(SkipToOutcome::Found(result)))
            }
            Some((SkipToOutcome::NotFound(result), offset)) => {
                let val = self.metric_data[offset - 1];
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
