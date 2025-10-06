/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Metric iterator implementation
use ffi::{RLookupKey, RS_NewValue, RSValue, RSValueType_RSValue_Number, RSYieldableMetric, t_docId};
use inverted_index::{RSIndexResult, RSYieldableMetric_Concat};
use crate::{RQEIterator, RQEIteratorError, SkipToOutcome, id_list::IdList};

pub enum MetricType {
    VectorDistance,
}

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
pub struct Metric {
    base: IdList,
    type_: MetricType,
    metric_data: Vec<u8>,    
}

impl Metric {
    pub fn new(ids: Vec<t_docId>, metric_data: Vec<u8>) -> Self {
        Metric {
            base: IdList::new(ids),
            type_: MetricType::VectorDistance,
            metric_data,
        }
    }
    fn set_result_metrics(&self, result: &mut RSIndexResult<'_>) {
        unsafe {
            let v = RS_NewValue(RSValueType_RSValue_Number);
            (*v).__bindgen_anon_1.numval = self.metric_data[result.doc_id as usize] as f64;
            let new_metrics = RSYieldableMetric{key: None, value: v};
            RSYieldableMetric_Concat(&mut result.metrics, &new_metrics);
        }
    }
}

impl RQEIterator for Metric {
    fn read(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        if self.base.at_eof() {
            return Ok(None);
        }

        let result = self.base.read()?.as_mut();
        match result {
            Some(res) => {
                self.set_result_metrics(res);
                Ok(Some(res))
            }
            None => {
                Ok(None)
            }
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        self.base.skip_to(doc_id)
    }

    fn rewind(&mut self) {
        self.base.rewind();
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.current_id
    }

    fn at_eof(&self) -> bool {
        self.current_id >= self.top_id
    }
}
