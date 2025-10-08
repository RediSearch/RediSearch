/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Metric iterator implementation

use ffi::{RSValue_NewNumber, RSYieldableMetric, t_docId};
use inverted_index::{RSIndexResult, RSYieldableMetric_Concat};
use crate::{RQEIterator, RQEIteratorError, SkipToOutcome, id_list::IdList};

pub enum MetricType {
    VectorDistance,
}

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
pub struct Metric {
    base: IdList,
    metric_data: Vec<f64>,
    #[allow(dead_code)]
    type_: MetricType,
}

impl Metric {
    pub fn new(ids: Vec<t_docId>, metric_data: Vec<f64>) -> Self {
        debug_assert!(ids.len() == metric_data.len());
        Metric {
            base: IdList::new(ids),
            metric_data,
            type_: MetricType::VectorDistance,
        }
    }
    #[inline(always)]
    fn set_result_metrics(&mut self, val: f64) -> &RSIndexResult<'static> {
        let result = self.base.get_mut_result();
        unsafe {
            let new_metrics = RSYieldableMetric{key: std::ptr::null_mut(), value: RSValue_NewNumber(val)};
            RSYieldableMetric_Concat(&mut result.metrics, &new_metrics);
        }
        result
    }
}

impl RQEIterator for Metric {
    fn read(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        if self.base.at_eof() {
            return Ok(None);
        }
        
        self.base.read()?;
        let val = self.metric_data[self.base.offset()-1];
        let result = self.set_result_metrics(val);
        Ok(Some(result))
        
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        let skip_outcome = self.base.skip_to(doc_id)?;
        match skip_outcome{
            Some(SkipToOutcome::Found(_)) => {
                let val = self.metric_data[self.base.offset()-1];
                let result = self.set_result_metrics(val);
                Ok(Some(SkipToOutcome::Found(result)))
            },
            Some(SkipToOutcome::NotFound(_)) => {
                let val = self.metric_data[self.base.offset()-1];
                let result = self.set_result_metrics(val);
                Ok(Some(SkipToOutcome::NotFound(result)))
            },
            None => Ok(None)
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
