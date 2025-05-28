use crate::{ResultProcessor, ResultProcessorType, SearchResult, upstream::Upstream};

pub struct Counter {
    count: usize,
}

impl ResultProcessor for Counter {
    fn next(&mut self, upstream: Option<Upstream>) -> crate::Result<Option<SearchResult>> {
        let Some(mut upstream) = upstream else {
            return Ok(None);
        };

        while let Some(_) = upstream.next()? {
            self.count += 1;
        }

        Ok(Some(SearchResult::default()))
    }

    fn type_() -> crate::ResultProcessorType {
        ResultProcessorType::Counter
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self { count: 0 }
    }
}
