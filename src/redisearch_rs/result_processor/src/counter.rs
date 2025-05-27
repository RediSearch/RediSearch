use crate::{ResultProcessor, ResultProcessorType};

pub struct Counter {
    count: usize,
}

impl ResultProcessor for Counter {
    const TYPE: ResultProcessorType = ResultProcessorType::Counter;

    fn next(
        &mut self,
        mut cx: crate::Context,
    ) -> Result<Option<crate::ffi::SearchResult>, crate::Error> {
        let mut upstream = cx.upstream().unwrap();

        while upstream.next()?.is_some() {
            self.count += 1;
        }

        Ok(Some(crate::ffi::SearchResult::default()))
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self { count: 0 }
    }
}
