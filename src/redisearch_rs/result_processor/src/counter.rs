use crate::{Header, ResultProcessor, SearchResult};

#[repr(C)]
pub struct Counter {
    // THIS MUST BE THE FIRST FIELD
    header: Header,
    count: usize,
}

impl ResultProcessor for Counter {
    fn next(&mut self) -> crate::Result<Option<SearchResult>> {
        let mut upstream = self.header.upstream().unwrap();

        while let Some(_) = upstream.next()? {
            self.count += 1;
        }

        Ok(Some(SearchResult::default()))
    }
}

impl Counter {
    pub const fn new(header: Header) -> Self {
        Self { header, count: 0 }
    }
}
