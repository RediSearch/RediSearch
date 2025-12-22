use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::IndexReader;
use speedb::{DBIteratorWithThreadMode, IteratorMode};

use crate::search_disk::{AsKeyExt, FromKeyExt};

/// Lazy reader to get the document IDs from the document table
pub struct DocTableReader<'iterator, DBAccess: speedb::DBAccess> {
    /// The underlying Speedb iterator
    iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,

    /// Estimated number of unique documents
    estimate: u64,
}

impl<'iterator, DBAccess: speedb::DBAccess> DocTableReader<'iterator, DBAccess> {
    /// Create a new DocTableReader from the given Speedb iterator
    pub fn new(
        mut iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,
    ) -> Result<Self, speedb::Error> {
        let estimate = Self::get_estimate(&mut iterator)?;

        iterator.set_mode(IteratorMode::Start);

        Ok(Self { iterator, estimate })
    }

    /// Get an estimate of the number of unique documents in the iterator
    fn get_estimate(
        iterator: &mut DBIteratorWithThreadMode<'iterator, DBAccess>,
    ) -> Result<u64, speedb::Error> {
        let Some(next) = iterator.next() else {
            return Ok(0);
        };

        let (first_key, _first_value) = next?;
        let first_key = t_docId::from_key(&first_key);

        iterator.set_mode(IteratorMode::End);
        let Some(prev) = iterator.next() else {
            return Ok(0);
        };

        let (last_key, _last_value) = prev?;
        let last_key = t_docId::from_key(&last_key);

        Ok(last_key - first_key + 1)
    }
}

impl<'index, 'iterator, DBAccess: speedb::DBAccess> IndexReader<'index>
    for DocTableReader<'iterator, DBAccess>
{
    fn next_record(
        &mut self,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let key = match self.iterator.next() {
            Some(Ok((key_bytes, _value_bytes))) => key_bytes,
            Some(Err(e)) => return Err(std::io::Error::other(e)),
            None => return Ok(false),
        };

        let doc_id = t_docId::from_key(&key);

        result.doc_id = doc_id;

        Ok(true)
    }

    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        self.iterator.set_mode(IteratorMode::From(
            &doc_id.as_key(),
            speedb::Direction::Forward,
        ));

        self.next_record(result)
    }

    fn skip_to(&mut self, _doc_id: t_docId) -> bool {
        // There are no blocks so we are already in the correct "block"
        true
    }

    fn reset(&mut self) {
        self.iterator.set_mode(IteratorMode::Start);
    }

    fn unique_docs(&self) -> u64 {
        self.estimate
    }

    fn has_duplicates(&self) -> bool {
        // We can't have duplicate keys
        false
    }

    fn flags(&self) -> ffi::IndexFlags {
        IndexFlags_Index_DocIdsOnly
    }

    fn needs_revalidation(&self) -> bool {
        // We own the iterator so no revalidation is needed
        false
    }
}
