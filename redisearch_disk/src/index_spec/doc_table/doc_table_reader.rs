use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::IndexReader;
use speedb::{DBIteratorWithThreadMode, IteratorMode};
use thiserror::Error;

use crate::key_traits::{AsKeyExt, FromKeyExt};

/// Lazy reader to get the document IDs from the document table
pub struct DocTableReader<'iterator, DBAccess: speedb::DBAccess> {
    /// The underlying Speedb iterator
    iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,

    /// Estimated number of unique documents
    estimate: u64,

    /// The last read key from the iterator. This is only needed to be able to implement the
    /// `skip_to` method of the `IndexReader` trait, which requires us to be able to return
    /// whether the seek was successful without advancing the iterator.
    last_read: Option<Result<Box<[u8]>, speedb::Error>>,
}

#[derive(Debug, Error)]
pub enum ReaderCreateError {
    #[error("Failed to parse first key in document table")]
    ParseFirstKey,
    #[error("Failed to parse last key in document table")]
    ParseLastKey,
    #[error("Speedb error: {0}")]
    SpeedbError(#[from] speedb::Error),
}

impl<'iterator, DBAccess: speedb::DBAccess> DocTableReader<'iterator, DBAccess> {
    /// Create a new DocTableReader from the given Speedb iterator
    pub fn new(
        mut iterator: DBIteratorWithThreadMode<'iterator, DBAccess>,
    ) -> Result<Self, ReaderCreateError> {
        let estimate = Self::get_estimate(&mut iterator)?;

        iterator.set_mode(IteratorMode::Start);

        let mut this = Self {
            iterator,
            estimate,
            last_read: None,
        };

        this.get_next();

        Ok(this)
    }

    /// Get the next key from the iterator
    fn get_next(&mut self) {
        self.last_read = self.iterator.next().map(|res| res.map(|(key, _value)| key));
    }

    /// Get an estimate of the number of unique documents in the iterator
    fn get_estimate(
        iterator: &mut DBIteratorWithThreadMode<'iterator, DBAccess>,
    ) -> Result<u64, ReaderCreateError> {
        let Some(next) = iterator.next() else {
            return Ok(0);
        };

        let (first_key, _first_value) = next?;
        let first_key =
            t_docId::from_key(&first_key).map_err(|_e| ReaderCreateError::ParseFirstKey)?;

        iterator.set_mode(IteratorMode::End);
        let Some(prev) = iterator.next() else {
            debug_assert!(
                false,
                "Failed to get last document ID from key, even when there is a first key"
            );
            return Ok(0);
        };

        let (last_key, _last_value) = prev?;
        let last_key =
            t_docId::from_key(&last_key).map_err(|_e| ReaderCreateError::ParseLastKey)?;

        debug_assert!(
            first_key <= last_key,
            "first key should be less than or equal to last key"
        );

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
        let key = match self.last_read.take() {
            Some(Ok(key_bytes)) => key_bytes,
            Some(Err(e)) => return Err(std::io::Error::other(e)),
            None => return Ok(false),
        };

        let doc_id = t_docId::from_key(&key).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse document ID from key: {e}"),
            )
        })?;

        result.doc_id = doc_id;

        self.get_next();

        Ok(true)
    }

    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut inverted_index::RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if !self.skip_to(doc_id) {
            return Ok(false);
        }

        self.next_record(result)
    }

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        let Some(Ok(current_key)) = &self.last_read else {
            // We are already at the end of the iterator
            return false;
        };

        let Ok(current_doc_id) = t_docId::from_key(current_key) else {
            // Current key is invalid, cannot skip
            return false;
        };

        if doc_id <= current_doc_id {
            // We cannot skip backwards so continue from the current position
            return true;
        }

        self.iterator.set_mode(IteratorMode::From(
            &doc_id.as_key(),
            speedb::Direction::Forward,
        ));
        self.get_next();

        self.last_read.as_ref().is_some()
    }

    fn reset(&mut self) {
        self.iterator.set_mode(IteratorMode::Start);
        self.get_next()
    }

    fn unique_docs(&self) -> u64 {
        self.estimate
    }

    fn has_duplicates(&self) -> bool {
        // We can't have duplicate doc IDs in the document table
        false
    }

    fn flags(&self) -> ffi::IndexFlags {
        IndexFlags_Index_DocIdsOnly
    }

    fn needs_revalidation(&self) -> bool {
        // We own the iterator so no revalidation is needed
        false
    }

    fn refresh_buffer_pointers(&mut self) {
        // We don't have any buffers to refresh
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        database::SpeedbMultithreadedDatabase, index_spec::doc_table::DocumentMetadata,
        value_traits::ValueExt,
    };

    use super::*;

    /// Test to make sure skipping backwards is not possible for the reader. This test is here
    /// because it can't be trigger from the higher level doc reader integration tests.
    /// This test is skipped when running under Miri because it will invoke C code which Miri
    /// cannot handle.
    #[cfg(not(miri))]
    #[test]
    fn do_not_skip_backwards() {
        let path = TempDir::new().unwrap();
        let mut opts = speedb::Options::default();
        opts.create_if_missing(true);

        let db = SpeedbMultithreadedDatabase::open(&opts, path).unwrap();

        // Insert multiple documents
        db.put(1.as_key(), DocumentMetadata::default().as_speedb_value())
            .unwrap();
        db.put(3.as_key(), DocumentMetadata::default().as_speedb_value())
            .unwrap();
        db.put(5.as_key(), DocumentMetadata::default().as_speedb_value())
            .unwrap();

        let iter = db.iterator(IteratorMode::Start);
        let mut reader = DocTableReader::new(iter).unwrap();

        // Skip to doc ID 3
        assert!(reader.skip_to(3));
        assert_eq!(
            &reader
                .last_read
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .to_vec(),
            &3u64.as_key()
        );

        // Skipping to doc ID 4 should move the iterator to doc ID 5, since there is no doc ID 4
        assert!(reader.skip_to(4));
        assert_eq!(
            &reader
                .last_read
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .to_vec(),
            &5u64.as_key()
        );

        // Skipping to the current doc ID 5 should keep the iterator at doc ID 5
        assert!(reader.skip_to(5));
        assert_eq!(
            &reader
                .last_read
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .to_vec(),
            &5u64.as_key()
        );

        // Skip to doc ID 1, which is before the current position. This should not move the iterator backwards, so it should still be at doc ID 5.
        assert!(reader.skip_to(1));
        assert_eq!(
            &reader
                .last_read
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .to_vec(),
            &5u64.as_key()
        );

        // Read past the iterator to make sure it still works after trying to skip backwards
        assert!(!reader.skip_to(10));
        assert_eq!(&reader.last_read, &None);

        // Skip backwards again which should fails since we reached the end of the iterator
        assert!(!reader.skip_to(1));
        assert_eq!(&reader.last_read, &None);
    }
}
