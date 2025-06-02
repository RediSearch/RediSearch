use std::io::{Read, Seek, Write};

pub use ffi::{t_docId, t_fieldMask, NumericFilter, RSIndexResult};

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode(
        writer: impl Write + Seek,
        delta: t_docId,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. The offset is the base value for any delta being
    /// decoded.
    ///
    /// Returns 'None' if the record is filtered out.
    fn decode(&self, reader: impl Read, offset: t_docId) -> Option<RSIndexResult>;

    /// Like `[Decoder::decode]`, but seeks to a specific document ID and returns it.
    ///
    /// Returns `None` if there is no record greater than or equal to the target document ID.
    fn seek(
        &self,
        reader: impl Read + Seek,
        offset: t_docId,
        target: t_docId,
    ) -> Option<RSIndexResult>;
}
