use std::io::{Read, Seek, Write};

pub use ffi::{t_docId, t_fieldMask, RSIndexResult};

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

/// Filter details the decoder should use to determine whether a record should be filtered out or
/// not.
pub struct DecoderCtx {
    mask: usize,
    wide_mask: t_fieldMask,
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader, using the provided context. The offset is the base
    /// value of any delta document IDs being read.
    ///
    /// Returns 'None' if the record is filtered out.
    fn decode(reader: impl Read, ctx: &DecoderCtx, offset: t_docId) -> Option<RSIndexResult>;

    /// Like `[Decoder::decode]`, but seeks to a specific document ID and return it.
    fn seek(
        reader: impl Read + Seek,
        ctx: &DecoderCtx,
        offset: t_docId,
        target: t_docId,
    ) -> Option<RSIndexResult>;
}
