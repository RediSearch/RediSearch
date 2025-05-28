use std::io::Write;

pub use ffi::{t_docId, RSIndexResult};

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode(writer: impl Write, delta: t_docId, record: &RSIndexResult)
        -> std::io::Result<usize>;
}
