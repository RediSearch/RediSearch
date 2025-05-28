use std::io::Write;

/// TODO: generate using bindgen
pub struct RSIndexResult {
    // Stub
}

/// TODO: generate using bindgen
pub type t_doc_id = u64;

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode(
        writer: impl Write,
        delta: t_doc_id,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;
}
