#[repr(C)]
pub struct CIterator {
    type_: IteratorType,

    /// Can the iterator yield more results?
    at_eof: bool,
  
    /// the last docId read. Initially should be 0.
    last_doc_id: DocId,
  
    /// Current result. Should always point to a valid current result, except when `lastDocId` is 0
    /// in C: RSIndexResult *current;
    current: *mut std::ffi::c_void,
}

