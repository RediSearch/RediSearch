use inverted_index::{t_docId, Encoder, RSIndexResult};

use crate::buffer::BufferWriter;

pub struct IndexEncoder {
    encode_fn: unsafe extern "C" fn(
        writer: *mut buffer::BufferWriter,
        delta: t_docId,
        record: *const RSIndexResult,
    ) -> usize,
}

/// Write the record and the delta using the given encoder to the writing buffer. The returned value
/// is how many bytes the buffer grew after the write operation.
///
/// # Safety
/// - The `ie` pointer must point to a valid `IndexEncoder` instance.
/// - The `writer` pointer must point to a valid `BufferWriter` instance.
/// - The `record` pointer must point to a valid `RSIndexResult` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexEncoder_Encode(
    ie: *const IndexEncoder,
    writer: *mut BufferWriter,
    delta: t_docId,
    record: *mut RSIndexResult,
) -> usize {
    let writer = unsafe { &mut *writer };
    let writer = &mut writer.0;

    let encoder = unsafe { &*ie };
    unsafe { (encoder.encode_fn)(writer, delta, record) }
}

impl<E: Encoder> From<E> for IndexEncoder {
    fn from(_value: E) -> Self {
        // We can't expose generic types for C, so we are generating this function for each
        // `[Encoder]` implementation.
        unsafe extern "C" fn encode_impl<E: Encoder>(
            writer: *mut buffer::BufferWriter,
            delta: t_docId,
            record: *const RSIndexResult,
        ) -> usize {
            let writer = unsafe { &*writer };
            let record = unsafe { &*record };
            let old_mem_size = unsafe { writer.mem_size() };

            match E::encode(*writer, delta, record) {
                Ok(_bytes_written) => {
                    let new_mem_size = unsafe { writer.mem_size() };

                    new_mem_size - old_mem_size
                }
                Err(_) => 0, // TODO: handle correctly
            }
        }

        Self {
            encode_fn: encode_impl::<E>,
        }
    }
}
