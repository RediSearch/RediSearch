use buffer::BufferWriter;
use inverted_index::{t_docId, EncodeDocIdsOnly, Encoder, RSIndexResult};

use crate::buffer::BufferWriterRS;

pub struct IndexEncoderRS {
    encode_fn: unsafe extern "C" fn(
        writer: *mut BufferWriter,
        delta: t_docId,
        record: *const RSIndexResult,
    ) -> usize,
}

/// Replaces this C:
///
/// ```c
/// typedef size_t (*IndexEncoder)(BufferWriter *bw, t_docId delta, RSIndexResult *record);
/// ````
///
/// The change being that we now also have to pass in the index encoder as the first arg
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexEncoder_RS_Encode(
    ie: *const IndexEncoderRS,
    writer: *mut BufferWriterRS,
    delta: t_docId,
    record: *mut RSIndexResult,
) -> usize {
    let writer = unsafe { &mut *writer };
    let writer = &mut writer.0;

    let encoder = unsafe { &*ie };
    unsafe { (encoder.encode_fn)(writer, delta, record) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Get_DocsIdOnly_Encoder() -> *mut IndexEncoderRS {
    let b = Box::new(IndexEncoderRS::from(EncodeDocIdsOnly));
    Box::into_raw(b)
}

impl<E: Encoder> From<E> for IndexEncoderRS {
    fn from(_value: E) -> Self {
        // We can't expose generic types for C, so we are generating this function for each
        // `[Encoder]` implementation.
        unsafe extern "C" fn encode_impl<E: Encoder>(
            writer: *mut BufferWriter,
            delta: t_docId,
            record: *const RSIndexResult,
        ) -> usize {
            let writer = unsafe { &*writer };
            let record = unsafe { &*record };

            match E::encode(*writer, delta, record) {
                Ok(memory_growth) => memory_growth,
                Err(_) => 0, // TODO: handle correctly
            }
        }

        Self {
            encode_fn: encode_impl::<E>,
        }
    }
}
