use std::io::Write;

use inverted_index::{Encoder, RSIndexResult, t_docId};

pub struct BufferWriter;

impl Write for &BufferWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        todo!("Coming from Zeeshan's PR")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!("Coming from Zeeshan's PR")
    }
}

#[repr(C)]
pub struct IndexEncoder {
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
pub unsafe extern "C" fn IndexEncoder_Encode(
    ie: *const IndexEncoder,
    writer: *mut BufferWriter,
    delta: t_docId,
    record: *const RSIndexResult,
) -> usize {
    let encoder = unsafe { &*ie };
    unsafe { (encoder.encode_fn)(writer, delta, record) }
}

impl<E: Encoder> From<E> for IndexEncoder {
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

            match E::encode(writer, delta, record) {
                Ok(bytes_written) => bytes_written,
                Err(_) => 0, // TODO: handle correctly
            }
        }

        Self {
            encode_fn: encode_impl::<E>,
        }
    }
}
