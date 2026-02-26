/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Read};

use crate::{Encoder, RSIndexResult};

mod gc;
mod index;
mod index_result;
mod reader;

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any test to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut ffi::RSQueryTerm) {
    panic!("No test created a term record");
}

/// Dummy encoder which allows defaults for testing, encoding only the delta
#[derive(Clone)]
struct Dummy;

impl Encoder for Dummy {
    type Delta = u32;

    fn encode<W: std::io::Write + std::io::Seek>(
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_be_bytes())?;

        Ok(4)
    }
}

impl crate::Decoder for Dummy {
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        prev_doc_id: u64,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let mut buffer = [0; 4];
        cursor.read_exact(&mut buffer)?;

        let delta = u32::from_be_bytes(buffer);
        let doc_id = prev_doc_id + (delta as u64);

        result.doc_id = doc_id;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::default()
    }
}

/// Helper macro to encode a series of doc IDs using the provided encoder. The first ID is encoded
/// as a delta from 0, and each subsequent ID is encoded as a delta from the previous ID.
macro_rules! encode_ids {
    ($encoder:ty, $first_id:expr $(, $doc_id:expr)* ) => {
        {
            let mut writer = Cursor::new(Vec::new());
            <$encoder>::encode(&mut writer, 0, &RSIndexResult::default().doc_id($first_id)).unwrap();

            let mut _last_id = $first_id;
            $(
                let delta = $doc_id - _last_id;
                <$encoder>::encode(&mut writer, delta, &RSIndexResult::default().doc_id($doc_id)).unwrap();
                _last_id = $doc_id;
            )*
            writer.into_inner()
        }
    };
}

pub(super) use encode_ids;
