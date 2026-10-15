/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod field_offsets;
mod legacy;

use byte_offsets::ByteOffsets;
use varint::VectorWriter;

/// A map of `fields`, each given as `(field_id, first, offsets)`, whose `n`-th stored
/// offset is `token_offsets[n - 1]`.
fn byte_offsets(fields: &[(u16, u32, u32)], token_offsets: &[u32]) -> ByteOffsets {
    let mut writer = VectorWriter::new(0);
    for &offset in token_offsets {
        writer.write(offset).expect("writing to a Vec cannot fail");
    }
    let mut offsets = ByteOffsets::new();
    offsets.reserve_fields(fields.len());
    for &(field_id, first, field_offsets) in fields {
        offsets.add_field(field_id, first, field_offsets);
    }
    offsets.take_offsets(&mut writer);
    offsets
}
