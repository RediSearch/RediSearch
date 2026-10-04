/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared scaffolding: building lookups and rows, encoding them, and comparing what comes
//! back out.

use rlookup::{RLookup, RLookupKeyFlags, RLookupRow};
use row_block::{
    Block, ColumnKind, DecodeError, MAGIC, RefusedRow, RowBlockDecoder, RowBlockWriter, Tag,
    TrioMember, VERSION,
};
use std::{
    ffi::{CStr, CString},
    ptr::NonNull,
};
use value::{SharedValue, Value};

/// A lookup whose keys are all plain output columns.
pub fn lookup(columns: &[&str]) -> RLookup<'static> {
    lookup_with_flags(
        &columns
            .iter()
            .map(|n| (*n, RLookupKeyFlags::empty()))
            .collect::<Vec<_>>(),
    )
}

/// A lookup whose keys carry the given flags, so tests can exercise [`ColumnFilter`].
///
/// [`ColumnFilter`]: row_block::ColumnFilter
pub fn lookup_with_flags(columns: &[(&str, RLookupKeyFlags)]) -> RLookup<'static> {
    let mut lookup = RLookup::new();
    for (name, flags) in columns {
        let name = CString::new(*name).expect("a column name holds no NUL byte");
        lookup
            .get_key_write(name, *flags)
            .expect("column names are distinct");
    }
    lookup
}

/// A row holding `values` for the named columns and nothing for the rest.
pub fn row(lookup: &RLookup<'_>, values: &[(&str, SharedValue)]) -> RLookupRow<'static> {
    let mut row = RLookupRow::new();
    for (name, value) in values {
        let name = CString::new(*name).expect("a column name holds no NUL byte");
        let cursor = lookup
            .find_key_by_name(&name)
            .expect("every written name is a column of the lookup");
        let key = cursor.current().expect("the cursor found the key");
        row.write_key(key, value.clone());
    }
    row
}

/// Encodes `rows` as one block, panicking if the writer refuses anything.
///
/// The filter is the empty one, which selects every key, so a test that is not about
/// [`ColumnFilter`] does not have to mention it.
///
/// [`ColumnFilter`]: row_block::ColumnFilter
pub fn encode(lookup: &RLookup<'_>, rows: &[RLookupRow<'_>]) -> Vec<u8> {
    encode_with_trio(lookup, rows, TrioMember::Middle)
}

/// Like [`encode`], but choosing which member a top-level trio field resolves to.
pub fn encode_with_trio(
    lookup: &RLookup<'_>,
    rows: &[RLookupRow<'_>],
    trio: TrioMember,
) -> Vec<u8> {
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(lookup, Default::default())
        .expect("the schema is encodable");
    for row in rows {
        writer
            .write_row(lookup, row, trio)
            .expect("the row is encodable");
    }
    assert_eq!(writer.nrows(), rows.len(), "every row was accepted");
    writer.as_bytes().to_vec()
}

/// Encodes one row and reports whether the writer accepted it, along with the resulting block.
pub fn try_encode_one(
    lookup: &RLookup<'_>,
    row: &RLookupRow<'_>,
    trio: TrioMember,
) -> (Result<(), RefusedRow>, Vec<u8>, usize) {
    let mut writer = RowBlockWriter::new();
    writer
        .write_schema(lookup, Default::default())
        .expect("the schema is encodable");
    let outcome = writer.write_row(lookup, row, trio);
    (outcome, writer.as_bytes().to_vec(), writer.nrows())
}

/// A decoded value, in a form tests can build, print and compare.
#[derive(Debug, Clone)]
pub enum Decoded {
    Number(f64),
    /// A string value's raw bytes: the format carries neither an encoding nor a NUL rule.
    Bytes(Vec<u8>),
    Null,
    Array(Vec<Decoded>),
    Map(Vec<(Decoded, Decoded)>),
}

// Numbers compare by bit pattern rather than by value: this is a wire format, so `NaN` must
// survive a round trip as the same `NaN`, and `-0.0` must not come back as `0.0`.
impl PartialEq for Decoded {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Number(a), Self::Number(b)) => a.to_bits() == b.to_bits(),
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Null, Self::Null) => true,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl Decoded {
    /// Flattens a decoded [`Value`] for comparison.
    ///
    /// Panics on anything the decoder cannot produce, which is how a decoder that started
    /// producing it would be caught.
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Number(number) => Self::Number(*number),
            Value::String(string) => Self::Bytes(string.as_bytes().to_vec()),
            Value::Null => Self::Null,
            Value::Array(array) => {
                Self::Array(array.iter().map(|item| Self::from_value(item)).collect())
            }
            Value::Map(map) => Self::Map(
                map.iter()
                    .map(|(key, value)| (Self::from_value(key), Self::from_value(value)))
                    .collect(),
            ),
            other => panic!("the decoder does not produce {}", other.variant_name()),
        }
    }

    /// The `SharedValue` a row would hold for this value, for feeding the encoder.
    pub fn to_value(&self) -> SharedValue {
        match self {
            Self::Number(number) => SharedValue::new_num(*number),
            Self::Bytes(bytes) => SharedValue::new_string(bytes.clone()),
            Self::Null => SharedValue::null_static(),
            Self::Array(items) => {
                SharedValue::new_array(items.iter().map(Self::to_value).collect::<Vec<_>>())
            }
            Self::Map(entries) => SharedValue::new_map(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_value(), value.to_value()))
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

/// Shorthand for a string value in an expectation.
pub fn bytes(s: &str) -> Decoded {
    Decoded::Bytes(s.as_bytes().to_vec())
}

/// The block's column names, in schema order.
pub fn columns_of(block: &[u8]) -> Vec<String> {
    Block::parse(block)
        .expect("the block parses")
        .columns()
        .iter()
        .map(|name| name.to_string_lossy().into_owned())
        .collect()
}

/// Every row of the block as name / value pairs, in schema order.
pub fn decode(block: &[u8]) -> Vec<Vec<(String, Decoded)>> {
    try_decode(block).expect("the block decodes")
}

/// Like [`decode`], but surfacing the error a malformed block produces.
pub fn try_decode(block: &[u8]) -> Result<Vec<Vec<(String, Decoded)>>, DecodeError> {
    let block = Block::parse(block)?;
    block
        .rows()
        .map(|row| Ok(row?.fields().iter().map(field_of).collect()))
        .collect()
}

/// The rows a possibly malformed block yields before its first error, and that error.
///
/// Unlike [`try_decode`] this keeps the rows decoded before the failure, which is what a
/// truncated block has to be judged by.
pub fn decode_prefix(block: &[u8]) -> (Vec<Vec<(String, Decoded)>>, Option<DecodeError>) {
    let parsed = match Block::parse(block) {
        Ok(parsed) => parsed,
        Err(error) => return (vec![], Some(error)),
    };

    let mut rows = Vec::new();
    for row in parsed.rows() {
        match row {
            Ok(row) => rows.push(row.fields().iter().map(field_of).collect()),
            Err(error) => return (rows, Some(error)),
        }
    }
    (rows, None)
}

fn field_of((name, value): &(&CStr, SharedValue)) -> (String, Decoded) {
    (
        name.to_string_lossy().into_owned(),
        Decoded::from_value(value),
    )
}

/// Decodes `block` through the coordinator's [`RowBlockDecoder`] into rows of `lookup`, and
/// reads every row back as name / value pairs in `lookup`'s key order.
///
/// Fails with the first error either [`RowBlockDecoder::begin`] or
/// [`RowBlockDecoder::next_row`] reports.
pub fn decode_into(
    block: &[u8],
    lookup: &mut RLookup<'_>,
) -> Result<Vec<Vec<(String, Decoded)>>, DecodeError> {
    let mut decoder = RowBlockDecoder::new();
    begin(&mut decoder, lookup, block)?;

    let mut rows = Vec::new();
    while decoder.has_rows() {
        let mut row = RLookupRow::new();
        // SAFETY: `lookup` outlives `decoder`.
        unsafe { decoder.next_row(&mut row) }?;
        rows.push(
            lookup
                .iter()
                .filter_map(|key| {
                    let value = row.get(key)?;
                    Some((
                        key.name().to_string_lossy().into_owned(),
                        Decoded::from_value(value),
                    ))
                })
                .collect(),
        );
    }
    Ok(rows)
}

/// Hands `decoder` a copy of `block` in a buffer of its own, the way the coordinator hands it
/// a shard reply's.
///
/// The lookup half of [`RowBlockDecoder::begin`]'s contract is left to the caller, whose
/// [`RowBlockDecoder::next_row`] calls have to uphold it anyway.
pub fn begin(
    decoder: &mut RowBlockDecoder,
    lookup: &mut RLookup<'_>,
    block: &[u8],
) -> Result<(), DecodeError> {
    let (buffer, len) = allocate(block);
    // SAFETY: `buffer` is a fresh allocation of `len` bytes that nothing else refers to, and
    // `release` frees it the way it was allocated.
    unsafe { decoder.begin(lookup, buffer, len, release) }
}

/// Copies `bytes` into a buffer from the allocator the Redis module allocator is mocked by.
pub fn allocate(bytes: &[u8]) -> (NonNull<u8>, usize) {
    let buffer = redis_mock::allocator::alloc_shim(bytes.len().max(1)).cast::<u8>();
    let buffer = NonNull::new(buffer).expect("the allocation succeeded");
    // SAFETY: the allocation is at least `bytes.len()` long and fresh.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.as_ptr(), bytes.len()) };
    (buffer, bytes.len())
}

/// Frees a buffer from [`allocate`]; the [`Dealloc`](value::shared_buffer::Dealloc) the
/// decoder is handed.
///
/// # Safety
///
/// 1. `buffer` must come from [`allocate`] and not be freed since.
pub unsafe fn release(buffer: NonNull<u8>, _len: usize) {
    redis_mock::allocator::free_shim(buffer.as_ptr().cast());
}

/// Builds a header for `ncols` columns, then whatever `rest` adds.
pub fn block(ncols: u16, rest: &[&[u8]]) -> Vec<u8> {
    let mut bytes = MAGIC.to_le_bytes().to_vec();
    bytes.push(VERSION);
    bytes.extend_from_slice(&ncols.to_le_bytes());
    for part in rest {
        bytes.extend_from_slice(part);
    }
    bytes
}

/// One [`ColumnKind::Tagged`] schema entry for a single-byte column name.
pub fn column(name: u8) -> Vec<u8> {
    typed_column(name, ColumnKind::Tagged)
}

/// One schema entry of the given `kind` for a single-byte column name.
pub fn typed_column(name: u8, kind: ColumnKind) -> Vec<u8> {
    let mut bytes = 1u16.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[name, 0, kind.to_byte()]);
    bytes
}

/// A valid, reasonably varied block, used as the starting point for corruption.
pub fn valid_block() -> Vec<u8> {
    let lookup = lookup(&["a", "bb", "ccc"]);
    encode(
        &lookup,
        &[
            row(
                &lookup,
                &[
                    ("a", SharedValue::new_num(-1.5)),
                    (
                        "ccc",
                        Decoded::Map(vec![(bytes("k"), Decoded::Array(vec![Decoded::Null]))])
                            .to_value(),
                    ),
                ],
            ),
            row(&lookup, &[]),
            row(
                &lookup,
                &[("bb", SharedValue::new_string(b"hello".to_vec()))],
            ),
        ],
    )
}

/// One block for every way the format can be malformed, each labelled for assertion messages.
pub fn malformed_blocks() -> Vec<(&'static str, Vec<u8>)> {
    let valid = valid_block();
    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 0xff;
    let mut bad_version = valid.clone();
    bad_version[4] = VERSION.wrapping_add(1);

    let mut nested = vec![0b1u8];
    for _ in 0..10_000 {
        nested.push(Tag::Array as u8);
        nested.extend_from_slice(&1u32.to_le_bytes());
    }
    nested.push(Tag::Null as u8);

    vec![
        ("bad magic", bad_magic),
        ("bad version", bad_version),
        ("truncated header", valid[..6].to_vec()),
        ("truncated schema", valid[..10].to_vec()),
        ("truncated value", valid[..valid.len() - 1].to_vec()),
        ("absurd column count", block(u16::MAX, &[&column(b'a')])),
        (
            "unterminated name",
            block(1, &[&1u16.to_le_bytes()[..], b"a", b"a", &[0]]),
        ),
        (
            "interior NUL in a name",
            block(1, &[&2u16.to_le_bytes()[..], b"a\0", &[0, 0]]),
        ),
        (
            "unknown column kind",
            block(1, &[&1u16.to_le_bytes()[..], b"a\0", &[6]]),
        ),
        (
            "truncated typed value",
            block(
                1,
                &[
                    &typed_column(b'a', ColumnKind::Typed(Tag::Number)),
                    &[0b1, 0, 0],
                ],
            ),
        ),
        (
            "absurd typed string length",
            block(
                1,
                &[
                    &typed_column(b'a', ColumnKind::Typed(Tag::String)),
                    &[0b1],
                    &u32::MAX.to_le_bytes()[..],
                ],
            ),
        ),
        ("rows without columns", block(0, &[&[0u8][..]])),
        (
            "truncated bitmap",
            block(9, &[&column(b'a').repeat(9), &[0xff]]),
        ),
        ("unknown tag", block(1, &[&column(b'a'), &[0b1, 200]])),
        (
            "absurd array count",
            block(
                1,
                &[
                    &column(b'a'),
                    &[0b1, Tag::Array as u8],
                    &u32::MAX.to_le_bytes()[..],
                ],
            ),
        ),
        (
            "absurd map count",
            block(
                1,
                &[
                    &column(b'a'),
                    &[0b1, Tag::Map as u8],
                    &u32::MAX.to_le_bytes()[..],
                ],
            ),
        ),
        (
            "absurd string length",
            block(
                1,
                &[
                    &column(b'a'),
                    &[0b1, Tag::String as u8],
                    &1_000_000u32.to_le_bytes()[..],
                    b"short",
                ],
            ),
        ),
        ("nesting too deep", block(1, &[&column(b'a'), &nested])),
        (
            "unterminated string",
            block(
                1,
                &[
                    &typed_column(b'a', ColumnKind::Typed(Tag::String)),
                    &[0b1],
                    &1u32.to_le_bytes()[..],
                    b"xy",
                ],
            ),
        ),
    ]
}
