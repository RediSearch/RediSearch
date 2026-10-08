/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared scaffolding: building lookups and rows, encoding them, and comparing what comes back out.

use rlookup::{RLookup, RLookupKeyFlags, RLookupRow};
use row_block::{
    Block, ColumnKind, DecodeError, MAGIC, MAX_NESTING_DEPTH, RowBlockDecoder, RowBlockWriter, Tag,
    TrioMember, VERSION,
};
use std::ffi::{CStr, CString};
use value::{SharedValue, Value};

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

/// Encodes `rows` as one block with every key a column, panicking if the writer refuses anything.
pub fn encode(lookup: &RLookup<'_>, rows: &[RLookupRow<'_>]) -> Vec<u8> {
    encode_with_trio(lookup, rows, TrioMember::Middle)
}

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

#[derive(Debug, Clone)]
pub enum Decoded {
    Number(f64),
    /// A string value's raw bytes: the format carries neither an encoding nor a NUL rule.
    Bytes(Vec<u8>),
    Null,
    Array(Vec<Decoded>),
    Map(Vec<(Decoded, Decoded)>),
}

// Numbers compare by bit pattern rather than by value: this is a wire format, so `NaN` must survive a round trip as the
// same `NaN`, and `-0.0` must not come back as `0.0`.
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
    /// Panics on anything the decoder should never produce.
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

pub fn bytes(s: &str) -> Decoded {
    Decoded::Bytes(s.as_bytes().to_vec())
}

pub fn columns_of(block: &[u8]) -> Vec<String> {
    Block::parse(block)
        .expect("the block parses")
        .columns()
        .iter()
        .map(|name| name.to_string_lossy().into_owned())
        .collect()
}

/// Every row of the block as name / value pairs, in schema order, decoded the way the coordinator decodes it.
pub fn decode(block: &[u8]) -> Vec<Vec<(String, Decoded)>> {
    // A fresh lookup creates the columns' keys in schema order, which is the order `decode_into` reads them back in.
    decode_into(block, &mut RLookup::new()).expect("the block decodes")
}

/// The rows the reader the replay path uses yields, or the first error it reports.
pub fn try_decode(block: &[u8]) -> Result<Vec<Vec<(String, Decoded)>>, DecodeError> {
    let block = Block::parse(block)?;
    block
        .rows()
        .map(|row| Ok(row?.fields().iter().map(field_of).collect()))
        .collect()
}

/// The rows a possibly malformed block yields before its first error, and that error.
///
/// Unlike [`try_decode`] this keeps the rows decoded before the failure, which is what a truncated block has to be
/// judged by.
#[cfg(not(miri))] // Only the property tests use it, and Miri skips them.
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

/// Decodes `block` through [`RowBlockDecoder`] into rows of `lookup`, read back as name / value pairs in `lookup`'s key
/// order.
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

/// Makes `block` the active block of `decoder`. [`RowBlockDecoder::begin`]'s contract is left to the caller: `block`
/// must outlive the block, and `lookup` its [`RowBlockDecoder::next_row`] calls.
pub fn begin(
    decoder: &mut RowBlockDecoder,
    lookup: &mut RLookup<'_>,
    block: &[u8],
) -> Result<(), DecodeError> {
    // SAFETY: left to the caller, as documented above.
    unsafe { decoder.begin(lookup, block) }
}

pub fn block(ncols: u16, rest: &[&[u8]]) -> Vec<u8> {
    let mut bytes = MAGIC.to_le_bytes().to_vec();
    bytes.push(VERSION);
    bytes.extend_from_slice(&ncols.to_le_bytes());
    for part in rest {
        bytes.extend_from_slice(part);
    }
    bytes
}

/// A [`ColumnKind::Tagged`] schema entry for a column named `a`.
pub const TAGGED_COLUMN: [u8; 5] = [1, 0, b'a', 0, 0];

/// A schema entry of the given `kind` for a single-byte column name.
pub fn typed_column(name: u8, kind: ColumnKind) -> Vec<u8> {
    let mut bytes = 1u16.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[name, 0, kind.to_byte()]);
    bytes
}

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

/// Where a malformed block is caught: by [`Block::parse`] / [`RowBlockDecoder::begin`], or by the row that holds the
/// corruption. The coordinator reports the two differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Schema,
    Row,
}

pub struct Malformed {
    pub what: &'static str,
    pub block: Vec<u8>,
    pub error: DecodeError,
    pub phase: Phase,
}

pub fn malformed_blocks() -> Vec<Malformed> {
    use DecodeError::*;
    use Phase::*;

    let valid = valid_block();
    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 0xff;
    let magic = u32::from_le_bytes(bad_magic[..4].try_into().unwrap());
    // Not read as best-effort: another version may have reused a tag, so guessing at the payloads would produce wrong
    // values rather than an error.
    let mut bad_version = valid.clone();
    bad_version[4] = VERSION.wrapping_add(1);

    // One array tag plus a count of 1 buys a level of recursion, so a few kilobytes of block would otherwise recurse
    // deep enough to overflow the stack.
    let mut nested = vec![0b1u8];
    for _ in 0..10_000 {
        nested.push(Tag::Array as u8);
        nested.extend_from_slice(&1u32.to_le_bytes());
    }
    nested.push(Tag::Null as u8);

    let tagged = |rest: &[&[u8]]| {
        let mut parts: Vec<&[u8]> = vec![&TAGGED_COLUMN, &[0b1]];
        parts.extend_from_slice(rest);
        block(1, &parts)
    };
    let typed = |tag: Tag, rest: &[&[u8]]| {
        let column = typed_column(b'a', ColumnKind::Typed(tag));
        let mut parts: Vec<&[u8]> = vec![&column, &[0b1]];
        parts.extend_from_slice(rest);
        block(1, &parts)
    };
    let case = |what, block, error, phase| Malformed {
        what,
        block,
        error,
        phase,
    };

    vec![
        case("bad magic", bad_magic, BadMagic { magic }, Schema),
        case(
            "bad version",
            bad_version,
            UnsupportedVersion {
                version: VERSION.wrapping_add(1),
            },
            Schema,
        ),
        case("truncated header", valid[..6].to_vec(), Truncated, Schema),
        case("truncated schema", valid[..10].to_vec(), Truncated, Schema),
        // Every column costs at least four bytes, so this is caught before the decoder tries to reserve room for 65535
        // names.
        case(
            "absurd column count",
            block(u16::MAX, &[&TAGGED_COLUMN]),
            Truncated,
            Schema,
        ),
        case(
            "unterminated name",
            block(1, &[&1u16.to_le_bytes(), b"a", b"a", &[0]]),
            MalformedName,
            Schema,
        ),
        // An interior NUL would make the name the decoder hands on shorter than its declared length, so the two sides
        // would disagree about which column this is.
        case(
            "interior NUL in a name",
            block(1, &[&2u16.to_le_bytes(), b"a\0", &[0, 0]]),
            MalformedName,
            Schema,
        ),
        case(
            "unknown column kind",
            block(1, &[&1u16.to_le_bytes(), b"a\0", &[6]]),
            UnknownColumnKind { kind: 6 },
            Schema,
        ),
        // Such rows would be zero bytes long, so no reader could tell one from a thousand.
        case(
            "rows without columns",
            block(0, &[&[0]]),
            RowsWithoutColumns,
            Schema,
        ),
        case(
            "truncated value",
            valid[..valid.len() - 1].to_vec(),
            Truncated,
            Row,
        ),
        case(
            "truncated bitmap",
            block(9, &[&TAGGED_COLUMN.repeat(9), &[0xff]]),
            Truncated,
            Row,
        ),
        case(
            "truncated typed value",
            typed(Tag::Number, &[&[0, 0]]),
            Truncated,
            Row,
        ),
        // A tag's payload length is unknown, so the cursor cannot step over it.
        case(
            "unknown tag",
            tagged(&[&[200]]),
            UnknownTag { tag: 200 },
            Row,
        ),
        case(
            "the tagged kind byte as a tag",
            tagged(&[&[0]]),
            UnknownTag { tag: 0 },
            Row,
        ),
        // Without the count checks these would be handed to `Vec::with_capacity`, turning four corrupt bytes into a
        // multi-gigabyte allocation.
        case(
            "absurd array count",
            tagged(&[&[Tag::Array as u8], &u32::MAX.to_le_bytes()]),
            ImplausibleCount { count: u32::MAX },
            Row,
        ),
        case(
            "absurd map count",
            tagged(&[&[Tag::Map as u8], &u32::MAX.to_le_bytes()]),
            ImplausibleCount { count: u32::MAX },
            Row,
        ),
        // A map entry is a key *and* a value, so it costs two bytes at the very least; a count checked against one byte
        // per entry would run off the end.
        case(
            "map count only half backed",
            tagged(&[
                &[Tag::Map as u8],
                &2u32.to_le_bytes(),
                &[Tag::Null as u8; 3],
            ]),
            ImplausibleCount { count: 2 },
            Row,
        ),
        case(
            "absurd string length",
            tagged(&[&[Tag::String as u8], &1_000_000u32.to_le_bytes(), b"short"]),
            ImplausibleCount { count: 1_000_000 },
            Row,
        ),
        case(
            "absurd typed string length",
            typed(Tag::String, &[&u32::MAX.to_le_bytes()]),
            ImplausibleCount { count: u32::MAX },
            Row,
        ),
        case(
            "unterminated string",
            typed(Tag::String, &[&1u32.to_le_bytes(), b"xy"]),
            UnterminatedString,
            Row,
        ),
        case(
            "nesting too deep",
            block(1, &[&TAGGED_COLUMN, &nested]),
            TooDeeplyNested,
            Row,
        ),
    ]
}

pub fn too_deep() -> SharedValue {
    (0..=MAX_NESTING_DEPTH).fold(SharedValue::new_num(1.0), |inner, _| {
        SharedValue::new_array(vec![inner])
    })
}
