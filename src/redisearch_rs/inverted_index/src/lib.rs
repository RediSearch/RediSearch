/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{c_char, c_int},
    fmt::Debug,
    io::{Read, Seek, Write},
    mem::ManuallyDrop,
};

use enumflags2::{BitFlags, bitflags};
pub use ffi::{RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask};

/// A delta is the difference between document IDs. It is mostly used to save space in the index
/// because document IDs are usually sequential and the difference between them are small. With the
/// help of encoding, we can optionally store the difference (delta) efficiently instead of the full document
/// ID.
pub struct Delta(usize);

impl Delta {
    fn pack(self) -> Vec<u8> {
        let mut delta = self.0;
        let mut delta_vec = Vec::with_capacity(7);

        while delta > 0 {
            let byte = delta & 0b1111_1111;
            delta_vec.push(byte as u8);
            delta >>= 8;
        }
        delta_vec
    }

    fn unpack(data: &[u8]) -> Self {
        let mut delta = 0;
        for (i, &byte) in data.iter().enumerate() {
            delta |= (byte as usize) << (i * 8);
        }
        Delta(delta)
    }
}

impl Delta {
    /// Make a new delta value
    pub fn new(delta: usize) -> Self {
        Delta(delta)
    }
}

impl From<Delta> for usize {
    fn from(delta: Delta) -> Self {
        delta.0
    }
}

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSOffsetVector_Iterator
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSOffsetVector {
    pub data: *mut c_char,
    pub len: u32,
}

/// Represents a single record of a document inside a term in the inverted index
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSTermRecord {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector,
}

#[bitflags]
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultType {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

pub type RSResultTypeMask = BitFlags<RSResultType, u32>;

/// Represents an aggregate array of values in an index record.
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSAggregateResult {
    /// The number of child records
    pub num_children: c_int,

    /// The capacity of the records array. Has no use for extensions
    pub children_cap: c_int,

    /// An array of records
    pub children: *mut *mut RSIndexResult,

    /// A map of the aggregate type of the underlying records
    pub type_mask: RSResultTypeMask,
}

/// Represents a virtual result in an index record.
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSVirtualResult;

/// Holds the actual data of an ['IndexResult']
#[repr(C)]
pub union RSIndexResultData {
    pub agg: ManuallyDrop<RSAggregateResult>,
    pub term: ManuallyDrop<RSTermRecord>,
    pub num: ManuallyDrop<RSNumericRecord>,
    pub virt: ManuallyDrop<RSVirtualResult>,
}

/// The result of an inverted index
/// cbindgen:field-names=[docId, dmd, fieldMask, freq, offsetsSz, data, type, isCopy, metrics, weight]
#[repr(C)]
pub struct RSIndexResult {
    /// The document ID of the result
    pub doc_id: t_docId,

    /// Some metadata about the result document
    pub dmd: *const RSDocumentMetadata,

    /// The aggregate field mask of all the records in this result
    pub field_mask: t_fieldMask,

    /// The total frequency of all the records in this result
    pub freq: u32,

    /// For term records only. This is used as an optimization, allowing the result to be loaded
    /// directly into memory
    pub offsets_sz: u32,

    data: RSIndexResultData,

    /// The type of data stored at ['Self::data']
    pub result_type: RSResultType,

    /// We mark copied results so we can treat them a bit differently on deletion, and pool them if
    /// we want
    pub is_copy: bool,

    /// Holds an array of metrics yielded by the different iterators in the AST
    pub metrics: *mut RSYieldableMetric,

    /// Relative weight for scoring calculations. This is derived from the result's iterator weight
    pub weight: f64,
}

impl RSIndexResult {
    /// Create a new numeric index result with the given numeric value
    pub fn numeric(doc_id: t_docId, num: f64) -> Self {
        Self {
            doc_id,
            dmd: std::ptr::null(),
            field_mask: 0,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                num: ManuallyDrop::new(RSNumericRecord(num)),
            },
            result_type: RSResultType::Numeric,
            is_copy: false,
            metrics: std::ptr::null_mut(),
            weight: 0.0,
        }
    }
}

impl Debug for RSIndexResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RSIndexResult");

        d.field("doc_id", &self.doc_id)
            .field("dmd", &self.dmd)
            .field("field_mask", &self.field_mask)
            .field("freq", &self.freq)
            .field("offsets_sz", &self.offsets_sz);

        match self.result_type {
            RSResultType::Numeric | RSResultType::Metric => {
                d.field(
                    "data.num",
                    // SAFETY: we just checked the type to ensure the data union has numeric data
                    unsafe { &self.data.num },
                );
            }
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                d.field(
                    "data.agg",
                    // SAFETY: we just checked the type to ensure the data union has aggregate data
                    unsafe { &self.data.agg },
                );
            }
            RSResultType::Term => {
                d.field(
                    "data.term",
                    // SAFETY: we just checked the type to ensure the data union has term data
                    unsafe { &self.data.term },
                );
            }
            RSResultType::Virtual => {}
        }

        d.field("result_type", &self.result_type)
            .field("is_copy", &self.is_copy)
            .field("metrics", &self.metrics)
            .field("weight", &self.weight)
            .finish()
    }
}

impl PartialEq for RSIndexResult {
    fn eq(&self, other: &Self) -> bool {
        if !(self.doc_id == other.doc_id
            && self.dmd == other.dmd
            && self.field_mask == other.field_mask
            && self.freq == other.freq
            && self.offsets_sz == other.offsets_sz
            && self.result_type == other.result_type
            && self.is_copy == other.is_copy
            && self.metrics == other.metrics
            && self.weight == other.weight)
        {
            return false;
        }

        match self.result_type {
            RSResultType::Numeric | RSResultType::Metric => {
                // SAFETY: we just checked the type of self to ensure the data union has numeric data
                let self_num = unsafe { &self.data.num };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has numeric data in its union.
                let other_num = unsafe { &other.data.num };

                self_num == other_num
            }
            RSResultType::Union | RSResultType::Intersection | RSResultType::HybridMetric => {
                // SAFETY: we just checked the type of self to ensure the data union has aggregate data
                let self_agg = unsafe { &self.data.agg };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has aggregate data in its union.
                let other_agg = unsafe { &other.data.agg };

                self_agg == other_agg
            }
            RSResultType::Term => {
                // SAFETY: we just checked the type of self to ensure the data union has term data
                let self_term = unsafe { &self.data.term };

                // SAFETY: from the previous checks we already know `other` has the same result
                // type as `self`. Therefore `other` also has term data in its union.
                let other_term = unsafe { &other.data.term };

                self_term == other_term
            }
            RSResultType::Virtual => true,
        }
    }
}

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
        writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;
}

pub enum DecoderResult {
    /// The record was successfully decoded.
    Record(RSIndexResult),
    /// The record was filtered out and should not be returned.
    FilteredOut,
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    ///
    /// Returns `Ok(None)` if there is nothing left on the reader to decode.
    fn decode<R: Read>(&self, reader: R, base: t_docId) -> std::io::Result<Option<DecoderResult>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<R: Read + Seek + Copy>(
        &self,
        reader: R,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult>> {
        loop {
            match self.decode(reader, base)? {
                Some(DecoderResult::Record(record)) if record.doc_id >= target => {
                    return Ok(Some(record));
                }
                Some(DecoderResult::Record(_)) | Some(DecoderResult::FilteredOut) => continue,
                None => return Ok(None),
            }
        }
    }
}

pub struct Numeric;

impl Encoder for Numeric {
    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        if matches!(
            record.result_type,
            RSResultType::Union
                | RSResultType::Intersection
                | RSResultType::Term
                | RSResultType::Virtual
                | RSResultType::HybridMetric
        ) {
            panic!("Numeric encoding only supports numeric types")
        }

        let delta = delta.pack();

        let num_record = unsafe { &record.data.num };

        let bytes_written = match FloatValue::from(num_record.0) {
            FloatValue::Tiny(i) => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Tiny(i),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)?
            }
            FloatValue::PosInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::PositiveInteger((end - 1) as _),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(bytes)?
            }
            FloatValue::NegInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::NegativeInteger((end - 1) as _),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(bytes)?
            }
            FloatValue::F32Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
            }
            FloatValue::F32Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: false,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
            }
            FloatValue::F64Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: true,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
            }
            FloatValue::F64Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: true,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
            }
            FloatValue::Infinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)?
            }
            FloatValue::NegInfinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: true,
                        is_f64: false,
                    },
                };

                writer.write(&[header.pack()])? + writer.write(&delta)?
            }
        };

        Ok(bytes_written)
    }
}

impl Decoder for Numeric {
    fn decode<R: Read>(
        &self,
        mut reader: R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>> {
        let mut header = [0u8; 1];
        let _bytes_read = reader.read(&mut header)?;
        let header = Header::unpack(header[0]);

        let mut delta = vec![0; header.delta_bytes as _];
        let _bytes_read = reader.read(&mut delta)?;
        let delta = Delta::unpack(&delta);

        let doc_id = base + (delta.0 as u64);

        let num = match header.typ {
            HeaderType::Tiny(i) => i as _,
            HeaderType::PositiveInteger(len) => {
                let mut bytes = vec![0; (len + 1) as usize];
                let _bytes_read = reader.read(&mut bytes)?;
                let mut num = 0;

                for (i, byte) in bytes.iter().enumerate() {
                    num |= (*byte as u64) << (8 * i);
                }

                num as _
            }
            HeaderType::NegativeInteger(len) => {
                let mut bytes = vec![0; (len + 1) as usize];
                let _bytes_read = reader.read(&mut bytes)?;
                let mut num = 0;

                for (i, byte) in bytes.iter().enumerate() {
                    num |= (*byte as u64) << (8 * i);
                }

                (num as f64) * -1.0
            }
            HeaderType::Float {
                is_infinite,
                is_negative,
                is_f64,
            } => {
                if is_infinite && !is_negative {
                    f64::INFINITY
                } else if is_infinite {
                    f64::NEG_INFINITY
                } else {
                    if is_f64 {
                        let multiplier = if is_negative { -1.0 } else { 1.0 };
                        let mut bytes = [0u8; 8];
                        let _bytes_read = reader.read(&mut bytes)?;

                        f64::from_le_bytes(bytes) * multiplier
                    } else {
                        let multiplier = if is_negative { -1.0 } else { 1.0 };
                        let mut bytes = [0u8; 4];
                        let _bytes_read = reader.read(&mut bytes)?;
                        let f = f32::from_le_bytes(bytes) * multiplier;

                        f as _
                    }
                }
            }
        };
        let record = RSIndexResult::numeric(doc_id, num);

        Ok(Some(DecoderResult::Record(record)))
    }
}

enum FloatValue {
    Tiny(u8),
    PosInt(u64),
    NegInt(u64),
    F32Pos(f32),
    F32Neg(f32),
    F64Pos(f64),
    F64Neg(f64),
    Infinity,
    NegInfinity,
}

impl From<f64> for FloatValue {
    fn from(value: f64) -> Self {
        if value.fract() == 0.0 {
            if value >= 0.0 {
                let i = value as u64;

                if i <= 0b111 {
                    FloatValue::Tiny(i as u8)
                } else {
                    FloatValue::PosInt(i)
                }
            } else {
                FloatValue::NegInt((value * -1.0) as _)
            }
        } else {
            match value {
                f64::INFINITY => FloatValue::Infinity,
                f64::NEG_INFINITY => FloatValue::NegInfinity,
                v => {
                    let f32_value = v as f32;
                    let back_to_f64 = f32_value as f64;

                    if back_to_f64 == v {
                        if v < 0.0 {
                            FloatValue::F32Neg(f32_value.abs())
                        } else {
                            FloatValue::F32Pos(f32_value)
                        }
                    } else {
                        if v < 0.0 {
                            FloatValue::F64Neg(v.abs())
                        } else {
                            FloatValue::F64Pos(v)
                        }
                    }
                }
            }
        }
    }
}

enum HeaderType {
    Tiny(u8),
    Float {
        is_infinite: bool,
        is_negative: bool,
        is_f64: bool,
    },
    PositiveInteger(u8),
    NegativeInteger(u8),
}

struct Header {
    delta_bytes: u8,
    typ: HeaderType,
}

impl Header {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const POS_INT_TYPE: u8 = 0b10;
    const NEG_INT_TYPE: u8 = 0b11;

    fn pack(self) -> u8 {
        let mut packed = 0;
        packed |= self.delta_bytes & 0b111; // 3 bits for delta bytes

        match self.typ {
            HeaderType::Tiny(t) => {
                packed |= Self::TINY_TYPE << 3; // 2 bits for type
                packed |= (t & 0b111) << 5; // 3 bits for value
            }
            HeaderType::PositiveInteger(b) => {
                packed |= Self::POS_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::NegativeInteger(b) => {
                packed |= Self::NEG_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::Float {
                is_infinite,
                is_negative,
                is_f64,
            } => {
                packed |= Self::FLOAT_TYPE << 3; // 2 bits for type

                if is_infinite {
                    packed |= 1 << 5;
                }
                if is_negative {
                    packed |= 1 << 6;
                }
                if is_f64 {
                    packed |= 1 << 7;
                }
            }
        }

        packed
    }

    fn unpack(data: u8) -> Self {
        let delta_bytes = data & 0b111; // 3 bits for the delta bytes

        match (data >> 3) & 0b11 {
            Self::TINY_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::Tiny(data >> 5 & 0b111), // 3 bits for the value
                }
            }
            Self::FLOAT_TYPE => {
                let is_infinite = (data >> 5) & 0b1 != 0;
                let is_negative = (data >> 6) & 0b1 != 0;
                let is_f64 = (data >> 7) & 0b1 != 0;
                Self {
                    delta_bytes,
                    typ: HeaderType::Float {
                        is_infinite,
                        is_negative,
                        is_f64,
                    },
                }
            }
            Self::POS_INT_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::PositiveInteger(data >> 5 & 0b111), // 3 bits for the value bytes
                }
            }
            Self::NEG_INT_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::NegativeInteger(data >> 5 & 0b111), // 3 bits for the value bytes
                }
            }
            _ => unreachable!("All four possible combinations are covered"),
        }
    }
}

#[cfg(test)]
mod tests;
