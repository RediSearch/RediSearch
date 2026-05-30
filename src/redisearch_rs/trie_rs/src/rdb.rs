/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the lex-mode trie.
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad` in `src/trie/trie.c`, but for a Rust
//! [`TrieMap<TrieEntry>`].
//!
//! # Wire format
//!
//! ```text
//! u64  count                            // map.n_unique_keys()
//! [ bytes(key + '\0')
//!   f64  score
//!   bytes(payload + '\0')               // only if RdbOpts::payloads
//!   u64  num_docs                       // only if RdbOpts::num_docs
//! ] * count
//! ```
//!
//! # Architecture
//!
//! The wire format is dictated by C interop (parity with `RedisModule_Save*`
//! typed frames), so a general-purpose binary format like bincode/postcard
//! cannot drive it directly. Instead, the algorithm is expressed via
//! `serde::{Serialize, Deserialize}` impls on internal wrapper types
//! ([`MapSer`], [`EntrySer`] and their seeded counterparts), driven by a
//! bespoke [`RdbSerializer`]/[`RdbDeserializer`] pair that funnels the
//! serde primitives onto the [`RdbWrite`]/[`RdbRead`] IO seam. The IO seam
//! stays small (three typed methods each side); the schema lives in the
//! wrapper Serialize/Deserialize impls.
//!
//! # Trailing-NUL framing
//!
//! Both keys and payloads are written with a trailing NUL byte (so the wire
//! bytes are `key.len() + 1` long, matching C's `SaveStringBuffer(..., len + 1)`)
//! and the loader strips one byte. The NUL accounting is delegated to the
//! [`RdbWrite::save_bytes_nul_terminated`] / [`RdbRead::load_bytes_strip_nul`]
//! trait methods, which are the only paths bound by
//! [`Serializer::serialize_bytes`](serde::Serializer::serialize_bytes) and
//! [`Deserializer::deserialize_byte_buf`](serde::Deserializer::deserialize_byte_buf)
//! in the adapter.
//!
//! # Empty-payload normalization
//!
//! When `RdbOpts::payloads` is `true`, both `payload: None` and
//! `payload: Some(vec![])` emit the wire bytes `"\0"` and load back as
//! `None`. This mirrors the C-side collapse `payload.len ? &payload : NULL`
//! at `src/trie/trie.c:415`.
//!
//! # IO model
//!
//! Save is infallible at the Rust API level (the serde chain only ever
//! returns [`RdbError::Unsupported`] on a programmer error in the wrapper
//! schemas, never on IO). Errors only surface on load, through
//! [`RdbError`].

use std::fmt;
use std::fmt::Display;

use serde::de::{DeserializeSeed, Deserializer, Error as DeError, SeqAccess, Visitor};
use serde::ser::{
    Error as SerError, Impossible, Serialize, SerializeSeq, SerializeTuple, Serializer,
};

use crate::TrieMap;

/// One trie entry: insertion score, optional opaque payload, and number of
/// documents indexed under the key.
///
/// `payload: None` and `payload: Some(vec![])` are wire-indistinguishable
/// when payloads are persisted — both round-trip as `None`. See
/// [`RdbOpts::payloads`].
#[derive(Clone, Debug, PartialEq)]
pub struct TrieEntry {
    /// Insertion score. The C trie stores this as `float`; the RDB wire
    /// format widens it to `f64` (via `RedisModule_SaveDouble`).
    pub score: f64,
    /// Optional opaque payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Number of documents currently indexed under this key.
    pub num_docs: u64,
}

/// Controls which optional fields are present on the wire.
///
/// The same value must be used at save and load time; mismatches produce
/// [`RdbError`]s or silently parse following bytes as the wrong field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RdbOpts {
    /// Persist each entry's payload (with trailing NUL).
    pub payloads: bool,
    /// Persist each entry's `num_docs`.
    pub num_docs: bool,
}

/// Sink for the typed RDB save primitives.
///
/// One method per primitive type — `RedisModule_Save*` is a typed framing
/// API (length-prefixed buffers, fixed-width numbers) rather than a byte
/// stream, so [`std::io::Write`] would not be a faithful abstraction.
pub trait RdbWrite {
    /// Write a 64-bit unsigned integer.
    fn save_u64(&mut self, v: u64);
    /// Write a 64-bit IEEE-754 double.
    fn save_f64(&mut self, v: f64);
    /// Write `b` followed by one trailing NUL byte as a single length-prefixed
    /// buffer (total `b.len() + 1` bytes on the wire).
    ///
    /// The trailing NUL is the wire format expected by the C trie loader
    /// (`SaveStringBuffer(s, len + 1)` in `src/trie/trie.c`); centralizing
    /// it here keeps the algorithm body free of NUL-padding bookkeeping.
    fn save_bytes_nul_terminated(&mut self, b: &[u8]);
}

/// Source for the typed RDB load primitives.
///
/// Counterpart to [`RdbWrite`]. Every primitive may fail with [`RdbError`].
pub trait RdbRead {
    /// Read a 64-bit unsigned integer.
    fn load_u64(&mut self) -> Result<u64, RdbError>;
    /// Read a 64-bit IEEE-754 double.
    fn load_f64(&mut self) -> Result<f64, RdbError>;
    /// Read a length-prefixed buffer that is expected to end in a NUL byte
    /// and return its contents with the trailing NUL stripped.
    ///
    /// Returns [`RdbError::MissingTrailingNul`] when the wire buffer is
    /// empty or does not end in `0x00`.
    fn load_bytes_strip_nul(&mut self) -> Result<Vec<u8>, RdbError>;
}

/// Errors that can occur while reading a trie RDB payload, or that the
/// serde adapter surfaces when the schema is misused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdbError {
    /// The underlying RDB read failed (EOF, corrupted stream, etc.).
    Io,
    /// A bytes buffer expected to end with a NUL terminator did not.
    MissingTrailingNul,
    /// A serde primitive outside the supported set was requested. Always a
    /// bug in the schema wrappers — never a runtime stream condition.
    Unsupported,
    /// Schema-level error message produced by serde itself
    /// (via [`SerError::custom`] / [`DeError::custom`]).
    Custom(String),
}

impl Display for RdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => f.write_str("rdb io error"),
            Self::MissingTrailingNul => f.write_str("rdb bytes buffer missing trailing NUL"),
            Self::Unsupported => f.write_str("rdb serde adapter: unsupported primitive"),
            Self::Custom(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for RdbError {}

impl SerError for RdbError {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl DeError for RdbError {
    fn custom<T: Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

// ---------------------------------------------------------------------------
// serde Serializer adapter over RdbWrite
// ---------------------------------------------------------------------------

/// serde [`Serializer`] that emits onto an [`RdbWrite`] sink.
///
/// Only the primitives used by the trie schema are supported
/// (`u64`, `f64`, `bytes`, `seq`, `tuple`); every other serde primitive
/// returns [`RdbError::Unsupported`]. The compound `seq` and `tuple` types
/// reuse `Self` so that re-entrant element serialization just reborrows
/// the underlying writer.
pub struct RdbSerializer<'w, W>(pub &'w mut W);

impl<'w, W: RdbWrite> Serializer for &mut RdbSerializer<'w, W> {
    type Ok = ();
    type Error = RdbError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<(), RdbError>;
    type SerializeTupleVariant = Impossible<(), RdbError>;
    type SerializeMap = Impossible<(), RdbError>;
    type SerializeStruct = Impossible<(), RdbError>;
    type SerializeStructVariant = Impossible<(), RdbError>;

    fn serialize_u64(self, v: u64) -> Result<(), RdbError> {
        self.0.save_u64(v);
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), RdbError> {
        self.0.save_f64(v);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), RdbError> {
        self.0.save_bytes_nul_terminated(v);
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, RdbError> {
        let n = len.ok_or_else(|| <RdbError as SerError>::custom("seq must have known length"))?;
        self.0.save_u64(n as u64);
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, RdbError> {
        Ok(self)
    }

    fn serialize_bool(self, _: bool) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_i8(self, _: i8) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_i16(self, _: i16) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_i32(self, _: i32) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_i64(self, _: i64) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_u8(self, _: u8) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_u16(self, _: u16) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_u32(self, _: u32) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_f32(self, _: f32) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_char(self, _: char) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_str(self, _: &str) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_none(self) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _: &T) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_unit(self) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, RdbError> {
        Err(RdbError::Unsupported)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, RdbError> {
        Err(RdbError::Unsupported)
    }
}

impl<'w, W: RdbWrite> SerializeSeq for &mut RdbSerializer<'w, W> {
    type Ok = ();
    type Error = RdbError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), RdbError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), RdbError> {
        Ok(())
    }
}

impl<'w, W: RdbWrite> SerializeTuple for &mut RdbSerializer<'w, W> {
    type Ok = ();
    type Error = RdbError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), RdbError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), RdbError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// serde Deserializer adapter over RdbRead
// ---------------------------------------------------------------------------

/// serde [`Deserializer`] that pulls from an [`RdbRead`] source.
///
/// Symmetric to [`RdbSerializer`]: only the primitives the schema uses are
/// supported; everything else routes to `deserialize_any` and returns
/// [`RdbError::Unsupported`] via the [`serde::forward_to_deserialize_any!`]
/// fallback.
pub struct RdbDeserializer<'r, R>(pub &'r mut R);

impl<'de, 'r, R: RdbRead> Deserializer<'de> for &mut RdbDeserializer<'r, R> {
    type Error = RdbError;

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RdbError> {
        visitor.visit_u64(self.0.load_u64()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RdbError> {
        visitor.visit_f64(self.0.load_f64()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RdbError> {
        visitor.visit_byte_buf(self.0.load_bytes_strip_nul()?)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RdbError> {
        let n = self.0.load_u64()? as usize;
        visitor.visit_seq(SeqReader {
            de: self,
            remaining: n,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, RdbError> {
        visitor.visit_seq(SeqReader {
            de: self,
            remaining: len,
        })
    }

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, RdbError> {
        Err(RdbError::Unsupported)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u128 f32 char str string
        bytes option unit unit_struct newtype_struct tuple_struct map struct
        enum identifier ignored_any
    }
}

struct SeqReader<'a, 'r, R> {
    de: &'a mut RdbDeserializer<'r, R>,
    remaining: usize,
}

impl<'de, 'r, R: RdbRead> SeqAccess<'de> for SeqReader<'_, 'r, R> {
    type Error = RdbError;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, RdbError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

// ---------------------------------------------------------------------------
// schema wrappers
// ---------------------------------------------------------------------------

/// Forces a byte slice through `Serializer::serialize_bytes` (rather than
/// the default `Vec<u8>` → seq-of-u8 path) so the RdbSerializer hits the
/// NUL-framing primitive.
struct BytesField<'a>(&'a [u8]);

impl Serialize for BytesField<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// Counterpart to [`BytesField`]: forces `deserialize_byte_buf` so the
/// RdbDeserializer hits the NUL-stripping primitive and yields owned bytes.
struct OwnedBytes(Vec<u8>);

impl<'de> serde::Deserialize<'de> for OwnedBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a NUL-terminated byte buffer")
            }
            fn visit_byte_buf<E: DeError>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }
            fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }
        }
        deserializer.deserialize_byte_buf(V).map(OwnedBytes)
    }
}

struct MapSer<'a> {
    map: &'a TrieMap<TrieEntry>,
    opts: RdbOpts,
}

impl Serialize for MapSer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let n = self.map.n_unique_keys();
        let mut seq = serializer.serialize_seq(Some(n))?;
        for (key, entry) in self.map.iter() {
            seq.serialize_element(&EntrySer {
                key: &key,
                entry,
                opts: self.opts,
            })?;
        }
        seq.end()
    }
}

struct EntrySer<'a> {
    key: &'a [u8],
    entry: &'a TrieEntry,
    opts: RdbOpts,
}

impl Serialize for EntrySer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let len = 2 + (self.opts.payloads as usize) + (self.opts.num_docs as usize);
        let mut tup = serializer.serialize_tuple(len)?;
        tup.serialize_element(&BytesField(self.key))?;
        tup.serialize_element(&self.entry.score)?;
        if self.opts.payloads {
            tup.serialize_element(&BytesField(
                self.entry.payload.as_deref().unwrap_or(&[]),
            ))?;
        }
        if self.opts.num_docs {
            tup.serialize_element(&self.entry.num_docs)?;
        }
        tup.end()
    }
}

struct EntrySeed {
    opts: RdbOpts,
}

impl<'de> DeserializeSeed<'de> for EntrySeed {
    type Value = (Vec<u8>, TrieEntry);
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        let len = 2 + (self.opts.payloads as usize) + (self.opts.num_docs as usize);
        deserializer.deserialize_tuple(len, EntryVisitor { opts: self.opts })
    }
}

struct EntryVisitor {
    opts: RdbOpts,
}

impl<'de> Visitor<'de> for EntryVisitor {
    type Value = (Vec<u8>, TrieEntry);
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("trie entry tuple")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let key: OwnedBytes = seq
            .next_element()?
            .ok_or_else(|| A::Error::custom("missing key"))?;
        let score: f64 = seq
            .next_element()?
            .ok_or_else(|| A::Error::custom("missing score"))?;
        let payload = if self.opts.payloads {
            let buf: OwnedBytes = seq
                .next_element()?
                .ok_or_else(|| A::Error::custom("missing payload"))?;
            (!buf.0.is_empty()).then_some(buf.0)
        } else {
            None
        };
        let num_docs = if self.opts.num_docs {
            seq.next_element()?
                .ok_or_else(|| A::Error::custom("missing num_docs"))?
        } else {
            0
        };
        Ok((
            key.0,
            TrieEntry {
                score,
                payload,
                num_docs,
            },
        ))
    }
}

struct MapSeed {
    opts: RdbOpts,
}

impl<'de> DeserializeSeed<'de> for MapSeed {
    type Value = TrieMap<TrieEntry>;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(MapVisitor { opts: self.opts })
    }
}

struct MapVisitor {
    opts: RdbOpts,
}

impl<'de> Visitor<'de> for MapVisitor {
    type Value = TrieMap<TrieEntry>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("trie map (length-prefixed sequence of entries)")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut map = TrieMap::new();
        while let Some((key, entry)) = seq.next_element_seed(EntrySeed { opts: self.opts })? {
            map.insert(&key, entry);
        }
        Ok(map)
    }
}

// ---------------------------------------------------------------------------
// public entry points
// ---------------------------------------------------------------------------

/// Serialize a [`TrieMap<TrieEntry>`] to `writer` in the lex-mode RDB format.
///
/// Iterates entries in lexicographic key order. Keys, and payloads when
/// [`RdbOpts::payloads`] is set, are written with a trailing NUL byte to
/// match the C wire format (the loader strips it back off).
pub fn save<W: RdbWrite>(map: &TrieMap<TrieEntry>, writer: &mut W, opts: RdbOpts) {
    let mut ser = RdbSerializer(writer);
    MapSer { map, opts }
        .serialize(&mut ser)
        .expect("RDB save only invokes supported serde primitives");
}

/// Deserialize a [`TrieMap<TrieEntry>`] from `reader`.
///
/// `opts` must match the [`RdbOpts`] used at save time.
///
/// The trailing NUL byte is stripped from every key (and every payload
/// when [`RdbOpts::payloads`] is set). An empty payload (i.e. the wire
/// bytes `"\0"`) is normalized to `payload: None`.
pub fn load<R: RdbRead>(reader: &mut R, opts: RdbOpts) -> Result<TrieMap<TrieEntry>, RdbError> {
    let mut de = RdbDeserializer(reader);
    MapSeed { opts }.deserialize(&mut de)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        U64(u64),
        F64(f64),
        Bytes(Vec<u8>),
    }

    #[derive(Default)]
    struct Recorder(Vec<Op>);
    impl RdbWrite for Recorder {
        fn save_u64(&mut self, v: u64) {
            self.0.push(Op::U64(v));
        }
        fn save_f64(&mut self, v: f64) {
            self.0.push(Op::F64(v));
        }
        fn save_bytes_nul_terminated(&mut self, b: &[u8]) {
            let mut buf = Vec::with_capacity(b.len() + 1);
            buf.extend_from_slice(b);
            buf.push(0);
            self.0.push(Op::Bytes(buf));
        }
    }

    struct Replayer {
        ops: std::vec::IntoIter<Op>,
        fail_after: Option<usize>,
        calls: usize,
    }

    impl Replayer {
        fn new(ops: Vec<Op>) -> Self {
            Self {
                ops: ops.into_iter(),
                fail_after: None,
                calls: 0,
            }
        }

        fn fail_after(ops: Vec<Op>, n: usize) -> Self {
            Self {
                ops: ops.into_iter(),
                fail_after: Some(n),
                calls: 0,
            }
        }

        fn step(&mut self) -> Result<Op, RdbError> {
            if let Some(n) = self.fail_after
                && self.calls >= n
            {
                return Err(RdbError::Io);
            }
            self.calls += 1;
            self.ops.next().ok_or(RdbError::Io)
        }
    }

    impl RdbRead for Replayer {
        fn load_u64(&mut self) -> Result<u64, RdbError> {
            match self.step()? {
                Op::U64(v) => Ok(v),
                op => panic!("mock: expected U64, got {op:?}"),
            }
        }
        fn load_f64(&mut self) -> Result<f64, RdbError> {
            match self.step()? {
                Op::F64(v) => Ok(v),
                op => panic!("mock: expected F64, got {op:?}"),
            }
        }
        fn load_bytes_strip_nul(&mut self) -> Result<Vec<u8>, RdbError> {
            match self.step()? {
                Op::Bytes(mut v) => {
                    if v.pop() != Some(0) {
                        return Err(RdbError::MissingTrailingNul);
                    }
                    Ok(v)
                }
                op => panic!("mock: expected Bytes, got {op:?}"),
            }
        }
    }

    fn entry(score: f64, payload: Option<&[u8]>, num_docs: u64) -> TrieEntry {
        TrieEntry {
            score,
            payload: payload.map(<[u8]>::to_vec),
            num_docs,
        }
    }

    fn round_trip(map: &TrieMap<TrieEntry>, opts: RdbOpts) -> TrieMap<TrieEntry> {
        let mut rec = Recorder::default();
        save(map, &mut rec, opts);
        let mut rep = Replayer::new(rec.0);
        load(&mut rep, opts).expect("load should succeed")
    }

    #[test]
    fn save_empty_map() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        assert_eq!(rec.0, vec![Op::U64(0)]);
    }

    #[test]
    fn save_protocol_shape_keys_only() {
        let mut map = TrieMap::new();
        map.insert(b"alpha", entry(1.0, None, 0));
        map.insert(b"beta", entry(2.5, None, 0));
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        assert_eq!(
            rec.0,
            vec![
                Op::U64(2),
                Op::Bytes(b"alpha\0".to_vec()),
                Op::F64(1.0),
                Op::Bytes(b"beta\0".to_vec()),
                Op::F64(2.5),
            ]
        );
    }

    #[test]
    fn save_protocol_shape_with_all_opts() {
        let mut map = TrieMap::new();
        map.insert(b"x", entry(1.0, Some(b"pay"), 7));
        let mut rec = Recorder::default();
        save(
            &map,
            &mut rec,
            RdbOpts {
                payloads: true,
                num_docs: true,
            },
        );
        assert_eq!(
            rec.0,
            vec![
                Op::U64(1),
                Op::Bytes(b"x\0".to_vec()),
                Op::F64(1.0),
                Op::Bytes(b"pay\0".to_vec()),
                Op::U64(7),
            ]
        );
    }

    #[test]
    fn roundtrip_no_opts() {
        let mut map = TrieMap::new();
        map.insert(b"a", entry(1.0, None, 0));
        map.insert(b"b", entry(2.0, None, 0));
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.n_unique_keys(), 2);
        assert_eq!(loaded.find(b"a"), Some(&entry(1.0, None, 0)));
        assert_eq!(loaded.find(b"b"), Some(&entry(2.0, None, 0)));
    }

    #[test]
    fn roundtrip_payloads_only() {
        let mut map = TrieMap::new();
        // num_docs is set but not persisted by the opts; it must come back as 0.
        map.insert(b"foo", entry(1.0, Some(b"payload"), 99));
        let opts = RdbOpts {
            payloads: true,
            num_docs: false,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, Some(b"payload"), 0)));
    }

    #[test]
    fn roundtrip_num_docs_only() {
        let mut map = TrieMap::new();
        // Payload is set but not persisted; it must come back as None.
        map.insert(b"foo", entry(1.0, Some(b"ignored"), 42));
        let opts = RdbOpts {
            payloads: false,
            num_docs: true,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(1.0, None, 42)));
    }

    #[test]
    fn roundtrip_both() {
        let mut map = TrieMap::new();
        map.insert(b"foo", entry(3.5, Some(b"pay"), 11));
        map.insert(b"bar", entry(0.5, Some(b"x"), 1));
        let opts = RdbOpts {
            payloads: true,
            num_docs: true,
        };
        let loaded = round_trip(&map, opts);
        assert_eq!(loaded.find(b"foo"), Some(&entry(3.5, Some(b"pay"), 11)));
        assert_eq!(loaded.find(b"bar"), Some(&entry(0.5, Some(b"x"), 1)));
    }

    #[test]
    fn empty_trie_roundtrip() {
        let map: TrieMap<TrieEntry> = TrieMap::new();
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.n_unique_keys(), 0);
    }

    #[test]
    fn lex_order_preserved() {
        let mut map = TrieMap::new();
        for key in [b"zebra".as_slice(), b"apple", b"mango", b"banana"] {
            map.insert(key, entry(1.0, None, 0));
        }
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        let keys: Vec<Vec<u8>> = rec
            .0
            .into_iter()
            .filter_map(|op| match op {
                Op::Bytes(mut b) => {
                    b.pop();
                    Some(b)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                b"apple".to_vec(),
                b"banana".to_vec(),
                b"mango".to_vec(),
                b"zebra".to_vec(),
            ]
        );
    }

    #[test]
    fn empty_payload_normalizes_to_none() {
        let mut from_empty = TrieMap::new();
        from_empty.insert(b"k", entry(1.0, Some(b""), 0));
        let mut from_none = TrieMap::new();
        from_none.insert(b"k", entry(1.0, None, 0));

        let opts = RdbOpts {
            payloads: true,
            num_docs: false,
        };
        let mut rec_empty = Recorder::default();
        let mut rec_none = Recorder::default();
        save(&from_empty, &mut rec_empty, opts);
        save(&from_none, &mut rec_none, opts);
        assert_eq!(
            rec_empty.0, rec_none.0,
            "empty Vec and None must match on the wire"
        );

        let loaded = load(&mut Replayer::new(rec_empty.0), opts).unwrap();
        assert_eq!(loaded.find(b"k").unwrap().payload, None);
    }

    #[test]
    fn trailing_nul_on_every_bytes_op() {
        let mut map = TrieMap::new();
        map.insert(b"abc", entry(1.0, Some(b"def"), 1));
        let mut rec = Recorder::default();
        save(
            &map,
            &mut rec,
            RdbOpts {
                payloads: true,
                num_docs: true,
            },
        );
        for op in &rec.0 {
            if let Op::Bytes(b) = op {
                assert_eq!(b.last(), Some(&0), "bytes op missing trailing NUL: {b:?}");
            }
        }
    }

    #[test]
    fn io_error_propagates() {
        let mut map = TrieMap::new();
        map.insert(b"a", entry(1.0, None, 0));
        let mut rec = Recorder::default();
        save(&map, &mut rec, RdbOpts::default());
        // Ops: U64(1), Bytes("a\0"), F64(1.0). Inject an error after the count read.
        let mut rep = Replayer::fail_after(rec.0, 1);
        let err = load(&mut rep, RdbOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::Io);
    }

    #[test]
    fn multibyte_utf8_keys_roundtrip() {
        let mut map = TrieMap::new();
        let k1 = "héllo".as_bytes();
        let k2 = "日本語".as_bytes();
        map.insert(k1, entry(1.0, None, 0));
        map.insert(k2, entry(2.0, None, 0));
        let loaded = round_trip(&map, RdbOpts::default());
        assert_eq!(loaded.find(k1), Some(&entry(1.0, None, 0)));
        assert_eq!(loaded.find(k2), Some(&entry(2.0, None, 0)));
    }

    #[test]
    fn missing_trailing_nul_errors() {
        let ops = vec![
            Op::U64(1),
            Op::Bytes(b"abc".to_vec()), // missing trailing NUL
            Op::F64(1.0),
        ];
        let err = load(&mut Replayer::new(ops), RdbOpts::default()).unwrap_err();
        assert_eq!(err, RdbError::MissingTrailingNul);
    }
}
