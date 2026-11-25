/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{self, Cursor, Read, Write};

use index_gc::fork::wire::{deserialize, serialize};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

struct PartialIo<T>(T);

impl<T: Write> Write for PartialIo<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(&bytes[..bytes.len().min(137)])
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<T: Read> Read for PartialIo<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = bytes.len().min(113);
        self.0.read(&mut bytes[..count])
    }
}

/// The top bit of a chunk's length header, set on a message's last chunk.
const LAST_CHUNK: usize = 1 << (usize::BITS - 1);

/// Encode one chunk the way the codec does.
fn chunk(payload: &[u8], last: bool) -> Vec<u8> {
    let header = payload.len() | if last { LAST_CHUNK } else { 0 };
    let mut bytes = header.to_ne_bytes().to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

/// Split `wire` into its chunks as `(length, last)`, stopping after a last chunk.
/// Returns the chunks and the bytes left after them.
fn chunks(mut wire: &[u8]) -> (Vec<(usize, bool)>, &[u8]) {
    let mut found = Vec::new();
    while wire.len() >= size_of::<usize>() {
        let (header, rest) = wire.split_at(size_of::<usize>());
        let header = usize::from_ne_bytes(header.try_into().unwrap());
        let len = header & !LAST_CHUNK;
        let last = header & LAST_CHUNK != 0;
        found.push((len, last));
        wire = &rest[len.min(rest.len())..];
        if last {
            break;
        }
    }
    (found, wire)
}

#[test]
fn large_messages_use_bounded_chunks_and_preserve_the_next_message() {
    let payload = vec![42; 128 * 1024 + 7];
    let mut bytes = PartialIo(Vec::new());
    serialize(&mut bytes, serde_bytes::Bytes::new(&payload)).unwrap();
    let boundary = bytes.0.len();
    serialize(&mut bytes, Some(17u32)).unwrap();

    let (found, rest) = chunks(&bytes.0[..boundary]);
    assert!(found.len() >= 3);
    assert!(found.iter().all(|&(len, _)| (1..=64 * 1024).contains(&len)));
    assert_eq!(
        found.iter().filter(|&&(_, last)| last).count(),
        1,
        "only the final chunk is flagged"
    );
    assert!(found.last().unwrap().1);
    assert!(rest.is_empty(), "the flagged chunk ends the message");

    let mut reader = PartialIo(Cursor::new(&bytes.0));
    let decoded: serde_bytes::ByteBuf = deserialize(&mut reader, "payload").unwrap();
    assert_eq!(decoded.as_ref(), payload);
    assert_eq!(reader.0.position() as usize, boundary);
    assert_eq!(
        deserialize::<Option<u32>>(&mut reader, "next").unwrap(),
        Some(17)
    );
    assert_eq!(reader.0.position() as usize, bytes.0.len());
}

#[test]
fn rejects_invalid_chunks_truncated_messages_and_trailing_data() {
    let mut valid = Vec::new();
    serialize(&mut valid, Some(17u32)).unwrap();
    for cut in [0, 1, size_of::<usize>(), valid.len() - 1] {
        assert!(deserialize::<Option<u32>>(&mut &valid[..cut], "truncated").is_err());
    }
    for size in [
        0,
        64 * 1024 + 1,
        LAST_CHUNK,
        LAST_CHUNK | (64 * 1024 + 1),
        usize::MAX,
    ] {
        assert!(deserialize::<u32>(&mut size.to_ne_bytes().as_slice(), "invalid").is_err());
    }
    let trailing = chunk(&[17, 18], true);
    assert!(deserialize::<u32>(&mut trailing.as_slice(), "trailing").is_err());
    // A message whose chunks never include a flagged one runs into the stream's end.
    let unfinished = chunk(&[17], false);
    assert!(deserialize::<u32>(&mut unfinished.as_slice(), "unfinished").is_err());
}

#[derive(Default)]
struct FailingWriter {
    bytes: Vec<u8>,
    writes: usize,
    fail_write: Option<usize>,
    fail_flush: bool,
    failed: bool,
}

impl Write for FailingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(!self.failed, "write after failure");
        self.writes += 1;
        if self.fail_write == Some(self.writes) {
            self.failed = true;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        assert!(!self.failed, "flush after failure");
        if self.fail_flush {
            self.failed = true;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(())
    }
}

struct FailingMessage(usize);

impl serde::Serialize for FailingMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let mut sequence = serializer.serialize_seq(Some(3))?;
        sequence.serialize_element(serde_bytes::Bytes::new(&vec![42; self.0]))?;
        sequence.serialize_element(&17u8)?;
        Err(S::Error::custom("injected serialization failure"))
    }
}

/// A failed message must never look finished: no chunk it sent is flagged last.
fn assert_no_terminator(bytes: &[u8]) {
    assert!(chunks(bytes).0.iter().all(|&(_, last)| !last));
}

#[test]
fn failures_do_not_flush_pending_bytes_or_finish_the_message() {
    for size in [1, 128 * 1024] {
        let mut writer = FailingWriter::default();
        let error = serialize(&mut writer, FailingMessage(size)).unwrap_err();
        assert!(error.to_string().contains("injected serialization failure"));
        if size == 1 {
            assert!(writer.bytes.is_empty());
        } else {
            assert!(writer.bytes.len() >= 64 * 1024);
            assert_eq!(writer.bytes.last(), Some(&42));
        }
        assert_no_terminator(&writer.bytes);
    }

    for fail_write in [Some(1), Some(2), Some(3)] {
        let mut writer = FailingWriter {
            fail_write,
            ..FailingWriter::default()
        };
        let payload = vec![42; 128 * 1024];
        assert!(serialize(&mut writer, serde_bytes::Bytes::new(&payload)).is_err());
        assert!(writer.failed);
        assert_no_terminator(&writer.bytes);
    }

    // The flush comes after the whole message is written, so its failure leaves no
    // partial message behind, but it is still reported.
    let mut writer = FailingWriter {
        fail_flush: true,
        ..FailingWriter::default()
    };
    assert!(serialize(&mut writer, 17u8).is_err());
    assert!(writer.failed);
}

/// Each chunk costs one write, so a message that fits one chunk goes out as a
/// single write.
#[test]
fn each_chunk_costs_one_write() {
    for size in [1, 64 * 1024 - 16, 128 * 1024 + 7] {
        let mut writer = FailingWriter::default();
        serialize(&mut writer, serde_bytes::Bytes::new(&vec![42; size])).unwrap();

        let (found, rest) = chunks(&writer.bytes);
        assert!(rest.is_empty());
        assert_eq!(writer.writes, found.len(), "a {size}-byte message");
        if size < 64 * 1024 - 8 {
            assert_eq!(writer.writes, 1, "a {size}-byte message");
        }
    }
}
