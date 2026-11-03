/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{self, Read, Write};

use serde::{Serialize, de::DeserializeOwned};

use crate::GcError;

const CHUNK_SIZE: usize = 64 * 1024;

/// Set in a chunk's length header on the last chunk of a message, so no separate
/// frame has to mark the message's end.
const LAST_CHUNK: usize = 1 << (usize::BITS - 1);

const HEADER_SIZE: usize = size_of::<usize>();

/// Serialize one MessagePack message as length-prefixed chunks of at most
/// [`CHUNK_SIZE`] bytes, the last flagged with [`LAST_CHUNK`].
///
/// Each chunk is a single write, so a message that fits one chunk costs one
/// write: one message per dirty key makes per-message writes the cost that counts.
pub fn serialize<T: Serialize>(writer: &mut impl Write, message: T) -> io::Result<()> {
    let mut chunks = ChunkWriter {
        writer,
        buffer: vec![0; HEADER_SIZE],
    };
    message
        .serialize(&mut rmp_serde::Serializer::new(&mut chunks))
        .map_err(io::Error::other)?;
    // A failed message never reaches here, so its last chunk is never sent flagged.
    chunks.send(LAST_CHUNK)?;
    chunks.writer.flush()
}

/// Decode one MessagePack message without reading past its last chunk.
pub fn deserialize<T: DeserializeOwned>(
    reader: &mut impl Read,
    context: &'static str,
) -> Result<T, GcError> {
    let mut chunks = ChunkReader {
        reader,
        chunk: Vec::new(),
        position: 0,
        last: false,
    };
    let message = rmp_serde::from_read(&mut chunks).map_err(|e| GcError::codec(context, e))?;
    if !chunks.last || chunks.position != chunks.chunk.len() {
        return Err(GcError::codec(
            context,
            "trailing bytes in MessagePack message",
        ));
    }
    Ok(message)
}

/// Collects a chunk behind room for its header, sending each full chunk as it fills.
struct ChunkWriter<W> {
    writer: W,
    /// A header's worth of space, then the chunk's bytes so far.
    buffer: Vec<u8>,
}

impl<W: Write> ChunkWriter<W> {
    /// Write the buffered chunk, with `flags` in its header, and start the next one.
    fn send(&mut self, flags: usize) -> io::Result<()> {
        let len = self.buffer.len() - HEADER_SIZE;
        self.buffer[..HEADER_SIZE].copy_from_slice(&(len | flags).to_ne_bytes());
        self.writer.write_all(&self.buffer)?;
        self.buffer.truncate(HEADER_SIZE);
        Ok(())
    }
}

impl<W: Write> Write for ChunkWriter<W> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        while !bytes.is_empty() {
            // A full chunk is sent only once more bytes arrive, so the message's last
            // chunk is always the one still buffered when serialization ends.
            if self.buffer.len() == HEADER_SIZE + CHUNK_SIZE {
                self.send(0)?;
            }
            // The buffer stays within one header and one chunk: it starts at the header,
            // `send` truncates back to it, and an append never exceeds the chunk's room.
            let room = HEADER_SIZE + CHUNK_SIZE - self.buffer.len();
            let (now, rest) = bytes.split_at(bytes.len().min(room));
            self.buffer.extend_from_slice(now);
            bytes = rest;
        }
        Ok(written)
    }

    /// Sends nothing: a flush mid-message would send the last chunk unflagged.
    /// [`serialize`] sends it, flagged, once the message is complete.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reads a message's chunks one whole chunk at a time: a header read, then a
/// payload read, so MessagePack's small reads never reach the pipe.
struct ChunkReader<R> {
    reader: R,
    chunk: Vec<u8>,
    position: usize,
    last: bool,
}

impl<R: Read> Read for ChunkReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.position == self.chunk.len() {
            if self.last {
                return Ok(0);
            }
            let mut header = [0; HEADER_SIZE];
            self.reader.read_exact(&mut header)?;
            let header = usize::from_ne_bytes(header);
            let len = header & !LAST_CHUNK;
            if !(1..=CHUNK_SIZE).contains(&len) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid GC chunk size",
                ));
            }
            self.last = header & LAST_CHUNK != 0;
            self.chunk.resize(len, 0);
            self.reader.read_exact(&mut self.chunk)?;
            self.position = 0;
        }
        let available = &self.chunk[self.position..];
        let n = bytes.len().min(available.len());
        bytes[..n].copy_from_slice(&available[..n]);
        self.position += n;
        Ok(n)
    }
}
