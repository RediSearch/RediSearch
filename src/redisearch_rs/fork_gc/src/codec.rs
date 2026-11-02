/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{self, BufReader, BufWriter, Read, Write};

use serde::{Serialize, de::DeserializeOwned};

use crate::{Frame, HandleError};

const CHUNK_SIZE: usize = 64 * 1024;

/// Serialize one MessagePack message using bounded buffers and bulk pipe writes.
pub fn serialize<T: Serialize>(writer: &mut impl Write, message: T) -> io::Result<()> {
    let mut chunks = ChunkWriter(&mut *writer);
    let mut buffered = BufWriter::with_capacity(CHUNK_SIZE, &mut chunks);
    let result = message
        .serialize(&mut rmp_serde::Serializer::new(&mut buffered))
        .map_err(io::Error::other)
        .and_then(|()| buffered.flush());
    // A failed message must not flush another partial chunk on drop.
    let _ = buffered.into_parts();
    result?;
    Frame::<&[u8]>::Terminator.encode(writer)
}

/// Decode one MessagePack message without reading past its terminating frame.
pub fn deserialize<T: DeserializeOwned>(
    reader: &mut impl Read,
    context: &'static str,
) -> Result<T, HandleError> {
    let chunks = ChunkReader {
        reader,
        remaining: 0,
        finished: false,
    };
    let mut buffered = BufReader::with_capacity(CHUNK_SIZE, chunks);
    let message =
        rmp_serde::from_read(&mut buffered).map_err(|e| HandleError::codec(context, e))?;
    match buffered.read(&mut [0]) {
        Ok(0) => Ok(message),
        Ok(_) => Err(HandleError::codec(
            context,
            "trailing bytes in MessagePack message",
        )),
        Err(error) => Err(HandleError::codec(context, error)),
    }
}

struct ChunkWriter<W>(W);

impl<W: Write> Write for ChunkWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(CHUNK_SIZE) {
            Frame::data(chunk).encode(&mut self.0)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

struct ChunkReader<R> {
    reader: R,
    remaining: usize,
    finished: bool,
}

impl<R: Read> Read for ChunkReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() || self.finished {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut header = [0; size_of::<usize>()];
            self.reader.read_exact(&mut header)?;
            self.remaining = usize::from_ne_bytes(header);
            if self.remaining == usize::MAX {
                self.finished = true;
                return Ok(0);
            }
            if !(1..=CHUNK_SIZE).contains(&self.remaining) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid GC chunk size",
                ));
            }
        }
        let count = bytes.len().min(self.remaining);
        let read = self.reader.read(&mut bytes[..count])?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.remaining -= read;
        Ok(read)
    }
}
