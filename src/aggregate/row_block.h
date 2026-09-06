/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "reply.h"
#include "rlookup.h"
#include "search_result.h"

#ifdef __cplusplus
extern "C" {
#endif

/// Compact binary encoding for a chunk of aggregation rows on the internal
/// coordinator<->shard path.
///
/// RESP is a fine envelope but a poor record format: it is self-describing per value, so a
/// chunk of N rows costs one reply object per value *and* repeats every field name N times.
/// Measured on a 6-shard cluster with 668K intermediate rows, that came to ~344 bytes and
/// ~90 coordinator allocations per row, of which ~120 bytes/row was field names alone.
///
/// A block carries the field names once, then the rows, and rides inside a single RESP bulk
/// string - so the coordinator receives one reply object per chunk instead of ~15 per row,
/// and hiredis needs no modification.
///
/// Layout, all integers little-endian:
///
///     header   magic u32 | version u8 | ncols u16
///     schema   ncols x { name_len u16, name bytes, NUL }
///              (the NUL lets the reader hand a pointer straight into the block to the
///               RLookup key lookups, whose FFI contract requires NUL-terminated names)
///     rows     nrows x {
///                presence bitmap  ceil(ncols/8) bytes
///                per present column, in schema order:
///                  tag u8, then payload:
///                    TAG_NUM     f64
///                    TAG_STR     len u32, bytes
///                    TAG_NULL    (no payload)
///                    TAG_ARRAY   count u32, then `count` tagged values
///                    TAG_MAP     count u32, then `count` x (tagged key, tagged value)
///              }
///
/// The row count is implicit: the reader consumes rows until the buffer is exhausted, so a
/// writer never has to backpatch a count it does not know up front. A chunk with no rows is
/// therefore a header and schema with nothing after it, and is a valid block.
///
/// The same implicit count is why a schema with no columns cannot be encoded: such rows are
/// zero bytes long, and no reader could tell one from a thousand. Callers must reply in RESP
/// when RowBlockWriter_WriteSchema reports no columns.
#define ROW_BLOCK_MAGIC 0x52534252u  // 'R','S','B','R' little-endian
/// Bumped whenever the tag set or the layout changes: the reader rejects any other version,
/// and the two sides are always the same build in practice (internal path, no negotiation).
#define ROW_BLOCK_VERSION 2

#define ROW_BLOCK_TAG_NUM 1
#define ROW_BLOCK_TAG_STR 2
#define ROW_BLOCK_TAG_NULL 3
#define ROW_BLOCK_TAG_ARRAY 4
#define ROW_BLOCK_TAG_MAP 5

/// Growable output buffer for building a block. Reused across chunks of one request so the
/// per-chunk cost is amortised to zero after the first.
typedef struct {
  char *buf;
  size_t len;
  size_t cap;
  /// Number of columns declared in the header, for bitmap sizing.
  uint16_t ncols;
  /// Rows appended since the last reset, so the replay path can check it re-emitted every
  /// row the block held.
  size_t nrows;
  /// Set once the header and schema have been written.
  bool headerWritten;
} RowBlockWriter;

void RowBlockWriter_Init(RowBlockWriter *w);
void RowBlockWriter_Free(RowBlockWriter *w);

/// Reset for a new chunk, keeping the allocated capacity.
void RowBlockWriter_Reset(RowBlockWriter *w);

/// Write the header and the schema taken from `lk`'s visible keys, and return how many
/// columns it declares. Must be called once per chunk before any row.
/// `excludeFlags`/`requiredFlags` select the same key subset the RESP serializer would emit.
///
/// A zero return means this chunk cannot be encoded at all - see the layout notes above - and
/// the caller must reply in RESP instead.
uint16_t RowBlockWriter_WriteSchema(RowBlockWriter *w, const RLookup *lk, uint32_t requiredFlags,
                                    uint32_t excludeFlags);

/// Append one row, reading values for the schema's keys out of `r`'s row data.
///
/// Returns false when the row holds a value the format cannot represent, in which case
/// nothing is appended for it: the block still holds exactly the rows written before, so the
/// caller can emit it as is or discard it, but must not treat this row as encoded. Encoding
/// such a value as null instead would silently destroy it, which is the one outcome a
/// wire format may never produce.
bool RowBlockWriter_WriteRow(RowBlockWriter *w, const RLookup *lk, const SearchResult *r,
                             uint32_t requiredFlags, uint32_t excludeFlags, uint32_t reqFlags,
                             unsigned int apiVersion);

/// Emit the rows appended so far as ordinary RESP rows, and return how many were emitted.
///
/// The encoder read backwards, for abandoning a block after rows have already gone into it:
/// those rows exist nowhere else - the pipeline row they came from is long released - and a
/// chunk's reply carries either a block or RESP rows, never both. Each row is emitted as the
/// same name/value map the RESP row serializer produces, so a chunk that falls back is
/// indistinguishable on the wire from one a shard with the format off would have sent.
///
/// Only rows are replayed. The block never carried the per-row extras (id, score, sortkey)
/// that `serializeResult` can add, so a request that asks for those cannot use blocks in the
/// first place.
size_t RowBlockWriter_ReplayAsResp(const RowBlockWriter *w, RedisModule_Reply *reply,
                                   uint32_t reqFlags);

#ifdef __cplusplus
}
#endif
