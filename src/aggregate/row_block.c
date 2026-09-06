/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "row_block.h"

#include <string.h>

#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "query_flags.h"
#include "search_ctx.h"
#include "value_ffi.h"

#define ROW_BLOCK_INITIAL_CAP 8192

void RowBlockWriter_Init(RowBlockWriter *w) {
  w->buf = rm_malloc(ROW_BLOCK_INITIAL_CAP);
  w->len = 0;
  w->cap = ROW_BLOCK_INITIAL_CAP;
  w->ncols = 0;
  w->nrows = 0;
  w->headerWritten = false;
}

void RowBlockWriter_Free(RowBlockWriter *w) {
  rm_free(w->buf);
  w->buf = NULL;
  w->len = w->cap = 0;
}

void RowBlockWriter_Reset(RowBlockWriter *w) {
  w->len = 0;
  w->ncols = 0;
  w->nrows = 0;
  w->headerWritten = false;
}

// Ensure room for `extra` more bytes. Growth is doubling, so a request's first chunk pays
// the reallocations and every later chunk reuses the capacity.
static inline void reserve(RowBlockWriter *w, size_t extra) {
  if (w->len + extra <= w->cap) return;
  size_t cap = w->cap ? w->cap : ROW_BLOCK_INITIAL_CAP;
  while (cap < w->len + extra) cap *= 2;
  w->buf = rm_realloc(w->buf, cap);
  w->cap = cap;
}

static inline void put_bytes(RowBlockWriter *w, const void *p, size_t n) {
  reserve(w, n);
  memcpy(w->buf + w->len, p, n);
  w->len += n;
}

static inline void put_u8(RowBlockWriter *w, uint8_t v) { put_bytes(w, &v, 1); }
static inline void put_u16(RowBlockWriter *w, uint16_t v) { put_bytes(w, &v, 2); }
static inline void put_u32(RowBlockWriter *w, uint32_t v) { put_bytes(w, &v, 4); }
static inline void put_f64(RowBlockWriter *w, double v) { put_bytes(w, &v, 8); }

// True when `key` is one of the columns the RESP serializer would emit.
static inline bool keyIsVisible(const RLookupKey *key, uint32_t requiredFlags,
                               uint32_t excludeFlags) {
  if (!RLookupKey_GetName(key)) return false;
  uint32_t flags = RLookupKey_GetFlags(key);
  if (flags & excludeFlags) return false;
  if (requiredFlags && !(flags & requiredFlags)) return false;
  return true;
}

uint16_t RowBlockWriter_WriteSchema(RowBlockWriter *w, const RLookup *lk, uint32_t requiredFlags,
                                    uint32_t excludeFlags) {
  RS_ASSERT(!w->headerWritten);
  put_u32(w, ROW_BLOCK_MAGIC);
  put_u8(w, ROW_BLOCK_VERSION);

  // The column count is not known until the keys have been walked, so reserve its slot and
  // backpatch rather than iterating twice.
  size_t ncolsAt = w->len;
  put_u16(w, 0);

  uint16_t ncols = 0;
  RLOOKUP_FOREACH(key, lk, {
    if (!keyIsVisible(key, requiredFlags, excludeFlags)) continue;
    size_t nameLen = RLookupKey_GetNameLen(key);
    RS_ASSERT(nameLen <= UINT16_MAX);
    put_u16(w, (uint16_t)nameLen);
    // Include the terminator: the decoder resolves names by handing a pointer into this
    // buffer to RLookup_GetKey_ReadEx, which requires NUL-terminated input.
    put_bytes(w, RLookupKey_GetName(key), nameLen + 1);
    ncols++;
  })

  memcpy(w->buf + ncolsAt, &ncols, sizeof(ncols));
  w->ncols = ncols;
  w->headerWritten = true;
  return ncols;
}

// Append one value. Mirrors the type handling of RedisModule_Reply_RSValue, including the
// trio resolution, so a block and a RESP reply carry the same values for the same row.
//
// Returns false, having written nothing decodable for this value, when it is of a type the
// format has no tag for. Every RSValueType the pipeline can produce today is handled
// explicitly; the refusal exists so a type added later cannot be quietly flattened to null
// by a catch-all branch. Callers must abandon the whole row on false - see
// RowBlockWriter_WriteRow.
static bool writeValue(RowBlockWriter *w, const RSValue *v, uint32_t reqFlags,
                       unsigned int apiVersion) {
  if (!v) {
    put_u8(w, ROW_BLOCK_TAG_NULL);
    return true;
  }

  // Loop rather than resolve once: the RESP path recurses through
  // RedisModule_Reply_RSValue, so a trio nested in a trio resolves there too.
  while (RSValue_IsTrio(v)) {
    // Same selection the RESP path makes: EXPAND takes the right member, otherwise the
    // middle one from the multi-value API onwards and the left one before it.
    if (reqFlags & QEXEC_FORMAT_EXPAND) {
      v = RSValue_Trio_GetRight(v);
    } else if (apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST) {
      v = RSValue_Trio_GetMiddle(v);
    } else {
      v = RSValue_Trio_GetLeft(v);
    }
  }

  // No `default` branch: a newly added RSValueType then fails the build's -Wswitch instead
  // of silently taking a lossy path, and an out-of-range value falls through to the refusal
  // at the end.
  switch (RSValue_Type(v)) {
    case RSValueType_Number:
      put_u8(w, ROW_BLOCK_TAG_NUM);
      put_f64(w, RSValue_Number_Get(v));
      return true;

    case RSValueType_String:
    case RSValueType_RedisString: {
      size_t len;
      const char *s = RSValue_StringPtrLen(v, &len);
      put_u8(w, ROW_BLOCK_TAG_STR);
      put_u32(w, (uint32_t)len);
      put_bytes(w, s, len);
      return true;
    }

    case RSValueType_Array: {
      uint32_t n = RSValue_ArrayLen(v);
      put_u8(w, ROW_BLOCK_TAG_ARRAY);
      put_u32(w, n);
      for (uint32_t i = 0; i < n; i++) {
        if (!writeValue(w, RSValue_ArrayItem(v, i), reqFlags, apiVersion)) return false;
      }
      return true;
    }

    case RSValueType_Map: {
      // Entry count, not the flattened key+value count: the reader's RSValue_NewMapBuilder
      // is sized in entries too.
      uint32_t n = RSValue_Map_Len(v);
      put_u8(w, ROW_BLOCK_TAG_MAP);
      put_u32(w, n);
      for (uint32_t i = 0; i < n; i++) {
        RSValue *key, *val;
        RSValue_Map_GetEntry(v, i, &key, &val);
        if (!writeValue(w, key, reqFlags, apiVersion)) return false;
        if (!writeValue(w, val, reqFlags, apiVersion)) return false;
      }
      return true;
    }

    case RSValueType_Reference:
      return writeValue(w, RSValue_Dereference(v), reqFlags, apiVersion);

    case RSValueType_Null:
    case RSValueType_Undef:
      // Undef is what the RESP path replies as null too, and carries nothing to lose.
      put_u8(w, ROW_BLOCK_TAG_NULL);
      return true;

    case RSValueType_Trio:
      // Unreachable: the loop above leaves no trio behind. Refusing beats guessing which
      // member a future trio shape means.
      break;
  }

  return false;
}

bool RowBlockWriter_WriteRow(RowBlockWriter *w, const RLookup *lk, const SearchResult *r,
                             uint32_t requiredFlags, uint32_t excludeFlags, uint32_t reqFlags,
                             unsigned int apiVersion) {
  RS_ASSERT(w->headerWritten);
  const RLookupRow *row = SearchResult_GetRowData(r);

  size_t bitmapBytes = (w->ncols + 7) / 8;
  // The bitmap sits at the row's first byte, so this doubles as the rollback point.
  size_t bitmapAt = w->len;
  reserve(w, bitmapBytes);
  memset(w->buf + w->len, 0, bitmapBytes);
  w->len += bitmapBytes;

  uint16_t col = 0;
  bool ok = true;
  RLOOKUP_FOREACH(key, lk, {
    if (!keyIsVisible(key, requiredFlags, excludeFlags)) continue;
    if (col >= w->ncols) {
      // More visible columns than the schema declared, so the lookup grew after the schema
      // was written. The presence bitmap has no bit for this column and setting one would
      // overwrite the row's own value bytes: refuse the row rather than corrupt it.
      ok = false;
      break;
    }
    const RSValue *v = RLookupRow_Get(key, row);
    if (v) {
      w->buf[bitmapAt + col / 8] |= (char)(1u << (col % 8));
      if (!writeValue(w, v, reqFlags, apiVersion)) {
        ok = false;
        break;
      }
    }
    col++;
  })

  if (!ok) {
    // Roll the half-written row back so the block ends on a row boundary and stays
    // decodable, whether the caller emits it or throws it away.
    w->len = bitmapAt;
    return false;
  }

  RS_ASSERT(col == w->ncols);
  w->nrows++;
  return true;
}

// ---- replay: the encoder read backwards, for the fallback path ---------------------------
//
// Reads a block this process just wrote, so a bounds failure means the encoder and this
// reader disagree - a bug, not bad input. Checked in release builds too: the path is cold,
// and reading past the buffer would be undefined behaviour rather than a wrong reply.

typedef struct {
  const char *cur;
  const char *end;
} BlockReader;

static inline void take_bytes(BlockReader *rd, void *dst, size_t n) {
  RS_LOG_ASSERT_ALWAYS((size_t)(rd->end - rd->cur) >= n, "row block replay ran past the block");
  memcpy(dst, rd->cur, n);
  rd->cur += n;
}

static inline uint8_t take_u8(BlockReader *rd) {
  uint8_t v;
  take_bytes(rd, &v, sizeof(v));
  return v;
}

static inline uint16_t take_u16(BlockReader *rd) {
  uint16_t v;
  take_bytes(rd, &v, sizeof(v));
  return v;
}

static inline uint32_t take_u32(BlockReader *rd) {
  uint32_t v;
  take_bytes(rd, &v, sizeof(v));
  return v;
}

static inline double take_f64(BlockReader *rd) {
  double v;
  take_bytes(rd, &v, sizeof(v));
  return v;
}

// Emit one encoded value the way RedisModule_Reply_RSValue would have emitted the value it
// came from. Numbers go through an RSValue because their reply shape depends on `flags`.
static void replayValue(BlockReader *rd, RedisModule_Reply *reply, SendReplyFlags flags) {
  uint8_t tag = take_u8(rd);
  switch (tag) {
    case ROW_BLOCK_TAG_NUM: {
      RSValue *v = RSValue_NewNumber(take_f64(rd));
      RedisModule_Reply_RSValue(reply, v, flags);
      RSValue_DecrRef(v);
      break;
    }
    case ROW_BLOCK_TAG_STR: {
      uint32_t n = take_u32(rd);
      RS_LOG_ASSERT_ALWAYS((size_t)(rd->end - rd->cur) >= n, "row block replay ran past the block");
      RedisModule_Reply_StringBuffer(reply, rd->cur, n);
      rd->cur += n;
      break;
    }
    case ROW_BLOCK_TAG_ARRAY: {
      uint32_t n = take_u32(rd);
      RedisModule_Reply_Array(reply);
      for (uint32_t i = 0; i < n; i++) replayValue(rd, reply, flags);
      RedisModule_Reply_ArrayEnd(reply);
      break;
    }
    case ROW_BLOCK_TAG_MAP: {
      uint32_t n = take_u32(rd);
      RedisModule_Reply_Map(reply);
      for (uint32_t i = 0; i < n; i++) {
        replayValue(rd, reply, flags);  // key
        replayValue(rd, reply, flags);  // value
      }
      RedisModule_Reply_MapEnd(reply);
      break;
    }
    case ROW_BLOCK_TAG_NULL:
      RedisModule_Reply_Null(reply);
      break;
    default:
      RS_ABORT_ALWAYS("row block replay hit a tag the writer cannot produce");
  }
}

size_t RowBlockWriter_ReplayAsResp(const RowBlockWriter *w, RedisModule_Reply *reply,
                                   uint32_t reqFlags) {
  RS_ASSERT(w->headerWritten);
  SendReplyFlags flags = (reqFlags & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
  flags |= (reqFlags & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

  BlockReader rd = {.cur = w->buf, .end = w->buf + w->len};
  uint32_t magic = take_u32(&rd);
  uint8_t version = take_u8(&rd);
  RS_LOG_ASSERT_ALWAYS(magic == ROW_BLOCK_MAGIC && version == ROW_BLOCK_VERSION,
                       "row block replay read a header this build did not write");
  uint16_t ncols = take_u16(&rd);

  // Heap rather than a VLA: ncols is bounded only by the schema's column count, and this
  // path runs at most once per chunk. The names point into the block, which outlives them.
  const char **names = rm_malloc(ncols * sizeof(*names));
  uint16_t *nameLens = rm_malloc(ncols * sizeof(*nameLens));
  for (uint16_t i = 0; i < ncols; i++) {
    nameLens[i] = take_u16(&rd);
    names[i] = rd.cur;
    // The stored name carries a terminator, which is not part of the name itself.
    size_t stored = (size_t)nameLens[i] + 1;
    RS_LOG_ASSERT_ALWAYS((size_t)(rd.end - rd.cur) >= stored,
                         "row block replay ran past the block");
    rd.cur += stored;
  }

  size_t bitmapBytes = (ncols + 7) / 8;
  size_t nrows = 0;
  while (rd.cur < rd.end) {
    RS_LOG_ASSERT_ALWAYS((size_t)(rd.end - rd.cur) >= bitmapBytes,
                         "row block replay ran past the block");
    const unsigned char *bitmap = (const unsigned char *)rd.cur;
    rd.cur += bitmapBytes;

    RedisModule_Reply_Map(reply);
    for (uint16_t i = 0; i < ncols; i++) {
      if (!(bitmap[i / 8] & (1u << (i % 8)))) continue;
      RedisModule_Reply_StringBuffer(reply, names[i], nameLens[i]);
      replayValue(&rd, reply, flags);
    }
    RedisModule_Reply_MapEnd(reply);
    nrows++;
  }

  rm_free(names);
  rm_free(nameLens);
  RS_ASSERT(nrows == w->nrows);
  return nrows;
}
