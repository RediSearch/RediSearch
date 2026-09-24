/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "reply.h"

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h> // for ssize_t

#include "resp3.h"
#include "query_error_ffi.h"
#include "value_ffi.h"
#include "rlookup.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "hiredis/sds.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef ENABLE_ASSERT

// The shadow of the reply: one frame per open collection, declared ones included, plus a JSON
// rendering of everything written. Only ever consulted by asserts.

typedef struct RedisModule_Reply_Frame Frame;

static Frame *topFrame(RedisModule_Reply *reply) {
  if (!reply->frames) {
    // Bottom frame: the top level of the reply, which has no declared length.
    reply->frames = array_new(Frame, 4);
    Frame *root = array_ensure_tail(&reply->frames, Frame);
    *root = (Frame){ .type = 0, .known = -1, .count = 0 };
    reply->json = sdsempty();
  }
  return &array_tail(reply->frames);
}

static const char *replyJson(RedisModule_Reply *reply) {
  return reply->json ? reply->json : "";
}

// Whether the frame is a map on the wire, and the next element is therefore a key.
static bool atMapKey(RedisModule_Reply *reply, const Frame *f) {
  return f->type == REDISMODULE_REPLY_MAP && reply->resp3 && f->count % 2 == 0;
}

// Declared collections have no End: pop them (and any declared parent they complete) as soon as their
// last element is written.
static void closeCompletedDeclared(RedisModule_Reply *reply) {
  Frame *f = topFrame(reply);
  while (f->known >= 0 && f->count == f->known) {
    reply->json = sdscat(reply->json, f->type == REDISMODULE_REPLY_ARRAY ? " ]" : " }");
    reply->frames = array_trimm_len(reply->frames, 1);
    f = topFrame(reply);
  }
}

// Account for one element of the innermost open collection and render its separator.
static void trackElement(RedisModule_Reply *reply) {
  Frame *f = topFrame(reply);
  if (f->type == REDISMODULE_REPLY_MAP && f->count % 2 == 1) {
    reply->json = sdscat(reply->json, ": ");
  } else if (f->count > 0) {
    reply->json = sdscat(reply->json, ", ");
  }
  f->count++;
}

void _RedisModule_Reply_TrackElement(RedisModule_Reply *reply, const char *fmt, ...) {
  trackElement(reply);
  va_list ap;
  va_start(ap, fmt);
  reply->json = sdscatvprintf(reply->json, fmt, ap);
  va_end(ap);
  closeCompletedDeclared(reply);
}

void _RedisModule_Reply_TrackOpen(RedisModule_Reply *reply, int type, int known) {
  Frame *parent = topFrame(reply);
  RS_LOG_ASSERT_FMT(!atMapKey(reply, parent), "reply: a collection cannot be a map key: %s", replyJson(reply));
  trackElement(reply);
  reply->json = sdscat(reply->json, type == REDISMODULE_REPLY_ARRAY ? "[ " : "{ ");
  Frame *f = array_ensure_tail(&reply->frames, Frame);
  *f = (Frame){ .type = type, .known = known, .count = 0 };
  closeCompletedDeclared(reply); // an empty declared collection is complete on open
}

// `count` is what the release-side counter saw; the shadow frame must agree.
static void trackClose(RedisModule_Reply *reply, int type, int count) {
  Frame *f = topFrame(reply);
  RS_LOG_ASSERT_FMT(f->known < 0, "reply: declared %d elements, wrote %d: %s", f->known, f->count, replyJson(reply));
  RS_LOG_ASSERT_FMT(array_len(reply->frames) > 1 && f->type == type, "reply: closing a collection that was not opened: %s", replyJson(reply));
  RS_LOG_ASSERT_FMT(f->count == count, "reply: counted %d elements, shadow saw %d: %s", count, f->count, replyJson(reply));
  // Redis emits a RESP3 map as a flat array for RESP2 clients and closes it as `pairs * 2`,
  // so an odd element count would corrupt the RESP2 stream.
  RS_LOG_ASSERT_FMT(type != REDISMODULE_REPLY_MAP || count % 2 == 0, "reply: map closed with %d elements: %s", count, replyJson(reply));
  reply->json = sdscat(reply->json, type == REDISMODULE_REPLY_ARRAY ? " ]" : " }");
  reply->frames = array_trimm_len(reply->frames, 1);
  closeCompletedDeclared(reply);
}

// `buffer`'s top-level elements become elements of `reply`'s innermost collection, then the buffer's shadow is emptied.
static void trackBuffered(RedisModule_Reply *reply, RedisModule_Reply *buffer, int elements) {
  Frame *b = topFrame(buffer);
  RS_LOG_ASSERT_FMT(array_len(buffer->frames) == 1, "reply: buffer moved with an open collection: %s", replyJson(buffer));
  RS_LOG_ASSERT_FMT(b->count == elements, "reply: buffer counted %d elements, shadow saw %d: %s", elements, b->count, replyJson(buffer));
  if (b->count) {
    trackElement(reply); // separator for the first moved element
    reply->json = sdscatsds(reply->json, buffer->json);
    Frame *f = topFrame(reply);
    f->count += b->count - 1;
  }
  closeCompletedDeclared(reply);
  b->count = 0;
  sdsclear(buffer->json);
}

static void trackEnd(RedisModule_Reply *reply) {
  if (reply->frames) {
    Frame *f = topFrame(reply);
    RS_LOG_ASSERT_FMT(array_len(reply->frames) == 1, f->known >= 0 ? "reply: declared %d elements, wrote %d: %s" : "incomplete reply (%d open, %d elements): %s", f->known, f->count, replyJson(reply));
    array_free(reply->frames);
    reply->frames = NULL;
  }
  sdsfree(reply->json);
  reply->json = NULL;
}

#define REPLY_TRACK_CLOSE(reply, type, count) trackClose(reply, type, count)
#define REPLY_TRACK_BUFFERED(reply, buffer, elements) trackBuffered(reply, buffer, elements)
#define REPLY_TRACK_END(reply) trackEnd(reply)

#else

#define REPLY_TRACK_CLOSE(reply, type, count) ((void)0)
#define REPLY_TRACK_BUFFERED(reply, buffer, elements) ((void)0)
#define REPLY_TRACK_END(reply) ((void)0)

#endif

//---------------------------------------------------------------------------------------------

RedisModule_Reply RedisModule_NewReply(RedisModuleCtx *ctx) {
  RedisModule_Reply reply = { .ctx = ctx, .resp3 = is_resp3(ctx) };
  return reply;
}

RedisModule_Reply RedisModule_NewReplyBuffer(RedisModuleCtx *bufferCtx) {
  RedisModule_Reply reply = RedisModule_NewReply(bufferCtx);
  // The buffer's own counter: no Redis collection is opened for it, RedisModule_Reply_Buffered reads it.
  int *count = array_ensure_tail(&reply.counts, int);
  *count = 0;
  reply.cur = count;
  return reply;
}

int RedisModule_EndReply(RedisModule_Reply *reply) {
  REPLY_TRACK_END(reply);
  // A buffer keeps its own counter open for its whole life.
  RS_LOG_ASSERT(!reply->cur || array_len(reply->counts) == 1, "incomplete reply: a postponed collection is still open");
  if (reply->counts) {
    array_free(reply->counts);
    reply->counts = NULL;
  }
  reply->cur = NULL; // a closed buffer is closed again by its owner's destroy path
  if (reply->scratch) {
    rm_free(reply->scratch);
    reply->scratch = NULL;
    reply->scratch_cap = 0;
  }
  return REDISMODULE_OK;
}

// Retention bound for the reply-owned scratch buffer. Values that fit reuse one
// retained allocation (power-of-two growth capped by this bound); larger values
// take an exact-sized temporary freed right after emission, so a huge field can
// neither be rounded up by the geometric growth nor stay pinned until EndReply.
#define REPLY_SCRATCH_RETAIN_MAX 4096

static char *reply_ScratchBuffer(RedisModule_Reply *reply, size_t len) {
  RS_LOG_ASSERT(len <= REPLY_SCRATCH_RETAIN_MAX, "scratch request above retention bound");
  if (reply->scratch_cap < len) {
    size_t cap = reply->scratch_cap ? reply->scratch_cap : 128;
    while (cap < len) {
      cap *= 2;
    }
    reply->scratch = rm_realloc(reply->scratch, cap);
    reply->scratch_cap = cap;
  }
  return reply->scratch;
}

int RedisModule_Reply_PrefixedStringBuffer(RedisModule_Reply *reply, char prefix, const char *s,
                                           size_t n) {
  RS_LOG_ASSERT(n < SIZE_MAX, "prefixed string length overflow");
  const size_t total = n + 1;
  char *buf = total <= REPLY_SCRATCH_RETAIN_MAX ? reply_ScratchBuffer(reply, total)
                                                : rm_malloc(total);
  buf[0] = prefix;
  memcpy(buf + 1, s, n);
  int rc = RedisModule_Reply_StringBuffer(reply, buf, total);
  if (buf != reply->scratch) {
    rm_free(buf);
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

// A postponed collection is one element of its parent and then counts its own.
static void pushPostponed(RedisModule_Reply *reply) {
  RedisModule_Reply_CountElement(reply);
  int *count = array_ensure_tail(&reply->counts, int);
  *count = 0;
  reply->cur = count;
}

static int popPostponed(RedisModule_Reply *reply) {
  RS_LOG_ASSERT(reply->cur, "reply: closing a collection that was not opened");
  int count = *reply->cur;
  reply->counts = array_trimm_len(reply->counts, 1);
  reply->cur = array_len(reply->counts) ? &array_tail(reply->counts) : NULL;
  return count;
}

int RedisModule_Reply_Array(RedisModule_Reply *reply) {
  pushPostponed(reply);
  REPLY_TRACK_OPEN(reply, REDISMODULE_REPLY_ARRAY, -1);
  return RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
}

int RedisModule_Reply_ArrayEnd(RedisModule_Reply *reply) {
  int count = popPostponed(reply);
  REPLY_TRACK_CLOSE(reply, REDISMODULE_REPLY_ARRAY, count);
  RedisModule_ReplySetArrayLength(reply->ctx, count);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Map(RedisModule_Reply *reply) {
  pushPostponed(reply);
  REPLY_TRACK_OPEN(reply, REDISMODULE_REPLY_MAP, -1);
  return RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
}

int RedisModule_Reply_MapEnd(RedisModule_Reply *reply) {
  int count = popPostponed(reply);
  REPLY_TRACK_CLOSE(reply, REDISMODULE_REPLY_MAP, count);
  RedisModule_ReplySetMapLength(reply->ctx, count / 2);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Set(RedisModule_Reply *reply) {
  pushPostponed(reply);
  REPLY_TRACK_OPEN(reply, REDISMODULE_REPLY_SET, -1);
  return RedisModule_ReplyWithSet(reply->ctx, REDISMODULE_POSTPONED_LEN);
}

int RedisModule_Reply_SetEnd(RedisModule_Reply *reply) {
  int count = popPostponed(reply);
  REPLY_TRACK_CLOSE(reply, REDISMODULE_REPLY_SET, count);
  RedisModule_ReplySetSetLength(reply->ctx, count);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Buffered(RedisModule_Reply *reply, RedisModule_Reply *buffer) {
  RS_ASSERT(buffer->cur && array_len(buffer->counts) == 1); // a buffer with no collection left open
  int elements = *buffer->cur;
  *buffer->cur = 0;
  REPLY_TRACK_BUFFERED(reply, buffer, elements);
  if (reply->cur) {
    *reply->cur += elements;
  }
  return RedisModule_ReplyWithBufferedReply(reply->ctx, buffer->ctx);
}

//---------------------------------------------------------------------------------------------

int RedisModule_Reply_SimpleStringf(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p;
  rm_vasprintf(&p, fmt, args);
  RedisModule_Reply_SimpleString(reply, p);
  rm_free(p);
  va_end(args);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Stringf(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p;
  size_t len = rm_vasprintf(&p, fmt, args);
  RedisModule_Reply_StringBuffer(reply, p, len);
  rm_free(p);
  va_end(args);
  return REDISMODULE_OK;
}

void RedisModule_Reply_QueryError(RedisModule_Reply *reply, QueryError *error) {
  RedisModule_Reply_Error(reply, QueryError_GetUserError(error));
}

int RedisModule_ReplyKV_SimpleStringf(RedisModule_Reply *reply, const char *key, const char *fmt, ...) {
  RedisModule_Reply_SimpleString(reply, key);
  va_list args;
  va_start(args, fmt);
  char *p;
  rm_vasprintf(&p, fmt, args);
  RedisModule_Reply_SimpleString(reply, p);
  rm_free(p);
  va_end(args);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Array(RedisModule_Reply *reply, const char *key) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_Array(reply);
}

int RedisModule_ReplyKV_Map(RedisModule_Reply *reply, const char *key) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_Map(reply);
}

int RedisModule_ReplyKV_Set(RedisModule_Reply *reply, const char *key) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_Set(reply);
}

//---------------------------------------------------------------------------------------------

char *escapeSimpleString(const char *str) {
  size_t len = strlen(str);
  // This is a short lived string, so we can afford to allocate twice the size
  char *escaped = rm_malloc(len * 2 + 1);
  char *p = escaped;
  for (size_t i = 0; i < len; i++) {
    char c = str[i];
    switch (c) {
    case '\n':
      *p++ = '\\';
      *p++ = 'n';
      break;
    case '\r':
      *p++ = '\\';
      *p++ = 'r';
      break;
    default:
      *p++ = c;
    }
  }
  *p = '\0';
  return escaped;
}

/* Based on the value type, serialize the RSValue into redis client response.
 * The value is resolved (references followed, trios collapsed) and its payload
 * fetched in a single FFI call. */
static int replyRSValue(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags,
                        RSValueTrioSelection trioSelection) {
  RSValueView view = RSValue_GetReplyView(v, trioSelection);

  switch (view.view_type) {
    case RSValueViewType_String:
      return RedisModule_Reply_StringBuffer(reply, view.string.bytes, view.str_len);

    case RSValueViewType_RedisString:
      return RedisModule_Reply_String(reply, view.string.redis_string);

    case RSValueViewType_Number: {
      if (!(flags & SENDREPLY_FLAG_EXPAND)) {
        if (flags & SENDREPLY_FLAG_TYPED) {
          if (reply->resp3) {
            return RedisModule_Reply_Double(reply, view.num);
          } else {
             // In RESP2, RM_ReplyWithDouble() does not tag the response as
             // double, it's just a plain string. So we send it as simple string
             // that is converted to double by MRReply_ToValue().
            char buf[32];
            RSValue_NumToString(view.resolved, buf, sizeof(buf));
            return RedisModule_Reply_Error(reply, buf);
          }
        } else {
          char buf[32];
          size_t len = RSValue_NumToString(view.resolved, buf, sizeof(buf));
          return RedisModule_Reply_StringBuffer(reply, buf, len);
        }
      } else {
        long long ll = view.num;
        if (ll == view.num) {
          return RedisModule_Reply_LongLong(reply, ll);
        } else {
          return RedisModule_Reply_Double(reply, view.num);
        }
      }
    }

    case RSValueViewType_Null:
      return RedisModule_Reply_Null(reply);

    case RSValueViewType_Array:
      RedisModule_Reply_ArrayWithLen(reply, view.len);
      for (uint32_t i = 0; i < view.len; i++) {
        replyRSValue(reply, RSValue_ArrayItem(view.resolved, i), flags,
                     RSValueTrioSelection_Middle);
      }
      return REDISMODULE_OK;

    case RSValueViewType_Map:
      // If Map value is used, assume Map api exists (RedisModule_IsRESP3)
      RedisModule_Reply_MapWithLen(reply, view.len);
      for (uint32_t i = 0; i < view.len; i++) {
        RSValue *key, *val;
        RSValue_Map_GetEntry(view.resolved, i, &key, &val);
        replyRSValue(reply, key, flags, RSValueTrioSelection_Middle);
        replyRSValue(reply, val, flags, RSValueTrioSelection_Middle);
      }
      break;
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_RSValue(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags) {
  return replyRSValue(reply, v, flags, RSValueTrioSelection_Middle);
}

// The row value RedisModule_Reply_RLookupRow emits for `kk`, or NULL when the key is skipped. Shared with the
// counting pass so a declared map length can never disagree with what gets written.
static inline const RSValue *rlookupRowReplyValue(const RLookupKey *kk, const RLookupRow *row, uint32_t requiredFlags, uint32_t excludeFlags) {
  const uint32_t kflags = RLookupKey_GetFlags(kk);
  if ((kflags & excludeFlags) || (kflags & requiredFlags) != requiredFlags) {
    return NULL;
  }
  return RLookupRow_Get(kk, row);
}

size_t RedisModule_Reply_RLookupRowLen(const RLookup *lk, const RLookupRow *row, uint32_t requiredFlags, uint32_t excludeFlags) {
  size_t n = 0;
  RLOOKUP_FOREACH(kk, lk, {
    if (rlookupRowReplyValue(kk, row, requiredFlags, excludeFlags)) {
      n++;
    }
  });
  return n;
}

int RedisModule_Reply_RLookupRow(RedisModule_Reply *reply, const RLookup *lk, const RLookupRow *row,
                                 uint32_t requiredFlags, uint32_t excludeFlags,
                                 SendReplyFlags flags, unsigned int apiVersion) {
  RSValueTrioSelection trioSelection = RSValueTrioSelection_Left;
  if (flags & SENDREPLY_FLAG_EXPAND) {
    trioSelection = RSValueTrioSelection_Right;
  } else if (apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST) {
    trioSelection = RSValueTrioSelection_Middle;
  }

  RLOOKUP_FOREACH(kk, lk, {
    const RSValue *v = rlookupRowReplyValue(kk, row, requiredFlags, excludeFlags);
    if (!v) {
      continue;
    }
    RedisModule_Reply_StringBuffer(reply, RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk));
    replyRSValue(reply, v, flags, trioSelection);
  });
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
