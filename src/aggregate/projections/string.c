#include "project.h"
#include "util/minmax.h"
#include "util/array.h"
#include "util/block_alloc.h"
#include <ctype.h>

#define STRING_BLOCK_SIZE 512

static int upper_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                     &pc->properties->keys[0]);
  if (RSValue_IsString(v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = toupper(p[i]);
    }
    // we set the value again, in case it was in the table or the alias is not the same as the key
    RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key, v);
  }

  return RS_RESULT_OK;
}

static int lower_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                     &pc->properties->keys[0]);
  if (v && RSValue_IsString(v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = tolower(p[i]);
    }
  }
  // we set the value again, in case it was in the table or the alias is not the same as the key
  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key, v);
  return RS_RESULT_OK;
}

static int tostring_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                     &pc->properties->keys[0]);
  // we set the value again, in case it was in the table or the alias is not the same as the key
  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0].key,
                 RSValue_ToString(v));
  return RS_RESULT_OK;
}

// Frees the ctx and the attached block allocator. Assumes the block allocator
// is the first member of the structure
static void blkalloc_GenericFree(ResultProcessor *rp) {
  BlkAlloc *alloc = rp->ctx.privdata;
  BlkAlloc_FreeAll(alloc, NULL, 0, 0);
  free(rp->ctx.privdata);
  free(rp);
}

ResultProcessor *NewLowerArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err) {
  return NewProjectorGeneric(lower_Next, upstream, alias, args, NULL, 1, 1, err);
}

ResultProcessor *NewUpperArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err) {
  return NewProjectorGeneric(upper_Next, upstream, alias, args, NULL, 1, 1, err);
}

ResultProcessor *NewToStringArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                 char **err) {
  return NewProjectorGeneric(tostring_Next, upstream, alias, args, NULL, 1, 1, err);
}

typedef struct {
  BlkAlloc alloc;
  RSKey key;
  const char *alias;
  size_t off;
  size_t len;
} substrCtx;

static const char *getString(ResultProcessorCtx *ctx, RSKey *key, SearchResult *res, size_t *len) {
  RSSortingTable *stbl = QueryProcessingCtx_GetSortingTable(ctx->qxc);
  RSValue *v = SearchResult_GetValue(res, stbl, key);
  if (v == NULL || RSValue_IsString(v)) {
    return NULL;
  }
  return RSValue_StringPtrLen(v, len);
}

static int substr_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);
  substrCtx *bctx = ctx->privdata;
  // Get the field:
  size_t n = 0;
  const char *s = getString(ctx, &bctx->key, res, &n);
  if (s == NULL) {
    return RS_RESULT_OK;
  }

  size_t maxlen = Min(bctx->len ? bctx->len : n, n - bctx->off);
  RSFieldMap_Set(&res->fields, bctx->alias, RS_StringVal((char *)s + bctx->off, maxlen));
  return RS_RESULT_OK;
}

static int cmdArgStrToNum(const CmdArg *arg, int *n) {
  assert(arg->type == CmdArg_String);
  char buf[1024] = {0};
  memcpy(buf, arg->s.str, arg->s.len);
  return sscanf(buf, "%d", n);
}

static char *cmdArgToZstr(const CmdArg *arg, BlkAlloc *alloc) {
  size_t len = arg->s.len;
  const char *s = arg->s.str;

  char *tmps = BlkAlloc_Alloc(alloc, len + 1, Max(len + 1, STRING_BLOCK_SIZE));
  memcpy(tmps, s, len);
  tmps[len] = '\0';
  return tmps;
}

ResultProcessor *NewSubstrArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                               char **err) {
  size_t off, len = 0;
  if (CMDARG_ARRLEN(args) < 2 || CMDARG_ARRLEN(args) > 3) {
    *err = strdup("Bad arguments");
    return NULL;
    // Property, length, offset
  }

  // First is the property, second is the
  int tmp;
  if (!cmdArgStrToNum(CMDARG_ARRELEM(args, 1), &tmp) || tmp <= 0) {
    *err = strdup("Bad offset");
    return NULL;
  }

  off = tmp;
  if (CMDARG_ARRLEN(args) == 3) {
    if (!cmdArgStrToNum(CMDARG_ARRELEM(args, 2), &tmp) || tmp <= 0) {
      *err = strdup("Bad length");
      return NULL;
    }
    len = tmp;
  }

  substrCtx *sctx = calloc(1, sizeof(*sctx));
  BlkAlloc_Init(&sctx->alloc);
  sctx->alias = alias ? alias : "SUBSTR";
  sctx->key.key = cmdArgToZstr(CMDARG_ARRELEM(args, 0), &sctx->alloc);
  sctx->key.cachedIdx = 0;
  sctx->len = len;
  sctx->off = off;

  ResultProcessor *rp = NewResultProcessor(upstream, sctx);
  rp->Next = substr_Next;
  rp->Free = blkalloc_GenericFree;
  return rp;
}

typedef struct {
  BlkAlloc alloc;
  RSKey key;
  const char *alias;
  const char *separator;
  size_t sepLen;
} joinCtx;

static int join_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  joinCtx *jctx = ctx->privdata;
  RSSortingTable *stbl = QueryProcessingCtx_GetSortingTable(ctx->qxc);
  RSValue *v = SearchResult_GetValue(res, stbl, &jctx->key);
  if (v == NULL || v->t != RSValue_Array) {
    return RS_RESULT_OK;
  }

  // TODO: Better way to concatenate any of this?
  size_t finalLen = 0;
  for (size_t ii = 0; ii < v->arrval.len; ++ii) {
    size_t n;
    if (!RSValue_IsString(v->arrval.vals[ii])) {
      continue;
    }
    const char *s = RSValue_StringPtrLen(v->arrval.vals[ii], &n);
    finalLen += n;
    if (ii != 0) {
      finalLen += jctx->sepLen;
    }
  }

  char *buf = BlkAlloc_Alloc(&jctx->alloc, finalLen, Max(finalLen, STRING_BLOCK_SIZE));
  size_t offset = 0;
  for (size_t ii = 0; ii < v->arrval.len; ++ii) {
    size_t n;
    const char *s = RSValue_StringPtrLen(v->arrval.vals[ii], &n);
    memcpy(buf + offset, s, n);
    offset += n;
    if (ii != v->arrval.len - 1) {
      memcpy(buf, jctx->separator, jctx->sepLen);
      offset += jctx->sepLen;
    }
  }

  RSFieldMap_Set(&res->fields, jctx->alias, (RS_StringVal(buf, finalLen)));
  return RS_RESULT_OK;
}

ResultProcessor *NewJoinArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                             char **err) {
  if (CMDARG_ARRLEN(args) != 2) {
    *err = strdup("Bad arguments");
    return NULL;
  }
  // Again, assume it's all strings

  joinCtx *jctx = calloc(1, sizeof(*jctx));
  BlkAlloc_Init(&jctx->alloc);
  jctx->alias = alias ? alias : "JOIN";
  jctx->key.key = cmdArgToZstr(CMDARG_ARRELEM(args, 0), &jctx->alloc);
  jctx->separator = CMDARG_ARRELEM(args, 1)->s.str;
  jctx->sepLen = CMDARG_ARRELEM(args, 1)->s.len;
  ResultProcessor *rp = NewResultProcessor(upstream, jctx);
  rp->Next = join_Next;
  rp->Free = blkalloc_GenericFree;
  return rp;
}

typedef struct {
  BlkAlloc alloc;
  RSKey key;
  const char *property;
  const char *alias;
  char *fmt;
} timeCtx;

static int time_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);
  timeCtx *tctx = ctx->privdata;
  RSSortingTable *stbl = QueryProcessingCtx_GetSortingTable(ctx->qxc);
  RSValue *v = SearchResult_GetValue(res, stbl, &tctx->key);
  if (v == NULL || v->t != RSValue_Number) {
    return RS_RESULT_OK;
  }

  // Get the format
  char timebuf[1024] = {0};  // Should be enough for any human time string

  time_t tt = (time_t)v->numval;
  struct tm tm;
  if (!gmtime_r(&tt, &tm)) {
    return RS_RESULT_OK;
  }

  size_t rv = strftime(timebuf, sizeof timebuf, tctx->fmt, &tm);
  if (rv == 0) {
    return RS_RESULT_OK;
  }

  // Finally, allocate a buffer to store the time!
  char *buf = BlkAlloc_Alloc(&tctx->alloc, rv, Max(rv, STRING_BLOCK_SIZE));
  memcpy(buf, timebuf, rv);
  RSFieldMap_Set(&res->fields, tctx->alias, (RS_ConstStringVal(buf, rv)));
  return RS_RESULT_OK;
}

static ResultProcessor *NewTimeResultProcessor(ResultProcessor *upstream, const char *alias,
                                               CmdArg *propertyArg, char *fmt, size_t formatLen) {
  timeCtx *tctx = calloc(1, sizeof(*tctx));
  BlkAlloc_Init(&tctx->alloc);
  tctx->alias = alias ? alias : "TIME";

  tctx->fmt = BlkAlloc_Alloc(&tctx->alloc, formatLen + 1, Max(formatLen + 1, STRING_BLOCK_SIZE));
  memcpy(tctx->fmt, fmt, formatLen);
  tctx->fmt[formatLen] = '\0';

  tctx->key.key = cmdArgToZstr(propertyArg, &tctx->alloc);
  tctx->key.cachedIdx = 0;

  ResultProcessor *proc = NewResultProcessor(upstream, tctx);
  proc->Next = time_Next;
  proc->Free = blkalloc_GenericFree;
  return proc;
}

ResultProcessor *NewStrftimeArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                 char **err) {
  if (CMDARG_ARRLEN(args) != 2) {
    *err = strdup("Bad args");
    return NULL;
  }

  return NewTimeResultProcessor(upstream, alias, CMDARG_ARRELEM(args, 0),
                                CMDARG_ARRELEM(args, 1)->s.str, CMDARG_ARRELEM(args, 1)->s.len);
}

#define ISOFMT "%FT%TZ"
#define ISOFMT_LEN sizeof(ISOFMT) - 1

ResultProcessor *NewISOTimeArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                char **err) {
  if (CMDARG_ARRLEN(args) != 1) {
    *err = strdup("Bad args");
    return NULL;
  }
  return NewTimeResultProcessor(upstream, alias, CMDARG_ARRELEM(args, 0), ISOFMT, ISOFMT_LEN);
}