#include "project.h"
#include "util/minmax.h"
#include "util/minmax.h"
#include "util/array.h"
#include "util/block_alloc.h"
#include <ctype.h>
#include <string.h>

#define STRING_BLOCK_SIZE 512

typedef struct {
  int isKey;
  union {
    RSKey key;
    struct {
      const char *s;
      size_t n;
    } user;
  } u;
} formatSegment;

typedef struct {
  BlkAlloc alloc;
  Array scratch;
  const char *alias;
  size_t numSegs;
  formatSegment *segs;
} formatCtx;

#ifdef __linux__
char *strnstr(const char *haystack, const char *needle, size_t len)
{
        int i;
        size_t needle_len;

        if (0 == (needle_len = strnlen(needle, len)))
                return (char *)haystack;

        for (i=0; i<=(int)(len-needle_len); i++)
        {
                if ((haystack[0] == needle[0]) &&
                        (0 == strncmp(haystack, needle, needle_len)))
                        return (char *)haystack;

                haystack++;
        }
        return NULL;
}
#endif

static int format_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);
  formatCtx *fctx = ctx->privdata;
  RSSortingTable *stbl = QueryProcessingCtx_GetSortingTable(ctx->qxc);
  Array_Resize(&fctx->scratch, 0);

  for (size_t ii = 0; ii < fctx->numSegs; ++ii) {
    formatSegment *seg = fctx->segs + ii;
    const char *toAdd;
    size_t addLen;

    if (seg->isKey) {
      // Fetch the key
      RSValue *v = SearchResult_GetValue(res, stbl, &seg->u.key);
      if (!v) {
        continue;
      }
      char tmpbuf[1024];
      toAdd = RSValue_ConvertStringPtrLen(v, &addLen, tmpbuf, sizeof tmpbuf);
      if (toAdd == NULL) {
        continue;
      }
    } else {
      toAdd = seg->u.user.s;
      addLen = seg->u.user.n;
    }

    memcpy(Array_Add(&fctx->scratch, addLen), toAdd, addLen);
  }
  char *buf =
      BlkAlloc_Alloc(&fctx->alloc, fctx->scratch.len, Max(fctx->scratch.len, STRING_BLOCK_SIZE));
  memcpy(buf, fctx->scratch.data, fctx->scratch.len);
  RSFieldMap_Set(&res->fields, fctx->alias, RS_ConstStringVal(buf, fctx->scratch.len));
  return 1;
}

static formatSegment *addSeg(formatSegment *segs, size_t *numSegs, const char *s, size_t n,
                             int isKey) {
  segs = realloc(segs, sizeof(*segs) * (*numSegs + 1));
  segs[*numSegs].isKey = isKey;

  if (isKey) {
    char *key = malloc(n + 1);
    memcpy(key, s, n);
    key[n] = '\0';
    segs[*numSegs].u.key = (RSKey){.cachedIdx = 0, .key = key};
  } else {
    segs[*numSegs].u.user.s = s;
    segs[*numSegs].u.user.n = n;
  }

  (*numSegs)++;
  return segs;
}

static formatSegment *getSegs(const char *s, size_t n, size_t *numSegs) {
  const char *cur = s, *end = s + n;
  const char *next;
  formatSegment *segs = NULL;
  *numSegs = 0;

  while ((next = strnstr(cur, "{", end - cur)) && next < end) {
    const char *keyBegin = next + 1;
    const char *close = strnstr(keyBegin, "}", end - keyBegin);
    if (!close) {
      cur = next;
      break;
    }

    if (next - cur) {
      segs = addSeg(segs, numSegs, cur, next - cur, 0);
    }

    size_t keyLen = close - keyBegin;
    segs = addSeg(segs, numSegs, keyBegin, keyLen, 1);
    cur = close + 1;
  }

  if (cur && *cur) {
    segs = addSeg(segs, numSegs, cur, end - cur, 0);
  }

  return segs;
}

static void format_Free(ResultProcessor *rp) {
  formatCtx *fctx = rp->ctx.privdata;
  BlkAlloc_FreeAll(&fctx->alloc, NULL, 0, 0);
  Array_Free(&fctx->scratch);

  for (size_t ii = 0; ii < fctx->numSegs; ++ii) {
    if (fctx->segs[ii].isKey) {
      free((void *)fctx->segs[ii].u.key.key);
    }
  }

  free(fctx->segs);
  free(fctx);
  free(rp);
}


ResultProcessor *NewFormatArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                               char **err) {
  formatCtx *fctx = calloc(1, sizeof(*fctx));
  BlkAlloc_Init(&fctx->alloc);
  Array_Init(&fctx->scratch);
  fctx->alias = alias ? alias : "FORMAT";
  fctx->segs =
      getSegs(CMDARG_ARRELEM(args, 0)->s.str, CMDARG_ARRELEM(args, 0)->s.len, &fctx->numSegs);

  ResultProcessor *rp = NewResultProcessor(upstream, fctx);
  rp->Next = format_Next;
  rp->Free = format_Free;
  return rp;
}