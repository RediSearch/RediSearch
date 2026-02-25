/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "fork_gc.h"
#include "triemap.h"
#include "util/arr.h"
#include "search_ctx.h"
#include "inverted_index.h"
#include "iterators/inverted_index_iterator.h"
#include "redis_index.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "time_sample.h"
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <poll.h>
#include "rwlock.h"
#include "hll/hll.h"
#include <float.h>
#include "module.h"
#include "rmutil/rm_assert.h"
#include "suffix.h"
#include "resp3.h"
#include "info/global_stats.h"
#include "info/info_redis/threads/current_thread.h"
#include "obfuscation/obfuscation_api.h"
#include "obfuscation/hidden.h"
#include "util/redis_mem_info.h"

#define GC_WRITERFD 1
#define GC_READERFD 0
// Number of attempts to wait for the child to exit gracefully before trying to terminate it
#define GC_WAIT_ATTEMPTS 4

typedef enum {
  // Terms have been collected
  FGC_COLLECTED,
  // No more terms remain
  FGC_DONE,
  // Pipe error, child probably crashed
  FGC_CHILD_ERROR,
  // Error on the parent
  FGC_PARENT_ERROR,
  // The spec was deleted
  FGC_SPEC_DELETED,
} FGCError;

// Assumes the spec is locked.
static void FGC_updateStats(ForkGC *gc, RedisSearchCtx *sctx,
            size_t recordsRemoved, size_t bytesCollected, size_t bytesAdded, bool ignoredLastBlock) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize += bytesAdded;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
  gc->stats.totalCollected -= bytesAdded;
  gc->stats.gcBlocksDenied += ignoredLastBlock ? 1 : 0;
}

// Buff shouldn't be NULL.
static void FGC_sendFixed(ForkGC *fgc, const void *buff, size_t len) {
  RS_LOG_ASSERT(len > 0, "buffer length cannot be 0");
  ssize_t size = write(fgc->pipe_write_fd, buff, len);
  if (size != len) {
    perror("broken pipe, exiting GC fork: write() failed");
    // just exit, do not abort(), which will trigger a watchdog on RLEC, causing adverse effects
    RedisModule_Log(fgc->ctx, "warning", "GC fork: broken pipe, exiting");
    RedisModule_ExitFromChild(1);
  }
}

#define FGC_SEND_VAR(fgc, v) FGC_sendFixed(fgc, &v, sizeof v)

static void FGC_sendBuffer(ForkGC *fgc, const void *buff, size_t len) {
  FGC_SEND_VAR(fgc, len);
  if (len > 0) {
    FGC_sendFixed(fgc, buff, len);
  }
}

static int FGC_recvFixed(ForkGC *fgc, void *buf, size_t len);

/**
 * Send instead of a string to indicate that no more buffers are to be received
 */
static void FGC_sendTerminator(ForkGC *fgc) {
  size_t smax = SIZE_MAX;
  FGC_SEND_VAR(fgc, smax);
}

static int __attribute__((warn_unused_result)) FGC_recvFixed(ForkGC *fgc, void *buf, size_t len) {
  // poll the pipe, so that we don't block while read, with timeout of 3 minutes
  int poll_rc;
  while ((poll_rc = poll(fgc->pollfd_read, 1, 180000)) == 1) {
    ssize_t nrecvd = read(fgc->pipe_read_fd, buf, len);
    if (nrecvd > 0) {
      buf += nrecvd;
      len -= nrecvd;
    } else if (nrecvd <= 0 && errno != EINTR) {
      break;
    }
    if (len == 0) {
      return REDISMODULE_OK;
    }
  }
  short revents = fgc->pollfd_read[0].revents;
  const char *what = (poll_rc == 0) ? "timeout" : "error";
  RedisModule_Log(fgc->ctx, "warning", "ForkGC - got %s while reading from pipe. errno: %s, revents: 0x%x (POLLIN=%x POLLERR=%x POLLHUP=%x POLLNVAL=%x)",
                  what, strerror(errno), revents, (revents & POLLIN), (revents & POLLERR), (revents & POLLHUP), (revents & POLLNVAL));
  return REDISMODULE_ERR;
}

static void *RECV_BUFFER_EMPTY = (void *)0x0deadbeef;

static int __attribute__((warn_unused_result))
FGC_recvBuffer(ForkGC *fgc, void **buf, size_t *len) {
  size_t temp_len;
  if (FGC_recvFixed(fgc, &temp_len, sizeof temp_len) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (temp_len == SIZE_MAX) {
    *len = temp_len;
    *buf = RECV_BUFFER_EMPTY;
    return REDISMODULE_OK;
  }
  if (temp_len == 0) {
    *len = temp_len;
    *buf = NULL;
    return REDISMODULE_OK;
  }

  char *buf_data = rm_malloc(temp_len + 1);
  buf_data[temp_len] = 0;
  if (FGC_recvFixed(fgc, buf_data, temp_len) != REDISMODULE_OK) {
    rm_free(buf_data);
    return REDISMODULE_ERR;
  }
  *len = temp_len;
  *buf = buf_data;
  return REDISMODULE_OK;
}

// glue to use process pipe as writer for II GC delta info
static void pipe_write_cb(void *ctx, const void *buf, size_t len) {
  ForkGC *gc = ctx;
  FGC_sendFixed(gc, buf, len);
}

// glue to use process pipe as reader for II GC delta info
static int pipe_read_cb(void *ctx, void *buf, size_t len) {
  ForkGC *gc = ctx;
  return FGC_recvFixed(gc, buf, len);
}

// used in code such as ScanRepair
typedef struct {
    ForkGC *gc;
    void *hdrarg;
} CTX_II_GC_Callback;

static void sendHeaderString(void* ptrCtx) {
  CTX_II_GC_Callback* ctx = ptrCtx;
  struct iovec *iov = ctx->hdrarg;
  FGC_sendBuffer(ctx->gc, iov->iov_base, iov->iov_len);
}

static void FGC_childCollectTerms(ForkGC *gc, RedisSearchCtx *sctx) {
  TrieIterator *iter = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, NULL, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    InvertedIndex *idx = Redis_OpenInvertedIndex(sctx, term, termLen, DONT_CREATE_INDEX, NULL);
    if (idx) {
      struct iovec iov = {.iov_base = (void *)term, termLen};

      CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
      II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

      II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

      InvertedIndex_GcDelta_Scan(
          &wr, sctx, idx,
          &cb, NULL
      );
    }
    rm_free(term);
  }
  TrieIterator_Free(iter);

  // we are done with terms
  FGC_sendTerminator(gc);
}

typedef struct {
  struct HLL majority_card;     // Holds the majority cardinality of all the blocks we've seen so far
  struct HLL last_block_card;   // Holds the cardinality of the last block we've seen
  const IndexBlock *last_block; // The last block we've seen, to know when to merge the cardinalities
} numCbCtx;

static void countRemain(const RSIndexResult *r, const IndexBlock *blk, void *arg) {
  numCbCtx *ctx = arg;

  if (ctx->last_block != blk) {
    // We are in a new block, merge the last block's cardinality into the majority, and clear the last block
    hll_merge(&ctx->majority_card, &ctx->last_block_card);
    hll_clear(&ctx->last_block_card);
    ctx->last_block = blk;
  }
  // Add the current record to the last block's cardinality
  double value = IndexResult_NumValue(r);
  hll_add(&ctx->last_block_card, &value, sizeof(value));
}

typedef struct {
  int type;
  const char *field;
  const void *curPtr;
  char *tagValue;
  size_t tagLen;
  uint64_t uniqueId;
  int sentFieldName;
} tagNumHeader;

static void sendNumericTagHeader(void *opaqueCtx) {
    CTX_II_GC_Callback* ctx = opaqueCtx;

  tagNumHeader *info = ctx->hdrarg;
  if (!info->sentFieldName) {
    info->sentFieldName = 1;
    FGC_sendBuffer(ctx->gc, info->field, strlen(info->field));
    FGC_sendFixed(ctx->gc, &info->uniqueId, sizeof info->uniqueId);
  }
  FGC_SEND_VAR(ctx->gc, info->curPtr);
  if (info->type == RSFLDTYPE_TAG) {
    FGC_sendBuffer(ctx->gc, info->tagValue, info->tagLen);
  }
}

// If anything other than FGC_COLLECTED is returned, it is an error or done
static FGCError recvNumericTagHeader(ForkGC *fgc, char **fieldName, size_t *fieldNameLen,
                                     uint64_t *id) {
  if (FGC_recvBuffer(fgc, (void **)fieldName, fieldNameLen) != REDISMODULE_OK) {
    return FGC_PARENT_ERROR;
  }
  if (*fieldName == RECV_BUFFER_EMPTY) {
    *fieldName = NULL;
    return FGC_DONE;
  }

  if (FGC_recvFixed(fgc, id, sizeof(*id)) != REDISMODULE_OK) {
    rm_free(*fieldName);
    *fieldName = NULL;
    return FGC_PARENT_ERROR;
  }
  return FGC_COLLECTED;
}

static void FGC_childCollectNumeric(ForkGC *gc, RedisSearchCtx *sctx) {
  arrayof(FieldSpec*) numericFields = getFieldsByType(sctx->spec, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);

  for (int i = 0; i < array_len(numericFields); ++i) {
    NumericRangeTree *rt = openNumericOrGeoIndex(sctx->spec, numericFields[i], DONT_CREATE_INDEX);

    // No entries were added to the numeric field, hence the tree was not initialized
    if (!rt) {
      continue;
    }

    NumericRangeTreeIterator *gcIterator = NumericRangeTreeIterator_New(rt);

    NumericRangeNode *currNode = NULL;
    tagNumHeader header = {.type = RSFLDTYPE_NUMERIC,
                           .field = HiddenString_GetUnsafe(numericFields[i]->fieldName, NULL),
                           .uniqueId = rt->uniqueId};

    numCbCtx nctx;
    IndexRepairParams params = {.repair_callback = countRemain, .repair_arg = &nctx};
    hll_init(&nctx.majority_card, NR_BIT_PRECISION);
    hll_init(&nctx.last_block_card, NR_BIT_PRECISION);
    while ((currNode = NumericRangeTreeIterator_Next(gcIterator))) {
      if (!currNode->range) {
        continue;
      }
      nctx.last_block = NULL;
      hll_clear(&nctx.majority_card);
      hll_clear(&nctx.last_block_card);

      InvertedIndex *idx = currNode->range->entries;
      header.curPtr = currNode;

      CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &header };
      II_GCCallback cb = { .ctx = &cbCtx, .call = sendNumericTagHeader };

      II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

      bool repaired = InvertedIndex_GcDelta_Scan(
          &wr, sctx, idx,
          &cb, &params
      );

      if (repaired) {
        // Instead of sending the majority cardinality and the last block's cardinality, we now
        // merge the majority cardinality into the last block's cardinality, and send its registers
        // as the cardinality WITH the last block's cardinality, and then send the majority registers
        // as the cardinality WITHOUT the last block's cardinality. This way, the main process can
        // choose which registers to use without having to merge them itself.
        hll_merge(&nctx.last_block_card, &nctx.majority_card);
        FGC_sendFixed(gc, nctx.last_block_card.registers, NR_REG_SIZE);
        FGC_sendFixed(gc, nctx.majority_card.registers, NR_REG_SIZE);
      }
    }
    hll_destroy(&nctx.majority_card);
    hll_destroy(&nctx.last_block_card);

    if (header.sentFieldName) {
      // If we've repaired at least one entry, send the terminator;
      // note that "terminator" just means a zero address and not the
      // "no more strings" terminator in FGC_sendTerminator
      void *pdummy = NULL;
      FGC_SEND_VAR(gc, pdummy);
    }

    NumericRangeTreeIterator_Free(gcIterator);
  }

  array_free(numericFields);
  // we are done with numeric fields
  FGC_sendTerminator(gc);
}

static void FGC_childCollectTags(ForkGC *gc, RedisSearchCtx *sctx) {
  RS_ASSERT(sctx->spec->diskSpec == NULL);
  arrayof(FieldSpec*) tagFields = getFieldsByType(sctx->spec, INDEXFLD_T_TAG);
  if (array_len(tagFields) != 0) {
    for (int i = 0; i < array_len(tagFields); ++i) {
      TagIndex *tagIdx = TagIndex_Open(tagFields[i], DONT_CREATE_INDEX, NULL);
      if (!tagIdx) {
        continue;
      }

      tagNumHeader header = {.type = RSFLDTYPE_TAG,
                             .field = HiddenString_GetUnsafe(tagFields[i]->fieldName, NULL),
                             .uniqueId = tagIdx->uniqueId};

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        header.curPtr = value;
        header.tagValue = ptr;
        header.tagLen = len;

        // send repaired data

        CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &header };
        II_GCCallback cb = { .ctx = &cbCtx, .call = sendNumericTagHeader };

        II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

        InvertedIndex_GcDelta_Scan(
            &wr, sctx, value,
            &cb, NULL
        );
      }

      // we are done with the current field
      if (header.sentFieldName) {
        void *pdummy = NULL;
        FGC_SEND_VAR(gc, pdummy);
      }
    }
  }

  array_free(tagFields);
  // we are done with tag fields
  FGC_sendTerminator(gc);
}

static void FGC_childCollectMissingDocs(ForkGC *gc, RedisSearchCtx *sctx) {
  IndexSpec *spec = sctx->spec;

  dictIterator* iter = dictGetIterator(spec->missingFieldDict);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    const HiddenString *hiddenFieldName = dictGetKey(entry);
    InvertedIndex *idx = dictGetVal(entry);
    if(idx) {
      size_t length;
      const char* fieldName = HiddenString_GetUnsafe(hiddenFieldName, &length);
      struct iovec iov = {.iov_base = (void *)fieldName, length};

      CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
      II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

      II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

      InvertedIndex_GcDelta_Scan(
          &wr, sctx, idx,
          &cb, NULL
      );
    }
  }
  dictReleaseIterator(iter);

  // we are done with missing field docs inverted indexes
  FGC_sendTerminator(gc);
}

static void FGC_childCollectExistingDocs(ForkGC *gc, RedisSearchCtx *sctx) {
  IndexSpec *spec = sctx->spec;

  InvertedIndex *idx = spec->existingDocs;
  if (idx) {
    struct iovec iov = {.iov_base = (void *)"", 0};

    CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
    II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

    II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

    InvertedIndex_GcDelta_Scan(
        &wr, sctx, idx,
        &cb, NULL
    );
  }

  // we are done with existing docs inverted index
  FGC_sendTerminator(gc);
}

static void FGC_childScanIndexes(ForkGC *gc, IndexSpec *spec) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(gc->ctx, spec);
  const char* indexName = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(sctx.redisCtx, "debug", "ForkGC in index %s - child scanning indexes start", indexName);
  FGC_childCollectTerms(gc, &sctx);
  FGC_childCollectNumeric(gc, &sctx);
  FGC_childCollectTags(gc, &sctx);
  FGC_childCollectMissingDocs(gc, &sctx);
  FGC_childCollectExistingDocs(gc, &sctx);
  RedisModule_SendChildHeartbeat(1.0); // final heartbeat
  RedisModule_Log(sctx.redisCtx, "debug", "ForkGC in index %s - child scanning indexes end", indexName);
  // Let the parent wait for the terminal terminator, so we manage to send the heartbeat before exiting
  FGC_sendTerminator(gc);
}

typedef struct {
  // Node in the tree that was GC'd
  NumericRangeNode *node;
  InvertedIndexGcDelta* delta;
  II_GCScanStats info;

  void *registersWithLastBlock;
  void *registersWithoutLastBlock; // In case the last block was modified
} NumGcInfo;

static int recvRegisters(ForkGC *fgc, NumGcInfo *ninfo) {
  if (FGC_recvFixed(fgc, ninfo->registersWithLastBlock, NR_REG_SIZE) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  return FGC_recvFixed(fgc, ninfo->registersWithoutLastBlock, NR_REG_SIZE);
}

static FGCError recvNumIdx(ForkGC *gc, NumGcInfo *ninfo) {
  if (FGC_recvFixed(gc, &ninfo->node, sizeof(ninfo->node)) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }
  if (ninfo->node == NULL) {
    return FGC_DONE;
  }

  II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };
  ninfo->delta = InvertedIndex_GcDelta_Read(&rd);
  if (ninfo->delta == NULL) {
    goto error;
  }

  if (recvRegisters(gc, ninfo) != REDISMODULE_OK) {
    goto error;
  }
  return FGC_COLLECTED;

error:
  InvertedIndex_GcDelta_Free(ninfo->delta);
  return FGC_CHILD_ERROR;
}

static void resetCardinality(NumGcInfo *info, NumericRange *range, size_t blocksSinceFork) {
  if (!info->info.ignored_last_block) {
    hll_set_registers(&range->hll, info->registersWithLastBlock, NR_REG_SIZE);
    if (blocksSinceFork == 0) {
      return; // No blocks were added since the fork. We're done
    }
  } else {
    hll_set_registers(&range->hll, info->registersWithoutLastBlock, NR_REG_SIZE);
    blocksSinceFork++; // Count the ignored block as well
  }
  // Add the entries that were added since the fork to the HLL
  size_t startIdx = InvertedIndex_NumBlocks(range->entries) - blocksSinceFork; // Here `blocksSinceFork` > 0
  const IndexBlock *startBlock = InvertedIndex_BlockRef(range->entries, startIdx);
  t_docId startId = IndexBlock_FirstId(startBlock);
  IndexDecoderCtx decoderCtx = {.tag = IndexDecoderCtx_None};
  IndexReader *reader = NewIndexReader(range->entries, decoderCtx);
  RSIndexResult *res = NewNumericResult();
  IndexReader_SkipTo(reader, startId);
  bool reading = IndexReader_Next(reader, res);

  // Continue reading the rest
  while (reading) {
    double value = IndexResult_NumValue(res);
    hll_add(&range->hll, &value, sizeof(value));
    reading = IndexReader_Next(reader, res);
  }
  IndexReader_Free(reader);
  IndexResult_Free(res);
}

static void applyNumIdx(ForkGC *gc, RedisSearchCtx *sctx, NumGcInfo *ninfo) {
  NumericRangeNode *currNode = ninfo->node;
  InvertedIndexGcDelta *delta = ninfo->delta;
  II_GCScanStats *info = &ninfo->info;
  size_t blocksSinceFork = InvertedIndex_NumBlocks(currNode->range->entries) - GcScanDelta_LastBlockIdx(delta) - 1; // record before applying changes
  InvertedIndex_ApplyGcDelta(currNode->range->entries, delta, info);
  ninfo->delta = NULL;
  currNode->range->invertedIndexSize += info->bytes_allocated;
  currNode->range->invertedIndexSize -= info->bytes_freed;

  FGC_updateStats(gc, sctx, info->entries_removed, info->bytes_freed, info->bytes_allocated, info->ignored_last_block);

  resetCardinality(ninfo, currNode->range, blocksSinceFork);
}

static FGCError FGC_parentHandleTerms(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  size_t len;
  char *term = NULL;
  if (FGC_recvBuffer(gc, (void **)&term, &len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (term == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  II_GCScanStats info = {0};
  II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };

  InvertedIndexGcDelta *delta = InvertedIndex_GcDelta_Read(&rd);
  bool shouldFreeDeltas = true;

  if (delta == NULL) {
    rm_free(term);
    return FGC_CHILD_ERROR;
  }

  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  RedisSearchCtx sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  RedisSearchCtx *sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);

  InvertedIndex *idx = Redis_OpenInvertedIndex(sctx, term, len, DONT_CREATE_INDEX, NULL);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  InvertedIndex_ApplyGcDelta(idx, delta, &info);
  delta = NULL;

  if (InvertedIndex_NumDocs(idx) == 0) {

    // inverted index was cleaned entirely lets free it
    if (sctx->spec->keysDict) {
      CharBuf termKey = {.buf = term, .len = len};
      // get memory before deleting the inverted index
      size_t inv_idx_size = InvertedIndex_MemUsage(idx);
      if (dictDelete(sctx->spec->keysDict, &termKey) == DICT_OK) {
        info.bytes_freed += inv_idx_size;
      }
    }

    if (!Trie_Delete(sctx->spec->terms, term, len)) {
      const char* name = IndexSpec_FormatName(sctx->spec, RSGlobalConfig.hideUserDataFromLog);
      RedisModule_Log(sctx->redisCtx, "warning", "RedisSearch fork GC: deleting a term '%s' from"
                      " trie in index '%s' failed", RSGlobalConfig.hideUserDataFromLog ? Obfuscate_Text(term) : term, name);
    }
    sctx->spec->stats.scoring.numTerms--;
    sctx->spec->stats.termsSize -= len;
    if (sctx->spec->suffix) {
      deleteSuffixTrie(sctx->spec->suffix, term, len);
    }
  }

  FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:

  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }
  rm_free(term);

  InvertedIndex_GcDelta_Free(delta);
  return status;
}

static FGCError FGC_parentHandleNumeric(ForkGC *gc) {
  size_t fieldNameLen;
  char *fieldName = NULL;
  const FieldSpec *fs = NULL;
  uint64_t rtUniqueId;
  NumericRangeTree *rt = NULL;
  FGCError status = recvNumericTagHeader(gc, &fieldName, &fieldNameLen, &rtUniqueId);
  bool initialized = false;
  if (status == FGC_DONE) {
    return FGC_DONE;
  }

  NumGcInfo ninfo = {
    .registersWithLastBlock = rm_malloc(NR_REG_SIZE),
    .registersWithoutLastBlock = rm_malloc(NR_REG_SIZE),
  };
  while (status == FGC_COLLECTED) {
    // Read from GC process
    FGCError status2 = recvNumIdx(gc, &ninfo);
    bool shouldFreeDeltas = true;
    if (status2 == FGC_DONE) {
      break;
    } else if (status2 != FGC_COLLECTED) {
      status = status2;
      break;
    }

    StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (!sp) {
      status = FGC_SPEC_DELETED;
      goto loop_cleanup;
    }
    RedisSearchCtx _sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx *sctx = &_sctx;

    RedisSearchCtx_LockSpecWrite(sctx);

    if (!initialized) {
      fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, fieldNameLen);
      rt = openNumericOrGeoIndex(sctx->spec, fs, DONT_CREATE_INDEX);
      initialized = true;
    }

    if (rt->uniqueId != rtUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    if (!ninfo.node->range) {
      gc->stats.gcNumericNodesMissed++;
      goto loop_cleanup;
    }

    applyNumIdx(gc, sctx, &ninfo);
    shouldFreeDeltas = false; // ownership passed to applyNumIdx
    rt->numEntries -= ninfo.info.entries_removed;
    rt->invertedIndexesSize -= ninfo.info.bytes_freed;
    rt->invertedIndexesSize += ninfo.info.bytes_allocated;

    if (InvertedIndex_NumDocs(ninfo.node->range->entries) == 0) {
      rt->emptyLeaves++;
    }

  loop_cleanup:
    if (shouldFreeDeltas) {
      InvertedIndex_GcDelta_Free(ninfo.delta);
    }
    if (sp) {
      RedisSearchCtx_UnlockSpec(sctx);
      IndexSpecRef_Release(spec_ref);
    }
  }

  rm_free(ninfo.registersWithLastBlock);
  rm_free(ninfo.registersWithoutLastBlock);
  rm_free(fieldName);

  if (status == FGC_COLLECTED && rt && gc->cleanNumericEmptyNodes) {
    // We need to have a valid strong reference to the spec in order to dereference rt
    StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (!sp) return FGC_SPEC_DELETED;
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx_LockSpecWrite(&sctx);
    if (rt->emptyLeaves >= rt->numLeaves / 2) {
      NRN_AddRv rv = NumericRangeTree_TrimEmptyLeaves(rt);
      // rv.sz is the number of bytes added. Since we are cleaning empty leaves, it should be negative
      FGC_updateStats(gc, &sctx, 0, -rv.sz, 0, 0);
    }
    RedisSearchCtx_UnlockSpec(&sctx);
    IndexSpecRef_Release(spec_ref);
  }

  return status;
}

static FGCError FGC_parentHandleTags(ForkGC *gc) {
  size_t fieldNameLen;
  char *fieldName = NULL;
  uint64_t tagUniqueId;
  InvertedIndex *value = NULL;
  FGCError status = recvNumericTagHeader(gc, &fieldName, &fieldNameLen, &tagUniqueId);

  while (status == FGC_COLLECTED) {
    InvertedIndexGcDelta *delta = NULL;
    II_GCScanStats info = {0};
    TagIndex *tagIdx = NULL;
    char *tagVal = NULL;
    size_t tagValLen;

    if (FGC_recvFixed(gc, &value, sizeof value) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    // No more tags values in tag field
    if (value == NULL) {
      RS_LOG_ASSERT(status == FGC_COLLECTED, "GC status is COLLECTED");
      break;
    }

    StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (!sp) {
      status = FGC_SPEC_DELETED;
      break;
    }
    RedisSearchCtx _sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx *sctx = &_sctx;

    if (FGC_recvBuffer(gc, (void **)&tagVal, &tagValLen) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };
    delta = InvertedIndex_GcDelta_Read(&rd);
    bool shouldFreeDeltas = true;

    if (delta == NULL) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    RedisSearchCtx_LockSpecWrite(sctx);

    FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, fieldNameLen);
    RS_LOG_ASSERT_FMT(fs, "tag field '%.*s' not found in index during GC", (int)fieldNameLen, fieldName);
    tagIdx = TagIndex_Open(fs, DONT_CREATE_INDEX, NULL);
    RS_LOG_ASSERT_FMT(tagIdx, "tag field '%.*s' was not opened", (int)fieldNameLen, fieldName);

    if (tagIdx->uniqueId != tagUniqueId) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    size_t dummy_size;
    InvertedIndex *idx = TagIndex_OpenIndex(tagIdx, tagVal, tagValLen, DONT_CREATE_INDEX, &dummy_size);
    if (idx == TRIEMAP_NOTFOUND || idx != value) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    InvertedIndex_ApplyGcDelta(idx, delta, &info);
    delta = NULL;

    // if tag value is empty, let's remove it.
    if (InvertedIndex_NumDocs(idx) == 0) {
      // get memory before deleting the inverted index
      info.bytes_freed += InvertedIndex_MemUsage(idx);
      TrieMap_Delete(tagIdx->values, tagVal, tagValLen, (void (*)(void *))InvertedIndex_Free);

      if (tagIdx->suffix) {
        deleteSuffixTrieMap(tagIdx->suffix, tagVal, tagValLen);
      }
    }

    FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

  loop_cleanup:
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
    InvertedIndex_GcDelta_Free(delta);

    if (tagVal) {
      rm_free(tagVal);
    }
  }

  rm_free(fieldName);
  return status;
}

static FGCError FGC_parentHandleMissingDocs(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  size_t fieldNameLen;
  char *rawFieldName = NULL;

  if (FGC_recvBuffer(gc, (void **)&rawFieldName, &fieldNameLen) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (rawFieldName == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  II_GCScanStats info = {0};
  II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };
  InvertedIndexGcDelta *delta = InvertedIndex_GcDelta_Read(&rd);
  bool shouldFreeDeltas = true;

  if (delta == NULL) {
    rm_free(rawFieldName);
    return FGC_CHILD_ERROR;
  }

  HiddenString *fieldName = NewHiddenString(rawFieldName, fieldNameLen, false);
  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  RedisSearchCtx sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  RedisSearchCtx *sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);
  InvertedIndex *idx = dictFetchValue(sctx->spec->missingFieldDict, fieldName);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  InvertedIndex_ApplyGcDelta(idx, delta, &info);
  delta = NULL;

  if (InvertedIndex_NumDocs(idx) == 0) {
    // inverted index was cleaned entirely lets free it
    info.bytes_freed += InvertedIndex_MemUsage(idx);
    dictDelete(sctx->spec->missingFieldDict, fieldName);
  }
  FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:

  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }
  HiddenString_Free(fieldName, false);
  rm_free(rawFieldName);
  InvertedIndex_GcDelta_Free(delta);

  return status;
}

static FGCError FGC_parentHandleExistingDocs(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;

  size_t ei_len;
  char *empty_indicator = NULL;

  if (FGC_recvBuffer(gc, (void **)&empty_indicator, &ei_len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (empty_indicator == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  II_GCScanStats info = {0};
  II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };
  InvertedIndexGcDelta *delta = InvertedIndex_GcDelta_Read(&rd);
  bool shouldFreeDeltas = true;

  if (delta == NULL) {
    rm_free(empty_indicator);
    return FGC_CHILD_ERROR;
  }

  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  RedisSearchCtx sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  RedisSearchCtx *sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);

  InvertedIndex *idx = sp->existingDocs;

  InvertedIndex_ApplyGcDelta(idx, delta, &info);
  delta = NULL;

  // We don't count the records that we removed, because we also don't count
  // their addition (they are duplications so we have no such desire).

  if (InvertedIndex_NumDocs(idx) == 0) {
    // inverted index was cleaned entirely, let's free it
    info.bytes_freed += InvertedIndex_MemUsage(idx);
    InvertedIndex_Free(idx);
    sp->existingDocs = NULL;
  }
  FGC_updateStats(gc, sctx, 0, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:
  rm_free(empty_indicator);
  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }

  InvertedIndex_GcDelta_Free(delta);

  return status;
}

FGCError FGC_parentHandleFromChild(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  RedisModule_Log(gc->ctx, "debug", "ForkGC - parent start applying changes");

#define COLLECT_FROM_CHILD(e)               \
  while ((status = (e)) == FGC_COLLECTED) { \
  }                                         \
  if (status != FGC_DONE) {                 \
    return status;                          \
  }

  COLLECT_FROM_CHILD(FGC_parentHandleTerms(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleNumeric(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleTags(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleMissingDocs(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleExistingDocs(gc));

  // Wait for the final terminator from the child, so it can finish post-processing chores before we kill it
  size_t terminator_check;
  int rc = FGC_recvFixed(gc, &terminator_check, sizeof(terminator_check)); // final status from child
  if (rc != REDISMODULE_OK || terminator_check != SIZE_MAX) {
    return FGC_CHILD_ERROR;
  }
  RedisModule_Log(gc->ctx, "debug", "ForkGC - parent ends applying changes");

  return status;
}

// GIL must be held before calling this function
static inline bool isOutOfMemory(RedisModuleCtx *ctx) {
  // Check if we are a slave/replica
  bool isSlave = RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE;
  float used_memory_ratio = 0;
  if (!isSlave) {
    // On master, use the original unified logic
    used_memory_ratio = RedisMemory_GetUsedMemoryRatioUnified(ctx);
  } else {
    // On slaves, only consider max_process_mem
    RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "memory");
    size_t used_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);
    size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL);
    RedisModule_FreeServerInfo(ctx, info);

    used_memory_ratio = max_process_mem ? (float)used_memory / (float)max_process_mem : 0;
  }
  RedisModule_Log(ctx, "debug", "ForkGC - used memory ratio: %f", used_memory_ratio);

  return used_memory_ratio > 1;
}

static int periodicCb(void *privdata) {
  ForkGC *gc = privdata;
  RedisModuleCtx *ctx = gc->ctx;

  // This check must be done first, because some values (like `deletedDocsFromLastRun`) that are used for
  // early termination might never change after index deletion and will cause periodicCb to always return 1,
  // which will cause the GC to never stop rescheduling itself.
  // If the index was deleted, we don't want to reschedule the GC, so we return 0.
  // If the index is still valid, we MUST hold the strong reference to it until after the fork, to make sure
  // the child process has a valid reference to the index.
  // If we were to try and revalidate the index after the fork, it might already be dropped and the child
  // will exit before sending any data, and might left the parent waiting for data that will never arrive.
  // Attempting to revalidate the index after the fork is also problematic because the parent and child are
  // not synchronized, and the parent might see the index alive while the child sees it as deleted.
  StrongRef early_check = IndexSpecRef_Promote(gc->index);
  if (!StrongRef_Get(early_check)) {
    // Index was deleted
    return 0;
  }

  if (gc->deletedDocsFromLastRun < RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold) {
    IndexSpecRef_Release(early_check);
    return 1;
  }

  int gcrv = 1;
  pid_t cpid;
  TimeSample ts;

  while (gc->pauseState == FGC_PAUSED_CHILD) {
    gc->execState = FGC_STATE_WAIT_FORK;
    // spin or sleep
    usleep(500);
  }

  TimeSampler_Start(&ts);
  int pipefd[2];
  int rc = pipe(pipefd);  // create the pipe
  if (rc == -1) {
    RedisModule_Log(ctx, "warning", "Couldn't create pipe - got errno %d, aborting fork GC", errno);
    IndexSpecRef_Release(early_check);
    return 1;
  }
  gc->pipe_read_fd = pipefd[GC_READERFD];
  gc->pipe_write_fd = pipefd[GC_WRITERFD];
  // initialize the pollfd for the read pipe
  gc->pollfd_read[0].fd = gc->pipe_read_fd;
  gc->pollfd_read[0].events = POLLIN;

  // We need to acquire the GIL to use the fork api
  RedisModule_ThreadSafeContextLock(ctx);

  // Check if we are out of memory before even trying to fork
  if (isOutOfMemory(ctx)) {
    RedisModule_Log(ctx, "warning", "Not enough memory for GC fork, skipping GC job");
    gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRetryInterval;
    IndexSpecRef_Release(early_check);
    RedisModule_ThreadSafeContextUnlock(ctx);
    close(gc->pipe_read_fd);
    close(gc->pipe_write_fd);
    return 1;
  }

  gc->execState = FGC_STATE_SCANNING;

  cpid = RedisModule_Fork(NULL, NULL);  // duplicate the current process

  if (cpid == -1) {
    RedisModule_Log(ctx, "warning", "fork failed - got errno %d, aborting fork GC", errno);
    gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRetryInterval;
    IndexSpecRef_Release(early_check);

    RedisModule_ThreadSafeContextUnlock(ctx);

    close(gc->pipe_read_fd);
    close(gc->pipe_write_fd);

    return 1;
  }

  // Now that we hold the GIL, we can cache this value knowing it won't change by the main thread
  // upon deleting a document (this is the actual number of documents to be cleaned by the fork).
  size_t num_docs_to_clean = gc->deletedDocsFromLastRun;
  gc->deletedDocsFromLastRun = 0;

  gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec;

  RedisModule_ThreadSafeContextUnlock(ctx);


  if (cpid == 0) {
    // fork process
    setpriority(PRIO_PROCESS, getpid(), 19);
    close(gc->pipe_read_fd);
    // Pass the index to the child process
    FGC_childScanIndexes(gc, StrongRef_Get(early_check));
    close(gc->pipe_write_fd);
    sleep(RSGlobalConfig.gcConfigParams.forkGc.forkGcSleepBeforeExit);
    RedisModule_ExitFromChild(EXIT_SUCCESS);
  } else {
    // main process
    // release the strong reference to the index for the main process (see comment above)
    IndexSpecRef_Release(early_check);
    close(gc->pipe_write_fd);
    while (gc->pauseState == FGC_PAUSED_PARENT) {
      gc->execState = FGC_STATE_WAIT_APPLY;
      // spin
      usleep(500);
    }

    gc->execState = FGC_STATE_APPLYING;
    gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes;
    if (FGC_parentHandleFromChild(gc) == FGC_SPEC_DELETED) {
      gcrv = 0;
    }
    close(gc->pipe_read_fd);
    // give the child some time to exit gracefully
    for (int attempt = 0; attempt < GC_WAIT_ATTEMPTS; ++attempt) {
      if (waitpid(cpid, NULL, WNOHANG) == 0) {
        usleep(500);
      }
    }
    // KillForkChild must be called when holding the GIL
    // otherwise it might cause a pipe leak and eventually run
    // out of file descriptor
    RedisModule_ThreadSafeContextLock(ctx);
    RedisModule_KillForkChild(cpid);
    RedisModule_ThreadSafeContextUnlock(ctx);

    if (gcrv) {
      gcrv = VecSim_CallTieredIndexesGC(gc->index);
    }
  }

  IndexsGlobalStats_UpdateLogicallyDeleted(-num_docs_to_clean);
  gc->execState = FGC_STATE_IDLE;
  TimeSampler_End(&ts);
  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return gcrv;
}

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define NO_TSAN_CHECK __attribute__((no_sanitize("thread")))
#endif
#endif
#ifndef NO_TSAN_CHECK
#define NO_TSAN_CHECK
#endif

void FGC_WaitBeforeFork(ForkGC *gc) NO_TSAN_CHECK {
  RS_LOG_ASSERT(gc->pauseState == 0, "FGC pause state should be 0");
  gc->pauseState = FGC_PAUSED_CHILD;

  while (gc->execState != FGC_STATE_WAIT_FORK) {
    usleep(500);
  }
}

void FGC_ForkAndWaitBeforeApply(ForkGC *gc) NO_TSAN_CHECK {
  // Ensure that we're waiting for the child to begin
  RS_LOG_ASSERT(gc->pauseState == FGC_PAUSED_CHILD, "FGC pause state should be CHILD");
  RS_LOG_ASSERT(gc->execState == FGC_STATE_WAIT_FORK, "FGC exec state should be WAIT_FORK");

  gc->pauseState = FGC_PAUSED_PARENT;
  while (gc->execState != FGC_STATE_WAIT_APPLY) {
    usleep(500);
  }
}

void FGC_Apply(ForkGC *gc) NO_TSAN_CHECK {
  gc->pauseState = FGC_PAUSED_UNPAUSED;
  while (gc->execState != FGC_STATE_IDLE) {
    usleep(500);
  }
}

static void onTerminateCb(void *privdata) {
  ForkGC *gc = privdata;
  IndexsGlobalStats_UpdateLogicallyDeleted(-gc->deletedDocsFromLastRun);
  WeakRef_Release(gc->index);
  RedisModule_FreeThreadSafeContext(gc->ctx);
  rm_free(gc);
}

static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
  ForkGC *gc = gcCtx;
  if (!gc) return;
  REPLY_KVNUM("bytes_collected", gc->stats.totalCollected);
  REPLY_KVNUM("total_ms_run", gc->stats.totalMSRun);
  REPLY_KVNUM("total_cycles", gc->stats.numCycles);
  REPLY_KVNUM("average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  REPLY_KVNUM("last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  REPLY_KVNUM("gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  REPLY_KVNUM("gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  ForkGC *gc = gcCtx;
  RedisModule_InfoBeginDictField(ctx, "gc_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "bytes_collected", gc->stats.totalCollected);
  RedisModule_InfoAddFieldLongLong(ctx, "total_ms_run", gc->stats.totalMSRun);
  RedisModule_InfoAddFieldLongLong(ctx, "total_cycles", gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  RedisModule_InfoAddFieldDouble(ctx, "gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  RedisModule_InfoAddFieldDouble(ctx, "gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
  RedisModule_InfoEndDictField(ctx);
}

static void deleteCb(void *ctx) {
  ForkGC *gc = ctx;
  ++gc->deletedDocsFromLastRun;
  IndexsGlobalStats_UpdateLogicallyDeleted(1);
}

static struct timespec getIntervalCb(void *ctx) {
  ForkGC *gc = ctx;
  return gc->retryInterval;
}

ForkGC *FGC_New(StrongRef spec_ref, GCCallbacks *callbacks) {
  ForkGC *forkGc = rm_calloc(1, sizeof(*forkGc));
  *forkGc = (ForkGC){
      .index = StrongRef_Demote(spec_ref),
      .deletedDocsFromLastRun = 0,
  };
  forkGc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec;
  forkGc->retryInterval.tv_nsec = 0;

  forkGc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes;
  forkGc->ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteCb;

  return forkGc;
}
