#include "fork_gc.h"
#include "util/arr.h"
#include "search_ctx.h"
#include "inverted_index.h"
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
#include "rwlock.h"
#include "util/khash.h"
#include <float.h>
#include "module.h"
#include "rmutil/rm_assert.h"

#ifdef __linux__
#include <sys/prctl.h>
#endif

#define GC_WRITERFD 1
#define GC_READERFD 0

typedef enum {
  // Terms have been collected
  FGC_COLLECTED,
  // No more terms remain
  FGC_DONE,
  // Pipe error, child probably crashed
  FGC_CHILD_ERROR,
  // Error on the parent
  FGC_PARENT_ERROR
} FGCError;

static int __attribute__((warn_unused_result)) FGC_lock(ForkGC *gc, RedisModuleCtx *ctx) {
  if (gc->type == FGC_TYPE_NOKEYSPACE) {
    RWLOCK_ACQUIRE_WRITE();
    if (gc->deleting) {
      RWLOCK_RELEASE();
      return 0;
    }
  } else {
    RedisModule_ThreadSafeContextLock(ctx);
    if (gc->deleting) {
      RedisModule_ThreadSafeContextUnlock(ctx);
      return 0;
    }
  }
  return 1;
}

static void FGC_unlock(ForkGC *gc, RedisModuleCtx *ctx) {
  if (gc->type == FGC_TYPE_NOKEYSPACE) {
    RWLOCK_RELEASE();
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

static RedisSearchCtx *FGC_getSctx(ForkGC *gc, RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = NULL;
  if (gc->type == FGC_TYPE_NOKEYSPACE) {
    sctx = rm_malloc(sizeof(*sctx));
    *sctx = (RedisSearchCtx)SEARCH_CTX_STATIC(ctx, gc->sp);
  } else if (gc->type == FGC_TYPE_INKEYSPACE) {
    sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName, false);
  }
  return sctx;
}

static void FGC_updateStats(RedisSearchCtx *sctx, ForkGC *gc, size_t recordsRemoved,
                            size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
}

static void FGC_sendFixed(ForkGC *fgc, const void *buff, size_t len) {
  RS_LOG_ASSERT(len > 0, "buffer length cannot be 0");
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], buff, len);
  if (size != len) {
    perror("broken pipe, exiting GC fork: write() failed");
    // just exit, do not abort(), which will trigger a watchdog on RLEC, causing adverse effects
    RedisModule_Log(NULL, "warning", "GC fork: broken pipe, exiting");
    exit(1);
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
  while (len) {
    ssize_t nrecvd = read(fgc->pipefd[GC_READERFD], buf, len);
    if (nrecvd > 0) {
      buf += nrecvd;
      len -= nrecvd;
    } else if (nrecvd < 0 && errno != EINTR) {
      printf("Got error while reading from pipe (%s)", strerror(errno));
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

#define TRY_RECV_FIXED(gc, obj, len)                   \
  if (FGC_recvFixed(gc, obj, len) != REDISMODULE_OK) { \
    return REDISMODULE_ERR;                            \
  }

static void *RECV_BUFFER_EMPTY = (void *)0x0deadbeef;

static int __attribute__((warn_unused_result))
FGC_recvBuffer(ForkGC *fgc, void **buf, size_t *len) {
  TRY_RECV_FIXED(fgc, len, sizeof *len);
  if (*len == SIZE_MAX) {
    *buf = RECV_BUFFER_EMPTY;
    return REDISMODULE_OK;
  }
  if (*len == 0) {
    *buf = NULL;
    return REDISMODULE_OK;
  }

  *buf = rm_malloc(*len + 1);
  ((char *)(*buf))[*len] = 0;
  if (FGC_recvFixed(fgc, *buf, *len) != REDISMODULE_OK) {
    rm_free(buf);
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

#define TRY_RECV_BUFFER(gc, buf, len)                   \
  if (FGC_recvBuffer(gc, buf, len) != REDISMODULE_OK) { \
    return REDISMODULE_ERR;                             \
  }

typedef struct {
  // Number of blocks prior to repair
  uint32_t nblocksOrig;
  // Number of blocks repaired
  uint32_t nblocksRepaired;
  // Number of bytes cleaned in inverted index
  uint64_t nbytesCollected;
  // Number of document records removed
  uint64_t ndocsCollected;

  /** Specific information about the _last_ index block */
  size_t lastblkDocsRemoved;
  size_t lastblkBytesCollected;
  size_t lastblkNumDocs;
} MSG_IndexInfo;

/** Structure sent describing an index block */
typedef struct {
  IndexBlock blk;
  int64_t oldix;  // Old position of the block
  int64_t newix;  // New position of the block
  // the actual content of the block follows...
} MSG_RepairedBlock;

typedef struct {
  void *ptr;       // Address of the buffer to free
  uint32_t oldix;  // Old index of deleted block
  uint32_t _pad;   // Uninitialized reads, otherwise
} MSG_DeletedBlock;

/**
 * headerCallback and hdrarg are invoked before the inverted index is sent, only
 * iff the inverted index was repaired.
 * RepairCallback and its argument are passed directly to IndexBlock_Repair; see
 * that function for more details.
 */
static bool FGC_childRepairInvidx(ForkGC *gc, RedisSearchCtx *sctx, InvertedIndex *idx,
                                  void (*headerCallback)(ForkGC *, void *), void *hdrarg,
                                  IndexRepairParams *params) {
  MSG_RepairedBlock *fixed = array_new(MSG_RepairedBlock, 10);
  MSG_DeletedBlock *deleted = array_new(MSG_DeletedBlock, 10);
  IndexBlock *blocklist = array_new(IndexBlock, idx->size);
  MSG_IndexInfo ixmsg = {.nblocksOrig = idx->size};
  IndexRepairParams params_s = {0};
  bool rv = false;
  if (!params) {
    params = &params_s;
  }

  for (size_t i = 0; i < idx->size; ++i) {
    params->bytesCollected = 0;
    params->bytesBeforFix = 0;
    params->bytesAfterFix = 0;
    IndexBlock *blk = idx->blocks + i;
    if (blk->lastId - blk->firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      // todo: is it ok??
      blocklist = array_append(blocklist, *blk);
      continue;
    }

    // Capture the pointer address before the block is cleared; otherwise
    // the pointer might be freed!
    void *bufptr = blk->buf.data;
    int nrepaired = IndexBlock_Repair(blk, &sctx->spec->docs, idx->flags, params);
    // We couldn't repair the block - return 0
    if (nrepaired == -1) {
      goto done;
    } else if (nrepaired == 0) {
      // unmodified block
      blocklist = array_append(blocklist, *blk);
      continue;
    }

    if (blk->numDocs == 0) {
      // this block should be removed
      MSG_DeletedBlock *delmsg = array_ensure_tail(&deleted, MSG_DeletedBlock);
      *delmsg = (MSG_DeletedBlock){.ptr = bufptr, .oldix = i};
    } else {
      blocklist = array_append(blocklist, *blk);
      MSG_RepairedBlock *fixmsg = array_ensure_tail(&fixed, MSG_RepairedBlock);
      fixmsg->newix = array_len(blocklist) - 1;
      fixmsg->oldix = i;
      fixmsg->blk = *blk;
      ixmsg.nblocksRepaired++;
    }

    ixmsg.nbytesCollected += (params->bytesBeforFix - params->bytesAfterFix);
    ixmsg.ndocsCollected += nrepaired;
    if (i == idx->size - 1) {
      ixmsg.lastblkBytesCollected = ixmsg.nbytesCollected;
      ixmsg.lastblkDocsRemoved = nrepaired;
      ixmsg.lastblkNumDocs = blk->numDocs + nrepaired;
    }
  }

  if (array_len(fixed) == 0 && array_len(deleted) == 0) {
    // No blocks were removed or repaired
    goto done;
  }

  headerCallback(gc, hdrarg);
  FGC_sendFixed(gc, &ixmsg, sizeof ixmsg);
  if (array_len(blocklist) == idx->size) {
    // no empty block, there is no need to send the blocks array. Don't send
    // any new blocks
    FGC_sendBuffer(gc, NULL, 0);
  } else {
    FGC_sendBuffer(gc, blocklist, array_len(blocklist) * sizeof(*blocklist));
  }
  FGC_sendBuffer(gc, deleted, array_len(deleted) * sizeof(*deleted));

  for (size_t i = 0; i < array_len(fixed); ++i) {
    // write fix block
    const MSG_RepairedBlock *msg = fixed + i;
    const IndexBlock *blk = blocklist + msg->newix;
    FGC_sendFixed(gc, msg, sizeof(*msg));
    FGC_sendBuffer(gc, IndexBlock_DataBuf(blk), IndexBlock_DataLen(blk));
  }
  rv = true;

done:
  array_free(fixed);
  array_free(blocklist);
  array_free(deleted);
  return rv;
}

static void sendHeaderString(ForkGC *gc, void *arg) {
  struct iovec *iov = arg;
  FGC_sendBuffer(gc, iov->iov_base, iov->iov_len);
}

static void FGC_childCollectTerms(ForkGC *gc, RedisSearchCtx *sctx) {
  TrieIterator *iter = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    RedisModuleKey *idxKey = NULL;
    InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
    if (idx) {
      struct iovec iov = {.iov_base = (void *)term, termLen};
      FGC_childRepairInvidx(gc, sctx, idx, sendHeaderString, &iov, NULL);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    rm_free(term);
  }
  DFAFilter_Free(iter->ctx);
  rm_free(iter->ctx);
  TrieIterator_Free(iter);

  // we are done with terms
  FGC_sendTerminator(gc);
}

KHASH_MAP_INIT_INT64(cardvals, size_t)

typedef struct {
  const IndexBlock *lastblk;
  khash_t(cardvals) * delLast;
  khash_t(cardvals) * delRest;
} numCbCtx;

typedef union {
  uint64_t u64;
  double d48;
} numUnion;

static void countDeleted(const RSIndexResult *r, const IndexBlock *blk, void *arg) {
  numCbCtx *ctx = arg;
  khash_t(cardvals) *ht = NULL;
  if (blk == ctx->lastblk) {
    if ((ht = ctx->delLast) == NULL) {
      ht = ctx->delLast = kh_init(cardvals);
    }
  } else if ((ht = ctx->delRest) == NULL) {
    ht = ctx->delRest = kh_init(cardvals);
  }
  RS_LOG_ASSERT(ht, "cardvals should not be NULL");
  int added = 0;
  numUnion u = {r->num.value};
  khiter_t it = kh_put(cardvals, ht, u.u64, &added);
  if (!added) {
    // i.e. already existed
    kh_val(ht, it)++;
  } else {
    kh_val(ht, it) = 0;
  }
}

typedef struct {
  const char *field;
  const void *curPtr;
  uint64_t uniqueId;
  int sentFieldName;
} tagNumHeader;

static void sendNumericTagHeader(ForkGC *fgc, void *arg) {
  tagNumHeader *info = arg;
  if (!info->sentFieldName) {
    info->sentFieldName = 1;
    FGC_sendBuffer(fgc, info->field, strlen(info->field));
    FGC_sendFixed(fgc, &info->uniqueId, sizeof info->uniqueId);
  }
  FGC_SEND_VAR(fgc, info->curPtr);
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

static void sendKht(ForkGC *gc, const khash_t(cardvals) * kh) {
  size_t n = 0;
  if (!kh) {
    FGC_SEND_VAR(gc, n);
    return;
  }
  n = kh_size(kh);
  size_t nsent = 0;

  FGC_SEND_VAR(gc, n);
  for (khiter_t it = kh_begin(kh); it != kh_end(kh); ++it) {
    if (!kh_exist(kh, it)) {
      continue;
    }
    numUnion u = {kh_key(kh, it)};
    size_t count = kh_val(kh, it);
    CardinalityValue cu = {.value = u.d48, .appearances = count};
    FGC_SEND_VAR(gc, cu);
    nsent++;
  }
  RS_LOG_ASSERT(nsent == n, "Not all hashes has been sent");
}

static void FGC_childCollectNumeric(ForkGC *gc, RedisSearchCtx *sctx) {
  RedisModuleKey *idxKey = NULL;
  FieldSpec **numericFields = getFieldsByType(sctx->spec, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);

  for (int i = 0; i < array_len(numericFields); ++i) {
    RedisModuleString *keyName =
        IndexSpec_GetFormattedKey(sctx->spec, numericFields[i], INDEXFLD_T_NUMERIC);
    NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

    NumericRangeTreeIterator *gcIterator = NumericRangeTreeIterator_New(rt);

    NumericRangeNode *currNode = NULL;
    tagNumHeader header = {.field = numericFields[i]->name, .uniqueId = rt->uniqueId};

    while ((currNode = NumericRangeTreeIterator_Next(gcIterator))) {
      if (!currNode->range) {
        continue;
      }
      numCbCtx nctx = {0};
      InvertedIndex *idx = currNode->range->entries;
      nctx.lastblk = idx->blocks + idx->size - 1;
      IndexRepairParams params = {.RepairCallback = countDeleted, .arg = &nctx};
      header.curPtr = currNode;
      bool repaired = FGC_childRepairInvidx(gc, sctx, idx, sendNumericTagHeader, &header, &params);

      if (repaired) {
        sendKht(gc, nctx.delRest);
        sendKht(gc, nctx.delLast);
      }
      if (nctx.delRest) {
        kh_destroy(cardvals, nctx.delRest);
      }
      if (nctx.delLast) {
        kh_destroy(cardvals, nctx.delLast);
      }
    }

    if (header.sentFieldName) {
      // If we've repaired at least one entry, send the terminator;
      // note that "terminator" just means a zero address and not the
      // "no more strings" terminator in FGC_sendTerminator
      void *pdummy = NULL;
      FGC_SEND_VAR(gc, pdummy);
    }

    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }

    NumericRangeTreeIterator_Free(gcIterator);
  }

  // we are done with numeric fields
  FGC_sendTerminator(gc);
}

static void FGC_childCollectTags(ForkGC *gc, RedisSearchCtx *sctx) {
  RedisModuleKey *idxKey = NULL;
  FieldSpec **tagFields = getFieldsByType(sctx->spec, INDEXFLD_T_TAG);
  if (array_len(tagFields) != 0) {
    for (int i = 0; i < array_len(tagFields); ++i) {
      RedisModuleString *keyName =
          IndexSpec_GetFormattedKey(sctx->spec, tagFields[i], INDEXFLD_T_TAG);
      TagIndex *tagIdx = TagIndex_Open(sctx, keyName, false, &idxKey);
      if (!tagIdx) {
        continue;
      }

      tagNumHeader header = {.field = tagFields[i]->name, .uniqueId = tagIdx->uniqueId};

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values, "", 0);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        header.curPtr = value;
        // send repaired data
        FGC_childRepairInvidx(gc, sctx, value, sendNumericTagHeader, &header, NULL);
      }

      // we are done with the current field
      if (header.sentFieldName) {
        void *pdummy = NULL;
        FGC_SEND_VAR(gc, pdummy);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }
    }
  }
  // we are done with numeric fields
  FGC_sendTerminator(gc);
}

static void FGC_childScanIndexes(ForkGC *gc) {
  RedisSearchCtx *sctx = FGC_getSctx(gc, gc->ctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    // write log here
    return;
  }

  FGC_childCollectTerms(gc, sctx);
  FGC_childCollectNumeric(gc, sctx);
  FGC_childCollectTags(gc, sctx);

  SearchCtx_Free(sctx);
}

typedef struct {
  MSG_DeletedBlock *delBlocks;
  size_t numDelBlocks;

  MSG_RepairedBlock *changedBlocks;

  IndexBlock *newBlocklist;
  size_t newBlocklistSize;
  int lastBlockIgnored;
} InvIdxBuffers;

static int __attribute__((warn_unused_result))
FGC_recvRepairedBlock(ForkGC *gc, MSG_RepairedBlock *binfo) {
  if (FGC_recvFixed(gc, binfo, sizeof(*binfo)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  Buffer *b = &binfo->blk.buf;
  if (FGC_recvBuffer(gc, (void **)&b->data, &b->offset) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  b->cap = b->offset;
  return REDISMODULE_OK;
}

static int __attribute__((warn_unused_result))
FGC_recvInvIdx(ForkGC *gc, InvIdxBuffers *bufs, MSG_IndexInfo *info) {
  size_t nblocksRecvd = 0;
  if (FGC_recvFixed(gc, info, sizeof(*info)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (FGC_recvBuffer(gc, (void **)&bufs->newBlocklist, &bufs->newBlocklistSize) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (bufs->newBlocklistSize) {
    bufs->newBlocklistSize /= sizeof(*bufs->newBlocklist);
  }
  if (FGC_recvBuffer(gc, (void **)&bufs->delBlocks, &bufs->numDelBlocks) != REDISMODULE_OK) {
    goto error;
  }
  bufs->numDelBlocks /= sizeof(*bufs->delBlocks);
  bufs->changedBlocks = rm_malloc(sizeof(*bufs->changedBlocks) * info->nblocksRepaired);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    if (FGC_recvRepairedBlock(gc, bufs->changedBlocks + i) != REDISMODULE_OK) {
      goto error;
    }
    nblocksRecvd++;
  }
  return REDISMODULE_OK;

error:
  rm_free(bufs->newBlocklist);
  for (size_t ii = 0; ii < nblocksRecvd; ++ii) {
    rm_free(bufs->changedBlocks[ii].blk.buf.data);
  }
  rm_free(bufs->changedBlocks);
  memset(bufs, 0, sizeof(*bufs));
  return REDISMODULE_ERR;
}

static void freeInvIdx(InvIdxBuffers *bufs, MSG_IndexInfo *info) {
  rm_free(bufs->newBlocklist);
  rm_free(bufs->delBlocks);

  if (bufs->changedBlocks) {
    // could be null because of pipe error
    for (size_t ii = 0; ii < info->nblocksRepaired; ++ii) {
      rm_free(bufs->changedBlocks[ii].blk.buf.data);
    }
  }
  rm_free(bufs->changedBlocks);
}

static void checkLastBlock(ForkGC *gc, InvIdxBuffers *idxData, MSG_IndexInfo *info,
                           InvertedIndex *idx) {
  IndexBlock *lastOld = idx->blocks + info->nblocksOrig - 1;
  if (info->lastblkDocsRemoved == 0) {
    // didn't touch last block in child
    return;
  }
  if (info->lastblkNumDocs == lastOld->numDocs) {
    // didn't touch last block in parent
    return;
  }

  if (info->lastblkDocsRemoved == info->lastblkNumDocs) {
    // Last block was deleted entirely while updates on the main process.
    // We need to remove it from delBlocks list
    idxData->numDelBlocks--;

    // Then We need add it to the newBlocklist.
    idxData->newBlocklistSize++;
    idxData->newBlocklist = rm_realloc(idxData->newBlocklist,
                                       sizeof(*idxData->newBlocklist) * idxData->newBlocklistSize);
    idxData->newBlocklist[idxData->newBlocklistSize - 1] = *lastOld;
  } else {
    // Last block was modified on the child and on the parent.

    // we need to remove it from changedBlocks
    MSG_RepairedBlock *rb = idxData->changedBlocks + info->nblocksRepaired - 1;
    indexBlock_Free(&rb->blk);
    info->nblocksRepaired--;

    // Then add it to newBlocklist if newBlocklist is not NULL.
    // If newBlocklist!=NULL then the last block must be there (it was changed and not deleted)
    // If newBlocklist==NULL then by decreasing the nblocksOrig by one we make sure to keep the last
    // block
    if (idxData->newBlocklist) {
      idxData->newBlocklist[idxData->newBlocklistSize - 1] = *lastOld;
    } else {
      --info->nblocksOrig;
    }
  }

  info->ndocsCollected -= info->lastblkDocsRemoved;
  info->nbytesCollected -= info->lastblkBytesCollected;
  idxData->lastBlockIgnored = 1;
  gc->stats.gcBlocksDenied++;
}

static void FGC_applyInvertedIndex(ForkGC *gc, InvIdxBuffers *idxData, MSG_IndexInfo *info,
                                   InvertedIndex *idx) {
  checkLastBlock(gc, idxData, info, idx);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    MSG_RepairedBlock *blockModified = idxData->changedBlocks + i;
    indexBlock_Free(&idx->blocks[blockModified->oldix]);
  }
  for (size_t i = 0; i < idxData->numDelBlocks; ++i) {
    // Blocks that were deleted entirely:
    MSG_DeletedBlock *delinfo = idxData->delBlocks + i;
    rm_free(delinfo->ptr);
  }
  rm_free(idxData->delBlocks);

  // Ensure the old index is at least as big as the new index' size
  RS_LOG_ASSERT(idx->size >= info->nblocksOrig, "Old index should be larger or equal to new index");

  if (idxData->newBlocklist) {
    /**
     * At this point, we check if the last block has had new data added to it,
     * but was _not_ repaired. We check for a repaired last block in
     * checkLastBlock().
     */

    if (!info->lastblkDocsRemoved) {
      /**
       * Last block was unmodified-- let's prefer the last block's pointer
       * over our own (which may be stale).
       * If the last block was repaired, this is handled above
       */
      idxData->newBlocklist[idxData->newBlocklistSize - 1] = idx->blocks[info->nblocksOrig - 1];
    }

    // Number of blocks added in the parent process since the last scan
    size_t newAddedLen = idx->size - info->nblocksOrig;

    // The final size is the reordered block size, plus the number of blocks
    // which we haven't scanned yet, because they were added in the parent
    size_t totalLen = idxData->newBlocklistSize + newAddedLen;

    idxData->newBlocklist =
        rm_realloc(idxData->newBlocklist, totalLen * sizeof(*idxData->newBlocklist));
    memcpy(idxData->newBlocklist + idxData->newBlocklistSize, (idx->blocks + info->nblocksOrig),
           newAddedLen * sizeof(*idxData->newBlocklist));

    rm_free(idx->blocks);
    idxData->newBlocklistSize += newAddedLen;
    idx->blocks = idxData->newBlocklist;
    idx->size = idxData->newBlocklistSize;
  } else if (idxData->numDelBlocks) {
    // In this case, all blocks the child has seen need to be deleted. We don't
    // get a new block list, because they are all gone..
    size_t newAddedLen = idx->size - info->nblocksOrig;
    if (newAddedLen) {
      memmove(idx->blocks, idx->blocks + info->nblocksOrig, sizeof(*idx->blocks) * newAddedLen);
    }
    idx->size = newAddedLen;
    if (idx->size == 0) {
      InvertedIndex_AddBlock(idx, 0);
    }
  }

  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    MSG_RepairedBlock *blockModified = idxData->changedBlocks + i;
    idx->blocks[blockModified->newix] = blockModified->blk;
  }

  idx->numDocs -= info->ndocsCollected;
  idx->gcMarker++;
}

static FGCError FGC_parentHandleTerms(ForkGC *gc, RedisModuleCtx *rctx) {
  FGCError status = FGC_COLLECTED;
  size_t len;
  int hasLock = 0;
  char *term = NULL;
  if (FGC_recvBuffer(gc, (void **)&term, &len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;

  if (term == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  InvIdxBuffers idxbufs = {0};
  MSG_IndexInfo info = {0};
  if (FGC_recvInvIdx(gc, &idxbufs, &info) != REDISMODULE_OK) {
    rm_free(term);
    return FGC_CHILD_ERROR;
  }

  if (!FGC_lock(gc, rctx)) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  hasLock = 1;
  sctx = FGC_getSctx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, len, 1, &idxKey);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  FGC_applyInvertedIndex(gc, &idxbufs, &info, idx);
  FGC_updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

  if (idx->numDocs == 0) {
    // inverted index was cleaned entirely lets free it
    RedisModuleString *termKey = fmtRedisTermKey(sctx, term, len);
    size_t formatedTremLen;
    const char *formatedTrem = RedisModule_StringPtrLen(termKey, &formatedTremLen);
    if (sctx->spec->keysDict) {
      dictDelete(sctx->spec->keysDict, termKey);
    }
    Trie_Delete(sctx->spec->terms, term, len);
    RedisModule_FreeString(sctx->redisCtx, termKey);
  }

cleanup:

  if (idxKey) {
    RedisModule_CloseKey(idxKey);
  }
  if (sctx) {
    SearchCtx_Free(sctx);
  }
  if (hasLock) {
    FGC_unlock(gc, rctx);
  }
  rm_free(term);
  if (status != FGC_COLLECTED) {
    freeInvIdx(&idxbufs, &info);
  } else {
    rm_free(idxbufs.changedBlocks);
  }
  return status;
}

typedef struct {
  // Node in the tree that was GC'd
  NumericRangeNode *node;
  CardinalityValue *lastBlockDeleted;
  CardinalityValue *restBlockDeleted;
  size_t nlastBlockDel;
  size_t nrestBlockDel;
  InvIdxBuffers idxbufs;
  MSG_IndexInfo info;
} NumGcInfo;

static int recvCardvals(ForkGC *fgc, CardinalityValue **tgt, size_t *len) {
  if (FGC_recvFixed(fgc, len, sizeof(*len)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  *len *= sizeof(**tgt);
  if (!*len) {
    *tgt = NULL;
    return REDISMODULE_OK;
  }
  *tgt = rm_malloc(sizeof(**tgt) * *len);
  int rc = FGC_recvFixed(fgc, *tgt, *len);
  if (rc == REDISMODULE_OK) {
    *len /= sizeof(**tgt);
  }
  return rc;
}

static FGCError recvNumIdx(ForkGC *gc, NumGcInfo *ninfo) {
  if (FGC_recvFixed(gc, &ninfo->node, sizeof(ninfo->node)) != REDISMODULE_OK) {
    goto error;
  }
  if (ninfo->node == NULL) {
    return FGC_DONE;
  }

  if (FGC_recvInvIdx(gc, &ninfo->idxbufs, &ninfo->info) != REDISMODULE_OK) {
    goto error;
  }

  if (recvCardvals(gc, &ninfo->restBlockDeleted, &ninfo->nrestBlockDel) != REDISMODULE_OK) {
    goto error;
  }
  if (recvCardvals(gc, &ninfo->lastBlockDeleted, &ninfo->nlastBlockDel) != REDISMODULE_OK) {
    goto error;
  }

  return FGC_COLLECTED;

error:
  printf("Error receiving numeric index!\n");
  freeInvIdx(&ninfo->idxbufs, &ninfo->info);
  rm_free(ninfo->lastBlockDeleted);
  rm_free(ninfo->restBlockDeleted);
  memset(ninfo, 0, sizeof(*ninfo));
  return FGC_CHILD_ERROR;
}

static void resetCardinality(NumGcInfo *info, NumericRangeNode *currNone) {
  khash_t(cardvals) *kh = kh_init(cardvals);
  int added;
  for (size_t ii = 0; ii < info->nrestBlockDel; ++ii) {
    numUnion u = {info->restBlockDeleted[ii].value};
    khiter_t it = kh_put(cardvals, kh, u.u64, &added);
    kh_val(kh, it) = info->restBlockDeleted[ii].appearances;
  }
  if (!info->idxbufs.lastBlockIgnored) {
    for (size_t ii = 0; ii < info->nlastBlockDel; ++ii) {
      numUnion u = {info->lastBlockDeleted[ii].value};
      khiter_t it = kh_put(cardvals, kh, u.u64, &added);
      if (!added) {
        kh_val(kh, it) += info->lastBlockDeleted[ii].appearances;
      } else {
        kh_val(kh, it) = info->lastBlockDeleted[ii].appearances;
      }
    }
  }

  NumericRange *r = currNone->range;
  size_t n = array_len(r->values);
  double minVal = DBL_MAX, maxVal = -DBL_MIN, uniqueSum = 0;

  for (size_t ii = 0; ii < array_len(r->values); ++ii) {
  reeval:;
    numUnion u = {r->values[ii].value};
    khiter_t it = kh_get(cardvals, kh, u.u64);
    if (it != kh_end(kh) && (r->values[ii].appearances -= kh_val(kh, it)) == 0) {
      // delet this
      size_t isLast = array_len(r->values) == ii + 1;
      array_del_fast(r->values, ii);
      if (!isLast) {
        goto reeval;
      }
    } else {
      minVal = MIN(minVal, r->values[ii].value);
      maxVal = MAX(maxVal, r->values[ii].value);
      uniqueSum += r->values[ii].value;
    }
  }
  kh_destroy(cardvals, kh);
  // we can only update the min and the max value if the node is a leaf.
  // otherwise the min and the max also represent its children values and
  // we can not change it.
  if (NumericRangeNode_IsLeaf(currNone)) {
    r->minVal = minVal;
    r->maxVal = maxVal;
  }
  r->unique_sum = uniqueSum;
  r->card = array_len(r->values);
}

static void applyNumIdx(ForkGC *gc, RedisSearchCtx *sctx, NumGcInfo *ninfo) {
  NumericRangeNode *currNode = ninfo->node;
  InvIdxBuffers *idxbufs = &ninfo->idxbufs;
  MSG_IndexInfo *info = &ninfo->info;
  FGC_applyInvertedIndex(gc, idxbufs, info, currNode->range->entries);

  currNode->range->invertedIndexSize -= info->nbytesCollected;
  FGC_updateStats(sctx, gc, info->ndocsCollected, info->nbytesCollected);

  resetCardinality(ninfo, currNode);
}

static FGCError FGC_parentHandleNumeric(ForkGC *gc, RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName = NULL;
  uint64_t rtUniqueId;
  FGCError status = recvNumericTagHeader(gc, &fieldName, &fieldNameLen, &rtUniqueId);
  if (status == FGC_DONE) {
    return FGC_DONE;
  }

  while (status == FGC_COLLECTED) {
    NumGcInfo ninfo = {0};
    RedisSearchCtx *sctx = NULL;
    RedisModuleKey *idxKey = NULL;
    FGCError status2 = recvNumIdx(gc, &ninfo);
    if (status2 == FGC_DONE) {
      break;
    } else if (status2 != FGC_COLLECTED) {
      status = status2;
      break;
    }

    if (!FGC_lock(gc, rctx)) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    hasLock = 1;
    sctx = FGC_getSctx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }
    RedisModuleString *keyName =
        IndexSpec_GetFormattedKeyByName(sctx->spec, fieldName, INDEXFLD_T_NUMERIC);
    NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

    if (rt->uniqueId != rtUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    if (!ninfo.node->range) {
      gc->stats.gcNumericNodesMissed++;
      goto loop_cleanup;
    }

    applyNumIdx(gc, sctx, &ninfo);

  loop_cleanup:
    if (sctx) {
      SearchCtx_Free(sctx);
    }
    if (status != FGC_COLLECTED) {
      freeInvIdx(&ninfo.idxbufs, &ninfo.info);
    } else {
      rm_free(ninfo.idxbufs.changedBlocks);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    if (hasLock) {
      FGC_unlock(gc, rctx);
      hasLock = 0;
    }

    rm_free(ninfo.restBlockDeleted);
    rm_free(ninfo.lastBlockDeleted);
  }

  rm_free(fieldName);
  return status;
}

static FGCError FGC_parentHandleTags(ForkGC *gc, RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName;
  uint64_t tagUniqueId;
  InvertedIndex *value = NULL;
  FGCError status = recvNumericTagHeader(gc, &fieldName, &fieldNameLen, &tagUniqueId);

  while (status == FGC_COLLECTED) {
    RedisModuleString *keyName = NULL;
    RedisModuleKey *idxKey = NULL;
    RedisSearchCtx *sctx = NULL;
    MSG_IndexInfo info = {0};
    InvIdxBuffers idxbufs = {0};
    TagIndex *tagIdx = NULL;

    if (FGC_recvFixed(gc, &value, sizeof value) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    if (value == NULL) {
      RS_LOG_ASSERT(status == FGC_COLLECTED, "GC status is COLLECTED");
      break;
    }

    if (FGC_recvInvIdx(gc, &idxbufs, &info) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    if (!FGC_lock(gc, rctx)) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    hasLock = 1;
    sctx = FGC_getSctx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }
    keyName = IndexSpec_GetFormattedKeyByName(sctx->spec, fieldName, INDEXFLD_T_TAG);
    tagIdx = TagIndex_Open(sctx, keyName, false, &idxKey);

    if (tagIdx->uniqueId != tagUniqueId) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    FGC_applyInvertedIndex(gc, &idxbufs, &info, value);
    FGC_updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

  loop_cleanup:
    if (sctx) {
      SearchCtx_Free(sctx);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    if (hasLock) {
      FGC_unlock(gc, rctx);
      hasLock = 0;
    }
    if (status != FGC_COLLECTED) {
      freeInvIdx(&idxbufs, &info);
    } else {
      rm_free(idxbufs.changedBlocks);
    }
  }

  rm_free(fieldName);
  return status;
}

int FGC_parentHandleFromChild(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;

#define COLLECT_FROM_CHILD(e)               \
  while ((status = (e)) == FGC_COLLECTED) { \
  }                                         \
  if (status != FGC_DONE) {                 \
    return REDISMODULE_ERR;                 \
  }

  COLLECT_FROM_CHILD(FGC_parentHandleTerms(gc, gc->ctx));
  COLLECT_FROM_CHILD(FGC_parentHandleNumeric(gc, gc->ctx));
  COLLECT_FROM_CHILD(FGC_parentHandleTags(gc, gc->ctx));
  return REDISMODULE_OK;
}

/**
 * In future versions of Redis, Redis will have its own fork() call.
 * The following two functions wrap this functionality.
 */
static int FGC_haveRedisFork() {
  return RedisModule_Fork != NULL;
}

static int FGC_fork(ForkGC *gc, RedisModuleCtx *ctx) {
  if (FGC_haveRedisFork()) {
    int ret = RedisModule_Fork(NULL, NULL);
    return ret;
  } else {
    return fork();
  }
}

static int periodicCb(RedisModuleCtx *ctx, void *privdata) {
  ForkGC *gc = privdata;
  if (gc->deleting) {
    return 0;
  }
  if (gc->deletedDocsFromLastRun < RSGlobalConfig.forkGcCleanThreshold) {
    return 1;
  }

  int gcrv = 1;

  RedisModule_AutoMemory(ctx);

  // Check if RDB is loading - not needed after the first time we find out that rdb is not
  // reloading
  if (gc->rdbPossiblyLoading && !gc->sp) {
    RedisModule_ThreadSafeContextLock(ctx);
    if (isRdbLoading(ctx)) {
      RedisModule_Log(ctx, "notice", "RDB Loading in progress, not performing GC");
      RedisModule_ThreadSafeContextUnlock(ctx);
      return 1;
    } else {
      // the RDB will not load again, so it's safe to ignore the info check in the next cycles
      gc->rdbPossiblyLoading = 0;
    }
    RedisModule_ThreadSafeContextUnlock(ctx);
  }

  pid_t cpid;
  TimeSample ts;

  while (gc->pauseState == FGC_PAUSED_CHILD) {
    gc->execState = FGC_STATE_WAIT_FORK;
    // spin or sleep
    usleep(500);
  }

  pid_t ppid_before_fork = getpid();

  TimeSampler_Start(&ts);
  pipe(gc->pipefd);  // create the pipe

  if (gc->type == FGC_TYPE_NOKEYSPACE) {
    // If we are not in key space we still need to acquire the GIL to use the fork api
    RedisModule_ThreadSafeContextLock(ctx);
  }

  if (!FGC_lock(gc, ctx)) {

    if (gc->type == FGC_TYPE_NOKEYSPACE) {
      RedisModule_ThreadSafeContextUnlock(ctx);
    }

    close(gc->pipefd[GC_READERFD]);
    close(gc->pipefd[GC_WRITERFD]);

    return 0;
  }

  gc->execState = FGC_STATE_SCANNING;

  cpid = FGC_fork(gc, ctx);  // duplicate the current process

  if (cpid == -1) {
    gc->retryInterval.tv_sec = RSGlobalConfig.forkGcRetryInterval;

    if (gc->type == FGC_TYPE_NOKEYSPACE) {
      RedisModule_ThreadSafeContextUnlock(ctx);
    }

    FGC_unlock(gc, ctx);

    close(gc->pipefd[GC_READERFD]);
    close(gc->pipefd[GC_WRITERFD]);

    return 1;
  }

  gc->deletedDocsFromLastRun = 0;

  if (gc->type == FGC_TYPE_NOKEYSPACE) {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }

  FGC_unlock(gc, ctx);

  gc->retryInterval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;

  if (cpid == 0) {
    setpriority(PRIO_PROCESS, getpid(), 19);
    // fork process
    close(gc->pipefd[GC_READERFD]);
#ifdef __linux__
    if (!FGC_haveRedisFork()) {
      // set the parrent death signal to SIGTERM
      int r = prctl(PR_SET_PDEATHSIG, SIGKILL);
      if (r == -1) {
        exit(1);
      }
      // test in case the original parent exited just
      // before the prctl() call
      if (getppid() != ppid_before_fork) exit(1);
    }
#endif
    FGC_childScanIndexes(gc);
    close(gc->pipefd[GC_WRITERFD]);
    sleep(RSGlobalConfig.forkGcSleepBeforeExit);
    _exit(EXIT_SUCCESS);
  } else {
    // main process
    close(gc->pipefd[GC_WRITERFD]);
    while (gc->pauseState == FGC_PAUSED_PARENT) {
      gc->execState = FGC_STATE_WAIT_APPLY;
      // spin
      usleep(500);
    }

    gc->execState = FGC_STATE_APPLYING;
    if (FGC_parentHandleFromChild(gc) == REDISMODULE_ERR) {
      gcrv = 1;
    }
    close(gc->pipefd[GC_READERFD]);
    if (FGC_haveRedisFork()) {

      if (gc->type == FGC_TYPE_NOKEYSPACE) {
        // If we are not in key space we still need to acquire the GIL to use the fork api
        RedisModule_ThreadSafeContextLock(ctx);
      }

      if (!FGC_lock(gc, ctx)) {
        if (gc->type == FGC_TYPE_NOKEYSPACE) {
          RedisModule_ThreadSafeContextUnlock(ctx);
        }

        return 0;
      }

      // KillForkChild must be called when holding the GIL
      // otherwise it might cause a pipe leak and eventually run
      // out of file descriptor
      RedisModule_KillForkChild(cpid);

      if (gc->type == FGC_TYPE_NOKEYSPACE) {
        RedisModule_ThreadSafeContextUnlock(ctx);
      }

      FGC_unlock(gc, ctx);

    } else {
      pid_t id = wait4(cpid, NULL, 0, NULL);
      if (id == -1) {
        printf("an error acquire when waiting for fork to terminate, pid:%d", cpid);
      }
    }
  }
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

void FGC_WaitAtFork(ForkGC *gc) NO_TSAN_CHECK {
  RS_LOG_ASSERT(gc->pauseState == 0, "FGC pause state should be 0");
  gc->pauseState = FGC_PAUSED_CHILD;

  while (gc->execState != FGC_STATE_WAIT_FORK) {
    usleep(500);
  }
}

void FGC_WaitAtApply(ForkGC *gc) NO_TSAN_CHECK {
  // Ensure that we're waiting for the child to begin
  RS_LOG_ASSERT(gc->pauseState == FGC_PAUSED_CHILD, "FGC pause state should be CHILD");
  RS_LOG_ASSERT(gc->execState == FGC_STATE_WAIT_FORK, "FGC exec state should be WAIT_FORK");

  gc->pauseState = FGC_PAUSED_PARENT;
  while (gc->execState != FGC_STATE_WAIT_APPLY) {
    usleep(500);
  }
}

void FGC_WaitClear(ForkGC *gc) NO_TSAN_CHECK {
  gc->pauseState = FGC_PAUSED_UNPAUSED;
  while (gc->execState != FGC_STATE_IDLE) {
    usleep(500);
  }
}

static void onTerminateCb(void *privdata) {
  ForkGC *gc = privdata;
  if (gc->keyName && gc->type == FGC_TYPE_INKEYSPACE) {
    RedisModule_FreeString(gc->ctx, (RedisModuleString *)gc->keyName);
  }

  RedisModule_FreeThreadSafeContext(gc->ctx);
  rm_free(gc);
}

static void statsCb(RedisModuleCtx *ctx, void *gcCtx) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2
  ForkGC *gc = gcCtx;

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (gc) {
    REPLY_KVNUM(n, "bytes_collected", gc->stats.totalCollected);
    REPLY_KVNUM(n, "total_ms_run", gc->stats.totalMSRun);
    REPLY_KVNUM(n, "total_cycles", gc->stats.numCycles);
    REPLY_KVNUM(n, "average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
    REPLY_KVNUM(n, "last_run_time_ms", (double)gc->stats.lastRunTimeMs);
    REPLY_KVNUM(n, "gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
    REPLY_KVNUM(n, "gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
  }
  RedisModule_ReplySetArrayLength(ctx, n);
}

static void killCb(void *ctx) {
  ForkGC *gc = ctx;
  gc->deleting = 1;
}

static void deleteCb(void *ctx) {
  ForkGC *gc = ctx;
  ++gc->deletedDocsFromLastRun;
}

static struct timespec getIntervalCb(void *ctx) {
  ForkGC *gc = ctx;
  return gc->retryInterval;
}

ForkGC *FGC_New(const RedisModuleString *k, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGC *forkGc = rm_calloc(1, sizeof(*forkGc));
  *forkGc = (ForkGC){
      .rdbPossiblyLoading = 1,
      .specUniqueId = specUniqueId,
      .type = FGC_TYPE_INKEYSPACE,
      .deletedDocsFromLastRun = 0,
  };
  forkGc->retryInterval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;
  forkGc->retryInterval.tv_nsec = 0;
  forkGc->ctx = RedisModule_GetThreadSafeContext(NULL);
  if (k) {
    forkGc->keyName = RedisModule_CreateStringFromString(forkGc->ctx, k);
    RedisModule_FreeString(forkGc->ctx, (RedisModuleString *)k);
  }

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->kill = killCb;
  callbacks->onDelete = deleteCb;

  return forkGc;
}

ForkGC *FGC_NewFromSpec(IndexSpec *sp, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGC *ctx = FGC_New(NULL, specUniqueId, callbacks);
  ctx->sp = sp;
  ctx->type = FGC_TYPE_NOKEYSPACE;
  return ctx;
}
