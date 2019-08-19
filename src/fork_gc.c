#include "fork_gc.h"
#include "util/arr.h"
#include "search_ctx.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "tests/time_sample.h"
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include "rwlock.h"

#define GC_WRITERFD 1
#define GC_READERFD 0

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

static void FGC_sendLongLong(ForkGC *fgc, long long val) {
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], &val, sizeof(long long));
  assert(size == sizeof(long long));
}

static void FGC_sendPtrAddr(ForkGC *fgc, const void *val) {
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], &val, sizeof(void *));
  assert(size == sizeof(void *));
}

static void FGC_sendFixed(ForkGC *fgc, const void *buff, size_t len) {
  assert(len > 0);
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], buff, len);
  assert(size == len);
}

static void FGC_sendBuffer(ForkGC *fgc, const void *buff, size_t len) {
  FGC_sendLongLong(fgc, len);
  if (len > 0) {
    FGC_sendFixed(fgc, buff, len);
  }
}

static long long FGC_recvLongLong(ForkGC *fgc) {
  long long ret;
  ssize_t sizeRead = read(fgc->pipefd[GC_READERFD], &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void *FGC_recvPtrAddr(ForkGC *fgc) {
  void *ret;
  ssize_t sizeRead = read(fgc->pipefd[GC_READERFD], &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void FGC_recvFixed(ForkGC *fgc, void *buf, size_t len) {
  ssize_t nrecvd = read(fgc->pipefd[GC_READERFD], buf, len);
  if (nrecvd != len) {
    printf("warning: got a bad length when writing to pipe.\r\n");
  }
}

static void *FGC_recvBuffer(ForkGC *fgc, size_t *len) {
  *len = FGC_recvLongLong(fgc);
  if (*len == 0) {
    return NULL;
  }
  char *buff = rm_malloc(*len);
  FGC_recvFixed(fgc, buff, *len);
  return buff;
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

static bool FGC_childRepairInvidx(ForkGC *gc, RedisSearchCtx *sctx, InvertedIndex *idx,
                                  void (*RepairCallback)(const RSIndexResult *, void *),
                                  void *arg) {
  MSG_RepairedBlock *fixed = array_new(MSG_RepairedBlock, 10);
  MSG_DeletedBlock *deleted = array_new(MSG_DeletedBlock, 10);
  IndexBlock *blocklist = array_new(IndexBlock, idx->size);
  MSG_IndexInfo ixmsg = {.nblocksOrig = idx->size};
  bool rv = false;

  for (size_t i = 0; i < idx->size; ++i) {
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
    IndexRepairParams params = {.RepairCallback = RepairCallback, .arg = arg};
    int nrepaired = IndexBlock_Repair(blk, &sctx->spec->docs, idx->flags, &params);
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

    ixmsg.nbytesCollected += params.bytesCollected;
    ixmsg.ndocsCollected += nrepaired;
    if (i == idx->size - 1) {
      ixmsg.lastblkBytesCollected = params.bytesCollected;
      ixmsg.lastblkDocsRemoved = nrepaired;
      ixmsg.lastblkNumDocs = blk->numDocs + nrepaired;
    }
  }

  if (array_len(fixed) == 0 && array_len(deleted) == 0) {
    // No blocks were removed or repaired
    FGC_sendLongLong(gc, 0);
    goto done;
  }

  FGC_sendLongLong(gc, 1);  // indicating we have repaired blocks

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
      // inverted index name
      FGC_sendBuffer(gc, term, termLen);

      FGC_childRepairInvidx(gc, sctx, idx, NULL, NULL);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    free(term);
  }
  DFAFilter_Free(iter->ctx);
  free(iter->ctx);
  TrieIterator_Free(iter);

  // we are done with terms
  FGC_sendBuffer(gc, "\0", 1);
}

static void countDeletedCardinality(const RSIndexResult *r, void *arg) {
  CardinalityValue *valuesDeleted = arg;
  for (int i = 0; i < array_len(valuesDeleted); ++i) {
    if (valuesDeleted[i].value == r->num.value) {
      valuesDeleted[i].appearances++;
      return;
    }
  }
}

static void FGC_childCollectNumeric(ForkGC *gc, RedisSearchCtx *sctx) {
  RedisModuleKey *idxKey = NULL;
  FieldSpec **numericFields = getFieldsByType(sctx->spec, INDEXFLD_T_NUMERIC);

  if (array_len(numericFields) != 0) {
    for (int i = 0; i < array_len(numericFields); ++i) {
      RedisModuleString *keyName =
          IndexSpec_GetFormattedKey(sctx->spec, numericFields[i], INDEXFLD_T_NUMERIC);
      NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

      NumericRangeTreeIterator *gcIterator = NumericRangeTreeIterator_New(rt);
      NumericRangeNode *currNode = NULL;

      // numeric field name
      FGC_sendBuffer(gc, numericFields[i]->name, strlen(numericFields[i]->name) + 1);
      // numeric field unique id
      FGC_sendLongLong(gc, rt->uniqueId);

      while ((currNode = NumericRangeTreeIterator_Next(gcIterator))) {
        if (!currNode->range) {
          continue;
        }

        CardinalityValue *valuesDeleted = array_new(CardinalityValue, currNode->range->card);
        for (int i = 0; i < currNode->range->card; ++i) {
          CardinalityValue valueDeleted;
          valueDeleted.value = currNode->range->values[i].value;
          valueDeleted.appearances = 0;
          valuesDeleted = array_append(valuesDeleted, valueDeleted);
        }
        // write node pointer
        FGC_sendPtrAddr(gc, currNode);

        bool repaired = FGC_childRepairInvidx(gc, sctx, currNode->range->entries,
                                              countDeletedCardinality, valuesDeleted);

        if (repaired) {
          // send reduced cardinality size
          FGC_sendLongLong(gc, currNode->range->card);

          // send reduced cardinality
          for (int i = 0; i < currNode->range->card; ++i) {
            FGC_sendLongLong(gc, valuesDeleted[i].appearances);
          }
        }
        array_free(valuesDeleted);
      }

      // we are done with the current field
      FGC_sendPtrAddr(gc, 0);

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      NumericRangeTreeIterator_Free(gcIterator);
    }
  }

  // we are done with numeric fields
  FGC_sendBuffer(gc, "\0", 1);
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

      // tag field name
      FGC_sendBuffer(gc, tagFields[i]->name, strlen(tagFields[i]->name) + 1);
      // numeric field unique id
      FGC_sendLongLong(gc, tagIdx->uniqueId);

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values, "", 0);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        // send inverted index pointer
        FGC_sendPtrAddr(gc, value);
        // send repaired data
        FGC_childRepairInvidx(gc, sctx, value, NULL, NULL);
      }

      // we are done with the current field
      FGC_sendPtrAddr(gc, 0);

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }
    }
  }
  // we are done with numeric fields
  FGC_sendBuffer(gc, "\0", 1);
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
} InvIdxBuffers;

static void FGC_recvRepairedBlock(ForkGC *gc, MSG_RepairedBlock *binfo) {
  FGC_recvFixed(gc, binfo, sizeof(*binfo));
  Buffer *b = &binfo->blk.buf;
  b->data = FGC_recvBuffer(gc, &b->offset);
  b->cap = b->offset;
}

static bool FGC_recvInvIdx(ForkGC *gc, InvIdxBuffers *bufs, MSG_IndexInfo *info) {
  long long wasRepaired = FGC_recvLongLong(gc);
  if (!wasRepaired) {
    return false;
  }
  FGC_recvFixed(gc, info, sizeof(*info));
  bufs->newBlocklist = FGC_recvBuffer(gc, &bufs->newBlocklistSize);
  if (bufs->newBlocklistSize) {
    bufs->newBlocklistSize /= sizeof(*bufs->newBlocklist);
  }

  bufs->delBlocks = FGC_recvBuffer(gc, &bufs->numDelBlocks);
  bufs->numDelBlocks /= sizeof(*bufs->delBlocks);
  bufs->changedBlocks = rm_malloc(sizeof(*bufs->changedBlocks) * info->nblocksRepaired);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    FGC_recvRepairedBlock(gc, bufs->changedBlocks + i);
  }
  return true;
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
    MSG_DeletedBlock *db = idxData->delBlocks + idxData->numDelBlocks - 1;
    idxData->numDelBlocks--;
    idxData->newBlocklistSize++;
    idxData->newBlocklist = rm_realloc(idxData->newBlocklist,
                                       sizeof(*idxData->newBlocklist) * idxData->newBlocklistSize);
    idxData->newBlocklist[idxData->newBlocklistSize - 1] = *lastOld;
  } else {
    MSG_RepairedBlock *rb = idxData->changedBlocks + info->nblocksRepaired - 1;
    indexBlock_Free(&rb->blk);
    info->nblocksRepaired--;
  }

  info->ndocsCollected -= info->lastblkDocsRemoved;
  info->nbytesCollected -= info->lastblkBytesCollected;
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
  assert(idx->size >= info->nblocksOrig);

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
}

static bool FGC_parentHandleTerms(ForkGC *gc, int *ret_val, RedisModuleCtx *rctx) {
  size_t len;
  int hasLock = 0;
  char *term = FGC_recvBuffer(gc, &len);
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;

  if (term == NULL || term[0] == '\0') {
    if (term) {
      rm_free(term);
    }
    return false;
  }

  InvIdxBuffers idxbufs = {0};
  MSG_IndexInfo info = {0};
  if (!FGC_recvInvIdx(gc, &idxbufs, &info)) {
    rm_free(term);
    return true;
  }

  if (!FGC_lock(gc, rctx)) {
    *ret_val = 0;
    goto cleanup;
  }

  hasLock = 1;
  sctx = FGC_getSctx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    *ret_val = 0;
    goto cleanup;
  }

  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, len, 1, &idxKey);

  if (idx == NULL) {
    *ret_val = 0;
    goto cleanup;
  }

  FGC_applyInvertedIndex(gc, &idxbufs, &info, idx);
  FGC_updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

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
  rm_free(idxbufs.changedBlocks);
  return true;
}

// performs cleanup and return
#define RETURN         \
  *ret_val = 0;        \
  shouldReturn = true; \
  goto loop_cleanup;
// performs cleanup and continue with the loop
#define CONTINUE goto loop_cleanup;

static bool FGC_parentHandleNumeric(ForkGC *gc, int *ret_val, RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName = FGC_recvBuffer(gc, &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t rtUniqueId = FGC_recvLongLong(gc);

  NumericRangeNode *currNode = NULL;
  bool shouldReturn = false;
  RedisModuleString *keyName = NULL;
  while ((currNode = FGC_recvPtrAddr(gc))) {
    RedisSearchCtx *sctx = NULL;
    MSG_IndexInfo info = {0};
    InvIdxBuffers idxbufs = {0};
    if (!FGC_recvInvIdx(gc, &idxbufs, &info)) {
      continue;
    }

    // read reduced cardinality size
    long long reduceCardinalitySize = FGC_recvLongLong(gc);
    long long valuesDeleted[reduceCardinalitySize];

    // read reduced cardinality
    for (int i = 0; i < reduceCardinalitySize; ++i) {
      valuesDeleted[i] = FGC_recvLongLong(gc);
    }

    if (!FGC_lock(gc, rctx)) {
      RETURN;
    }
    hasLock = 1;

    sctx = FGC_getSctx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      RETURN;
    }

    keyName = fmtRedisNumericIndexKey(sctx, fieldName);
    RedisModuleKey *idxKey = NULL;
    NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

    if (rt->uniqueId != rtUniqueId) {
      RETURN;
    }

    if (!currNode->range) {
      gc->stats.gcNumericNodesMissed++;
      CONTINUE;
    }

    FGC_applyInvertedIndex(gc, &idxbufs, &info, currNode->range->entries);

    FGC_updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

    // fixing cardinality
    uint16_t newCard = 0;
    CardinalityValue *newCardValues = array_new(CardinalityValue, currNode->range->splitCard);
    for (int i = 0; i < array_len(currNode->range->values); ++i) {
      int appearances = currNode->range->values[i].appearances;
      if (i < reduceCardinalitySize) {
        appearances -= valuesDeleted[i];
      }
      if (appearances > 0) {
        CardinalityValue val;
        val.value = currNode->range->values[i].value;
        val.appearances = appearances;
        newCardValues = array_append(newCardValues, val);
        ++newCard;
      }
    }
    array_free(currNode->range->values);
    newCardValues = array_trimm_cap(newCardValues, newCard);
    currNode->range->values = newCardValues;
    currNode->range->card = newCard;

  loop_cleanup:
    if (sctx) {
      SearchCtx_Free(sctx);
    }
    rm_free(idxbufs.changedBlocks);
    if (keyName) {
      RedisModule_FreeString(rctx, keyName);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    if (hasLock) {
      FGC_unlock(gc, rctx);
      hasLock = 0;
    }
    if (shouldReturn) {
      if (fieldName) {
        rm_free(fieldName);
      }
      return false;
    }
  }

  if (fieldName) {
    rm_free(fieldName);
  }
  return true;
}

static bool FGC_parentHandleTags(ForkGC *gc, int *ret_val, RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName = FGC_recvBuffer(gc, &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t tagUniqueId = FGC_recvLongLong(gc);
  bool shouldReturn = false;
  InvertedIndex *value = NULL;
  RedisModuleString *keyName = NULL;

  while ((value = FGC_recvPtrAddr(gc))) {
    RedisModuleKey *idxKey = NULL;
    RedisSearchCtx *sctx = NULL;
    MSG_IndexInfo info = {0};
    InvIdxBuffers idxbufs = {0};
    TagIndex *tagIdx = NULL;

    if (!FGC_recvInvIdx(gc, &idxbufs, &info)) {
      continue;
    }

    if (!FGC_lock(gc, rctx)) {
      RETURN;
    }
    hasLock = 1;
    sctx = FGC_getSctx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      RETURN;
    }

    keyName = TagIndex_FormatName(sctx, fieldName);
    tagIdx = TagIndex_Open(sctx, keyName, false, &idxKey);

    if (tagIdx->uniqueId != tagUniqueId) {
      RETURN;
    }

    FGC_applyInvertedIndex(gc, &idxbufs, &info, value);
    FGC_updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

  loop_cleanup:
    if (sctx) {
      SearchCtx_Free(sctx);
    }
    rm_free(idxbufs.changedBlocks);
    if (keyName) {
      RedisModule_FreeString(rctx, keyName);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    if (hasLock) {
      FGC_unlock(gc, rctx);
      hasLock = 0;
    }
    if (shouldReturn) {
      if (fieldName) {
        rm_free(fieldName);
      }
      return false;
    }
  }

  if (fieldName) {
    rm_free(fieldName);
  }
  return true;
}

void FGC_parentHandleFromChild(ForkGC *gc, int *ret_val) {
  while (FGC_parentHandleTerms(gc, ret_val, gc->ctx))
    ;

  if (!(*ret_val)) {
    goto done;
  }

  while (FGC_parentHandleNumeric(gc, ret_val, gc->ctx))
    ;

  if (!(*ret_val)) {
    goto done;
  }

  while (FGC_parentHandleTags(gc, ret_val, gc->ctx))
    ;

done:;
}

static int periodicCb(RedisModuleCtx *ctx, void *privdata) {
  ForkGC *gc = privdata;
  if (gc->deleting) {
    return 0;
  }

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

  int ret_val = 1;

  while (gc->pauseState == FGC_PAUSED_CHILD) {
    gc->execState = FGC_STATE_WAIT_FORK;
    // spin or sleep
    usleep(500);
  }

  TimeSampler_Start(&ts);
  pipe(gc->pipefd);  // create the pipe
  if (!FGC_lock(gc, ctx)) {
    return 0;
  }

  gc->execState = FGC_STATE_SCANNING;
  cpid = fork();  // duplicate the current process
  FGC_unlock(gc, ctx);
  if (cpid == 0) {
    // fork process
    close(gc->pipefd[GC_READERFD]);
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
    FGC_parentHandleFromChild(gc, &ret_val);
    close(gc->pipefd[GC_READERFD]);
    pid_t id = wait4(cpid, NULL, 0, NULL);
    if (id == -1) {
      printf("an error acquire when waiting for fork to terminate, pid:%d", cpid);
    }
  }
  gc->execState = FGC_STATE_IDLE;
  TimeSampler_End(&ts);

  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return ret_val;
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
  assert(gc->pauseState == 0);
  gc->pauseState = FGC_PAUSED_CHILD;

  while (gc->execState != FGC_STATE_WAIT_FORK) {
    usleep(500);
  }
}

void FGC_WaitAtApply(ForkGC *gc) NO_TSAN_CHECK {
  // Ensure that we're waiting for the child to begin
  assert(gc->pauseState == FGC_PAUSED_CHILD);
  assert(gc->execState == FGC_STATE_WAIT_FORK);

  gc->pauseState = FGC_PAUSED_PARENT;
  while (gc->execState != FGC_STATE_WAIT_APPLY) {
    usleep(500);
  }
}

void FGC_WaitClear(ForkGC *gc) NO_TSAN_CHECK {
  gc->pauseState = 0;
  while (gc->execState != FGC_STATE_IDLE) {
    usleep(500);
  }
}

static void onTerminateCb(void *privdata) {
  ForkGC *gc = privdata;
  if (gc->keyName && gc->type == FGC_TYPE_INKEYSPACE) {
    RedisModule_ThreadSafeContextLock(gc->ctx);
    RedisModule_FreeString(gc->ctx, (RedisModuleString *)gc->keyName);
    RedisModule_ThreadSafeContextUnlock(gc->ctx);
  }

  RedisModule_FreeThreadSafeContext(gc->ctx);
  free(gc);
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
    REPLY_KVNUM(n, "avarage_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
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

static struct timespec getIntervalCb(void *ctx) {
  struct timespec interval;
  interval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;
  interval.tv_nsec = 0;
  return interval;
}

ForkGC *FGC_New(const RedisModuleString *k, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGC *forkGc = calloc(1, sizeof(*forkGc));
  *forkGc = (ForkGC){
      .rdbPossiblyLoading = 1,
      .specUniqueId = specUniqueId,
      .type = FGC_TYPE_INKEYSPACE,
  };
  forkGc->ctx = RedisModule_GetThreadSafeContext(NULL);
  if (k) {
    forkGc->keyName = RedisModule_CreateStringFromString(forkGc->ctx, k);
    RedisModule_FreeString(forkGc->ctx, k);
  }

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->kill = killCb;

  return forkGc;
}

ForkGC *FGC_NewFromSpec(IndexSpec *sp, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGC *ctx = FGC_New(NULL, specUniqueId, callbacks);
  ctx->sp = sp;
  ctx->type = FGC_TYPE_NOKEYSPACE;
  return ctx;
}
