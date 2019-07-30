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

// if return false, we should abort.
static void FGC_Lock(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    RWLOCK_ACQUIRE_WRITE();
  } else {
    RedisModule_ThreadSafeContextLock(ctx);
  }
}

static void FGC_Unlock(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    RWLOCK_RELEASE();
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

static RedisSearchCtx *getSctx(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = NULL;
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    sctx = rm_malloc(sizeof(*sctx));
    *sctx = (RedisSearchCtx)SEARCH_CTX_STATIC(ctx, gc->sp);
  } else if (gc->type == ForkGCCtxType_IN_KEYSPACE) {
    sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName, false);
  }
  return sctx;
}

static void updateStats(RedisSearchCtx *sctx, ForkGCCtx *gc, size_t recordsRemoved,
                        size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
}

static void FGC_SendLongLong(ForkGCCtx *fgc, long long val) {
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], &val, sizeof(long long));
  assert(size == sizeof(long long));
}

static void FGC_SendPtrAddr(ForkGCCtx *fgc, const void *val) {
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], &val, sizeof(void *));
  assert(size == sizeof(void *));
}

static void FGC_SendFixed(ForkGCCtx *fgc, const void *buff, size_t len) {
  assert(len > 0);
  ssize_t size = write(fgc->pipefd[GC_WRITERFD], buff, len);
  assert(size == len);
}

static void FGC_SendBuffer(ForkGCCtx *fgc, const void *buff, size_t len) {
  FGC_SendLongLong(fgc, len);
  if (len > 0) {
    FGC_SendFixed(fgc, buff, len);
  }
}

static long long FGC_RecvLongLong(ForkGCCtx *fgc) {
  long long ret;
  ssize_t sizeRead = read(fgc->pipefd[GC_READERFD], &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void *FGC_RecvPtrAddr(ForkGCCtx *fgc) {
  void *ret;
  ssize_t sizeRead = read(fgc->pipefd[GC_READERFD], &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void FGC_RecvFixed(ForkGCCtx *fgc, void *buf, size_t len) {
  ssize_t nrecvd = read(fgc->pipefd[GC_READERFD], buf, len);
  if (nrecvd != len) {
    printf("warning: got a bad length when writing to pipe.\r\n");
  }
}

static void *FGC_RecvBuffer(ForkGCCtx *fgc, size_t *len) {
  *len = FGC_RecvLongLong(fgc);
  if (*len == 0) {
    return NULL;
  }
  char *buff = rm_malloc(*len);
  FGC_RecvFixed(fgc, buff, *len);
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

/**
 * Structure sent describing an index block
 */
typedef struct {
  IndexBlock blk;
  int64_t oldix;  // Old position of the block
  int64_t newix;  // New position of the block
  // the actual content of the block follows...
} MSG_RepairedBlock;

typedef struct {
  uint32_t oldix;  // Old index of deleted block
  void *ptr;       // Address of the buffer to free
} MSG_DeletedBlock;

static bool ForkGc_InvertedIndexRepair(ForkGCCtx *gc, RedisSearchCtx *sctx, InvertedIndex *idx,
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
      delmsg->ptr = blk->buf.data;
      delmsg->oldix = i;
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
    FGC_SendLongLong(gc, 0);
    goto done;
  }

  FGC_SendLongLong(gc, 1);  // indicating we have repaired blocks

  FGC_SendFixed(gc, &ixmsg, sizeof ixmsg);

  if (array_len(blocklist) == idx->size) {
    // no empty block, there is no need to send the blocks array. Don't send
    // any new blocks
    FGC_SendBuffer(gc, NULL, 0);
  } else {
    FGC_SendBuffer(gc, blocklist, array_len(blocklist) * sizeof(*blocklist));
  }

  FGC_SendBuffer(gc, deleted, array_len(deleted) * sizeof(*deleted));

  for (size_t i = 0; i < array_len(fixed); ++i) {
    // write fix block
    const MSG_RepairedBlock *msg = fixed + i;
    const IndexBlock *blk = blocklist + msg->newix;
    FGC_SendFixed(gc, msg, sizeof(*msg));
    FGC_SendBuffer(gc, IndexBlock_DataBuf(blk), IndexBlock_DataLen(blk));
  }
  rv = true;

done:
  array_free(fixed);
  array_free(blocklist);
  array_free(deleted);
  return rv;
}

static void FGC_CollectTerms(ForkGCCtx *gc, RedisSearchCtx *sctx) {
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
      FGC_SendBuffer(gc, term, termLen);

      ForkGc_InvertedIndexRepair(gc, sctx, idx, NULL, NULL);
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
  FGC_SendBuffer(gc, "\0", 1);
}

static void ForkGc_CountDeletedCardinality(const RSIndexResult *r, void *arg) {
  CardinalityValue *valuesDeleted = arg;
  for (int i = 0; i < array_len(valuesDeleted); ++i) {
    if (valuesDeleted[i].value == r->num.value) {
      valuesDeleted[i].appearances++;
      return;
    }
  }
}

static void FGC_CollectNumeric(ForkGCCtx *gc, RedisSearchCtx *sctx) {
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
      FGC_SendBuffer(gc, numericFields[i]->name, strlen(numericFields[i]->name) + 1);
      // numeric field unique id
      FGC_SendLongLong(gc, rt->uniqueId);

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
        FGC_SendPtrAddr(gc, currNode);

        bool repaired = ForkGc_InvertedIndexRepair(gc, sctx, currNode->range->entries,
                                                   ForkGc_CountDeletedCardinality, valuesDeleted);

        if (repaired) {
          // send reduced cardinality size
          FGC_SendLongLong(gc, currNode->range->card);

          // send reduced cardinality
          for (int i = 0; i < currNode->range->card; ++i) {
            FGC_SendLongLong(gc, valuesDeleted[i].appearances);
          }
        }
        array_free(valuesDeleted);
      }

      // we are done with the current field
      FGC_SendPtrAddr(gc, 0);

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      NumericRangeTreeIterator_Free(gcIterator);
    }
  }

  // we are done with numeric fields
  FGC_SendBuffer(gc, "\0", 1);
}

static void FGC_CollectTags(ForkGCCtx *gc, RedisSearchCtx *sctx) {
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
      FGC_SendBuffer(gc, tagFields[i]->name, strlen(tagFields[i]->name) + 1);
      // numeric field unique id
      FGC_SendLongLong(gc, tagIdx->uniqueId);

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values, "", 0);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        // send inverted index pointer
        FGC_SendPtrAddr(gc, value);
        // send repaired data
        ForkGc_InvertedIndexRepair(gc, sctx, value, NULL, NULL);
      }

      // we are done with the current field
      FGC_SendPtrAddr(gc, 0);

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }
    }
  }
  // we are done with numeric fields
  FGC_SendBuffer(gc, "\0", 1);
}

static void ForkGc_CollectGarbage(ForkGCCtx *gc) {
  RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx *sctx = getSctx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    // write log here
    RedisModule_FreeThreadSafeContext(rctx);
    return;
  }

  FGC_CollectTerms(gc, sctx);
  FGC_CollectNumeric(gc, sctx);
  FGC_CollectTags(gc, sctx);

  SearchCtx_Free(sctx);
  RedisModule_FreeThreadSafeContext(rctx);
}

typedef struct {
  MSG_DeletedBlock *delBlocks;
  size_t numDelBlocks;

  MSG_RepairedBlock *changedBlocks;
  size_t numChangedBlocks;

  IndexBlock *newBlocklist;
  size_t newBlocklistSize;
} InvIdxBuffers;

static void FGC_RecvModifiedBlock(ForkGCCtx *gc, MSG_RepairedBlock *binfo) {
  FGC_RecvFixed(gc, binfo, sizeof(*binfo));
  Buffer *b = &binfo->blk.buf;
  b->data = FGC_RecvBuffer(gc, &b->offset);
  b->cap = b->offset;
}

static bool FGC_RecvInvIdx(ForkGCCtx *gc, InvIdxBuffers *bufs, MSG_IndexInfo *info) {
  long long wasRepaired = FGC_RecvLongLong(gc);
  if (!wasRepaired) {
    return false;
  }
  FGC_RecvFixed(gc, info, sizeof(*info));
  bufs->newBlocklist = FGC_RecvBuffer(gc, &bufs->newBlocklistSize);
  if (bufs->newBlocklistSize) {
    bufs->newBlocklistSize /= sizeof(*bufs->newBlocklist);
  }

  bufs->delBlocks = FGC_RecvBuffer(gc, &bufs->numDelBlocks);
  bufs->numDelBlocks /= sizeof(*bufs->delBlocks);
  bufs->changedBlocks = rm_malloc(sizeof(*bufs->changedBlocks) * info->nblocksRepaired);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    FGC_RecvModifiedBlock(gc, bufs->changedBlocks + i);
  }
  return true;
}

static void ForkGc_FixInvertedIndex(ForkGCCtx *gc, InvIdxBuffers *idxData, MSG_IndexInfo *info,
                                    InvertedIndex *idx) {
  size_t lastOldIdx = info->nblocksOrig - 1;
  IndexBlock *lastOld = idx->blocks + lastOldIdx;

  if (info->lastblkDocsRemoved && info->lastblkNumDocs != lastOld->numDocs) {
    if (info->lastblkDocsRemoved == info->lastblkNumDocs) {
      MSG_DeletedBlock *db = idxData->delBlocks + idxData->numDelBlocks - 1;
      idxData->numDelBlocks--;
      idxData->newBlocklistSize++;
      idxData->newBlocklist = rm_realloc(
          idxData->newBlocklist, sizeof(*idxData->newBlocklist) * idxData->newBlocklistSize);
      idxData->newBlocklist[idxData->newBlocklistSize - 1] = *lastOld;
    } else {
      MSG_RepairedBlock *rb = idxData->changedBlocks + idxData->numChangedBlocks - 1;
      indexBlock_Free(&rb->blk);
      idxData->numChangedBlocks--;
    }
    info->ndocsCollected -= info->lastblkDocsRemoved;
    info->nbytesCollected -= info->lastblkBytesCollected;
  }

  for (size_t i = 0; i < idxData->numChangedBlocks; ++i) {
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
  }

  for (size_t i = 0; i < idxData->numChangedBlocks; ++i) {
    MSG_RepairedBlock *blockModified = idxData->changedBlocks + i;
    idx->blocks[blockModified->newix] = blockModified->blk;
  }

  idx->numDocs -= info->ndocsCollected;
}

static bool ForkGc_ReadInvertedIndex(ForkGCCtx *gc, int *ret_val, RedisModuleCtx *rctx) {
  size_t len;
  char *term = FGC_RecvBuffer(gc, &len);
  if (term == NULL || term[0] == '\0') {
    if (term) {
      rm_free(term);
    }
    return false;
  }

  InvIdxBuffers idxbufs = {0};
  MSG_IndexInfo info = {0};
  if (!FGC_RecvInvIdx(gc, &idxbufs, &info)) {
    rm_free(term);
    return true;
  }

  FGC_Lock(gc, rctx);

  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;
  sctx = getSctx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    *ret_val = 0;
    goto cleanup;
  }

  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, len, 1, &idxKey);

  if (idx == NULL) {
    *ret_val = 0;
    goto cleanup;
  }

  ForkGc_FixInvertedIndex(gc, &idxbufs, &info, idx);
  updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

cleanup:

  if (idxKey) {
    RedisModule_CloseKey(idxKey);
  }
  if (sctx) {
    SearchCtx_Free(sctx);
  }
  FGC_Unlock(gc, rctx);
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

static bool ForkGc_ReadNumericInvertedIndex(ForkGCCtx *gc, int *ret_val, RedisModuleCtx *rctx) {
  size_t fieldNameLen;
  char *fieldName = FGC_RecvBuffer(gc, &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t rtUniqueId = FGC_RecvLongLong(gc);

  NumericRangeNode *currNode = NULL;
  bool shouldReturn = false;
  RedisModuleString *keyName = NULL;
  while ((currNode = FGC_RecvPtrAddr(gc))) {
    MSG_IndexInfo info = {0};
    InvIdxBuffers idxbufs = {0};
    if (!FGC_RecvInvIdx(gc, &idxbufs, &info)) {
      continue;
    }

    // read reduced cardinality size
    long long reduceCardinalitySize = FGC_RecvLongLong(gc);
    long long valuesDeleted[reduceCardinalitySize];

    // read reduced cardinality
    for (int i = 0; i < reduceCardinalitySize; ++i) {
      valuesDeleted[i] = FGC_RecvLongLong(gc);
    }

    FGC_Lock(gc, rctx);

    RedisSearchCtx *sctx = getSctx(gc, rctx);
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

    ForkGc_FixInvertedIndex(gc, &idxbufs, &info, currNode->range->entries);

    updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

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
    FGC_Unlock(gc, rctx);
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

static bool ForkGc_ReadTagIndex(ForkGCCtx *gc, int *ret_val, RedisModuleCtx *rctx) {
  size_t fieldNameLen;
  char *fieldName = FGC_RecvBuffer(gc, &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t tagUniqueId = FGC_RecvLongLong(gc);
  bool shouldReturn = false;
  InvertedIndex *value = NULL;
  RedisModuleString *keyName = NULL;
  while ((value = FGC_RecvPtrAddr(gc))) {
    MSG_IndexInfo info = {0};
    InvIdxBuffers idxbufs = {0};
    if (!FGC_RecvInvIdx(gc, &idxbufs, &info)) {
      continue;
    }

    FGC_Lock(gc, rctx);
    RedisSearchCtx *sctx = getSctx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      RETURN;
    }

    RedisModuleKey *idxKey = NULL;
    keyName = TagIndex_FormatName(sctx, fieldName);
    TagIndex *tagIdx = TagIndex_Open(sctx, keyName, false, &idxKey);

    if (tagIdx->uniqueId != tagUniqueId) {
      RETURN;
    }

    ForkGc_FixInvertedIndex(gc, &idxbufs, &info, value);

    updateStats(sctx, gc, info.ndocsCollected, info.nbytesCollected);

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
    FGC_Unlock(gc, rctx);
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

void ForkGc_ReadGarbageFromFork(ForkGCCtx *gc, int *ret_val) {
  RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);

  while (ForkGc_ReadInvertedIndex(gc, ret_val, rctx))
    ;

  if (!(*ret_val)) {
    goto done;
  }

  while (ForkGc_ReadNumericInvertedIndex(gc, ret_val, rctx))
    ;

  if (!(*ret_val)) {
    goto done;
  }

  while (ForkGc_ReadTagIndex(gc, ret_val, rctx))
    ;

done:
  RedisModule_FreeThreadSafeContext(rctx);
}

static int ForkGc_PeriodicCallback(RedisModuleCtx *ctx, void *privdata) {
  ForkGCCtx *gc = privdata;
  RedisModule_AutoMemory(ctx);

  // Check if RDB is loading - not needed after the first time we find out that rdb is not reloading
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

  TimeSampler_Start(&ts);
  pipe(gc->pipefd);  // create the pipe
  FGC_Lock(gc, ctx);
  if (gc->type == ForkGCCtxType_FREED) {
    return 0;
  }
  cpid = fork();  // duplicate the current process
  FGC_Unlock(gc, ctx);
  if (cpid == 0) {
    // fork process
    close(gc->pipefd[GC_READERFD]);
    ForkGc_CollectGarbage(gc);
    close(gc->pipefd[GC_WRITERFD]);
    sleep(RSGlobalConfig.forkGcSleepBeforeExit);
    _exit(EXIT_SUCCESS);
  } else {
    // main process
    close(gc->pipefd[GC_WRITERFD]);
    ForkGc_ReadGarbageFromFork(gc, &ret_val);
    close(gc->pipefd[GC_READERFD]);
    pid_t id = wait4(cpid, NULL, 0, NULL);
    if (id == -1) {
      printf("an error acquire when waiting for fork to terminate, pid:%d", cpid);
    }
  }
  TimeSampler_End(&ts);

  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return ret_val;
}

void ForkGc_OnTerm(void *privdata) {
  ForkGCCtx *gc = privdata;
  if (gc->keyName && gc->type == ForkGCCtxType_IN_KEYSPACE) {
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_ThreadSafeContextLock(ctx);
    RedisModule_FreeString(ctx, (RedisModuleString *)gc->keyName);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
  }
  free(gc);
}

void ForkGc_RenderStats(RedisModuleCtx *ctx, void *gcCtx) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

  ForkGCCtx *gc = gcCtx;

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

void ForkGc_OnDelete(void *ctx) {
}

struct timespec ForkGc_GetInterval(void *ctx) {
  struct timespec interval;
  interval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;
  interval.tv_nsec = 0;
  return interval;
}

ForkGCCtx *NewForkGC(const RedisModuleString *k, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGCCtx *forkGc = malloc(sizeof(*forkGc));

  *forkGc = (ForkGCCtx){
      .keyName = k,
      .stats = {},
      .rdbPossiblyLoading = 1,
      .specUniqueId = specUniqueId,
      .type = ForkGCCtxType_IN_KEYSPACE,
  };

  callbacks->onDelete = ForkGc_OnDelete;
  callbacks->onTerm = ForkGc_OnTerm;
  callbacks->periodicCallback = ForkGc_PeriodicCallback;
  callbacks->renderStats = ForkGc_RenderStats;
  callbacks->getInterval = ForkGc_GetInterval;

  return forkGc;
}

ForkGCCtx *NewForkGCFromSpec(IndexSpec *sp, uint64_t specUniqueId, GCCallbacks *callbacks) {
  ForkGCCtx *ctx = NewForkGC(NULL, specUniqueId, callbacks);
  ctx->sp = sp;
  ctx->type = ForkGCCtxType_OUT_KEYSPACE;
  return ctx;
}
