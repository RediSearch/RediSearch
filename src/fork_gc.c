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
static void ForkGc_AqcuireLock(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    RWLOCK_ACQUIRE_WRITE();
  } else {
    RedisModule_ThreadSafeContextLock(ctx);
  }
}

static void ForkGc_ReleaseLock(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    RWLOCK_RELEASE();
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

static RedisSearchCtx *ForkGc_GetSearchCtx(ForkGCCtx *gc, RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = NULL;
  if (gc->type == ForkGCCtxType_OUT_KEYSPACE) {
    sctx = rm_malloc(sizeof(*sctx));
    *sctx = (RedisSearchCtx)SEARCH_CTX_STATIC(ctx, gc->sp);
  } else if (gc->type == ForkGCCtxType_IN_KEYSPACE) {
    sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName, false);
  }
  return sctx;
}

static void ForkGc_updateStats(RedisSearchCtx *sctx, ForkGCCtx *gc, size_t recordsRemoved,
                               size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
}

static void ForkGc_FDWriteLongLong(int fd, long long val) {
  ssize_t size = write(fd, &val, sizeof(long long));
  assert(size == sizeof(long long));
}

static void ForkGc_FDWritePtr(int fd, const void *val) {
  ssize_t size = write(fd, &val, sizeof(void *));
  assert(size == sizeof(void *));
}

static void ForkGc_FDWriteBuffer(int fd, const void *buff, size_t len) {
  ForkGc_FDWriteLongLong(fd, len);
  if (len > 0) {
    ssize_t size = write(fd, buff, len);
    assert(size == len);
  }
}

static long long ForkGc_FDReadLongLong(int fd) {
  long long ret;
  ssize_t sizeRead = read(fd, &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void *ForkGc_FDReadPtr(int fd) {
  void *ret;
  ssize_t sizeRead = read(fd, &ret, sizeof(ret));
  if (sizeRead != sizeof(ret)) {
    return 0;
  }
  return ret;
}

static void *ForkGc_FDReadBuffer(int fd, size_t *len) {
  *len = ForkGc_FDReadLongLong(fd);
  if (*len == 0) {
    return NULL;
  }
  char *buff = rm_malloc(*len * sizeof(char));
  ssize_t l = read(fd, buff, *len);
  if (l != *len) {
    // we can not write logs here cause we are in the fork process, lets at least print to stdout.
    printf("warning: got a bad length when writing to pipe.\r\n");
  }
  return buff;
}

static bool ForkGc_InvertedIndexRepair(ForkGCCtx *gc, RedisSearchCtx *sctx, InvertedIndex *idx,
                                       void (*RepairCallback)(const RSIndexResult *, void *),
                                       void *arg) {
  typedef struct {
    size_t oldix;
    size_t newix;
  } FixedBlockInfo;
  FixedBlockInfo *fbino = array_new(FixedBlockInfo, 10);
  void **bufsToFree = array_new(void *, 10);
  IndexBlock *outblocks = array_new(IndexBlock, idx->size);

  // statistics
  size_t nbytes = 0, ndocs = 0;
  bool rv = false;

  for (size_t i = 0; i < idx->size - 1; ++i) {
    IndexBlock *blk = idx->blocks + i;
    if (blk->lastId - blk->firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      // todo: is it ok??
      outblocks = array_append(outblocks, *blk);
      continue;
    }

    IndexRepairParams params = {.RepairCallback = RepairCallback, .arg = arg};
    int repaired = IndexBlock_Repair(blk, &sctx->spec->docs, idx->flags, &params);
    // We couldn't repair the block - return 0
    if (repaired == -1) {
      goto done;
    }

    if (repaired > 0) {
      if (blk->numDocs == 0) {
        // this block should be removed
        bufsToFree = array_append(bufsToFree, blk->buf.data);
      } else {
        outblocks = array_append(outblocks, *blk);
        FixedBlockInfo *curfbi = array_ensure_tail(&fbino, FixedBlockInfo);
        curfbi->newix = array_len(outblocks) - 1;
        curfbi->oldix = i;
      }
    } else {
      outblocks = array_append(outblocks, *blk);
    }

    nbytes += params.bytesCollected;
    ndocs += repaired;
  }

  if (array_len(fbino) == 0 && array_len(bufsToFree) == 0) {
    // No blocks were removed or repaired
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], 0);
    goto done;
  }

  ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], 1);  // indicating we have repaired blocks

  // send original block count
  ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], idx->size);

  if (array_len(outblocks) == idx->size - 1) {
    // no empty block, there is no need to send the blocks array
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], 0);  // indicating we have no new invidx
  } else {
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], 1);  // indicating we have new invidx
    // empty blocks introduce, sending the new blocks array
    ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], outblocks,
                         array_len(outblocks) * sizeof(IndexBlock));
  }

  ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], bufsToFree,
                       array_len(bufsToFree) * sizeof(char **));

  // write number of repaired blocks
  ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], array_len(fbino));

  // write total bytes collected
  ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], nbytes);

  // write total docs collected
  ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], ndocs);

  for (size_t i = 0; i < array_len(fbino); ++i) {
    // write fix block
    IndexBlock *blk = outblocks + fbino[i].newix;
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], fbino[i].newix);  // writing the block index
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], fbino[i].oldix);  // writing old index
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], blk->firstId);
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], blk->lastId);
    ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], blk->numDocs);
    ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], IndexBlock_DataBuf(blk), IndexBlock_DataLen(blk));
  }
  rv = true;

done:
  array_free(fbino);
  array_free(outblocks);
  array_free(bufsToFree);
  return rv;
}

static void ForkGc_CollectTerm(ForkGCCtx *gc, RedisSearchCtx *sctx, char *term, size_t termLen) {
  RedisModuleKey *idxKey = NULL;
  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
  if (idx) {
    // inverted index name
    ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], term, termLen);

    ForkGc_InvertedIndexRepair(gc, sctx, idx, NULL, NULL);
  }
  if (idxKey) {
    RedisModule_CloseKey(idxKey);
  }
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

static void ForkGc_CollectGarbageFromInvIdx(ForkGCCtx *gc, RedisSearchCtx *sctx) {
  TrieIterator *iter = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    ForkGc_CollectTerm(gc, sctx, term, termLen);
    free(term);
  }
  DFAFilter_Free(iter->ctx);
  free(iter->ctx);
  TrieIterator_Free(iter);

  // we are done with terms
  ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], "\0", 1);
}

static void ForkGc_CollectGarbageFromNumIdx(ForkGCCtx *gc, RedisSearchCtx *sctx) {
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
      ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], numericFields[i]->name,
                           strlen(numericFields[i]->name) + 1);
      // numeric field unique id
      ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], rt->uniqueId);

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
        ForkGc_FDWritePtr(gc->pipefd[GC_WRITERFD], currNode);

        bool repaired = ForkGc_InvertedIndexRepair(gc, sctx, currNode->range->entries,
                                                   ForkGc_CountDeletedCardinality, valuesDeleted);

        if (repaired) {
          // send reduced cardinality size
          ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], currNode->range->card);

          // send reduced cardinality
          for (int i = 0; i < currNode->range->card; ++i) {
            ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], valuesDeleted[i].appearances);
          }
        }
        array_free(valuesDeleted);
      }

      // we are done with the current field
      ForkGc_FDWritePtr(gc->pipefd[GC_WRITERFD], 0);

      if (idxKey) RedisModule_CloseKey(idxKey);

      NumericRangeTreeIterator_Free(gcIterator);
    }
  }

  // we are done with numeric fields
  ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], "\0", 1);
}

static void ForkGc_CollectGarbageFromTagIdx(ForkGCCtx *gc, RedisSearchCtx *sctx) {
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
      ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], tagFields[i]->name,
                           strlen(tagFields[i]->name) + 1);
      // numeric field unique id
      ForkGc_FDWriteLongLong(gc->pipefd[GC_WRITERFD], tagIdx->uniqueId);

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values, "", 0);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        // send inverted index pointer
        ForkGc_FDWritePtr(gc->pipefd[GC_WRITERFD], value);
        // send repaired data
        ForkGc_InvertedIndexRepair(gc, sctx, value, NULL, NULL);
      }

      // we are done with the current field
      ForkGc_FDWritePtr(gc->pipefd[GC_WRITERFD], 0);

      if (idxKey) RedisModule_CloseKey(idxKey);
    }
  }
  // we are done with numeric fields
  ForkGc_FDWriteBuffer(gc->pipefd[GC_WRITERFD], "\0", 1);
}

static void ForkGc_CollectGarbage(ForkGCCtx *gc) {
  RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx *sctx = ForkGc_GetSearchCtx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    // write log here
    RedisModule_FreeThreadSafeContext(rctx);
    return;
  }

  ForkGc_CollectGarbageFromInvIdx(gc, sctx);

  ForkGc_CollectGarbageFromNumIdx(gc, sctx);

  ForkGc_CollectGarbageFromTagIdx(gc, sctx);

  if (sctx) {
    SearchCtx_Free(sctx);
    RedisModule_FreeThreadSafeContext(rctx);
  }
}

typedef struct ModifiedBlock {
  size_t blockIndex;
  size_t blockOldIndex;
  IndexBlock blk;
} ModifiedBlock;

typedef struct {
  size_t originalSize;
  size_t bytesCollected, docsCollected;
  void **addrsToFree;
  size_t numAddrsToFree;
  IndexBlock *newBlocks;
  size_t numNewBlocks;
  ModifiedBlock *changedBlocks;
  size_t numChangedBlocks;
} ForkGc_InvertedIndexData;

static void ForkGc_ReadModifiedBlock(ForkGCCtx *gc, ModifiedBlock *blockModified) {
  blockModified->blockIndex = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  blockModified->blockOldIndex = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  blockModified->blk.firstId = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  blockModified->blk.lastId = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  blockModified->blk.numDocs = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);

  Buffer *b = &blockModified->blk.buf;
  b->data = ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &b->offset);
  b->cap = b->offset;
}

static bool ForkGc_ReadInvertedIndexFromFork(ForkGCCtx *gc, ForkGc_InvertedIndexData *idxData) {
  long long blocksRepaired = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  if (!blocksRepaired) {
    return false;
  }

  idxData->originalSize = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  int hasNewBlocks = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  if (hasNewBlocks) {
    idxData->newBlocks =
        (IndexBlock *)ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &idxData->numNewBlocks);
    idxData->numNewBlocks /= sizeof(IndexBlock);
  }

  idxData->addrsToFree = ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &idxData->numAddrsToFree);
  idxData->numAddrsToFree /= sizeof(char *);

  idxData->numChangedBlocks = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  idxData->bytesCollected = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  idxData->docsCollected = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  idxData->changedBlocks = rm_malloc(sizeof(*idxData->changedBlocks) * idxData->numChangedBlocks);
  for (size_t i = 0; i < idxData->numChangedBlocks; ++i) {
    ForkGc_ReadModifiedBlock(gc, idxData->changedBlocks + i);
  }
  return true;
}

static void ForkGc_FixInvertedIndex(ForkGCCtx *gc, ForkGc_InvertedIndexData *idxData,
                                    InvertedIndex *idx) {
  if (idxData->addrsToFree) {
    for (int i = 0; i < idxData->numAddrsToFree; ++i) {
      rm_free(idxData->addrsToFree[i]);
    }
    rm_free(idxData->addrsToFree);
  }

  for (int i = 0; i < idxData->numChangedBlocks; ++i) {
    ModifiedBlock *blockModified = idxData->changedBlocks + i;
    indexBlock_Free(&idx->blocks[blockModified->blockOldIndex]);
  }

  assert(idx->size >= idxData->originalSize);
  if (idxData->newBlocks) {
    idxData->newBlocks = rm_realloc(
        idxData->newBlocks,
        (idxData->numNewBlocks +
         (idx->size - (idxData->originalSize - 1 /* we are copy the last block anyway*/))) *
            sizeof(IndexBlock));
    memcpy(idxData->newBlocks + idxData->numNewBlocks, idx->blocks + (idxData->originalSize - 1),
           (idx->size - (idxData->originalSize - 1)) * sizeof(IndexBlock));
    rm_free(idx->blocks);
    idxData->numNewBlocks += (idx->size - (idxData->originalSize - 1));
    idx->blocks = idxData->newBlocks;
    idx->size = idxData->numNewBlocks;
  }

  for (size_t i = 0; i < idxData->numChangedBlocks; ++i) {
    ModifiedBlock *blockModified = idxData->changedBlocks + i;
    idx->blocks[blockModified->blockIndex] = blockModified->blk;
  }
  idx->numDocs -= idxData->docsCollected;
}

static bool ForkGc_ReadInvertedIndex(ForkGCCtx *gc, int *ret_val, RedisModuleCtx *rctx) {
  size_t len;
  char *term = ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &len);
  if (term == NULL || term[0] == '\0') {
    if (term) {
      rm_free(term);
    }
    return false;
  }

  ForkGc_InvertedIndexData idxData = {0};
  if (!ForkGc_ReadInvertedIndexFromFork(gc, &idxData)) {
    rm_free(term);
    return true;
  }

  ForkGc_AqcuireLock(gc, rctx);

  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;
  sctx = ForkGc_GetSearchCtx(gc, rctx);
  if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
    *ret_val = 0;
    goto cleanup;
  }

  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, len, 1, &idxKey);

  if (idx == NULL) {
    *ret_val = 0;
    goto cleanup;
  }

  ForkGc_FixInvertedIndex(gc, &idxData, idx);

  ForkGc_updateStats(sctx, gc, idxData.docsCollected, idxData.bytesCollected);

cleanup:

  if (idxKey) {
    RedisModule_CloseKey(idxKey);
  }
  if (sctx) {
    SearchCtx_Free(sctx);
  }
  ForkGc_ReleaseLock(gc, rctx);
  rm_free(term);
  rm_free(idxData.changedBlocks);
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
  char *fieldName = ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t rtUniqueId = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);

  NumericRangeNode *currNode = NULL;
  bool shouldReturn = false;
  RedisModuleString *keyName = NULL;
  while ((currNode = ForkGc_FDReadPtr(gc->pipefd[GC_READERFD]))) {

    ForkGc_InvertedIndexData idxData = {0};
    if (!ForkGc_ReadInvertedIndexFromFork(gc, &idxData)) {
      continue;
    }

    // read reduced cardinality size
    long long reduceCardinalitySize = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
    long long valuesDeleted[reduceCardinalitySize];

    // read reduced cardinality
    for (int i = 0; i < reduceCardinalitySize; ++i) {
      valuesDeleted[i] = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
    }

    ForkGc_AqcuireLock(gc, rctx);

    RedisSearchCtx *sctx = ForkGc_GetSearchCtx(gc, rctx);
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

    ForkGc_FixInvertedIndex(gc, &idxData, currNode->range->entries);

    ForkGc_updateStats(sctx, gc, idxData.docsCollected, idxData.bytesCollected);

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
    rm_free(idxData.changedBlocks);
    if (keyName) {
      RedisModule_FreeString(rctx, keyName);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    ForkGc_ReleaseLock(gc, rctx);
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
  char *fieldName = ForkGc_FDReadBuffer(gc->pipefd[GC_READERFD], &fieldNameLen);
  if (fieldName == NULL || fieldName[0] == '\0') {
    if (fieldName) {
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t tagUniqueId = ForkGc_FDReadLongLong(gc->pipefd[GC_READERFD]);
  bool shouldReturn = false;
  InvertedIndex *value = NULL;
  RedisModuleString *keyName = NULL;
  while ((value = ForkGc_FDReadPtr(gc->pipefd[GC_READERFD]))) {
    ForkGc_InvertedIndexData idxData = {0};
    if (!ForkGc_ReadInvertedIndexFromFork(gc, &idxData)) {
      continue;
    }

    ForkGc_AqcuireLock(gc, rctx);
    RedisSearchCtx *sctx = ForkGc_GetSearchCtx(gc, rctx);
    if (!sctx || sctx->spec->uniqueId != gc->specUniqueId) {
      RETURN;
    }

    RedisModuleKey *idxKey = NULL;
    keyName = TagIndex_FormatName(sctx, fieldName);
    TagIndex *tagIdx = TagIndex_Open(sctx, keyName, false, &idxKey);

    if (tagIdx->uniqueId != tagUniqueId) {
      RETURN;
    }

    ForkGc_FixInvertedIndex(gc, &idxData, value);

    ForkGc_updateStats(sctx, gc, idxData.docsCollected, idxData.bytesCollected);

  loop_cleanup:
    if (sctx) {
      SearchCtx_Free(sctx);
    }
    rm_free(idxData.changedBlocks);
    if (keyName) {
      RedisModule_FreeString(rctx, keyName);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    ForkGc_ReleaseLock(gc, rctx);
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

  size_t totalCollectedBefore = gc->stats.totalCollected;

  TimeSampler_Start(&ts);
  pipe(gc->pipefd);  // create the pipe
  ForkGc_AqcuireLock(gc, ctx);
  if (gc->type == ForkGCCtxType_FREED) {
    return 0;
  }
  cpid = fork();  // duplicate the current process
  ForkGc_ReleaseLock(gc, ctx);
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
