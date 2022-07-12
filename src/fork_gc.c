
#include "fork_gc.h"
#include "util/arr.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "time_sample.h"
#include "rwlock.h"
#include "module.h"

#include "rmutil/rm_assert.h"

#include "rmalloc.h"
#include "util/map.h"

#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <float.h>
#include <sys/wait.h>
#include <sys/resource.h>

#ifdef __linux__
#include <sys/prctl.h>
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

#define GC_WRITERFD 1
#define GC_READERFD 0

//---------------------------------------------------------------------------------------------

bool ForkGC::lock(RedisModuleCtx *ctx) {
  if (type == FGC_TYPE_NOKEYSPACE) {
    RWLOCK_ACQUIRE_WRITE();
    if (deleting) {
      RWLOCK_RELEASE();
      return false;
    }
  } else {
    RedisModule_ThreadSafeContextLock(ctx);
    if (deleting) {
      RedisModule_ThreadSafeContextUnlock(ctx);
      return false;
    }
  }
  return true;
}

//---------------------------------------------------------------------------------------------

void ForkGC::unlock(RedisModuleCtx *ctx) {
  if (type == FGC_TYPE_NOKEYSPACE) {
    RWLOCK_RELEASE();
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

//---------------------------------------------------------------------------------------------

RedisSearchCtx *ForkGC::getSctx(RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = NULL;
  if (type == FGC_TYPE_NOKEYSPACE) {
    sctx = rm_malloc(sizeof(*sctx));
    *sctx = (RedisSearchCtx)SEARCH_CTX_STATIC(ctx, sp);
  } else if (type == FGC_TYPE_INKEYSPACE) {
    sctx = new RedisSearchCtx(ctx, (RedisModuleString *)keyName, false);
  }
  return sctx;
}

//---------------------------------------------------------------------------------------------

void ForkGC::updateStats(RedisSearchCtx *sctx, size_t recordsRemoved,
                                size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  stats.totalCollected += bytesCollected;
}

//---------------------------------------------------------------------------------------------

void ForkGC::sendFixed(const void *buff, size_t len) {
  RS_LOG_ASSERT(len > 0, "buffer length cannot be 0");
  ssize_t size = write(pipefd[GC_WRITERFD], buff, len);
  if (size != len) {
    if (size == -1) {
      perror("write()");
      abort();
    } else {
      RS_LOG_ASSERT(size == len, "buffer failed to write");
    }
  }
}

//---------------------------------------------------------------------------------------------

void ForkGC::sendBuffer(const void *buff, size_t len) {
  sendVar(len);
  if (len > 0) {
    sendFixed(buff, len);
  }
}

//---------------------------------------------------------------------------------------------

// Send instead of a string to indicate that no more buffers are to be received
void ForkGC::sendTerminator() {
  size_t smax = SIZE_MAX;
  sendVar(smax);
}

//---------------------------------------------------------------------------------------------

int __attribute__((warn_unused_result)) ForkGC::recvFixed(void *buf, size_t len) {
  while (len) {
    ssize_t nrecvd = read(pipefd[GC_READERFD], buf, len);
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

int ForkGC::tryRecvFixed(void *obj, size_t len){
  if (recvFixed(obj, len) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
}

//---------------------------------------------------------------------------------------------

static void *RECV_BUFFER_EMPTY = (void *)0x0deadbeef;

int __attribute__((warn_unused_result)) ForkGC::recvBuffer(void **buf, size_t *len) {
  tryRecvFixed(len, sizeof *len);
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
  if (tryRecvFixed(*buf, *len) != REDISMODULE_OK) {
    rm_free(buf);
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int ForkGC::tryRecvBuffer(void **buf, size_t *len) {
  if (recvBuffer(buf, len) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * sendHeader() is invoked before the inverted index is sent, if it was repaired.
 * blockrepair is passed directly to IndexBlock::Repair.
 */
bool ForkGC::childRepairInvIdx(RedisSearchCtx *sctx, InvertedIndex *idx, IndexRepair &repair,
                               IndexBlockRepair &blockrepair) {
  arrayof(MSG_RepairedBlock) fixed = array_new(MSG_RepairedBlock, 10);
  arrayof(MSG_DeletedBlock) deleted = array_new(MSG_DeletedBlock, 10);
  arrayof(IndexBlock) blocklist = array_new(IndexBlock, idx->size);
  MSG_IndexInfo ixmsg(idx->size);
  bool rv = false;

  for (size_t i = 0; i < idx->size; ++i) {
    IndexBlock &blk = idx->blocks[i];
    if (blk.lastId - blk.firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      // todo: is it ok??
      blocklist = array_append(blocklist, blk);
      continue;
    }

    // Capture the pointer address before the block is cleared; otherwise the pointer might be freed!
    void *bufptr = blk.buf.data;
    int nrepaired = blk.Repair(&sctx->spec->docs, idx->flags, blockrepair);
    // We couldn't repair the block - return 0
    if (nrepaired == -1) {
      goto done;
    } else if (nrepaired == 0) {
      // unmodified block
      blocklist = array_append(blocklist, blk);
      continue;
    }

    if (blk.numDocs == 0) {
      // this block should be removed
      MSG_DeletedBlock *delmsg = array_ensure_tail(&deleted, MSG_DeletedBlock);
      *delmsg = (MSG_DeletedBlock){.ptr = bufptr, .oldix = i};
    } else {
      blocklist = array_append(blocklist, blk);
      MSG_RepairedBlock *fixmsg = array_ensure_tail(&fixed, MSG_RepairedBlock);
      fixmsg->newix = array_len(blocklist) - 1;
      fixmsg->oldix = i;
      fixmsg->blk = blk; // copy
      ixmsg.nblocksRepaired++;
    }

    ixmsg.nbytesCollected += blockrepair.bytesCollected;
    ixmsg.ndocsCollected += nrepaired;
    if (i == idx->size - 1) {
      ixmsg.lastblkBytesCollected = blockrepair.bytesCollected;
      ixmsg.lastblkDocsRemoved = nrepaired;
      ixmsg.lastblkNumDocs = blk.numDocs + nrepaired;
    }
  }

  if (array_len(fixed) == 0 && array_len(deleted) == 0) {
    // No blocks were removed or repaired
    goto done;
  }

  repair.sendHeader();
  sendFixed(&ixmsg, sizeof ixmsg);
  if (array_len(blocklist) == idx->size) {
    // no empty block, there is no need to send the blocks array. Don't send
    // any new blocks
    sendBuffer(NULL, 0);
  } else {
    sendBuffer(blocklist, array_len(blocklist) * sizeof(*blocklist));
  }
  sendBuffer(deleted, array_len(deleted) * sizeof(*deleted));

  for (size_t i = 0; i < array_len(fixed); ++i) {
    // write fix block
    const MSG_RepairedBlock *msg = fixed + i;
    const IndexBlock *blk = blocklist + msg->newix;
    sendFixed(msg, sizeof(*msg));
    sendBuffer(blk->DataBuf(), blk->DataLen());
  }
  rv = true;

done:
  array_free(fixed);
  array_free(blocklist);
  array_free(deleted);
  return rv;
}

//---------------------------------------------------------------------------------------------

void InvertedIndexRepair::sendHeader() {
  fgc.sendBuffer(term, termLen);
}

//---------------------------------------------------------------------------------------------

void ForkGC::childCollectTerms(RedisSearchCtx *sctx) {
  TrieIterator iter = sctx->spec->terms->Iterate("", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (iter.Next(&rstr, &slen, NULL, &score, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    RedisModuleKey *idxKey = NULL;
    InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
    if (idx) {
      InvertedIndexRepair indexrepair(*this, term, termLen);
      IndexBlockRepair blockrepair;
      childRepairInvIdx(sctx, idx, indexrepair, blockrepair);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    rm_free(term);
  }

  delete iter.ctx;

  // we are done with terms
  sendTerminator();
}

//---------------------------------------------------------------------------------------------

NumericIndexBlockRepair::NumericIndexBlockRepair(const InvertedIndex &idx) {
  lastblk = &idx.blocks[idx.size - 1];
}

//---------------------------------------------------------------------------------------------

void NumericIndexBlockRepair::countDeleted(const NumericResult *r, const IndexBlock *blk) {
  UnorderedMap<uint64_t, size_t> &ht = blk == lastblk ? delLast : delRest;
  // RS_LOG_ASSERT(ht, "cardvals should not be NULL");
  double u = r->value;
  if (ht.contains(u)) {
    ht[u]++; // i.e. already existed
  } else {
    ht[u] = 0;
  }
}

//---------------------------------------------------------------------------------------------

void NumericAndTagIndexRepair::sendHeader() {
  if (!sentFieldName) {
    sentFieldName = true;
    fgc.sendBuffer(field, strlen(field));
    fgc.sendFixed(&uniqueId, sizeof(uniqueId));
  }
  fgc.sendVar(idx);
}

//---------------------------------------------------------------------------------------------

// If anything other than FGC_COLLECTED is returned, it is an error or done
FGCError ForkGC::recvNumericTagHeader(char **fieldName, size_t *fieldNameLen, uint64_t *id) {
  if (recvBuffer((void **)fieldName, fieldNameLen) != REDISMODULE_OK) {
    return FGC_PARENT_ERROR;
  }
  if (*fieldName == RECV_BUFFER_EMPTY) {
    *fieldName = NULL;
    return FGC_DONE;
  }

  if (recvFixed(id, sizeof(*id)) != REDISMODULE_OK) {
    rm_free(*fieldName);
    *fieldName = NULL;
    return FGC_PARENT_ERROR;
  }
  return FGC_COLLECTED;
}

//---------------------------------------------------------------------------------------------

void ForkGC::sendKht(const UnorderedMap<uint64_t, size_t> &kh) {
  size_t n = kh.size();
  size_t nsent = 0;

  sendVar(n);
  for (const auto& [key, count] : kh) {
    CardinalityValue cu(key, count);
    sendVar(cu);
    nsent++;
  }
  RS_LOG_ASSERT(nsent == n, "Not all hashes has been sent");
}

//---------------------------------------------------------------------------------------------

void ForkGC::childCollectNumeric(RedisSearchCtx *sctx) {
  FieldSpec **numericFields = sctx->spec->getFieldsByType(INDEXFLD_T_NUMERIC);
  for (int i = 0; i < array_len(numericFields); ++i) {
    RedisModuleString *keyName = sctx->spec->GetFormattedKey(numericFields[i], INDEXFLD_T_NUMERIC);
    RedisModuleKey *idxKey = NULL;
    NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

    NumericRangeTreeIterator gcIterator(rt);
    NumericIndexRepair indexrepair(*this, *numericFields[i], *rt);
    for (NumericRangeNode *node = NULL; (node = gcIterator.Next()); ) {
      if (!node->range) {
        continue;
      }
      indexrepair.set(node);
      InvertedIndex &idx = node->range->entries;
      NumericIndexBlockRepair blockrepair(idx);
      bool repaired = childRepairInvIdx(sctx, &idx, indexrepair, blockrepair);

      if (repaired) {
        sendKht(blockrepair.delRest);
        sendKht(blockrepair.delLast);
      }
    }

    if (indexrepair.sentFieldName) {
      // If we've repaired at least one entry, send the terminator;
      // note that "terminator" just means a zero address and not the
      // "no more strings" terminator in sendTerminator
      void *pdummy = NULL;
      sendVar(pdummy);
    }

    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
  }

  // we are done with numeric fields
  sendTerminator();
}

//---------------------------------------------------------------------------------------------

void ForkGC::childCollectTags(RedisSearchCtx *sctx) {
  FieldSpec **tagFields = sctx->spec->getFieldsByType(INDEXFLD_T_TAG);
  if (array_len(tagFields) != 0) {
    for (int i = 0; i < array_len(tagFields); ++i) {
      RedisModuleString *keyName = sctx->spec->GetFormattedKey(tagFields[i], INDEXFLD_T_TAG);
      RedisModuleKey *idxKey = NULL;
      TagIndex *tagIdx = TagIndex::Open(sctx, keyName, false, &idxKey);
      if (!tagIdx) {
        continue;
      }

      TrieMapIterator *iter = tagIdx->values->Iterate("", 0);
      TagIndexRepair indexrepair(*this, *tagFields[i], *tagIdx);
      IndexBlockRepair blockrepair;
      char *ptr;
      tm_len_t len;
      InvertedIndex *invidx = NULL;
      while (iter->Next(&ptr, &len, (void **)&invidx)) {
        indexrepair.set(invidx);
        // send repaired data
        childRepairInvIdx(sctx, invidx, indexrepair, blockrepair);
      }

      // we are done with the current field
      if (indexrepair.sentFieldName) {
        void *pdummy = NULL;
        sendVar(pdummy);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }
    }
  }
  // we are done with numeric fields
  sendTerminator();
}

//---------------------------------------------------------------------------------------------

void ForkGC::childScanIndexes() {
  RedisSearchCtx *sctx = getSctx(ctx);
  if (!sctx || sctx->spec->uniqueId != specUniqueId) {
    // write log here
    return;
  }

  childCollectTerms(sctx);
  childCollectNumeric(sctx);
  childCollectTags(sctx);

  delete sctx;
}

//---------------------------------------------------------------------------------------------

int ForkGC::recvRepairedBlock(MSG_RepairedBlock *binfo) {
  if (recvFixed(binfo, sizeof(*binfo)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  Buffer *b = &binfo->blk.buf;
  if (recvBuffer((void **)&b->data, &b->offset) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  b->cap = b->offset;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

int ForkGC::recvInvIdx(InvIdxBuffers *bufs, MSG_IndexInfo *info) {
  size_t nblocksRecvd = 0;
  if (recvFixed(info, sizeof(*info)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (recvBuffer((void **)&bufs->newBlocklist, &bufs->newBlocklistSize) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (bufs->newBlocklistSize) {
    bufs->newBlocklistSize /= sizeof(*bufs->newBlocklist);
  }
  if (recvBuffer((void **)&bufs->delBlocks, &bufs->numDelBlocks) != REDISMODULE_OK) {
    goto error;
  }
  bufs->numDelBlocks /= sizeof(*bufs->delBlocks);
  bufs->changedBlocks = rm_malloc(sizeof(*bufs->changedBlocks) * info->nblocksRepaired);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    if (recvRepairedBlock(bufs->changedBlocks + i) != REDISMODULE_OK) {
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

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

void ForkGC::checkLastBlock(InvIdxBuffers *idxData, MSG_IndexInfo *info, InvertedIndex *idx) {
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
    delete &rb->blk;
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
  stats.gcBlocksDenied++;
}

//---------------------------------------------------------------------------------------------

void ForkGC::applyInvertedIndex(InvIdxBuffers *idxData, MSG_IndexInfo *info, InvertedIndex *idx) {
  checkLastBlock(idxData, info, idx);
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    MSG_RepairedBlock *blockModified = idxData->changedBlocks + i;
    delete &idx->blocks[blockModified->oldix];
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
    // At this point, we check if the last block has had new data added to it, but was _not_ repaired.
    // We check for a repaired last block in checkLastBlock().

    if (!info->lastblkDocsRemoved) {
      // Last block was unmodified-- let's prefer the last block's pointer over our own (which may be stale).
      // If the last block was repaired, this is handled above.
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
      idx->AddBlock(0);
    }
  }

  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    MSG_RepairedBlock *blockModified = idxData->changedBlocks + i;
    idx->blocks[blockModified->newix] = blockModified->blk;
  }

  idx->numDocs -= info->ndocsCollected;
  idx->gcMarker++;
}

//---------------------------------------------------------------------------------------------

FGCError ForkGC::parentHandleTerms(RedisModuleCtx *rctx) {
  FGCError status = FGC_COLLECTED;
  size_t len;
  int hasLock = 0;
  char *term = NULL;
  InvertedIndex *idx = NULL;

  if (recvBuffer((void **)&term, &len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;

  if (term == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  InvIdxBuffers idxbufs;
  MSG_IndexInfo info;
  if (recvInvIdx(&idxbufs, &info) != REDISMODULE_OK) {
    rm_free(term);
    return FGC_CHILD_ERROR;
  }

  if (!lock(rctx)) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  hasLock = 1;
  sctx = getSctx(rctx);
  if (!sctx || sctx->spec->uniqueId != specUniqueId) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  idx = Redis_OpenInvertedIndexEx(sctx, term, len, 1, &idxKey);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  applyInvertedIndex(&idxbufs, &info, idx);
  updateStats(sctx, info.ndocsCollected, info.nbytesCollected);

cleanup:

  if (idxKey) {
    RedisModule_CloseKey(idxKey);
  }
  if (sctx) {
    delete sctx;
  }
  if (hasLock) {
    unlock(rctx);
  }
  rm_free(term);
  if (status != FGC_COLLECTED) {
    freeInvIdx(&idxbufs, &info);
  } else {
    rm_free(idxbufs.changedBlocks);
  }
  return status;
}

//---------------------------------------------------------------------------------------------

int ForkGC::recvCardvals(CardinalityValue **tgt, size_t *len) {
  if (recvFixed(len, sizeof(*len)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  *len *= sizeof(**tgt);
  if (!*len) {
    *tgt = NULL;
    return REDISMODULE_OK;
  }
  *tgt = rm_malloc(sizeof(**tgt) * *len);
  int rc = recvFixed(*tgt, *len);
  if (rc == REDISMODULE_OK) {
    *len /= sizeof(**tgt);
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

FGCError ForkGC::recvNumIdx(NumGcInfo *ninfo) {
  if (recvFixed(&ninfo->node, sizeof(ninfo->node)) != REDISMODULE_OK) {
    goto error;
  }
  if (ninfo->node == NULL) {
    return FGC_DONE;
  }
  if (recvInvIdx(&ninfo->idxbufs, &ninfo->info) != REDISMODULE_OK) {
    goto error;
  }
  if (recvCardvals(&ninfo->restBlockDeleted, &ninfo->nrestBlockDel) != REDISMODULE_OK) {
    goto error;
  }
  if (recvCardvals(&ninfo->lastBlockDeleted, &ninfo->nlastBlockDel) != REDISMODULE_OK) {
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

//---------------------------------------------------------------------------------------------

static void resetCardinality(NumGcInfo *info, NumericRangeNode *currNone) {
  UnorderedMap<uint64_t, size_t> kh;
  uint64_t u;
  for (size_t i = 0; i < info->nrestBlockDel; ++i) {
    u = info->restBlockDeleted[i].value;
    kh[u] = info->restBlockDeleted[i].appearances;
  }
  if (!info->idxbufs.lastBlockIgnored) {
    for (size_t i = 0; i < info->nlastBlockDel; ++i) {
      u = info->lastBlockDeleted[i].value;
      if (kh.contains(u)) {
        kh[u] += info->lastBlockDeleted[i].appearances;
      } else {
        kh[u] = info->lastBlockDeleted[i].appearances;
      }
    }
  }

  NumericRange *r = currNone->range;
  size_t n = array_len(r->values);
  double minVal = DBL_MAX, maxVal = -DBL_MIN, uniqueSum = 0;

  for (size_t i = 0; i < array_len(r->values); ++i) {
  reeval:;
    u = r->values[i].value;
    if (r->values[i].appearances -= kh[u] == 0) {
      // delet this
      size_t isLast = array_len(r->values) == i + 1;
      array_del_fast(r->values, i);
      if (!isLast) {
        goto reeval;
      }
    } else {
      minVal = MIN(minVal, r->values[i].value);
      maxVal = MAX(maxVal, r->values[i].value);
      uniqueSum += r->values[i].value;
    }
  }

  // we can only update the min and the max value if the node is a leaf.
  // otherwise the min and the max also represent its children values and
  // we can not change it.
  if (currNone->IsLeaf()) {
    r->minVal = minVal;
    r->maxVal = maxVal;
  }
  r->unique_sum = uniqueSum;
  r->card = array_len(r->values);
}

//---------------------------------------------------------------------------------------------

void ForkGC::applyNumIdx(RedisSearchCtx *sctx, NumGcInfo *ninfo) {
  NumericRangeNode *currNode = ninfo->node;
  InvIdxBuffers *idxbufs = &ninfo->idxbufs;
  MSG_IndexInfo *info = &ninfo->info;
  applyInvertedIndex(idxbufs, info, &currNode->range->entries);
  updateStats(sctx, info->ndocsCollected, info->nbytesCollected);
  resetCardinality(ninfo, currNode);
}

//---------------------------------------------------------------------------------------------

FGCError ForkGC::parentHandleNumeric(RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName = NULL;
  NumericRangeTree *rt = NULL;
  uint64_t rtUniqueId;
  RedisModuleString *keyName;

  FGCError status = recvNumericTagHeader(&fieldName, &fieldNameLen, &rtUniqueId);
  if (status == FGC_DONE) {
    return FGC_DONE;
  }

  while (status == FGC_COLLECTED) {
    NumGcInfo ninfo;
    RedisSearchCtx *sctx = NULL;
    RedisModuleKey *idxKey = NULL;
    FGCError status2 = recvNumIdx(&ninfo);
    if (status2 == FGC_DONE) {
      break;
    } else if (status2 != FGC_COLLECTED) {
      status = status2;
      break;
    }

    if (!lock(rctx)) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    hasLock = 1;
    sctx = getSctx(rctx);
    if (!sctx || sctx->spec->uniqueId != specUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }
    keyName = sctx->spec->GetFormattedKeyByName(fieldName, INDEXFLD_T_NUMERIC);
    rt = OpenNumericIndex(sctx, keyName, &idxKey);

    if (rt->uniqueId != rtUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    if (!ninfo.node->range) {
      stats.gcNumericNodesMissed++;
      goto loop_cleanup;
    }

    applyNumIdx(sctx, &ninfo);

  loop_cleanup:
    if (sctx) {
      delete sctx;
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
      unlock(rctx);
      hasLock = 0;
    }

    rm_free(ninfo.restBlockDeleted);
    rm_free(ninfo.lastBlockDeleted);
  }

  rm_free(fieldName);
  return status;
}

//---------------------------------------------------------------------------------------------

FGCError ForkGC::parentHandleTags(RedisModuleCtx *rctx) {
  int hasLock = 0;
  size_t fieldNameLen;
  char *fieldName;
  uint64_t tagUniqueId;
  InvertedIndex *value = NULL;
  FGCError status = recvNumericTagHeader(&fieldName, &fieldNameLen, &tagUniqueId);

  while (status == FGC_COLLECTED) {
    RedisModuleString *keyName = NULL;
    RedisModuleKey *idxKey = NULL;
    RedisSearchCtx *sctx = NULL;
    MSG_IndexInfo info;
    InvIdxBuffers idxbufs;
    TagIndex *tagIdx = NULL;

    if (recvFixed(&value, sizeof value) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    if (value == NULL) {
      RS_LOG_ASSERT(status == FGC_COLLECTED, "GC status is COLLECTED");
      break;
    }

    if (recvInvIdx(&idxbufs, &info) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    if (!lock(rctx)) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    hasLock = 1;
    sctx = getSctx(rctx);
    if (!sctx || sctx->spec->uniqueId != specUniqueId) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }
    keyName = sctx->spec->GetFormattedKeyByName(fieldName, INDEXFLD_T_TAG);
    tagIdx = TagIndex::Open(sctx, keyName, false, &idxKey);

    if (tagIdx->uniqueId != tagUniqueId) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    applyInvertedIndex(&idxbufs, &info, value);
    updateStats(sctx, info.ndocsCollected, info.nbytesCollected);

  loop_cleanup:
    if (sctx) {
      delete sctx;
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }
    if (hasLock) {
      unlock(rctx);
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

//---------------------------------------------------------------------------------------------

int ForkGC::parentHandleFromChild() {
  FGCError status = FGC_COLLECTED;

#define COLLECT_FROM_CHILD(e)               \
  while ((status = (e)) == FGC_COLLECTED) { \
  }                                         \
  if (status != FGC_DONE) {                 \
    return REDISMODULE_ERR;                 \
  }

  COLLECT_FROM_CHILD(parentHandleTerms(ctx));
  COLLECT_FROM_CHILD(parentHandleNumeric(ctx));
  COLLECT_FROM_CHILD(parentHandleTags(ctx));
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

/**
 * In future versions of Redis, Redis will have its own fork() call.
 * The following two functions wrap this functionality.
 */
bool ForkGC::haveRedisFork() {
  return RedisModule_Fork != NULL;
}

//---------------------------------------------------------------------------------------------

int ForkGC::Fork(RedisModuleCtx *ctx) { //@@ why we are not using here the instance nor the ctx
  if (haveRedisFork()) {
    int ret = RedisModule_Fork(NULL, NULL);
    return ret;
  } else {
    return fork();
  }
}

//---------------------------------------------------------------------------------------------

bool ForkGC::PeriodicCallback(RedisModuleCtx *ctx) {
  if (deleting) {
    return false;
  }
  if (deletedDocsFromLastRun < RSGlobalConfig.forkGcCleanThreshold) {
    return true;
  }

  int gcrv = 1;

  RedisModule_AutoMemory(ctx);

  // Check if RDB is loading - not needed after the first time we find out that rdb is not
  // reloading
  if (rdbPossiblyLoading && !sp) {
    RedisModule_ThreadSafeContextLock(ctx);
    if (isRdbLoading(ctx)) {
      RedisModule_Log(ctx, "notice", "RDB Loading in progress, not performing GC");
      RedisModule_ThreadSafeContextUnlock(ctx);
      return true;
    } else {
      // the RDB will not load again, so it's safe to ignore the info check in the next cycles
      rdbPossiblyLoading = 0;
    }
    RedisModule_ThreadSafeContextUnlock(ctx);
  }

  pid_t cpid;
  TimeSample ts;

  while (pauseState == FGC_PAUSED_CHILD) {
    execState = FGC_STATE_WAIT_FORK;
    // spin or sleep
    usleep(500);
  }

  pid_t ppid_before_fork = getpid();

  ts.Start();
  pipe(pipefd);  // create the pipe

  if (type == FGC_TYPE_NOKEYSPACE) {
    // If we are not in key space we still need to acquire the GIL to use the fork api
    RedisModule_ThreadSafeContextLock(ctx);
  }

  if (!lock(ctx)) {
    if (type == FGC_TYPE_NOKEYSPACE) {
      RedisModule_ThreadSafeContextUnlock(ctx);
    }

    return false;
  }

  execState = FGC_STATE_SCANNING;

  cpid = Fork(ctx);  // duplicate the current process

  if (cpid == -1) {
    retryInterval.tv_sec = RSGlobalConfig.forkGcRetryInterval;

    if (type == FGC_TYPE_NOKEYSPACE) {
      RedisModule_ThreadSafeContextUnlock(ctx);
    }

    unlock(ctx);

    close(pipefd[GC_READERFD]);
    close(pipefd[GC_WRITERFD]);

    return true;
  }

  deletedDocsFromLastRun = 0;

  if (type == FGC_TYPE_NOKEYSPACE) {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }

  unlock(ctx);

  retryInterval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;

  if (cpid == 0) {
    setpriority(PRIO_PROCESS, getpid(), 19);
    // fork process
    close(pipefd[GC_READERFD]);
#ifdef __linux__
    if (!haveRedisFork()) {
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
    childScanIndexes();
    close(pipefd[GC_WRITERFD]);
    sleep(RSGlobalConfig.forkGcSleepBeforeExit);
    _exit(EXIT_SUCCESS);
  } else {
    // main process
    close(pipefd[GC_WRITERFD]);
    while (pauseState == FGC_PAUSED_PARENT) {
      execState = FGC_STATE_WAIT_APPLY;
      // spin
      usleep(500);
    }

    execState = FGC_STATE_APPLYING;
    if (parentHandleFromChild() == REDISMODULE_ERR) {
      gcrv = 1;
    }
    close(pipefd[GC_READERFD]);
    if (haveRedisFork()) {

      if (type == FGC_TYPE_NOKEYSPACE) {
        // If we are not in key space we still need to acquire the GIL to use the fork api
        RedisModule_ThreadSafeContextLock(ctx);
      }

      if (!lock(ctx)) {
        if (type == FGC_TYPE_NOKEYSPACE) {
          RedisModule_ThreadSafeContextUnlock(ctx);
        }

        return false;
      }

      // KillForkChild must be called when holding the GIL
      // otherwise it might cause a pipe leak and eventually run
      // out of file descriptor
      RedisModule_KillForkChild(cpid);

      if (type == FGC_TYPE_NOKEYSPACE) {
        RedisModule_ThreadSafeContextUnlock(ctx);
      }

      unlock(ctx);

    } else {
      pid_t id = wait4(cpid, NULL, 0, NULL);
      if (id == -1) {
        printf("an error acquire when waiting for fork to terminate, pid:%d", cpid);
      }
    }
  }
  execState = FGC_STATE_IDLE;
  ts.End();

  long long msRun = ts.DurationMS();

  stats.numCycles++;
  stats.totalMSRun += msRun;
  stats.lastRunTimeMs = msRun;

  return gcrv;
}

//---------------------------------------------------------------------------------------------

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define NO_TSAN_CHECK __attribute__((no_sanitize("thread")))
#endif
#endif
#ifndef NO_TSAN_CHECK
#define NO_TSAN_CHECK
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

void ForkGC::WaitAtFork() NO_TSAN_CHECK {
  RS_LOG_ASSERT(pauseState == 0, "FGC pause state should be 0");
  pauseState = FGC_PAUSED_CHILD;

  while (execState != FGC_STATE_WAIT_FORK) {
    usleep(500);
  }
}

//---------------------------------------------------------------------------------------------

void ForkGC::WaitAtApply() NO_TSAN_CHECK {
  // Ensure that we're waiting for the child to begin
  RS_LOG_ASSERT(pauseState == FGC_PAUSED_CHILD, "FGC pause state should be CHILD");
  RS_LOG_ASSERT(execState == FGC_STATE_WAIT_FORK, "FGC exec state should be WAIT_FORK");

  pauseState = FGC_PAUSED_PARENT;
  while (execState != FGC_STATE_WAIT_APPLY) {
    usleep(500);
  }
}

//---------------------------------------------------------------------------------------------

void ForkGC::WaitClear() NO_TSAN_CHECK {
  pauseState = FGC_PAUSED_UNPAUSED;
  while (execState != FGC_STATE_IDLE) {
    usleep(500);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

void ForkGC::OnTerm() {
  if (keyName && type == FGC_TYPE_INKEYSPACE) {
    RedisModule_FreeString(ctx, (RedisModuleString *)keyName);
  }

  RedisModule_FreeThreadSafeContext(ctx);
  //@@ delete this; // GC::~GC does this
}

//---------------------------------------------------------------------------------------------

void ForkGC::RenderStats(RedisModuleCtx *ctx) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  REPLY_KVNUM(n, "bytes_collected", stats.totalCollected);
  REPLY_KVNUM(n, "total_ms_run", stats.totalMSRun);
  REPLY_KVNUM(n, "total_cycles", stats.numCycles);
  REPLY_KVNUM(n, "avarage_cycle_time_ms", (double)stats.totalMSRun / stats.numCycles);
  REPLY_KVNUM(n, "last_run_time_ms", (double)stats.lastRunTimeMs);
  REPLY_KVNUM(n, "gc_numeric_trees_missed", (double)stats.gcNumericNodesMissed);
  REPLY_KVNUM(n, "gc_blocks_denied", (double)stats.gcBlocksDenied);
  RedisModule_ReplySetArrayLength(ctx, n);
}

//---------------------------------------------------------------------------------------------

void ForkGC::Kill() {
  deleting = true;
}

//---------------------------------------------------------------------------------------------

void ForkGC::OnDelete() {
  ++deletedDocsFromLastRun;
}

//---------------------------------------------------------------------------------------------

struct timespec ForkGC::GetInterval() {
  return retryInterval;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void ForkGC::ctor(const RedisModuleString *k, uint64_t specUniqueId) {
  rdbPossiblyLoading = 1;
  specUniqueId = specUniqueId;
  type = FGC_TYPE_INKEYSPACE;
  deletedDocsFromLastRun = 0;
  retryInterval.tv_sec = RSGlobalConfig.forkGcRunIntervalSec;
  retryInterval.tv_nsec = 0;
  ctx = RedisModule_GetThreadSafeContext(NULL);
  if (k) {
    keyName = RedisModule_CreateStringFromString(ctx, k);
    RedisModule_FreeString(ctx, (RedisModuleString *)k);
  }
}

//---------------------------------------------------------------------------------------------

ForkGC::ForkGC(const RedisModuleString *k, uint64_t specUniqueId) {
  ctor(k, specUniqueId);
}

//---------------------------------------------------------------------------------------------

ForkGC::ForkGC(IndexSpec *spec, uint64_t specUniqueId) {
  ctor(NULL, specUniqueId);
  sp = spec;
  type = FGC_TYPE_NOKEYSPACE;
}

///////////////////////////////////////////////////////////////////////////////////////////////
