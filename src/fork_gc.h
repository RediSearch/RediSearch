
#pragma once

#include "gc.h"
#include "search_ctx.h"
#include "inverted_index.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "index_result.h"
#include "object.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct ForkGCStats {
  // total bytes collected by the GC
  size_t totalCollected;
  // number of cycle ran
  size_t numCycles;

  long long totalMSRun;
  long long lastRunTimeMs;

  uint64_t gcNumericNodesMissed;
  uint64_t gcBlocksDenied;
};

//---------------------------------------------------------------------------------------------

enum FGCType {
  FGC_TYPE_INKEYSPACE,
  FGC_TYPE_NOKEYSPACE
};

enum FGCError {
  // Terms have been collected
  FGC_COLLECTED,
  // No more terms remain
  FGC_DONE,
  // Pipe error, child probably crashed
  FGC_CHILD_ERROR,
  // Error on the parent
  FGC_PARENT_ERROR
};

//---------------------------------------------------------------------------------------------

struct MSG_IndexInfo {
  // Number of blocks prior to repair
  uint32_t nblocksOrig;
  // Number of blocks repaired
  uint32_t nblocksRepaired;
  // Number of bytes cleaned in inverted index
  uint64_t nbytesCollected;
  // Number of document records removed
  uint64_t ndocsCollected;

  // Specific information about the _last_ index block
  size_t lastblkDocsRemoved;
  size_t lastblkBytesCollected;
  size_t lastblkNumDocs;

  MSG_IndexInfo(uint32_t nblocksOrig = 0) : nblocksOrig(nblocksOrig) {}
};

// Structure sent describing an index block
struct MSG_RepairedBlock {
  IndexBlock blk;
  int64_t oldix;  // Old position of the block
  int64_t newix;  // New position of the block
  // the actual content of the block follows...
};

struct MSG_DeletedBlock {
  void *ptr;       // Address of the buffer to free
  uint32_t oldix;  // Old index of deleted block
  uint32_t _pad;   // Uninitialized reads, otherwise
};

//---------------------------------------------------------------------------------------------

//KHASH_MAP_INIT_INT64(cardvals, size_t)

struct NumericIndexBlockRepair : IndexBlockRepair {
  NumericIndexBlockRepair(const InvertedIndex &idx);

  const IndexBlock *lastblk;
  UnorderedMap<uint64_t, size_t> delLast;
  UnorderedMap<uint64_t, size_t> delRest;

  void countDeleted(const NumericResult *r, const IndexBlock *blk);

  virtual void collect(const IndexResult &r, const IndexBlock &blk) {
    const NumericResult *nr = dynamic_cast<const NumericResult*>(&r);
    if (nr)
      countDeleted(nr, &blk);
	}
};

union numUnion {
  uint64_t u64;
  double d48;
};

//---------------------------------------------------------------------------------------------

struct IndexRepair {
  struct ForkGC &fgc;
  IndexRepair(struct ForkGC &fgc) : fgc(fgc) {}
	virtual void sendHeader() = 0;
};

struct InvertedIndexRepair : IndexRepair {
	InvertedIndexRepair(struct ForkGC &fgc, char *term, size_t termLen) : IndexRepair(fgc), term(term), termLen(termLen) {}

  char *term;
  size_t termLen;

	void sendHeader();
};

struct NumericAndTagIndexRepair : IndexRepair {
  NumericAndTagIndexRepair(struct ForkGC &fgc, const FieldSpec &field, uint64_t uniqueId) :
	  IndexRepair(fgc), field(field.name), uniqueId(uniqueId), idx(NULL), sentFieldName(false) {}

  const char *field;
  uint64_t uniqueId;
  const void *idx;
  bool sentFieldName;

  void sendHeader();
};

struct NumericIndexRepair : NumericAndTagIndexRepair {
  NumericIndexRepair(struct ForkGC &fgc, const FieldSpec &field, const NumericRangeTree &tree) :
	  NumericAndTagIndexRepair(fgc, field, tree.uniqueId) {}

  void set(const NumericRangeNode *invidx) {
    idx = invidx;
  }

};

struct TagIndexRepair : NumericAndTagIndexRepair {
  TagIndexRepair(struct ForkGC &fgc, const FieldSpec &field, const TagIndex &tree) :
	  NumericAndTagIndexRepair(fgc, field, tree.uniqueId) {}

  void set(const InvertedIndex *invidx) {
    idx = invidx;
  }
};

//---------------------------------------------------------------------------------------------

struct IndexBlock;

struct InvIdxBuffers {
  MSG_DeletedBlock *delBlocks;
  size_t numDelBlocks;

  MSG_RepairedBlock *changedBlocks;

  IndexBlock *newBlocklist;
  size_t newBlocklistSize;
  int lastBlockIgnored;
};

//---------------------------------------------------------------------------------------------

struct NumericRangeNode;
struct CardinalityValue;

struct NumGcInfo {
  // Node in the tree that was GC'd
  NumericRangeNode *node;
  CardinalityValue *lastBlockDeleted;
  CardinalityValue *restBlockDeleted;
  size_t nlastBlockDel;
  size_t nrestBlockDel;
  InvIdxBuffers idxbufs;
  MSG_IndexInfo info;
};

//---------------------------------------------------------------------------------------------

struct InvertedIndex;

// Internal definition of the garbage collector context (each index has one)
struct ForkGC : public Object, public GCAPI {
  ForkGC(const RedisModuleString *k, uint64_t specUniqueId);
  ForkGC(IndexSpec *sp, uint64_t specUniqueId);

  void ctor(const RedisModuleString *k, uint64_t specUniqueId);

  // inverted index key name for reopening the index
  union {
    const RedisModuleString *keyName;
    IndexSpec *sp;
  };

  RedisModuleCtx *ctx;

  FGCType type;

  uint64_t specUniqueId;

  // statistics for reporting
  ForkGCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;
  // Whether the gc has been requested for deletion
  volatile bool deleting;
  int pipefd[2];
  volatile uint32_t pauseState;
  volatile uint32_t execState;

  struct timespec retryInterval;
  volatile size_t deletedDocsFromLastRun;

  virtual bool PeriodicCallback(RedisModuleCtx* ctx);
  virtual void RenderStats(RedisModuleCtx* ctx);
  virtual void OnDelete();
  virtual void OnTerm();
  virtual void Kill();
  virtual struct timespec GetInterval();
  virtual RedisModuleCtx *GetRedisCtx() { return ctx; }

  // Indicate that the gc should wait immediately prior to forking.
  // This is in order to perform some commands which may not be visible by the fork gc engine.
  // This function will return before the fork is performed.
  // You must call WaitAtApply or WaitClear to allow the GC to resume functioning.
  void WaitAtFork();

  // Indicate that the GC should unpause from WaitAtFork, and instead wait before the changes are applied.
  // This is in order to change the state of the index at the parent.
  void WaitAtApply();

  // Don't perform diagnostic waits
  void WaitClear();

//private:
  bool lock(RedisModuleCtx *ctx);
  void unlock(RedisModuleCtx *ctx);
  RedisSearchCtx *getSctx(RedisModuleCtx *ctx);
  void updateStats(RedisSearchCtx *sctx, size_t recordsRemoved, size_t bytesCollected);
  void sendFixed(const void *buff, size_t len);
  template <class T>
  void sendVar(const T &x) { sendFixed(reinterpret_cast<const T*>(&x), sizeof(x)); }
  void sendBuffer(const void *buff, size_t len);
  void sendTerminator();

  int recvFixed(void *buf, size_t len);
  int tryRecvFixed(void *obj, size_t len); //@@ Why do we need it for?
  int recvBuffer(void **buf, size_t *len);
  int tryRecvBuffer(void **buf, size_t *len); //@@ looks like nobody was using it
  int recvRepairedBlock(MSG_RepairedBlock *binfo);
  int recvInvIdx(InvIdxBuffers *bufs, MSG_IndexInfo *info);

  bool childRepairInvIdx(RedisSearchCtx *sctx, InvertedIndex *idx, IndexRepair &indexrepair, IndexBlockRepair &blockrepair);
  void sendHeaderString(struct iovec *iov);
  void sendNumericTagHeader(void *arg);

  void childScanIndexes();
  void childCollectTerms(RedisSearchCtx *sctx);
  void childCollectNumeric(RedisSearchCtx *sctx);
  void childCollectTags(RedisSearchCtx *sctx);

  int parentHandleFromChild();
  FGCError parentHandleTerms(RedisModuleCtx *rctx);
  FGCError parentHandleNumeric(RedisModuleCtx *rctx);
  FGCError parentHandleTags(RedisModuleCtx *rctx);
  FGCError recvNumericTagHeader(char **fieldName, size_t *fieldNameLen, uint64_t *id);
  FGCError recvNumIdx(NumGcInfo *ninfo);

  bool haveRedisFork();
  int Fork(RedisModuleCtx *ctx);

  void sendKht(const UnorderedMap<uint64_t, size_t> &kh);
  void checkLastBlock(InvIdxBuffers *idxData, MSG_IndexInfo *info, InvertedIndex *idx);
  void applyInvertedIndex(InvIdxBuffers *idxData, MSG_IndexInfo *info, InvertedIndex *idx);
  void applyNumIdx(RedisSearchCtx *sctx, NumGcInfo *ninfo);

  int recvCardvals(CardinalityValue **tgt, size_t *len);
};

//---------------------------------------------------------------------------------------------

enum FGCPauseFlags {
  // Normal "open" state. No pausing will happen
  FGC_PAUSED_UNPAUSED = 0x00,
  // Prevent invoking the child. The child is not invoked until this flag is
  // cleared
  FGC_PAUSED_CHILD = 0x01,
  // Prevent the parent reading from the child. The results from the child are
  // not read until this flag is cleared.
  FGC_PAUSED_PARENT = 0x02
};

//---------------------------------------------------------------------------------------------

enum FGCState {
  // Idle, "normal" state
  FGC_STATE_IDLE = 0,

  // Set when the PAUSED_CHILD flag is set, indicates that we are
  // awaiting this flag to be cleared.
  FGC_STATE_WAIT_FORK,

  // Set when the child has been launched, but before the first results have
  // been applied.
  FGC_STATE_SCANNING,

  // Set when the PAUSED_PARENT flag is set. The results will not be
  // scanned until the PAUSED_PARENT flag is unset
  FGC_STATE_WAIT_APPLY,

  // Set when results are being applied from the child to the parent
  FGC_STATE_APPLYING
};

///////////////////////////////////////////////////////////////////////////////////////////////
