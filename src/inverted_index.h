#ifndef __INVERTED_INDEX_H__
#define __INVERTED_INDEX_H__

#include "redisearch.h"
#include "buffer.h"
#include "doc_table.h"
#include "forward_index.h"
#include "index_iterator.h"
#include "index_result.h"
#include "spec.h"
#include "numeric_filter.h"
#include <stdint.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

extern uint64_t TotalIIBlocks;

/* A single block of data in the index. The index is basically a list of blocks we iterate */
typedef struct {
  t_docId firstId;
  t_docId lastId;
  Buffer buf;
  uint16_t numDocs;
} IndexBlock;

typedef struct InvertedIndex {
  IndexBlock *blocks;
  uint32_t size;
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;
  uint32_t gcMarker;
} InvertedIndex;

struct indexReadCtx;

/**
 * This context is passed to the decoder callback, and can contain either
 * a a pointer or integer. It is intended to relay along any kind of additional
 * configuration information to help the decoder determine whether to filter
 * the entry */
typedef struct {
  void *ptr;
  t_fieldMask num;

  // used by profile
  double rangeMin;    
  double rangeMax;
} IndexDecoderCtx;

/**
 * Called when an entry is removed
 */
typedef void (*RepairCallback)(const RSIndexResult *res, void *arg);

typedef struct {
  size_t bytesBeforFix;
  size_t bytesAfterFix;
  size_t bytesCollected; /** out: Number of bytes collected */
  size_t docsCollected;  /** out: Number of documents collected */
  size_t limit;          /** in: how many index blocks to scan at once */

  /** in: Callback to invoke when a document is collected */
  void (*RepairCallback)(const RSIndexResult *, const IndexBlock *, void *);
  /** argument to pass to callback */
  void *arg;
} IndexRepairParams;

/* Create a new inverted index object, with the given flag. If initBlock is 1, we create the first
 * block */
InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock);
IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId);
void indexBlock_Free(IndexBlock *blk);
void InvertedIndex_Free(void *idx);

#define IndexBlock_DataBuf(b) (b)->buf.data
#define IndexBlock_DataLen(b) (b)->buf.offset

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock,
                         IndexRepairParams *params);

/**
 * Decode a single record from the buffer reader. This function is responsible for:
 * (1) Decoding the record at the given position of br
 * (2) Advancing the reader's position to the next record
 * (3) Filtering the record based on any relevant information (can be passed through `ctx`)
 * (4) Populating `res` with the information from the record.
 *
 * If the record should not be processed, it should not be populated and 0 should
 * be returned. Otherwise, the function should return 1.
 */
typedef int (*IndexDecoder)(BufferReader *br, const IndexDecoderCtx *ctx, RSIndexResult *res);

struct IndexReader;
/**
 * Custom implementation of a seeking function. Seek to the specific ID within
 * the index, or at one position after it.
 *
 * The implementation of this function is optional. If this is not used, then
 * the decoder() implementation will be used instead.
 */
typedef int (*IndexSeeker)(BufferReader *br, const IndexDecoderCtx *ctx, struct IndexReader *ir,
                           t_docId to, RSIndexResult *res);

typedef struct {
  IndexDecoder decoder;
  IndexSeeker seeker;
} IndexDecoderProcs;

/* Get the decoder for the index based on the index flags. This is used to externally inject the
 * endoder/decoder when reading and writing */
IndexDecoderProcs InvertedIndex_GetDecoder(uint32_t flags);

/* An IndexReader wraps an inverted index record for reading and iteration */
typedef struct IndexReader {
  const IndexSpec *sp;

  // the underlying data buffer
  BufferReader br;

  InvertedIndex *idx;
  // last docId, used for delta encoding/decoding
  t_docId lastId;
  uint32_t currentBlock;

  /* The decoder's filtering context. It may be a number or a pointer. The number is used for
   * filtering field masks, the pointer for numeric filtering */
  IndexDecoderCtx decoderCtx;
  /* The decoding function for reading the index */
  IndexDecoderProcs decoders;

  /* The number of records read */
  size_t len;

  /* The record we are decoding into */
  RSIndexResult *record;

  int atEnd_;

  // If present, this pointer is updated when the end has been reached. This is
  // an optimization to avoid calling IR_HasNext() each time
  uint8_t *isValidP;

  /* This marker lets us know whether the garbage collector has visited this index while the reading
   * thread was asleep, and reset the state in a deeper way
   */
  uint32_t gcMarker;
} IndexReader;

void IndexReader_OnReopen(void *privdata);

/* An index encoder is a callback that writes records to the index. It accepts a pre-calculated
 * delta for encoding */
typedef size_t (*IndexEncoder)(BufferWriter *bw, uint32_t delta, RSIndexResult *record);

/* Write a ForwardIndexEntry into an indexWriter. Returns the number of bytes written to the index
 */
size_t InvertedIndex_WriteForwardIndexEntry(InvertedIndex *idx, IndexEncoder encoder,
                                            ForwardIndexEntry *ent);

/* Write a numeric index entry to the index. it includes only a float value and docId. Returns the
 * number of bytes written */
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, double value);

size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *entry);
/* Create a new index reader for numeric records, optionally using a given filter. If the filter
 * is
 * NULL we will return all the records in the index */
IndexReader *NewNumericReader(const IndexSpec *sp, InvertedIndex *idx, const NumericFilter *flt,
                              double rangeMin, double rangeMax);

/* Get the appropriate encoder for an inverted index given its flags. Returns NULL on invalid flags
 */
IndexEncoder InvertedIndex_GetEncoder(IndexFlags flags);

/* Create a new index reader on an inverted index buffer,
 * optionally with a skip index, docTable and scoreIndex.
 * If singleWordMode is set to 1, we ignore the skip index and use the score
 * index.
 */
IndexReader *NewTermIndexReader(InvertedIndex *idx, IndexSpec *sp, t_fieldMask fieldMask,
                                RSQueryTerm *term, double weight);

void IR_Abort(void *ctx);

/* free an index reader */
void IR_Free(IndexReader *ir);

/* Read an entry from an inverted index */
int IR_GenericRead(IndexReader *ir, RSIndexResult *res);

/* Read an entry from an inverted index into RSIndexResult */
int IR_Read(void *ctx, RSIndexResult **e);

/* Move to the next entry in an inverted index, without reading the whole entry
 */
int IR_Next(void *ctx);

/**
 * Skip to a specific document ID in the index, or one position after it
 * @param ctx the index reader
 * @param docId the document ID to search for
 * @param hit where to store the result pointer
 *
 * @return:
 *  - INDEXREAD_OK if the id was found
 *  - INDEXREAD_NOTFOUND if the reader is at the next position
 *  - INDEXREAD_EOF if the ID is out of the upper range
 */
int IR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit);

RSIndexResult *IR_Current(void *ctx);

/* The number of docs in an inverted index entry */
size_t IR_NumDocs(void *ctx);

/* LastDocId of an inverted index stateful reader */
t_docId IR_LastDocId(void *ctx);

/* Create a reader iterator that iterates an inverted index record */
IndexIterator *NewReadIterator(IndexReader *ir);

int IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, IndexRepairParams *params);

static inline double CalculateIDF(size_t totalDocs, size_t termDocs) {
  return logb(1.0F + totalDocs / (termDocs ? termDocs : (double)1));
}

#ifdef __cplusplus
}
#endif
#endif
