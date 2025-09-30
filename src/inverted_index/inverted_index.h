/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INVERTED_INDEX_H__
#define __INVERTED_INDEX_H__

#include "redisearch.h"
#include "buffer/buffer.h"
#include "doc_table.h"
#include "spec.h"
#include "numeric_filter.h"
#include <stdint.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

// The number of entries in each index block. A new block will be created after every N entries
#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_SIZE_DOCID_ONLY 1000

size_t TotalIIBlocks();

/* A single block of data in the index. The index is basically a list of blocks we iterate */
typedef struct {
  t_docId firstId;
  t_docId lastId;
  Buffer buf;
  uint16_t numEntries;  // Number of entries (i.e., docs)
} IndexBlock;

typedef struct InvertedIndex {
  IndexBlock *blocks; // Array containing the inverted index blocks
  uint32_t size;      // Number of blocks
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;   // Number of documents in the index
  uint32_t gcMarker;
  // The following union must remain at the end as memory is not allocated for it
  // if not required (see function `NewInvertedIndex`)
  union {
    t_fieldMask fieldMask;
    uint64_t numEntries;
  };
} InvertedIndex;

typedef struct IndexBlockReader {
  BufferReader buffReader;
  t_docId curBaseId; // The current value to add to the decoded delta, to get the actual docId.
} IndexBlockReader;

//--------- GC related types

typedef struct {
  size_t bytesBeforFix;
  size_t bytesAfterFix;
  size_t bytesCollected;    /** out: Number of bytes collected */
  size_t docsCollected;     /** out: Number of documents collected */
  size_t entriesCollected;  /** out: Number of entries collected */
  size_t limit;          /** in: how many index blocks to scan at once */

  /** in: Callback to invoke when a document is collected */
  void (*RepairCallback)(const RSIndexResult *, const IndexBlock *, void *);
  /** argument to pass to callback */
  void *arg;
} IndexRepairParams;

// opaque handle
typedef struct InvertedIndexGcDelta InvertedIndexGcDelta;

// input shims (layout-compatible with the MSG_* the child/parent already use)
typedef struct {
  IndexBlock blk;
  int64_t oldix;
  int64_t newix;
} InvertedIndex_RepairedInput;

typedef struct {
  void *ptr;
  uint32_t oldix;
} InvertedIndex_DeletedInput;

// -------------------------

// Create a new inverted index object, with the given flag.
// The out parameter memsize must be not NULL, the total of allocated memory
// will be returned in it
InvertedIndex *NewInvertedIndex(IndexFlags flags, size_t *memsize);

/* Add a new block to the index with a given document id as the initial id
  * Returns the new block
  * in/out parameter memsize must be not NULL, because the size (bytes) of the
  * new block is added to it
*/
IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId, size_t *memsize);
size_t indexBlock_Free(IndexBlock *blk);
void InvertedIndex_Free(InvertedIndex *idx);

IndexBlock *InvertedIndex_BlockRef(const InvertedIndex *idx, size_t blockIndex);
IndexBlock InvertedIndex_Block(InvertedIndex *idx, size_t blockIndex);
void InvertedIndex_SetBlock(InvertedIndex *idx, size_t blockIndex, IndexBlock block);
void InvertedIndex_SetBlocks(InvertedIndex *idx, IndexBlock *blocks, size_t size);
size_t InvertedIndex_BlocksShift(InvertedIndex *idx, size_t shift);
size_t InvertedIndex_NumBlocks(const InvertedIndex *idx);
void InvertedIndex_SetNumBlocks(InvertedIndex *idx, size_t numBlocks);
IndexFlags InvertedIndex_Flags(const InvertedIndex *idx);
t_docId InvertedIndex_LastId(const InvertedIndex *idx);
void InvertedIndex_SetLastId(InvertedIndex *idx, t_docId lastId);
uint32_t InvertedIndex_NumDocs(const InvertedIndex *idx);
void InvertedIndex_SetNumDocs(InvertedIndex *idx, uint32_t numDocs);
uint32_t InvertedIndex_GcMarker(const InvertedIndex *idx);
void InvertedIndex_SetGcMarker(InvertedIndex *idx, uint32_t marker);
t_fieldMask InvertedIndex_FieldMask(const InvertedIndex *idx);
uint64_t InvertedIndex_NumEntries(const InvertedIndex *idx);
void InvertedIndex_SetNumEntries(InvertedIndex *idx, uint64_t numEntries);

/* Retrieve comprehensive summary information about an inverted index */
IISummary InvertedIndex_Summary(const InvertedIndex *idx);

/* Retrieve basic summary information about an inverted index's blocks. The returned array should
 * be freed using `InvertedIndex_BlocksSummaryFree` */
IIBlockSummary *InvertedIndex_BlocksSummary(const InvertedIndex *idx, size_t *count);

/* Free the blocks summary */
void InvertedIndex_BlocksSummaryFree(IIBlockSummary *summaries);

t_docId IndexBlock_FirstId(const IndexBlock *b);
t_docId IndexBlock_LastId(const IndexBlock *b);
uint16_t IndexBlock_NumEntries(const IndexBlock *b);
char *IndexBlock_Data(const IndexBlock *b);
char **IndexBlock_DataPtr(IndexBlock *b);
void IndexBlock_DataFree(const IndexBlock *b);
size_t IndexBlock_Cap(const IndexBlock *b);
void IndexBlock_SetCap(IndexBlock *b, size_t cap);
size_t IndexBlock_Len(const IndexBlock *b);
size_t *IndexBlock_LenPtr(IndexBlock *b);
Buffer *IndexBlock_Buffer(IndexBlock *b);
void IndexBlock_SetBuffer(IndexBlock *b, Buffer buf);

/**
 * Decode a single record from the buffer reader. This function is responsible for:
 * (1) Decoding the record at the given position of br
 * (2) Advancing the reader's position to the next record
 * (3) Filtering the record based on any relevant information (can be passed through `ctx`)
 * (4) Populating `res` with the information from the record.
 * (5) Setting `br->curOffset` for reading the next record
 *
 * If the record should not be processed, it should not be populated and 0 should
 * be returned. Otherwise, the function should return 1.
 */
typedef bool (*IndexDecoder)(IndexBlockReader *, const IndexDecoderCtx *, RSIndexResult *out);

/**
 * Custom implementation of a seeking function. Seek to the specific ID within
 * the index, or at one position after it.
 *
 * The implementation of this function is optional. If this is not used, then
 * the decoder() implementation will be used instead.
 */
typedef bool (*IndexSeeker)(IndexBlockReader *, const IndexDecoderCtx *, t_docId to, RSIndexResult *out);

typedef struct {
  IndexDecoder decoder;
  IndexSeeker seeker;
} IndexDecoderProcs;

typedef struct IndexReader {
  const InvertedIndex *idx;

  // the underlying data buffer iterator
  IndexBlockReader blockReader;

  /* The decoding function for reading the index */
  IndexDecoderProcs decoders;
  /* The decoder's filtering context. It may be a number or a pointer. The number is used for
   * filtering field masks, the pointer for numeric filtering */
  IndexDecoderCtx decoderCtx;

  uint32_t currentBlock;

  /* This marker lets us know whether the garbage collector has visited this index while the reading
   * thread was asleep, and reset the state in a deeper way
   */
  uint32_t gcMarker;
} IndexReader;

/* Make a new inverted index reader. It should be freed using `IndexReader_Free`. */
IndexReader *NewIndexReader(const InvertedIndex *idx, IndexDecoderCtx ctx);

/* Free an index reader created using `NewIndexReader` */
void IndexReader_Free(IndexReader *ir);

/* Reset the index reader to the start of the index */
void IndexReader_Reset(IndexReader *ir);

/* Get the estimated number of documents in the index */
size_t IndexReader_NumEstimated(const IndexReader *ir);

/* Check if the index reader is reading from the given index */
bool IndexReader_IsIndex(const IndexReader *ir, const InvertedIndex *idx);

/* Revalidate the index reader in case the index underwent GC while we were asleep.
 * Returns true if revalidation is needed (i.e., GC happened) by anything using the
 * reader, false otherwise */
bool IndexReader_Revalidate(IndexReader *ir);

/* Check if the index reader's decoder has a seeker implementation */
bool IndexReader_HasSeeker(const IndexReader *ir);

/* Read the next record of the index onto `res`. Returns false when there is nothing
 * more to read. */
bool IndexReader_Next(IndexReader *ir, RSIndexResult *res);

/* Skip to the block that may contain the given docId. Returns false if the
 * docId is beyond the last id of the index */
bool IndexReader_SkipTo(IndexReader *ir, t_docId docId);

/* Seek to the given docId, or the next one after it. Returns true if a record
 * was found, false otherwise. If true is returned, `res` is populated with
 * the record found */
bool IndexReader_Seek(IndexReader *ir, t_docId docId, RSIndexResult *res);

/* Check if the index holds multi-value entries */
bool IndexReader_HasMulti(const IndexReader *ir);

/* Get the index flags */
IndexFlags IndexReader_Flags(const IndexReader *ir);

/* Get the numeric filter used by the index reader's decoder, if any */
const NumericFilter *IndexReader_NumericFilter(const IndexReader *ir);

/* Swap the inverted index of the reader with the supplied index. This is used by
 * tests to trigger a revalidation. */
void IndexReader_SwapIndex(IndexReader *ir, const InvertedIndex *newIdx);

/* Get the inverted index of the reader. This is only needed for some tests. */
InvertedIndex *IndexReader_II(const IndexReader *ir);

/* Get the decoder for the index based on the index flags. This is used to externally inject the
 * endoder/decoder when reading and writing */
IndexDecoderProcs InvertedIndex_GetDecoder(uint32_t flags);

/* An index encoder is a callback that writes records to the index. It accepts a pre-calculated
 * delta for encoding */
typedef size_t (*IndexEncoder)(BufferWriter *bw, t_docId delta, RSIndexResult *record);

// Create a new IndexBlockReader for a buffer. This is only used by benchmark functions
// for decoders
IndexBlockReader NewIndexBlockReader(BufferReader *buff, t_docId curBaseId);

// Create a new IndexDecoderCtx with a default numeric filter. Used only benchmarks
IndexDecoderCtx NewIndexDecoderCtx_NumericFilter();

// Create a new IndexDecoderCtx with a mask filter. Used only in benchmarks.
IndexDecoderCtx NewIndexDecoderCtx_MaskFilter(uint32_t mask);

/* Wrapper around the static encodeFreqsOnly to be able to access it in the Rust benchmarks. */
size_t encode_freqs_only(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFull to be able to access it in the Rust benchmarks. */
size_t encode_full(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFullWide to be able to access it in the Rust benchmarks. */
size_t encode_full_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFreqsFields to be able to access it in the Rust benchmarks. */
size_t encode_freqs_fields(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFreqsFieldsWide to be able to access it in the Rust benchmarks. */
size_t encode_freqs_fields_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFieldsOnly to be able to access it in the Rust benchmarks. */
size_t encode_fields_only(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFieldsOnlyWide to be able to access it in the Rust benchmarks. */
size_t encode_fields_only_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFieldsOffsets to be able to access it in the Rust benchmarks. */
size_t encode_fields_offsets(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static  encodeFieldsOffsetsWide to be able to access it in the Rust benchmarks. */
size_t encode_fields_offsets_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeOffsetsOnly to be able to access it in the Rust benchmarks. */
size_t encode_offsets_only(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeFreqsOffsets to be able to access it in the Rust benchmarks. */
size_t encode_freqs_offsets(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeNumeric to be able to access it in the Rust benchmarks */
size_t encode_numeric(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeDocIdsOnly to be able to access it in the Rust benchmarks */
size_t encode_docs_ids_only(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static encodeRawDocIdsOnly to be able to access it in the Rust benchmarks */
size_t encode_raw_doc_ids_only(BufferWriter *bw, t_docId delta, RSIndexResult *res);

/* Wrapper around the static readFreqs to be able to access it in the Rust benchmarks */
bool read_freqs(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFreqsFlags to be able to access it in the Rust benchmarks */
bool read_freqs_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFreqsFlagsWide to be able to access it in the Rust benchmarks */
bool read_freqs_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFlags to be able to access it in the Rust benchmarks */
bool read_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFlagsWide to be able to access it in the Rust benchmarks */
bool read_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFreqOffsetsFlags to be able to access it in the Rust benchmarks */
bool read_freq_offsets_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFreqOffsetsFlagsWide to be able to access it in the Rust benchmarks */
bool read_freq_offsets_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFlagsOffsets to be able to access it in the Rust benchmarks */
bool read_fields_offsets(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFlagsOffsetsWide to be able to access it in the Rust benchmarks */
bool read_fields_offsets_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readOffsetsOnly to be able to access it in the Rust benchmarks */
bool read_offsets_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readFreqsOffsets to be able to access it in the Rust benchmarks */
bool read_freqs_offsets(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readNumeric to be able to access it in the Rust benchmarks */
bool read_numeric(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readDocIdsOnly to be able to access it in the Rust benchmarks */
bool read_doc_ids_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Wrapper around the static readRawDocIdsOnly to be able to access it in the Rust benchmarks */
bool read_raw_doc_ids_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res);

/* Write a numeric index entry to the index. it includes only a float value and docId. Returns the
 * number of bytes written */
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, double value);

size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, RSIndexResult *entry);

/* Get the appropriate encoder for an inverted index given its flags. Returns NULL on invalid flags
 */
IndexEncoder InvertedIndex_GetEncoder(IndexFlags flags);

unsigned long InvertedIndex_MemUsage(const InvertedIndex *value);

// ----- GC related API

typedef struct {
  // Number of blocks prior to repair
  uint32_t nblocksOrig;
  // Number of blocks repaired
  uint32_t nblocksRepaired;
  // Number of bytes cleaned in inverted index
  uint64_t nbytesCollected;
  // Number of bytes added to inverted index
  uint64_t nbytesAdded;
  // Number of document records removed
  uint64_t ndocsCollected;
  // Number of numeric records removed
  uint64_t nentriesCollected;

  /** Specific information about the _last_ index block */
  size_t lastblkDocsRemoved;
  size_t lastblkBytesCollected;
  size_t lastblkNumEntries;
  size_t lastblkEntriesRemoved;

  uint64_t gcBlocksDenied;
} II_GCScanStats;

/* Repair an index block by removing garbage - records pointing at deleted documents,
 * and write valid entries in their place.
 * Returns the number of docs collected, and puts the number of bytes collected in the given
 * pointer.
 */
size_t IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, IndexRepairParams *params);

/* Apply a GC change set to an inverted index.
 * nblocks_orig is the number of blocks the child had scanned,
 * nbytes_added_io is the child reported "bytes added" figure used if a fresh block,
 * must be created when all scanned blocks were deleted. Updated for blocks that are added.
 *
 * delta requires ownership as data of new, deleted and repaired blocks
 * are moved into the given index, the pointers of delta are updated accordingly.
 */
void InvertedIndex_ApplyGcDelta(InvertedIndex *idx,
                                 InvertedIndexGcDelta *d,
                                 II_GCScanStats *info);

bool InvertedIndex_GcDelta_GetLastBlockIgnored(InvertedIndexGcDelta *d);
void InvertedIndex_GcDelta_Free(InvertedIndexGcDelta *d, II_GCScanStats *info);

// --------------------- II High Level GC API

typedef struct {
  void *ctx;
  void (*write)(void *ctx, const void *buf, size_t len);
} II_GCWriter;

typedef struct {
  void *ctx;
  // read exactly len or return nonzero
  int (*read)(void *ctx, void *buf, size_t len);
} II_GCReader;

typedef struct {
  void *ctx;
  void (*call)(void *ctx);
} II_GCCallback;

// ------------------------ GC Scan

/**
 * II_GCCallback is invoked before the inverted index is written, only
 * if the inverted index was repaired.
 * This function writes to the receiver an info message with general info on the inverted index garbage collection.
 * In addition, for each fixed block it writes a repair message. For deleted blocks it writes a delete message.
 * If the index size (number of blocks) wasn't modified (no deleted blocks) we don't write a new block list.
 * In this case, the receiver will get the modifications from the fix messages, that contains also a copy of the
 * repaired block.
 */
bool InvertedIndex_GcDelta_Scan(II_GCWriter *wr, RedisSearchCtx *sctx, InvertedIndex *idx,
                                     II_GCCallback *cb, IndexRepairParams *params);

/**
 * Read info written by InvertedIndex_GcDelta_Scan
 *
 * InvertedIndexGcDelta memory ownership is passed to the caller and should be freed by it
 */
InvertedIndexGcDelta *InvertedIndex_GcDelta_Read(II_GCReader *rd, II_GCScanStats *info);

// ---------------------

#ifdef __cplusplus
}
#endif
#endif
