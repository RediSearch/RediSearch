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

/* A single block of data in the index. The index is basically a list of blocks we iterate */
typedef struct {
  t_docId firstId;
  t_docId lastId;
  uint16_t numDocs;

  Buffer *data;
} IndexBlock;

typedef struct {
  IndexBlock *blocks;
  uint32_t size;
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;
} InvertedIndex;

struct indexReadCtx;

/**
 * This context is passed to the decoder callback, and can contain either
 * a a pointer or integer. It is intended to relay along any kind of additional
 * configuration information to help the decoder determine whether to filter
 * the entry */
typedef union {
  void *ptr;
  uint32_t num;
} IndexDecoderCtx;

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock);
void InvertedIndex_Free(void *idx);
int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num);

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
typedef int (*IndexDecoder)(BufferReader *br, IndexDecoderCtx ctx, RSIndexResult *res);

/* An IndexReader wraps an inverted index record for reading and iteration */
typedef struct indexReadCtx {
  // the underlying data buffer
  BufferReader br;

  InvertedIndex *idx;
  // last docId, used for delta encoding/decoding
  t_docId lastId;
  uint32_t currentBlock;
  // // skip index. If not null and is needed, will be used for intersects
  IndexDecoderCtx decoderCtx;
  IndexDecoder decoder;

  size_t len;
  RSIndexResult *record;

  int atEnd;
} IndexReader;

typedef size_t (*IndexEncoder)(BufferWriter *bw, t_docId lastId, RSIndexResult *record);

size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *record);

/* Write a ForwardIndexEntry into an indexWriter, updating its score and skip
 * indexes if needed. Returns the number of bytes written to the index */
size_t InvertedIndex_WriteForwardIndexEntry(InvertedIndex *idx, IndexEncoder encoder,
                                            ForwardIndexEntry *ent);

size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, float value);

IndexReader *NewIndexReaderGeneric(InvertedIndex *idx, IndexDecoder decoder,
                                   IndexDecoderCtx decoderCtx, RSIndexResult *record);

IndexReader *NewNumericReader(InvertedIndex *idx, NumericFilter *flt);

IndexEncoder InvertedIndex_GetEncoder(IndexFlags flags);

/* Create a new index reader on an inverted index buffer,
* optionally with a skip index, docTable and scoreIndex.
* If singleWordMode is set to 1, we ignore the skip index and use the score
* index.
*/
IndexReader *NewTermIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
                                RSQueryTerm *term);

/* free an index reader */
void IR_Free(IndexReader *ir);

/* Read an entry from an inverted index */
int IR_GenericRead(IndexReader *ir, RSIndexResult *res);

/* Read an entry from an inverted index into RSIndexResult */
int IR_Read(void *ctx, RSIndexResult **e);

/* Move to the next entry in an inverted index, without reading the whole entry
 */
int IR_Next(void *ctx);

/* Can we read more from an index reader? */
int IR_HasNext(void *ctx);

/* Skip to a specific docId in a reader,using the skip index, and read the entry
 * there */
int IR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);

RSIndexResult *IR_Current(void *ctx);

/* The number of docs in an inverted index entry */
size_t IR_NumDocs(void *ctx);

/* LastDocId of an inverted index stateful reader */
t_docId IR_LastDocId(void *ctx);

/* Seek the inverted index reader to a specific offset and set the last docId */
void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId);

/* Create a reader iterator that iterates an inverted index record */
IndexIterator *NewReadIterator(IndexReader *ir);

#endif