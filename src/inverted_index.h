#ifndef __INVERTED_INDEX_H__
#define __INVERTED_INDEX_H__

#include "buffer.h"
#include "doc_table.h"
#include "forward_index.h"
#include "index_iterator.h"
#include "index_result.h"
#include "spec.h"
#include "types.h"
#include <stdint.h>

/* A single block of data in the index. The index is basically a list of blocks we iterate */
typedef struct {
  t_docId firstId;
  t_docId lastId;
  uint16_t numDocs;

  Buffer data;
} IndexBlock;

typedef struct {
  IndexBlock *blocks;
  uint32_t size;
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;
} InvertedIndex;

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock);
void InvertedIndex_Free(void *idx);
int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num);

/* An IndexReader wraps an inverted index record for reading and iteration */
typedef struct indexReadCtx {
  // the underlying data buffer
  BufferReader br;

  InvertedIndex *idx;
  // last docId, used for delta encoding/decoding
  t_docId lastId;
  uint32_t currentBlock;
  // // skip index. If not null and is needed, will be used for intersects
  // SkipIndex *skipIdx;
  // u_int skipIdxPos;
  DocTable *docTable;

  uint8_t fieldMask;

  IndexFlags flags;

  int singleWordMode;

  size_t len;
  IndexRecord record;
  Term *term;
} IndexReader;

/* Write a ForwardIndexEntry into an indexWriter, updating its score and skip
 * indexes if needed.
 * Returns the number of bytes written to the index */
size_t InvertedIndex_WriteEntry(InvertedIndex *idx, ForwardIndexEntry *ent);

/* Create a new index reader on an inverted index buffer,
* optionally with a skip index, docTable and scoreIndex.
* If singleWordMode is set to 1, we ignore the skip index and use the score
* index.
*/
IndexReader *NewIndexReader(InvertedIndex *idx, DocTable *docTable, uint8_t fieldMask,
                            IndexFlags flags, Term *term, int singleWordMode);
/* free an index reader */
void IR_Free(IndexReader *ir);

/* Read an entry from an inverted index */
int IR_GenericRead(IndexReader *ir, t_docId *docId, uint32_t *freq, u_char *flags,
                   VarintVector *offsets);

int IR_TryRead(IndexReader *ir, t_docId *docId, t_docId expectedDocId);

/* Read an entry from an inverted index into IndexResult */
int IR_Read(void *ctx, IndexResult *e);

/* Move to the next entry in an inverted index, without reading the whole entry
 */
int IR_Next(void *ctx);

/* Can we read more from an index reader? */
int IR_HasNext(void *ctx);

/* Skip to a specific docId in a reader,using the skip index, and read the entry
 * there */
int IR_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit);

/* The number of docs in an inverted index entry */
size_t IR_NumDocs(void *ctx);

/* LastDocId of an inverted index stateful reader */
t_docId IR_LastDocId(void *ctx);

/* Seek the inverted index reader to a specific offset and set the last docId */
void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId);

/* Create a reader iterator that iterates an inverted index record */
IndexIterator *NewReadIterator(IndexReader *ir);

#endif