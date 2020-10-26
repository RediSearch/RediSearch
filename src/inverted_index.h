
#pragma once

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

///////////////////////////////////////////////////////////////////////////////////////////////

extern uint64_t TotalIIBlocks;

// A single block of data in the index. The index is basically a list of blocks we iterate

struct IndexBlock {
  ~IndexBlock();

  t_docId firstId;
  t_docId lastId;
  Buffer buf;
  uint16_t numDocs;

  char *DataBuf() { return buf.data; }
  size_t DataLen() const { return buf.offset; }

  int Repair(DocTable *dt, IndexFlags flags, struct IndexRepairParams *params);

  bool Matches(t_docId docId) const { return firstId <= docId && docId <= lastId; }
};

//---------------------------------------------------------------------------------------------

// Called when an entry is removed
typedef void (*RepairCallback)(const RSIndexResult *res, void *arg);

//---------------------------------------------------------------------------------------------

struct IndexRepairParams {
  size_t bytesCollected; // out: Number of bytes collected
  size_t docsCollected;  // out: Number of documents collected
  size_t limit;          // in: how many index blocks to scan at once

  // in: Callback to invoke when a document is collected
  void (*RepairCallback)(const RSIndexResult *, const IndexBlock *, void *);
  // argument to pass to callback
  void *arg;
};

// An index encoder is a callback that writes records to the index.
// It accepts a pre-calculated delta for encoding.
typedef size_t (*IndexEncoder)(BufferWriter *bw, uint32_t delta, const RSIndexResult *record);

#if 0
// Decode a single record from the buffer reader. This function is responsible for:
// (1) Decoding the record at the given position of br
// (2) Advancing the reader's position to the next record
// (3) Filtering the record based on any relevant information (can be passed through `ctx`)
// (4) Populating `res` with the information from the record.

// If the record should not be processed, it should not be populated and 0 should be returned.
// Otherwise, the function should return 1.
typedef int (*IndexDecoder)(BufferReader *br, const IndexDecoderCtx *ctx, RSIndexResult *res);

struct IndexReader;

// Custom implementation of a seeking function. Seek to the specific ID within the index, 
// or at one position after it.
// The implementation of this function is optional. 
// If this is not used, then the decoder() implementation will be used instead.

typedef int (*IndexSeeker)(BufferReader *br, const IndexDecoderCtx *ctx, struct IndexReader *ir,
                           t_docId to, RSIndexResult *res);
#endif // 0

struct IndexDecoder {
  // This context is passed to the decoder callback, and can contain either a pointer or integer.
  // It is intended to relay along any kind of additional configuration information to help the 
  // decoder determine whether to filter the entry.

  union {
    const NumericFilter *filter;
    t_fieldMask mask;
  };

  IndexDecoder(uint32_t flags);
  IndexDecoder(uint32_t flags, t_fieldMask mask);
  IndexDecoder(uint32_t flags, const NumericFilter *filter);

  void ctor(uint32_t flags);

  // Decode a single record from the buffer reader. This function is responsible for:
  // (1) Decoding the record at the given position of br
  // (2) Advancing the reader's position to the next record
  // (3) Filtering the record based on any relevant information (can be passed through `ctx`)
  // (4) Populating `res` with the information from the record.
  
  // If the record should not be processed, it should not be populated and 0 should be returned.
  // Otherwise, the function should return 1.

  int (IndexDecoder::*decoder)(BufferReader *br, RSIndexResult *res);

  // Custom implementation of a seeking function. Seek to the specific ID within the index, 
  // or at one position after it.
  // The implementation of this function is optional. 
  // If this is not used, then the decoder() implementation will be used instead.

  int (IndexDecoder::*seeker)(BufferReader *br, struct IndexReader *ir,
    t_docId to, RSIndexResult *res);

#define DECODER(name) \
  int name(BufferReader *br, RSIndexResult *res)

  DECODER(readFreqsFlags);
  DECODER(readFreqsFlagsWide);
  DECODER(readFreqOffsetsFlags);
  DECODER(readFreqOffsetsFlagsWide);
  DECODER(readNumeric);
  DECODER(readFreqs);
  DECODER(readFlags);
  DECODER(readFlagsWide);
  DECODER(readFlagsOffsets);
  DECODER(readFlagsOffsetsWide);
  DECODER(readOffsets);
  DECODER(readFreqsOffsets);
  DECODER(readDocIdsOnly);
#undef DECODER

#define SKIPPER(name) \
  int name(BufferReader *br, IndexReader *ir, t_docId expid, RSIndexResult *res)

  SKIPPER(seekFreqOffsetsFlags);
#undef SKIPPER
};

//---------------------------------------------------------------------------------------------

struct InvertedIndex : Object {
  IndexBlock *blocks;
  uint32_t size;
  IndexFlags flags;
  t_docId lastId;
  uint32_t numDocs;
  uint32_t gcMarker;

  // If initBlock is 1, we create the first block
  InvertedIndex(IndexFlags flags, int initBlock);
  ~InvertedIndex();

  IndexBlock *AddBlock(t_docId firstId);

  // Write a ForwardIndexEntry into an indexWriter. Returns the number of bytes written to the index
  size_t WriteForwardIndexEntry(IndexEncoder encoder, const ForwardIndexEntry &ent);

  // Write a numeric index entry to the index. it includes only a float value and docId. 
  // Returns the number of bytes written.
  size_t WriteNumericEntry(t_docId docId, double value);

  size_t WriteEntryGeneric(IndexEncoder encoder, t_docId docId, const RSIndexResult &entry);

  // Get the appropriate encoder for an inverted index given its flags. Returns NULL on invalid flags
  static IndexEncoder GetEncoder(IndexFlags flags);

  // Get the decoder for the index based on the index flags.
  // Used to externally inject the endoder/decoder when reading and writing.
  static IndexDecoder GetDecoder(uint32_t flags);

  int Repair(DocTable *dt, uint32_t startBlock, struct IndexRepairParams *params);

  // The last block of the index
  IndexBlock &LastBlock();
};

//---------------------------------------------------------------------------------------------

// An IndexReader wraps an inverted index record for reading and iteration

struct IndexReader : public IndexIterator {
  const IndexSpec *sp;

  // the underlying data buffer
  BufferReader br;

  InvertedIndex *idx;

  // last docId, used for delta encoding/decoding
  t_docId lastId;

  uint32_t currentBlock;

  IndexDecoder decoder;

  // number of records read
  size_t len;

  // The record we are decoding into
  RSIndexResult *record;

  int atEnd;

  // If present, this pointer is updated when the end has been reached.
  // This is an optimization to avoid calling IR_HasNext() each time.
  uint8_t *isValidP;

  // This marker lets us know whether the garbage collector has visited this index while the reading
  // thread was asleep, and reset the state in a deeper way
  uint32_t gcMarker;

  // boosting weight
  double weight;

  //-------------------------------------------------------------------------------------------

  IndexReader(const IndexSpec *sp, InvertedIndex *idx, IndexDecoder decoder, RSIndexResult *record,
    double weight);

  // free an index reader
  virtual ~IndexReader();

  //-------------------------------------------------------------------------------------------

  void OnReopen(RedisModuleKey *k);

  // Read an entry from an inverted index
  int GenericRead(RSIndexResult *res);

  // Read an entry from an inverted index into RSIndexResult
  int Read(RSIndexResult **e);

  // Move to the next entry in an inverted index, without reading the whole entry
  int Next();

  // Skip to a specific document ID in the index, or one position after it
  // @param ctx the index reader
  // @param docId the document ID to search for
  // @param hit where to store the result pointer
  //
  // @return:
  //  - INDEXREAD_OK if the id was found
  //  - INDEXREAD_NOTFOUND if the reader is at the next position
  //  - INDEXREAD_EOF if the ID is out of the upper range

  int SkipTo(t_docId docId, RSIndexResult **hit);
  int SkipToBlock(t_docId docId);

  void Abort();
  void Rewind();

  RSIndexResult *Current(void *ctx);

  // The number of docs in an inverted index entry
  size_t NumDocs() const;

  size_t NumEstimated() const;

  // LastDocId of an inverted index stateful reader
  t_docId LastDocId() const;

  // Create a reader iterator that iterates an inverted index record
  IndexIterator *NewReadIterator();

  void SetAtEnd(int value);

  // current block while reading the index
  IndexBlock &CurrentBlock();

  void AdvanceBlock();
};

//---------------------------------------------------------------------------------------------

class TermIndexReader : public IndexReader {
public:
  // Create a new index reader on an inverted index buffer,
  // optionally with a skip index, docTable and scoreIndex.
  // If singleWordMode is set to 1, we ignore the skip index and use the score index.
  TermIndexReader(InvertedIndex *idx, IndexSpec *sp, t_fieldMask fieldMask, RSQueryTerm *term, double weight);
};

//---------------------------------------------------------------------------------------------

class NumericIndexReader : public IndexReader {
public:
  // Create a new index reader for numeric records, optionally using a given filter.
  // If the filter is NULL we will return all the records in the index.
  NumericIndexReader(InvertedIndex *idx, const IndexSpec *sp = 0, const NumericFilter *flt = 0);
};

///////////////////////////////////////////////////////////////////////////////////////////////
