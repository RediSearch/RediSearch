
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

struct IndexBlockRepair;

//---------------------------------------------------------------------------------------------

// A single block of data in the index. The index is basically a list of blocks we iterate

struct IndexBlock {
  t_docId firstId;
  t_docId lastId;
  Buffer buf;
  uint16_t numDocs;

  char *DataBuf() { return buf.data; }
  size_t DataLen() const { return buf.offset; }

  int Repair(DocTable &dt, IndexFlags flags, struct IndexBlockRepair &blockrepair);

  bool Matches(t_docId docId) const { return firstId <= docId && docId <= lastId; }
};

//---------------------------------------------------------------------------------------------

// Called when an entry is removed
typedef void (*RepairCallback)(const IndexResult *res, void *arg);

//---------------------------------------------------------------------------------------------

struct IndexBlockRepair {
  IndexBlockRepair() : bytesCollected(0), docsCollected(0) {}
  size_t bytesCollected; // out: Number of bytes collected
  size_t docsCollected;  // out: Number of documents collected
  size_t limit;           // in: how many index blocks to scan at once

  // invoke when a document is collected
  virtual void collect(const IndexResult &, const IndexBlock &) {}
};

//---------------------------------------------------------------------------------------------

// An index encoder is a callback that writes records to the index.
// It accepts a pre-calculated delta for encoding.
typedef size_t (*IndexEncoder)(BufferWriter *bw, uint32_t delta, const IndexResult *record);

enum decoderType {Base, Term, Numeric};

//---------------------------------------------------------------------------------------------

struct IndexDecoder {
  // This context is passed to the decoder callback, and can contain either a pointer or integer.
  // It is intended to relay along any kind of additional configuration information to help the
  // decoder determine whether to filter the entry.

  t_fieldMask mask;
  decoderType type;

  IndexDecoder(uint32_t flags, decoderType type = decoderType::Base);
  IndexDecoder(uint32_t flags, t_fieldMask mask, decoderType type = decoderType::Base);

  virtual void ctor(uint32_t flags);

  // Decode a single record from the buffer reader. This function is responsible for:
  // (1) Decoding the record at the given position of br
  // (2) Advancing the reader's position to the next record
  // (3) Filtering the record based on any relevant information (can be passed through `ctx`)
  // (4) Populating `res` with the information from the record.

  // If the record should not be processed, it should not be populated and 0 should be returned.
  // Otherwise, the function should return true.

  bool (IndexDecoder::*decoder)(BufferReader *br, IndexResult *res);

  // Custom implementation of a seeking function. Seek to the specific ID within the index,
  // or at one position after it.
  // The implementation of this function is optional.
  // If this is not used, then the decoder() implementation will be used instead.

  bool (IndexDecoder::*seeker)(BufferReader *br, struct IndexReader *ir,
    t_docId to, IndexResult *res);

  // We have 9 distinct ways to decode the index records. Based on the index flags we select the
  // correct decoder for creating an index reader. A decoder both decodes the entry and does initial
  // filtering, returning true if the record is ok or false if it is filtered.

  bool readFreqsFlags(BufferReader *br, IndexResult *res);
  bool readFreqsFlagsWide(BufferReader *br, IndexResult *res);
  bool readFreqs(BufferReader *br, IndexResult *res);
  bool readFlags(BufferReader *br, IndexResult *res);
  bool readFlagsWide(BufferReader *br, IndexResult *res);
  bool readDocIdsOnly(BufferReader *br, IndexResult *res);

  bool CHECK_FLAGS(IndexResult *res) {
    return (res->fieldMask & mask) != 0;
  }
};

//---------------------------------------------------------------------------------------------

struct TermIndexDecoder : IndexDecoder {
  TermIndexDecoder(uint32_t flags) : IndexDecoder(flags, decoderType::Term) {}
  TermIndexDecoder(uint32_t flags, t_fieldMask mask) : IndexDecoder(flags, mask, decoderType::Term) {}

  void ctor(uint32_t flags);

  bool (TermIndexDecoder::*decoder)(BufferReader *br, TermResult *res);

  bool (TermIndexDecoder::*seeker)(BufferReader *br, struct IndexReader *ir,
    t_docId to, TermResult *res);

  bool readFreqOffsetsFlags(BufferReader *br, TermResult *res);
  bool readFreqOffsetsFlagsWide(BufferReader *br, TermResult *res);
  bool readFlagsOffsets(BufferReader *br, TermResult *res);
  bool readFlagsOffsetsWide(BufferReader *br, TermResult *res);
  bool readOffsets(BufferReader *br, TermResult *res);
  bool readFreqsOffsets(BufferReader *br, TermResult *res);

  // Skipper implements SkipTo. It is an optimized version of DECODER which reads

  bool seekFreqOffsetsFlags(BufferReader *br, IndexReader *ir, t_docId expid, TermResult *res);
};

//---------------------------------------------------------------------------------------------

struct NumericIndexDecoder : IndexDecoder {
  const NumericFilter *filter;

  NumericIndexDecoder(uint32_t flags) : IndexDecoder(flags, decoderType::Numeric) {}
  NumericIndexDecoder(uint32_t flags, t_fieldMask mask) : IndexDecoder(flags, mask, decoderType::Numeric) {}
  NumericIndexDecoder(uint32_t flags, const NumericFilter *filter) : IndexDecoder(flags, decoderType::Numeric), filter(filter) {}

  void ctor(uint32_t flags);

  bool (NumericIndexDecoder::*decoder)(BufferReader *br, NumericResult *res);

  bool (NumericIndexDecoder::*seeker)(BufferReader *br, struct IndexReader *ir,
    t_docId to, NumericResult *res);

  bool readNumeric(BufferReader *br, NumericResult *res);
};

//---------------------------------------------------------------------------------------------

struct InvertedIndex : BaseIndex {
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

  size_t WriteEntryGeneric(IndexEncoder encoder, t_docId docId, const IndexResult &entry);

  // Get the appropriate encoder for an inverted index given its flags. Returns NULL on invalid flags
  static IndexEncoder GetEncoder(IndexFlags flags);

  // Get the decoder for the index based on the index flags.
  // Used to externally inject the endoder/decoder when reading and writing.
  // static IndexDecoder GetDecoder(uint32_t flags);

  int Repair(DocTable &dt, uint32_t startBlock, IndexBlockRepair &blockrepair);

  // The last block of the index
  IndexBlock &LastBlock();
};

//---------------------------------------------------------------------------------------------

// An IndexReader wraps an inverted index record for reading and iteration

struct IndexReader : IndexIterator {
  const IndexSpec *sp;
  BufferReader br; // the underlying data buffer
  InvertedIndex *idx;
  t_docId lastId; // last docId, used for delta encoding/decoding
  uint32_t currentBlock;
  IndexDecoder decoder;
  size_t len; // number of records read
  TermResult *record; // The record we are decoding into
  int atEnd;

  // If present, this pointer is updated when the end has been reached.
  // This is an optimization to avoid calling HasNext() each time.
  bool isValidP;

  // This marker lets us know whether the garbage collector has visited this index while the reading
  // thread was asleep, and reset the state in a deeper way
  uint32_t gcMarker;
  double weight; // boosting weight

  //-------------------------------------------------------------------------------------------

  IndexReader(const IndexSpec *sp, InvertedIndex *idx, IndexDecoder decoder, IndexResult *record,
    double weight);

  virtual ~IndexReader();

  //-------------------------------------------------------------------------------------------

  void OnReopen(RedisModuleKey *k);
  
  int GenericRead(IndexResult *res); // Read an entry from an inverted index
  int Read(IndexResult **e); // Read an entry from an inverted index into IndexResult

  int Next(); // Move to the next entry in an inverted index, without reading the whole entry
  int SkipTo(t_docId docId, IndexResult **hit);
  int SkipToBlock(t_docId docId);
  void Abort();
  void Rewind();

  IndexResult *Current(void *ctx);
  IndexBlock &CurrentBlock(); // current block while reading the index

  size_t Len() const { return len; }
  size_t NumDocs() const; // The number of docs in an inverted index entry
  size_t NumEstimated() const;
  t_docId LastDocId() const; // LastDocId of an inverted index stateful reader

  // Create a reader iterator that iterates an inverted index record
  IndexIterator *NewReadIterator();

  void SetAtEnd(bool value);

  void AdvanceBlock();
};

//---------------------------------------------------------------------------------------------

struct IndexReadIterator : IndexIterator {
  IndexReader *_ir;

  IndexReadIterator(IndexReader *ir);
  ~IndexReadIterator();

  virtual int Read(IndexResult **hit) { return _ir->Read(hit); }
  virtual void Abort() { _ir->Abort(); }
  virtual void Rewind() { _ir->Rewind(); }
  virtual int SkipTo(t_docId docId, IndexResult **hit) { return _ir->SkipTo(docId, hit); }
  virtual size_t NumEstimated() const { return _ir->NumEstimated(); }
  virtual IndexCriteriaTester *GetCriteriaTester() { return _ir->GetCriteriaTester(); }
  virtual size_t Len() const { return _ir->Len(); }
};

//---------------------------------------------------------------------------------------------

struct TermIndexReader : IndexReader {
  // Create a new index reader on an inverted index buffer,
  // optionally with a skip index, docTable and scoreIndex.
  // If singleWordMode is set to 1, we ignore the skip index and use the score index.
  TermIndexReader(InvertedIndex *idx, IndexSpec *sp, t_fieldMask fieldMask, RSQueryTerm *term, double weight);
};

//---------------------------------------------------------------------------------------------

struct NumericIndexReader : IndexReader {
  // Create a new index reader for numeric records, optionally using a given filter.
  // If the filter is NULL we will return all the records in the index.
  NumericIndexReader(InvertedIndex *idx, const IndexSpec *sp = 0, const NumericFilter *flt = 0);
};

///////////////////////////////////////////////////////////////////////////////////////////////
