
#pragma once

#include "result_processor.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Common parameters passed around for highlighting one or more fields within
 * a document. This structure exists to avoid passing these four parameters
 * discreetly (as we did in previous versiosn)
 */

struct HighligherDoc {
  // Byte offsets, byte-wise
  const RSByteOffsets *byteOffsets;

  // Index result, which contains the term offsets (word-wise)
  const IndexResult *indexResult;

  // Array used for in/out when writing fields. Optimization cache
  IOVecArrays iovsArr;

  RLookupRow *row;

  HighligherDoc(RSByteOffsets *byteOffsets, IndexResult *indexResult, RLookupRow *row) :
    byteOffsets(byteOffsets), indexResult(indexResult), row(row) {}

  RSValue *summarizeField(IndexSpec *spec, const ReturnedField &field,
    std::string_view fieldName, const RSValue *value, int options);

  void resetIovsArr(size_t newSize);
};

//---------------------------------------------------------------------------------------------

struct Highlighter : ResultProcessor {
  Highlighter(const RSSearchOptions *searchopts, const FieldList &fields, const RLookup *lookup);

  int fragmentizeOptions;
  const FieldList fields;
  const RLookup *lookup;

  const IndexResult *getIndexResult(t_docId docId);

  void processField(HighligherDoc &doc, const ReturnedField &field);

  int Next(SearchResult *res) override;
};

///////////////////////////////////////////////////////////////////////////////////////////////
