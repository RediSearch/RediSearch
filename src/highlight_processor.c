#include "highlight_processor.h"
#include "fragmenter.h"
#include "value.h"
#include "toksep.h"

#include "util/minmax.h"

#include <ctype.h>
#include <string>

///////////////////////////////////////////////////////////////////////////////////////////////

// Strip spaces from a buffer in place. Returns the new length of the text,
// with all duplicate spaces stripped and converted to a single ' '.

static size_t stripDuplicateSpaces(char *s, size_t n) {
  int isLastSpace = 0;
  size_t oix = 0;
  char *out = s;
  for (size_t ii = 0; ii < n; ++ii) {
    if (isspace(s[ii])) {
      if (isLastSpace) {
        continue;
      } else {
        isLastSpace = 1;
        out[oix++] = ' ';
      }
    } else {
      isLastSpace = 0;
      out[oix++] = s[ii];
    }
  }
  return oix;
}

//---------------------------------------------------------------------------------------------

// Returns the length of the buffer without trailing spaces

static size_t trimTrailingSpaces(const char *s, size_t input) {
  for (; input && isspace(s[input - 1]); --input) {
    // Nothing
  }
  return input;
}

//---------------------------------------------------------------------------------------------

ReturnedField ReturnedField::normalizeSettings(const ReturnedField &defaults) const {
  ReturnedField out;

  // Otherwise it gets more complex
  if ((defaults.mode & SummarizeMode_Highlight) && (mode & SummarizeMode_Highlight) == 0) {
    out.highlightSettings = defaults.highlightSettings;
  } else if (mode & SummarizeMode_Highlight) {
    out.highlightSettings = highlightSettings;
  }

  if ((defaults.mode & SummarizeMode_Synopsis) && (mode & SummarizeMode_Synopsis) == 0) {
    out.summarizeSettings = defaults.summarizeSettings;
  } else {
    out.summarizeSettings = summarizeSettings;
  }

  out.mode |= defaults.mode | mode;
  out.name = name;
  out.lookupKey = lookupKey;
}

//---------------------------------------------------------------------------------------------

// Called when we cannot fragmentize based on byte offsets.
// docLen is an in/out parameter. On input it should contain the length of the
// field, and on output it contains the length of the trimmed summary.
// Returns a string which should be freed using free()

char *ReturnedField::trimField(const char *docStr, size_t *docLen, size_t estWordSize) const {
  // Number of desired fragments times the number of context words in each fragments,
  // in characters (estWordSize)
  size_t headLen = summarizeSettings.contextLen * summarizeSettings.numFrags * estWordSize;
  headLen += estWordSize;  // Because we trim off a word when finding the toksep
  headLen = Min(headLen, *docLen);

  size_t len = headLen;
  char *buf = rm_strndup(docStr, len);
  len = stripDuplicateSpaces(buf, len);
  buf = rm_realloc(buf, len);
  while (len > 1) {
    if (istoksep(buf[len - 1])) {
      break;
    }
    --len;
  }

  len = trimTrailingSpaces(buf, len);
  *docLen = len;
  return buf;
}

//---------------------------------------------------------------------------------------------

RSValue *HighligherDoc::summarizeField(IndexSpec *spec, const ReturnedField &fieldInfo,
    const char *fieldName, const RSValue *returnedField, int options) {
  FragmentList frags{8, 6};

  // Start gathering the terms
  HighlightTags tags{fieldInfo.highlightSettings};

  // First actually generate the fragments
  size_t docLen;
  const char *docStr = returnedField->StringPtrLen(&docLen);
  if (byteOffsets == NULL ||
      !frags.fragmentizeOffsets(spec, fieldName, docStr, docLen, indexResult, byteOffsets, options)) {
    if (fieldInfo.mode == SummarizeMode_Synopsis) {
      // If summarizing is requested then trim the field so that the user isn't
      // spammed with a large blob of text
      // note that summarized is allocated dynamically and freed by RSValue::Clear()
      char *summarized = fieldInfo.trimField(docStr, &docLen, frags.estAvgWordSize);
      return RS_StringVal(summarized, docLen);
    } else {
      // Otherwise, just return the whole field, but without highlighting
    }
    return NULL;
  }

  // Highlight only
  if (fieldInfo.mode == SummarizeMode_Highlight) {
    // No need to return snippets; just return the entire doc with relevant tags highlighted
    char *hlDoc = frags.HighlightWholeDocS(tags);
    return RS_StringValC(hlDoc);
  }

  size_t numIovArr = Min(fieldInfo.summarizeSettings.numFrags, frags.GetNumFrags());
  resetIovsArr(numIovArr);

  frags.HighlightFragments(tags, fieldInfo.summarizeSettings.contextLen,
                           iovsArr, HIGHLIGHT_ORDER_SCOREPOS);

  // Buffer to store concatenated fragments
  std::string s;

  for (auto &iovs_array: iovsArr) {
    size_t lastSize = s.length();

    for (auto &iov: iovs_array) {
	    s.append(reinterpret_cast<const char*>(iov.iov_base), iov.iov_len);
    }

    // Duplicate spaces for the current snippet are eliminated here. We shouldn't
    // move it to the end because the delimiter itself may contain a special kind
    // of whitespace.
    size_t newSize = stripDuplicateSpaces(s.c_str() + lastSize, s.length() - lastSize);
	  s.resize(newSize);
  	s += fieldInfo.summarizeSettings.separator;
  }

  // Set the string value to the contents of the array. It might be nice if we didn't
  // need to strndup it.
  size_t hlLen = s.length();
  char *hlText = rm_strdup(s.c_str());
  return RS_StringVal(hlText, hlLen);
}

//---------------------------------------------------------------------------------------------

void HighligherDoc::resetIovsArr(size_t newSize) {
  for (auto arr: iovsArr) {
    arr.clear();
  }
  iovsArr.resize(newSize);
}

//---------------------------------------------------------------------------------------------

void Highlighter::processField(HighligherDoc &doc, const ReturnedField &field) {
  const char *fname = field.name;
  const RSValue *fval = doc.row->GetItem(field.lookupKey);

  if (fval == NULL || !fval->IsString()) {
    return;
  }
  RSValue *v = doc.summarizeField(parent->sctx->spec, field, fname, fval, fragmentizeOptions);
  if (v) {
    doc.row->WriteOwnKey(field.lookupKey, v);
  }
}

//---------------------------------------------------------------------------------------------

const IndexResult *Highlighter::getIndexResult(t_docId docId) {
  IndexIterator *it = parent->GetRootFilter();
  IndexResult *ir;
  it->Rewind();
  if (INDEXREAD_OK != it->SkipTo(docId, &ir)) {
    return NULL;
  }
  //@@@TODO: release it?!
  return ir;
}

//---------------------------------------------------------------------------------------------

int Highlighter::Next(SearchResult *r) {
  int rc = upstream->Next(r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  // Get the index result for the current document from the root iterator.
  // The current result should not contain an index result
  const IndexResult *ir = r->indexResult ? r->indexResult : getIndexResult(r->docId);

  // we can't work withot the inex result, just return QUEUED
  if (!ir) {
    return RS_RESULT_OK;
  }

  size_t numIovsArr = 0;
  RSDocumentMetadata *dmd = r->dmd.get();
  if (!dmd) {
    return RS_RESULT_OK;
  }

  HighligherDoc doc{dmd->byteOffsets, ir, &r->rowdata};

  if (fields.NumFields()) {
    for (size_t i = 0; i < fields.NumFields(); ++i) {
      const ReturnedField ff = fields.fields[i];
      if (ff.mode == SummarizeMode_None && fields.defaultField.mode == SummarizeMode_None) {
        // Ignore - this is a field for `RETURN`, not `SUMMARIZE`
        continue;
      }
      ReturnedField field = ff.normalizeSettings(fields.defaultField);
      doc.resetIovsArr(field.summarizeSettings.numFrags);
      processField(doc, field);
    }
  } else if (fields.defaultField.mode != SummarizeMode_None) {
    for (const RLookupKey *k = lookup->head; k; k = k->next) {
      if (k->flags & RLOOKUP_F_HIDDEN) {
        continue;
      }
      ReturnedField field = fields.defaultField;
      field.lookupKey = k;
      field.name = k->name;
      doc.resetIovsArr(field.summarizeSettings.numFrags);
      processField(doc, field);
    }
  }

  return RS_RESULT_OK;
}

//---------------------------------------------------------------------------------------------

Highlighter::Highlighter(const RSSearchOptions *searchopts, const FieldList &fields,
    const RLookup *lookup) : ResultProcessor("Highlighter"), lookup(lookup), fields(fields) {
  if (searchopts->language == RS_LANG_CHINESE) {
    fragmentizeOptions = FRAGMENTIZE_TOKLEN_EXACT;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
