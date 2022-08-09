#include "forward_index.h"
#include "tokenize.h"

#include "rmalloc.h"
#include "util/fnv.h"
#include "util/logging.h"

#include <stdio.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define ENTRIES_PER_BLOCK 32
#define TERM_BLOCK_SIZE 128
#define CHARS_PER_TERM 5
#define TOKOPT_F_STEM 0x01
#define TOKOPT_F_COPYSTR 0x02

//---------------------------------------------------------------------------------------------

static size_t estimtateTermCount(const Document *doc) {
  size_t nChars = 0;
  for (auto f : doc->fields) {
    size_t n;
    RedisModule_StringPtrLen(f->text, &n);
    nChars += n;
  }
  return nChars / CHARS_PER_TERM;
}

//---------------------------------------------------------------------------------------------

void ForwardIndex::ctor(uint32_t idxFlags_) {
  idxFlags = idxFlags_;
  maxFreq = 0;
  totalFreq = 0;
}

//---------------------------------------------------------------------------------------------

ForwardIndex::ForwardIndex(Document *doc, uint32_t idxFlags_) :
    entries(ENTRIES_PER_BLOCK),
    terms(TERM_BLOCK_SIZE),
    vvw_pool(estimtateTermCount(doc)),
    stemmer(new Stemmer(SnowballStemmer, doc->language)),
    smap(NULL) {

  ctor(idxFlags_);
}

//---------------------------------------------------------------------------------------------

void ForwardIndex::Reset(Document *doc, uint32_t idxFlags_) {
  terms.Clear();
  entries.Clear();
  vvw_pool.Clear();
  hits.clear();

  delete smap;
  smap = NULL;

  if (stemmer && !stemmer->Reset(SnowballStemmer, doc->language)) {
    delete stemmer;
    stemmer = NULL;
  }

  if (!stemmer) {
    stemmer = new Stemmer(SnowballStemmer, doc->language);
  }

  ctor(idxFlags_);
}

//---------------------------------------------------------------------------------------------

int ForwardIndex::hasOffsets() const {
  return idxFlags & Index_StoreTermOffsets;
}

//---------------------------------------------------------------------------------------------

ForwardIndex::~ForwardIndex() {
  delete stemmer;
  delete smap;
}

//---------------------------------------------------------------------------------------------

ForwardIndexEntry::ForwardIndexEntry(ForwardIndex &idx, const char *tok, size_t tokLen, float fieldScore,
    t_fieldId fieldId, int options) {
  fieldMask = 0;
  next = NULL;
  if (options & TOKOPT_F_COPYSTR) {
    term = idx.terms.strncpy(tok, tokLen);
  } else {
    term = tok;
  }

  len = tokLen;
  freq = 0;

  vw = idx.hasOffsets() ? idx.vvw_pool.Alloc(VarintVectorWriter()) : NULL;
}

//---------------------------------------------------------------------------------------------

void ForwardIndex::HandleToken(const char *tok, size_t tokLen, uint32_t pos,
                               float fieldScore, t_fieldId fieldId, int options) {
  ForwardIndexEntry *h = entries.Alloc(ForwardIndexEntry(*this, tok, tokLen, fieldScore, fieldId, options));
  h->fieldMask |= ((t_fieldMask)1) << fieldId;
  float score = (float)fieldScore;

  // stem tokens get lower score
  if (options & TOKOPT_F_STEM) {
    score *= STEM_TOKEN_FACTOR;
  }
  h->freq += MAX(1, (uint32_t)score);
  maxFreq = MAX(h->freq, maxFreq);
  totalFreq += h->freq;
  if (h->vw) {
    h->vw->Write(pos);
  }

  std::string key{tok, tokLen};

  if (hits.contains(key)) {
    hits[key].push_back(h);
  } else {
    Vector<ForwardIndexEntry *> v;
    v.push_back(h);
    hits.insert({ key, v});
  }
}

//---------------------------------------------------------------------------------------------

#define SYNONYM_BUFF_LEN 100

void ForwardIndexTokenizer::tokenize(const Token &tok) {
  int options = 0;
  if (tok.flags & Token_CopyRaw) {
    options |= TOKOPT_F_COPYSTR;
  }
  idx->HandleToken(tok.tok, tok.tokLen, tok.pos, fieldScore, fieldId, options);

  if (allOffsets) {
    allOffsets->Write(tok.raw - doc);
  }

  if (tok.stem) {
    int stemopts = TOKOPT_F_STEM;
    if (tok.flags & Token_CopyStem) {
      stemopts |= TOKOPT_F_COPYSTR;
    }
    idx->HandleToken(tok.stem, tok.stemLen, tok.pos, fieldScore, fieldId, stemopts);
  }

  if (idx->smap) {
    TermData *t_data = idx->smap->GetIdsBySynonym(tok.tok, tok.tokLen);
    if (t_data) {
      char synonym_buff[SYNONYM_BUFF_LEN];
      size_t synonym_len;
      for (int i = 0; i < array_len(t_data->ids); ++i) {
        synonym_len = SynonymMap::IdToStr(t_data->ids[i], synonym_buff, SYNONYM_BUFF_LEN);
        idx->HandleToken(synonym_buff, synonym_len, tok.pos,
                         fieldScore, fieldId, TOKOPT_F_COPYSTR);
      }
    }
  }

  if (tok.phoneticsPrimary) {
    idx->HandleToken(tok.phoneticsPrimary, strlen(tok.phoneticsPrimary),
                     tok.pos, fieldScore, fieldId, TOKOPT_F_COPYSTR);
  }
}

//---------------------------------------------------------------------------------------------

ForwardIndexIterator::ForwardIndexIterator(const ForwardIndex &idx) :
  hits(&idx.hits), curBucketIdx(0), curVec(NULL) {
}

//---------------------------------------------------------------------------------------------

ForwardIndexEntry *ForwardIndexIterator::Next() {
  if (hits->empty()) {
    return NULL;
  }

  for(auto iter = hits->begin(); iter != hits->end() && curVec == NULL; ++iter) {
    curVec = &iter->second;
    if (!curVec->empty()){
      break;
    }
  }

  ForwardIndexEntry *ret = curVec->back();
  curVec->pop_back();
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////
