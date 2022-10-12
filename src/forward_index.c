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
  for (auto const &f: doc->fields) {
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
    stemmer(new SnowballStemmer(doc->language)),
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

  if (stemmer && !stemmer->Reset(StemmerType::Snowball, doc->language)) {
    delete stemmer;
    stemmer = NULL;
  }

  if (!stemmer) {
    stemmer = new SnowballStemmer(doc->language);
  }

  ctor(idxFlags_);
}

//---------------------------------------------------------------------------------------------

bool ForwardIndex::hasOffsets() const {
  return !!(idxFlags & Index_StoreTermOffsets);
}

//---------------------------------------------------------------------------------------------

ForwardIndex::~ForwardIndex() {
  delete stemmer;
  delete smap;
}

//---------------------------------------------------------------------------------------------

ForwardIndexEntry::ForwardIndexEntry(ForwardIndex &idx, std::string_view tok, float fieldScore,
    t_fieldId fieldId, int options) {
  fieldMask = 0;
  next = NULL;
  if (options & TOKOPT_F_COPYSTR) {
    term = idx.terms.strncpy(tok);
  } else {
    term = tok.data();
  }

  len = tok.size();
  freq = 0;

  vw = idx.hasOffsets() ? idx.vvw_pool.Alloc(VarintVectorWriter()) : NULL;
}

//---------------------------------------------------------------------------------------------

void ForwardIndex::HandleToken(std::string_view tok, uint32_t pos,
                               float fieldScore, t_fieldId fieldId, int options) {
  ForwardIndexEntry *h = entries.Alloc(ForwardIndexEntry(*this, tok, fieldScore, fieldId, options));
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

  String key{tok};

  if (hits.contains(key)) {
    hits[key].push_back(h);
  } else {
    Vector<ForwardIndexEntry *> v;
    v.push_back(h);
    hits.insert({ key, v});
  }
}

//---------------------------------------------------------------------------------------------

void ForwardIndexTokenizer::tokenize(const Token &tok) {
  int options = 0;
  if (tok.flags & Token_CopyRaw) {
    options |= TOKOPT_F_COPYSTR;
  }
  idx->HandleToken(tok.tok, tok.pos, fieldScore, fieldId, options);

  if (allOffsets) {
    allOffsets->Write(tok.raw - doc);
  }

  if (tok.stem) {
    int stemopts = TOKOPT_F_STEM;
    if (tok.flags & Token_CopyStem) {
      stemopts |= TOKOPT_F_COPYSTR;
    }
    idx->HandleToken(tok.stem, tok.pos, fieldScore, fieldId, stemopts);
  }

  if (idx->smap) {
    TermData *t_data = idx->smap->GetIdsBySynonym(tok.tok, tok.tokLen);
    if (t_data) {
      for (auto const &id: t_data->ids) {
        String synonym = SynonymMap::IdToStr(id);
        idx->HandleToken(synonym, tok.pos, fieldScore, fieldId, TOKOPT_F_COPYSTR);
      }
    }
  }

  if (tok.phoneticsPrimary) {
    idx->HandleToken(std::string_view{tok.phoneticsPrimary, strlen(tok.phoneticsPrimary)},
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
