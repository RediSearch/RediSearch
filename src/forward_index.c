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

void ForwardIndex::clear(uint32_t idxFlags_) {
  idxFlags = idxFlags_;
  maxFreq = 0;
  totalFreq = 0;
}

//---------------------------------------------------------------------------------------------

ForwardIndex::ForwardIndex(Document *doc, uint32_t idxFlags_, SynonymMap *smap_)
  : hits()
  , idxFlags(idxFlags_)
  , maxFreq(0)
  , totalFreq(0)
  , stemmer(new SnowballStemmer(doc->language))
  , smap(smap_)
  , terms(TERM_BLOCK_SIZE)
  , entries(ENTRIES_PER_BLOCK)
  , vvw_pool(estimtateTermCount(doc))
{ }

//---------------------------------------------------------------------------------------------

void ForwardIndex::Reset(Document *doc, uint32_t idxFlags_) {
  terms.Clear();
  entries.Clear();
  vvw_pool.Clear();
  hits.clear();

  delete smap;
  smap = nullptr;

  if (stemmer && !stemmer->Reset(StemmerType::Snowball, doc->language)) {
    delete stemmer;
    stemmer = nullptr;
  }

  if (!stemmer) {
    stemmer = new SnowballStemmer(doc->language);
  }

  clear(idxFlags_);
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

ForwardIndexEntry::ForwardIndexEntry(
  ForwardIndex &idx, std::string_view tok, float fieldScore, t_fieldId fieldId, int options
) : next{nullptr}
  , docId{}
  , freq{0}
  , fieldMask{0}
  , term{options & TOKOPT_F_COPYSTR ? idx.terms.strncpy(tok) : tok.data()}
  , len{tok.size()}
  , vw{idx.hasOffsets() ? idx.vvw_pool.Alloc() : nullptr}
{}

//---------------------------------------------------------------------------------------------

void ForwardIndex::HandleToken(std::string_view tok, uint32_t pos,
                               float fieldScore, t_fieldId fieldId, int options) {
  ForwardIndexEntry *h = entries.Alloc(*this, tok, fieldScore, fieldId, options);
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

  // std::unordered_map::operator[] creates a default-constructed
  // value at key if it didn't exist. trivial to upsert.
  hits[String{tok}].push_back(h);
}

//---------------------------------------------------------------------------------------------

void ForwardIndexTokenizer::tokenize(const Token &tok) {
  int options = 0;
  if (tok.flags & Token_CopyRaw) {
    options |= TOKOPT_F_COPYSTR;
  }
  idx->HandleToken({tok.tok, tok.tokLen}, tok.pos, fieldScore, fieldId, options);

  if (allOffsets) {
    allOffsets->Write(tok.raw - doc);
  }

  if (tok.stem) {
    int stemopts = TOKOPT_F_STEM;
    if (tok.flags & Token_CopyStem) {
      stemopts |= TOKOPT_F_COPYSTR;
    }
    idx->HandleToken({tok.stem, tok.stemLen}, tok.pos, fieldScore, fieldId, stemopts);
  }

  if (idx->smap) {
    TermData *t_data = idx->smap->GetIdsBySynonym(std::string_view(tok.tok, tok.tokLen));
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
  hitsMap(&idx.hits), hits(nullptr) {
}

//---------------------------------------------------------------------------------------------

ForwardIndexEntry *ForwardIndexIterator::Next() {
  if (hitsMap->empty()) return nullptr;

  if (!hits || hits->empty()) {
    for (auto it = hitsMap->begin(); it != hitsMap->end(); ++it) {
      auto vec = &it->second;
      if (!vec->empty()) {
        hits = vec;
        break;
      }
    }
  }

  if (!hits || hits->empty()) return nullptr;

  ForwardIndexEntry *entry = hits->back();
  hits->pop_back();
  return entry;
}

///////////////////////////////////////////////////////////////////////////////////////////////
