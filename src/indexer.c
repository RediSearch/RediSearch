#include "indexer.h"
#include "forward_index.h"
#include "numeric_index.h"
#include "inverted_index.h"
#include "geo_index.h"
#include "index.h"
#include "redis_index.h"
#include "rmutil/rm_assert.h"

#include <assert.h>
#include <unistd.h>

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec::writeIndexEntry(InvertedIndex *idx, const ForwardIndexEntry &entry) {
  IndexEncoder encoder = InvertedIndex::GetEncoder(flags);
  size_t sz = idx->WriteForwardIndexEntry(encoder, entry);

  // Update index statistics:

  // Number of additional bytes
  stats.invertedSize += sz;
  // Number of records
  stats.numRecords++;

  // Record the space saved for offset vectors
  if (flags & Index_StoreTermOffsets) {
    stats.offsetVecsSize += entry.vw->GetByteLength();
    stats.offsetVecRecords += entry.vw->GetCount();
  }
}

//---------------------------------------------------------------------------------------------

// Number of terms for each block-allocator block
#define TERMS_PER_BLOCK 128

// Effectively limits the maximum number of documents whose terms can be merged
#define MAX_BULK_DOCS 1024

//---------------------------------------------------------------------------------------------

// This function used for debugging, and returns how many items are actually in the list

size_t MergeMapEntry::countMerged() const {
  size_t n = 0;
  for (ForwardIndexEntry *cur = head; cur; cur = cur->next) {
    ++n;
  }
  return n;
}

//---------------------------------------------------------------------------------------------

// Merges all terms in the queue into a single hash table.
// parentMap is assumed to be a AddDocumentCtx*[] of capacity MAX_DOCID_ENTRIES
//
// This function returns the first aCtx which lacks its own document ID.
// This wil be used when actually assigning document IDs later on, so that we
// don't need to seek the document list again for it.

AddDocumentCtx *DocumentIndexer::merge(AddDocumentCtx *aCtx, AddDocumentCtx **parentMap) {
  // Make sure we don't block the CPU if there are many many items in the queue,
  // though in reality the number of iterations is also limited by MAX_DOCID_ENTRIES
  size_t counter = 0;

  // Current index within the parentMap, this is assigned as the placeholder doc ID value
  size_t curIdIdx = 0;

  AddDocumentCtx *cur = aCtx;
  AddDocumentCtx *firstZeroId = nullptr;

  while (cur && ++counter < 1000 && curIdIdx < MAX_BULK_DOCS) {
    ForwardIndexIterator it = cur->fwIdx->Iterate();
    ForwardIndexEntry *entry = it.Next();
    while (entry) {
      // Because we don't have the actual document ID at this point, the document ID field will be used
      // here to point to an index in the parentMap that will contain the parent.
      // The parent itself will contain the document ID when assigned (when the lock is held).
      entry->docId = curIdIdx;

      // Get the entry for it
      std::string key{entry->term, entry->len};
      auto marge_it = mergeMap.find(key);
      if (marge_it == mergeMap.end()) {
        MergeMapEntry *val = mergePool.Alloc(entry, entry);
        mergeMap.emplace(key, val);
      } else {
        marge_it->second->tail->next = entry;
        marge_it->second->tail = entry;
      }

      entry->next = nullptr;
      entry = it.Next();
    }

    // Set the document's text status as indexed. This is not strictly true,
    // but it means that there is no more index interaction with this specific document.
    cur->stateFlags |= ACTX_F_TEXTINDEXED;
    parentMap[curIdIdx++] = cur;
    if (firstZeroId == nullptr && cur->doc.docId == 0) {
      firstZeroId = cur;
    }

    cur = cur->next;
  }
  return firstZeroId;
}

//---------------------------------------------------------------------------------------------

// Writes all the entries in the hash table to the inverted index.
// parentMap contains the actual mapping between the `docID` field and the actual
// AddDocumentCtx which contains the document itself, which by this time should
// have been assigned an ID via makeDocumentId()

int DocumentIndexer::writeMergedEntries(AddDocumentCtx *aCtx, RedisSearchCtx *sctx, AddDocumentCtx **parentMap) {
  IndexSpec *spec = sctx->spec;
  int isBlocked = aCtx->IsBlockable();

  // This is used as a cache layer, so that we don't need to derefernce the AddDocumentCtx each time
  uint32_t docIdMap[MAX_BULK_DOCS] = {0};

  for (auto it: mergeMap) {
      MergeMapEntry *merged = it.second;
      ForwardIndexEntry *fwent = merged->head;

      // Add the term to the prefix trie. This only needs to be done once per term.
      spec->AddTerm(fwent->term, fwent->len);

      RedisModuleKey *idxKey = nullptr;
      InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, fwent->term, fwent->len, 1, &idxKey);
      if (invidx == nullptr) {
        continue;
      }

      for (; fwent != nullptr; fwent = fwent->next) {
        // Get the Doc ID for this entry.
        // Note that lookup result is cached, since accessing the parent each time causes some memory access overhead.
        // This saves about 3% overall.
        uint32_t docId = docIdMap[fwent->docId];
        if (docId == 0) {
          // Meaning the entry is not yet in the cache
          AddDocumentCtx *parent = parentMap[fwent->docId];
          if ((parent->stateFlags & ACTX_F_ERRORED) || parent->doc.docId == 0) {
            // Has an error, or for some reason it doesn't have a document ID(!? is this possible)
            continue;
          } else {
            // Place the entry in the cache, so we don't need a pointer dereference next time
            docId = docIdMap[fwent->docId] = parent->doc.docId;
          }
        }

        // Finally assign the document ID to the entry
        fwent->docId = docId;
        spec->writeIndexEntry(invidx, *fwent);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      if (isBlocked && concCtx.Tick() && spec == nullptr) {
        aCtx->status.SetError(QUERY_ENOINDEX, nullptr);
        return -1;
      }
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

// Simple implementation, writes all the entries for a single document.
// This function is used when there is only one item in the queue.
// In this case it's simpler to forego building the merged dictionary because there is
// nothing to merge.

void DocumentIndexer::writeEntries(AddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  IndexSpec *spec = sctx->spec;
  ForwardIndexIterator it = aCtx->fwIdx->Iterate();
  ForwardIndexEntry *entry = it.Next();
  const int isBlocked = aCtx->IsBlockable();

  while (entry != nullptr) {
    RedisModuleKey *idxKey = nullptr;
    spec->AddTerm(entry->term, entry->len);

    if (!sctx) throw Error("No search context");

    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      entry->docId = aCtx->doc.docId;
      if (!entry->docId) throw Error("docId should not be 0");
      spec->writeIndexEntry(invidx, *entry);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }

    entry = it.Next();
    if (isBlocked && concCtx.Tick() && spec == nullptr) {
      aCtx->status.SetError(QUERY_ENOINDEX, nullptr);
      return;
    }
  }
}

//---------------------------------------------------------------------------------------------

static void handleReplaceDelete(RedisSearchCtx *sctx, t_docId did) {
  IndexSpec *sp = sctx->spec;
  for (auto const &fs : sp->fields) {
    if (!fs.IsFieldType(INDEXFLD_T_GEO)) {
      continue;
    }
    // Open the key:
    RedisModuleString *fmtkey = sp->GetFormattedKey(fs, INDEXFLD_T_GEO);
    GeoIndex gi{sctx, fs};
    gi.RemoveEntries(did);
  }
}

//---------------------------------------------------------------------------------------------

// Assigns a document ID to a single document

bool AddDocumentCtx::makeDocumentId(RedisSearchCtx *sctx, bool replace, QueryError *status) {
  IndexSpec *spec = sctx->spec;
  DocTable &table = spec->docs;
  if (replace) {
    oldMd = std::shared_ptr<DocumentMetadata>(table.Pop(doc.docKey));
    if (oldMd) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
      //oldMd = dmd;
      if (oldMd->flags & Document_HasOnDemandDeletable) {
        // Delete all on-demand fields.. this means geo, but could mean other things..
        handleReplaceDelete(sctx, oldMd->id);
      }
      if (sctx->spec->gc) {
        sctx->spec->gc->OnDelete();
      }
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc.docKey, &n);

  doc.docId = table.Put(s, n, doc.score, docFlags, doc.payload);
  if (doc.docId == 0) {
    status->SetError(QUERY_EDOCEXISTS, nullptr);
    return false; //@@TODO: throw on error
  }
  ++spec->stats.numDocuments;

  return true;
}

//---------------------------------------------------------------------------------------------

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
 *
 * This function also sets the document's sorting vector, if present.
 */

void AddDocumentCtx::doAssignIds(RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  AddDocumentCtx *cur = this;
  for (; cur; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    if (cur->doc.docId) throw Error("docId must be 0");
    bool rv = cur->makeDocumentId(ctx, cur->options & DOCUMENT_ADD_REPLACE, &cur->status);
    if (!rv) {
      cur->stateFlags |= ACTX_F_ERRORED;
      continue;
    }

    DocumentMetadata *md = spec->docs.Get(cur->doc.docId);
    md->maxFreq = cur->fwIdx->maxFreq;
    md->len = cur->fwIdx->totalFreq;

    if (cur->sv) {
      spec->docs.SetSortingVector(cur->doc.docId, cur->sv);
      cur->sv = nullptr;
    }

    if (cur->byteOffsets) {
      cur->offsetsWriter.Move(cur->byteOffsets);
      spec->docs.SetByteOffsets(cur->doc.docId, cur->byteOffsets);
      cur->byteOffsets = nullptr;
    }
  }
}

//---------------------------------------------------------------------------------------------

void IndexBulkData::indexBulkFields(AddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Traverse all fields, seeing if there may be something which can be written!
  IndexBulkData bData[SPEC_MAX_FIELDS] = {{{nullptr}}};
  IndexBulkData *activeBulks[SPEC_MAX_FIELDS];
  size_t numActiveBulks = 0;

  for (AddDocumentCtx *cur = aCtx; cur && cur->doc.docId; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    const Document *doc = &cur->doc;
    for (size_t i = 0; i < doc->NumFields(); ++i) {
      const FieldSpec &fs = cur->fspecs[i];
      FieldIndexerData *fdata = &cur->fdatas[i];
      if (fs.name == "" || fs.types == INDEXFLD_T_FULLTEXT || !fs.IsIndexable()) {
        continue;
      }
      IndexBulkData *bulk = &bData[fs.index];
      if (!bulk->found) {
        bulk->found = 1;
        activeBulks[numActiveBulks++] = bulk;
      }

      if (bulk->Add(cur, sctx, doc->fields[i], &fs, fdata, &cur->status) != 0) {
        cur->stateFlags |= ACTX_F_ERRORED;
      }
      cur->stateFlags |= ACTX_F_OTHERINDEXED;
    }
  }

  // Flush it!
  for (size_t i = 0; i < numActiveBulks; ++i) {
    IndexBulkData *cur = activeBulks[i];
    cur->Cleanup(sctx);
  }
}

//---------------------------------------------------------------------------------------------

bool AddDocumentCtx::IsIndexed() const {
  return (stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED);
}

//---------------------------------------------------------------------------------------------

// Perform the processing chain on a single document entry, optionally merging
// the tokens of further entries in the queue

void DocumentIndexer::Process(AddDocumentCtx *aCtx) {
  AddDocumentCtx *parentMap[MAX_BULK_DOCS] = {0};
  AddDocumentCtx *firstZeroId = aCtx;
  RedisSearchCtx sctx{redisCtx, specId};
  //Document &doc = &aCtx->doc;

  if (aCtx->IsIndexed() || aCtx->stateFlags & ACTX_F_ERRORED) {
    // Document is complete or errored. No need for further processing.
    if (!(aCtx->stateFlags & ACTX_F_EMPTY)) {
      return;
    }
  }

  bool useTermMap = addQueue.size() > 1 && !(aCtx->stateFlags & ACTX_F_TEXTINDEXED);
  if (useTermMap) {
    firstZeroId = merge(aCtx, parentMap);
    if (firstZeroId && firstZeroId->stateFlags & ACTX_F_ERRORED) {
      // Don't treat an errored ctx as being the head of a new ID chain.
      // It's likely that subsequent entries do indeed have IDs.
      firstZeroId = nullptr;
    }
  }

  const int isBlocked = aCtx->IsBlockable();
  if (isBlocked) {
    // Force a context at this point:
    if (!isDbSelected) {
      RedisModuleCtx *thCtx = RedisModule_GetThreadSafeContext(aCtx->client.bc);
      RedisModule_SelectDb(redisCtx, RedisModule_GetSelectedDb(thCtx));
      RedisModule_FreeThreadSafeContext(thCtx);
      isDbSelected = true;
    }

    concCtx.AddKey(DocumentIndexerConcurrentKey(specKeyName, sctx));
    concCtx.ResetClock();
    concCtx.Lock();
  } else {
    sctx = *aCtx->client.sctx;
  }

  if (!sctx.spec) {
    aCtx->status.SetCode(QUERY_ENOINDEX);
    aCtx->stateFlags |= ACTX_F_ERRORED;
    goto cleanup;
  }

  // Document ID assignment:
  // In order to hold the GIL for as short a time as possible, we assign
  // document IDs in bulk. We begin using the first document ID that is assumed
  // to be zero.
  //
  // When merging multiple document IDs, the merge stage scans through the chain
  // of proposed documents and selects the first document in the chain missing an
  // ID - the subsequent documents should also all be missing IDs. If none of
  // the documents are missing IDs then the firstZeroId document is nullptr and
  // no ID assignment takes place.
  //
  // Assigning IDs in bulk speeds up indexing of smaller documents by about 10% overall.
  if (firstZeroId && firstZeroId->doc.docId == 0) {
    firstZeroId->doAssignIds(&sctx);
  }

  // Handle FULLTEXT indexes
  if (useTermMap) {
    writeMergedEntries(aCtx, &sctx, parentMap);
  } else if (aCtx->fwIdx && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    writeEntries(aCtx, &sctx);
  }

  if (!(aCtx->stateFlags & ACTX_F_OTHERINDEXED)) {
    IndexBulkData::indexBulkFields(aCtx, &sctx);
  }

cleanup:
  if (isBlocked) {
    concCtx.Unlock();
  }
  if (useTermMap) {
    mergeMap.clear();
    mergePool.Clear();
  }
}

//---------------------------------------------------------------------------------------------

class Thread {
  virtual int main() { return 0; }
  void stop(bool wait);
  int join();
};

template <class T>
class QThread: Thread {

};

class MyThread : Thread {
  virtual int main() { /* my logic */ return 0; }
};

void DocumentIndexer::main() {
  // _stopped = false;
  pthread_mutex_lock(&lock);
  while (!ShouldStop()) {
    while (addQueue.empty() && !ShouldStop()) {
      pthread_cond_wait(&cond, &lock); // @@TODO: use pthread_cond_timedwait()
    }

    if (ShouldStop()) {
      RS_LOG_ASSERT(ShouldStop(), "indexer was stopped");
      break;
    }

    if (addQueue.empty()) {
      break;
    }

    AddDocumentCtx *add = addQueue.back(); //@@@TODO: pull from front
    addQueue.pop_back();

    pthread_mutex_unlock(&lock);
    Process(add);
    add->Finish();
    pthread_mutex_lock(&lock);
  }

  pthread_mutex_unlock(&lock);
  // _stopped = true;
}

//---------------------------------------------------------------------------------------------

void *DocumentIndexer::_main(void *self_) {
  auto self = static_cast<DocumentIndexer *>(self_);
  try {
    self->main();
  } catch (Error &x) {
    throw Error("DocumentIndexer thread exception: %s", x.what());
  } catch (...) {
    throw Error("DocumentIndexer thread exception");
  }
  return nullptr;
}

//---------------------------------------------------------------------------------------------

// Add a document to the indexing queue. If successful, the indexer now takes
// ownership of the document context (until it DocumentAddCtx::Finish).

void DocumentIndexer::Add(AddDocumentCtx *aCtx) {
  if (!aCtx->IsBlockable() || !!(options & INDEXER_THREADLESS)) {
    Process(aCtx);
    aCtx->Finish();
    return;
  }

  pthread_mutex_lock(&lock);
  addQueue.push_back(aCtx);
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&lock);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Multiple Indexers

/**
 * Each index (i.e. IndexSpec) will have its own dedicated indexing thread.
 * This is because documents only need to be indexed in order with respect
 * to their document IDs, and the ID namespace is only unique among a given
 * index.
 *
 * Separating background threads also greatly simplifies the work of merging
 * or folding indexing and document ID assignment, as it can be assumed that
 * every item within the document ID belongs to the same index.
 */

// Creates a new DocumentIndexer. This initializes the structure and starts the
// thread. This does not insert it into the list of threads, though
// todo: remove the withIndexThread var once we switch to threadpool

DocumentIndexer::DocumentIndexer(IndexSpec &spec)
  : redisCtx{RedisModule_GetThreadSafeContext(nullptr)}
  , concCtx{redisCtx}
  , mergePool{TERMS_PER_BLOCK}
  , options{0}
  , isDbSelected{false}
{
  if (!!(spec.flags & Index_Temporary) || !RSGlobalConfig.concurrentMode) {
    options |= INDEXER_THREADLESS;
  }

  if (!(options & INDEXER_THREADLESS)) {
    pthread_cond_init(&cond, nullptr);
    pthread_mutex_init(&lock, nullptr);
    pthread_create(&thr, nullptr, _main, this);
    pthread_detach(thr);
  }

  //next = nullptr;
  specId = spec.uniqueId;
  specKeyName = RedisModule_CreateStringPrintf(redisCtx, INDEX_SPEC_KEY_FMT, spec.name);
}

//---------------------------------------------------------------------------------------------

// @@TODO: review termination logic (SyncStop?)
void DocumentIndexer::Stop() {
  options |= INDEXER_STOPPED;
}

//---------------------------------------------------------------------------------------------

DocumentIndexer::~DocumentIndexer() {
  options |= INDEXER_STOPPED;
  if (!(options & INDEXER_THREADLESS)) {
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&lock);
  }
  RedisModule_FreeString(redisCtx, specKeyName);
  RedisModule_FreeThreadSafeContext(redisCtx);
}

///////////////////////////////////////////////////////////////////////////////////////////////
