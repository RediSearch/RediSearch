#include "index_utils.h"
#include "common.h"
#include "src/index.h"
#include "src/inverted_index.h"
#include "src/redis_index.h"

std::string numToDocStr(unsigned id) {
  return "doc" + std::to_string(id);
}

size_t addDocumentWrapper(RedisModuleCtx *ctx, RSIndex *index, const char *docid, const char *field, const char *value) {
    size_t beforAddMem = get_spec(index)->stats.invertedSize;
    assert(RS::addDocument(ctx, index, docid, field, value));
    return (get_spec(index))->stats.invertedSize - beforAddMem;
}

InvertedIndex *createPopulateTermsInvIndex(int size, int idStep, int start_with) {
    size_t sz;
    InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &sz);

    IndexEncoder enc = InvertedIndex_GetEncoder(idx->flags);
    t_docId id = start_with > 0 ? start_with : idStep;
    for (int i = 0; i < size; i++) {
        // if (i % 10000 == 1) {
        //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
        //     w->ndocs);
        // }
        ForwardIndexEntry h;
        h.docId = id;
        h.fieldMask = 1;
        h.freq = 1;
        h.term = "hello";
        h.len = 5;

        h.vw = NewVarintVectorWriter(8);
        for (int n = idStep; n < idStep + i % 4; n++) {
            VVW_Write(h.vw, n);
        }

        InvertedIndex_WriteForwardIndexEntry(idx, enc, &h);
        VVW_Free(h.vw);

        id += idStep;
    }

    // printf("BEFORE: iw cap: %ld, iw size: %zd, numdocs: %d\n", w->bw.buf->cap,
    //        IW_Len(w), w->ndocs);

    return idx;
}

RefManager *createSpec(RedisModuleCtx *ctx) {
    RSIndexOptions opts = {0};
    opts.gcPolicy = GC_POLICY_FORK;
    auto ism = RediSearch_CreateIndex("idx", &opts);
    if (!ism) return ism;

    const char *pref = "";
    SchemaRuleArgs args = {0};
    args.type = "HASH";
    args.prefixes = &pref;
    args.nprefixes = 1;

    QueryError status = {};

    get_spec(ism)->rule = SchemaRule_Create(&args, {ism}, &status);
    Spec_AddToDict(ism);

    return ism;
}

void freeSpec(RefManager *ism) {
    IndexSpec_RemoveFromGlobals({ism}, false);
}

NumericRangeTree *getNumericTree(IndexSpec *spec, const char *field) {
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(spec, field, INDEXFLD_T_NUMERIC);

  return openNumericKeysDict(spec, fmtkey, DONT_CREATE_INDEX);
}

size_t NumericRangeGetMemory(const NumericRangeNode *Node) {
    InvertedIndex *idx = Node->range->entries;

    size_t curr_node_memory = sizeof_InvertedIndex(Index_StoreNumeric);

    // iterate idx blocks
    for (size_t i = 0; i < idx->size; ++i) {
        curr_node_memory += sizeof(IndexBlock);
        IndexBlock *blk = idx->blocks + i;
        curr_node_memory += blk->buf.cap;
    }

    return curr_node_memory;

}

size_t CalculateNumericInvertedIndexMemory(NumericRangeTree *rt, NumericRangeNode **failed_range) {
    if (!rt) {
        return 0;
    }

    NumericRangeTreeIterator *Iterator = NumericRangeTreeIterator_New(rt);
    NumericRangeNode *currNode = NULL;

    size_t total_tree_mem = 0;

    while ((currNode = NumericRangeTreeIterator_Next(Iterator))) {
        if (!currNode->range) {
            continue;
        }
        size_t curr_node_memory = NumericRangeGetMemory(currNode);

        // Ensure stats are correct
        if (curr_node_memory != currNode->range->invertedIndexSize) {
            *failed_range = currNode;
            break;
        }

        total_tree_mem += curr_node_memory;
    }

    NumericRangeTreeIterator_Free(Iterator);
    return total_tree_mem;
}
