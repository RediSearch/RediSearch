
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "index_utils.h"
#include "common.h"
#include "src/forward_index.h"
#include "inverted_index.h"
#include "src/redis_index.h"
#include "numeric_range_tree.h"

std::string numToDocStr(unsigned id) {
  return "doc" + std::to_string(id);
}

size_t addDocumentWrapper(RedisModuleCtx *ctx, RSIndex *index, const char *docid, const char *field, const char *value) {
    size_t beforAddMem = get_spec(index)->stats.invertedSize;
    bool rv = RS::addDocument(ctx, index, docid, field, value);
    assert(rv);
    return (get_spec(index))->stats.invertedSize - beforAddMem;
}

InvertedIndex *createPopulateTermsInvIndex(int size, int idStep, int start_with) {
    size_t sz;
    InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), &sz);

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

        InvertedIndex_WriteForwardIndexEntry(idx, &h);
        VVW_Free(h.vw);

        id += idStep;
    }

    // printf("BEFORE: iw cap: %ld, iw size: %zd, numdocs: %d\n", w->bw.buf->cap,
    //        IW_Len(w), w->ndocs);

    return idx;
}

RefManager *createSpec(RedisModuleCtx *ctx, const std::vector<const char*>& prefixes) {
    RSIndexOptions opts = {0};
    opts.gcPolicy = GC_POLICY_FORK;
    auto ism = RediSearch_CreateIndex("idx", &opts);
    if (!ism) return ism;

    SchemaRuleArgs args = {0};
    args.type = "HASH";
    const char *empty_prefix = "";

    if (!prefixes.empty()) {
        args.prefixes = const_cast<const char**>(prefixes.data());
        args.nprefixes = static_cast<int>(prefixes.size());
    } else {
        args.prefixes = &empty_prefix;
        args.nprefixes = 1;
    }

    QueryError status = QueryError_Default();

    get_spec(ism)->rule = SchemaRule_Create(&args, {ism}, &status);
    Spec_AddToDict(ism);

    return ism;
}

void freeSpec(RefManager *ism) {
    IndexSpec_RemoveFromGlobals({ism}, false);
}

NumericRangeTree *getNumericTree(IndexSpec *spec, const char *field) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, field, strlen(field));

  return openNumericOrGeoIndex(spec, const_cast<FieldSpec *>(fs), DONT_CREATE_INDEX);
}
