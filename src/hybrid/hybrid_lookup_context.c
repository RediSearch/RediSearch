#include "hybrid_lookup_context.h"

#include <stddef.h>                    // for size_t, NULL

#include "aggregate/aggregate_plan.h"  // for AGPLN_GetLookup, ...
#include "aggregate/aggregate.h"       // for AREQ_AGGPlan, AREQ
#include "rmalloc.h"                   // for rm_calloc, rm_free
#include "rmutil/rm_assert.h"          // for RS_ASSERT

#ifdef __cplusplus
extern "C" {
#endif

HybridLookupContext* HybridLookupContext_New(arrayof(AREQ*) requests, RLookup *tailLookup, bool createMissingKeys) {
    RS_ASSERT(requests && tailLookup);
    size_t nrequests = array_len(requests);

    // Build lookup context for field merging
    HybridLookupContext *lookupCtx = rm_calloc(1, sizeof(HybridLookupContext));
    lookupCtx->tailLookup = tailLookup;
    lookupCtx->sourceLookups = array_newlen(const RLookup*, nrequests);
    lookupCtx->createMissingKeys = createMissingKeys;

    // Add keys from all source lookups to create unified schema
    for (size_t i = 0; i < nrequests; i++) {
        RLookup *srcLookup = AGPLN_GetLookup(AREQ_AGGPlan(requests[i]), NULL, AGPLN_GETLOOKUP_FIRST);
        lookupCtx->sourceLookups[i] = srcLookup;
    }
    return lookupCtx;
}

void HybridLookupContext_Free(HybridLookupContext *lookupCtx) {
    array_free(lookupCtx->sourceLookups);
    rm_free(lookupCtx);
}

#ifdef __cplusplus
}
#endif
