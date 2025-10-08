/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_hybrid_plan.h"
#include "hybrid/hybrid_request.h"


int HybridRequest_BuildDistributedPipeline(HybridRequest *hreq,
    HybridPipelineParams *hybridParams, AREQDIST_UpstreamInfo *us,
    QueryError *status) {

    auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(HybridRequest_TailAGGPlan(hreq), NULL, NULL, PLN_T_DISTRIBUTE);
    RS_ASSERT(dstp);

    dstp->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
    int rc = HybridRequest_BuildPipeline(hreq, hybridParams);
    dstp->lk.options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
    if (rc != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    std::vector<const RLookupKey *> loadFields;
    for (RLookupKey *kk = dstp->lk.head; kk != NULL; kk = kk->next) {
        if (kk->flags & RLOOKUP_F_UNRESOLVED) {
            loadFields.push_back(kk);
        }
    }

    auto &ser_args = *dstp->serialized;
    if (!loadFields.empty()) {
        ser_args.push_back(rm_strndup("LOAD", 4));
        char *ldsze;
        rm_asprintf(&ldsze, "%lu", (unsigned long)loadFields.size());
        ser_args.push_back(ldsze);
        for (auto kk : loadFields) {
            ser_args.push_back(rm_strndup(kk->name, kk->name_len));
        }
    }

    us->lookup = &dstp->lk;
    us->serialized = ser_args.data();
    us->nserialized = ser_args.size();
    return REDISMODULE_OK;
}
