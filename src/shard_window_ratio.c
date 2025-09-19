/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "shard_window_ratio.h"
#include "param.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "vector_index.h"

void modifyKNNCommand(MRCommand *cmd, size_t query_arg_index, size_t effectiveK, VectorQuery *vq) {
    // Get original K value from the VectorQuery
    size_t originalK = vq->knn.k;

    // Fast path: No modification needed if K values are the same
    if (originalK == effectiveK) {
        return;
    }
    // Get saved position information
    size_t k_pos = vq->knn.k_token_pos;
    size_t k_len = vq->knn.k_token_len;

    char effectiveK_str[32];
    size_t newK_len = snprintf(effectiveK_str, sizeof(effectiveK_str), "%zu", effectiveK);

    // Replace just the K value substring at the exact position
    MRCommand_ReplaceArgSubstring(cmd, query_arg_index, k_pos, k_len, effectiveK_str, newK_len);
}
