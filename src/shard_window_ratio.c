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

/**
 * Fast parameter modification for "KNN $k" case.
 * Finds parameter name in PARAMS section and updates its value.
 *
 * Example: PARAMS 4 k 50 blob <data> -> PARAMS 4 k 25 blob <data>
 *
 * @param cmd The MRCommand containing the PARAMS section
 * @param paramName The parameter name to find (e.g., "k")
 * @param effectiveK The new K value to set
 * @return 0 on success, -1 if parameter not found
 */
static int modifyParameterKInParams(MRCommand *cmd, const char *paramName, size_t effectiveK) {
    // Scan command arguments to find parameter name
    for (size_t i = 0; i < cmd->num - 1; i++) {  // -1 because we check i+1
        if (strcmp(cmd->strs[i], paramName) == 0) {
            // Found parameter name, value is at next index
            char effectiveK_str[32];
            snprintf(effectiveK_str, sizeof(effectiveK_str), "%zu", effectiveK);
            MRCommand_ReplaceArg(cmd, i + 1, effectiveK_str, strlen(effectiveK_str));
            return 0;  // Success
        }
    }
    return -1;  // Parameter not found
}

/**
 * Precise literal K modification using saved position information.
 * Replaces the exact K value at the known position in the query string.
 *
 * Example: "*=>[KNN 50 @vec $blob]" -> "*=>[KNN 25 @vec $blob]"
 *
 * @param cmd The MRCommand containing the query string
 * @param originalK The original K value (for verification)
 * @param effectiveK The new K value to set
 * @param knnNode The QueryNode with saved position information
 * @return 0 on success, -1 on error
 */
static int modifyLiteralKUsingPosition(MRCommand *cmd, size_t originalK, size_t effectiveK, QueryNode *knnNode) {
    // Find the query string in the command arguments
    for (size_t i = 0; i < cmd->num; i++) {
        const char* queryStr = MRCommand_ArgStringPtrLen(cmd, i, NULL);
        if (queryStr && strstr(queryStr, "KNN")) {

            // Get saved position information
            size_t k_pos = knnNode->vn.vq->knn.k_literal_pos;
            size_t k_len = knnNode->vn.vq->knn.k_literal_len;

            // Verify position is valid and K value matches
            size_t queryLen;
            MRCommand_ArgStringPtrLen(cmd, i, &queryLen);
            if (k_pos > 0 && k_pos + k_len <= queryLen) {
                char originalK_str[32];
                snprintf(originalK_str, sizeof(originalK_str), "%zu", originalK);

                if (strncmp(queryStr + k_pos, originalK_str, k_len) == 0) {
                    // Create new query string with replaced K value
                    char effectiveK_str[32];
                    snprintf(effectiveK_str, sizeof(effectiveK_str), "%zu", effectiveK);

                    size_t newK_len = strlen(effectiveK_str);
                    size_t queryLen = strlen(queryStr);
                    size_t newQueryLen = queryLen - k_len + newK_len;

                    char* newQuery = malloc(newQueryLen + 1);
                    if (!newQuery) return -1;

                    // Copy: before K + new K + after K
                    memcpy(newQuery, queryStr, k_pos);
                    memcpy(newQuery + k_pos, effectiveK_str, newK_len);
                    strcpy(newQuery + k_pos + newK_len, queryStr + k_pos + k_len);

                    // Replace the query string in the command
                    MRCommand_ReplaceArg(cmd, i, newQuery, newQueryLen);
                    free(newQuery);
                    return 0;  // Success
                }
            }
        }
    }
    return -1;  // Query string not found or position invalid
}

// Note: We don't remove $SHARD_K_RATIO from query strings
// as shards can safely ignore this attribute as a no-op

int modifyKNNCommand(MRCommand *cmd, size_t originalK, size_t effectiveK, knnContext *knnCtx) {
    // Fast path: No modification needed if K values are the same
    QueryNode *knnNode = knnCtx->queryNode;
    if (!cmd || !knnNode || originalK == effectiveK) {
        return 0;
    }

    // Determine modification strategy based on how K was provided
    if (knnNode->params[1].type == PARAM_NONE) {
        // Case 1: Literal K - use saved position for precise replacement
        return modifyLiteralKUsingPosition(cmd, originalK, effectiveK, knnNode);
    } else {
        // Case 2: Parameter K - modify PARAMS section
        const char *paramName = knnNode->params[1].name;
        return modifyParameterKInParams(cmd, paramName, effectiveK);
    }
}
