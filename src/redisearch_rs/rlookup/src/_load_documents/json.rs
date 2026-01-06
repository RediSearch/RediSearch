/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::RSValueFFI;

use crate::{
    _load_documents::{LoadDocumentError, LoadDocumentOptions},
    RLookup, RLookupRow,
};

pub fn get_all(
    lookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // int rc = REDISMODULE_ERR;
    //   if (!japi) {
    //     return rc;
    //   }

    //   JSONResultsIterator jsonIter = NULL;
    //   RedisModuleCtx *ctx = options->sctx->redisCtx;

    //   RedisModuleString* keyName = RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));
    //   RedisJSON jsonRoot = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    //   RedisModule_FreeString(ctx, keyName);
    //   if (!jsonRoot) {
    //     goto done;
    //   }

    //   jsonIter = japi->get(jsonRoot, JSON_ROOT);
    //   if (jsonIter == NULL) {
    //     goto done;
    //   }

    //   RSValue *vptr;
    //   int res = jsonIterToValue(ctx, jsonIter, options->sctx->apiVersion, &vptr);
    //   japi->freeIter(jsonIter);
    //   if (res == REDISMODULE_ERR) {
    //     goto done;
    //   }
    //   RLookupKey *rlk = RLookup_FindKey(it, JSON_ROOT, strlen(JSON_ROOT));
    //   if (!rlk) {
    //     // First returned document, create the key.
    //     rlk = RLookup_GetKey_LoadEx(it, JSON_ROOT, strlen(JSON_ROOT), JSON_ROOT, RLOOKUP_F_NOFLAGS);
    //   }
    //   RLookup_WriteOwnKey(rlk, dst, vptr);

    //   rc = REDISMODULE_OK;

    // done:
    //   return rc;

    todo!()
}

pub fn get_one() -> Result<(), LoadDocumentError> {
    // if (!japi) {
    //     QueryError_SetCode(options->status, QUERY_ERROR_CODE_UNSUPP_TYPE);
    //     RedisModule_Log(RSDummyContext, "warning", "cannot operate on a JSON index as RedisJSON is not loaded");
    //     return REDISMODULE_ERR;
    //   }

    //   if (isValueAvailable(kk, dst, options)) {
    //     return REDISMODULE_OK;
    //   }

    //   // In this case, the flag must be obtained from JSON
    //   RedisModuleCtx *ctx = options->sctx->redisCtx;
    //   const bool keyPtrFromDMD = options->dmd != NULL;
    //   char *keyPtr = keyPtrFromDMD ? options->dmd->keyPtr : (char *)options->keyPtr;
    //   if (!*keyobj) {

    //     RedisModuleString* keyName = RedisModule_CreateString(ctx, keyPtr, keyPtrFromDMD ? sdslen(keyPtr) : strlen(keyPtr));
    //     *keyobj = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    //     RedisModule_FreeString(ctx, keyName);

    //     if (!*keyobj) {
    //       QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
    //       return REDISMODULE_ERR;
    //     }
    //   }

    //   // Get the actual json value
    //   RedisModuleString *val = NULL;
    //   RSValue *rsv = NULL;

    //   JSONResultsIterator jsonIter = (*kk->path == '$') ? japi->get(*keyobj, kk->path) : NULL;

    //   if (!jsonIter) {
    //     // The field does not exist and and it isn't `__key`
    //     if (!strcmp(kk->path, UNDERSCORE_KEY)) {
    //       rsv = RSValue_NewString(rm_strdup(keyPtr), keyPtrFromDMD ? sdslen(keyPtr) : strlen(keyPtr));
    //     } else {
    //       return REDISMODULE_OK;
    //     }
    //   } else {
    //     int res = jsonIterToValue(ctx, jsonIter, options->sctx->apiVersion, &rsv);
    //     japi->freeIter(jsonIter);
    //     if (res == REDISMODULE_ERR) {
    //       return REDISMODULE_OK;
    //     }
    //   }

    //   // Value has a reference count of 1
    //   RLookup_WriteOwnKey(kk, dst, rsv);
    //   return REDISMODULE_OK;

    todo!()
}
