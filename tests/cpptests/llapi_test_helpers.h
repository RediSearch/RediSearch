/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Test helpers that replace what the llAPI (`src/redisearch_api.{h,c}`) used
// to provide for C++ tests. These wrap the same internal functions the llAPI
// implementation called, but live in the test tree so the module no longer
// has to export them.

#pragma once

#include "redismodule.h"
#include "util/references.h"

#include <string>
#include <vector>

// Alias retained because several tests still pass spec references as
// `RSIndex *` (formerly a typedef in redisearch_api.h).
typedef RefManager RSIndex;

namespace RS {

/** Delete a document from the test index. Returns true if the document existed. */
bool deleteDocument(RedisModuleCtx *ctx, RSIndex *index, const char *docid);

/** Run a query string against the index and return the matching document keys. */
std::vector<std::string> search(RSIndex *index, const char *s);

}  // namespace RS

/** Allocate a bare-bones index with FORK GC enabled but no SchemaRule. The
 *  caller is responsible for attaching a rule before doing anything that
 *  requires one (e.g. adding fields). */
RefManager *createEmptySpec(const char *name);

/** Add a TAG/NUMERIC field to a test spec directly, bypassing FT.CREATE /
 *  FT.ALTER. The spec must already have a SchemaRule. */
void addTagField(RefManager *ism, const char *name);
void addNumericField(RefManager *ism, const char *name);
