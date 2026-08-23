/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __EXT_DEFAULT_H__
#define __EXT_DEFAULT_H__
#include "redisearch.h"
// The scorer- and expander-name macros (TFIDF_SCORER_NAME, DEFAULT_EXPANDER_NAME,
// ...) are the single source of truth in the Rust `query_types` crate and
// generated into this header by cheadergen.
#include "query_types.h"

int DefaultExtensionInit(RSExtensionCtx *ctx);

#endif
