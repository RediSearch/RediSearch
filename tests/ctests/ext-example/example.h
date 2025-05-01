/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef EXT_EXAMPLE_H__
#define EXT_EXAMPLE_H__

#include "redisearch.h"

#define EXPANDER_NAME "EXAMPLE_EXPANDER"
#define SCORER_NAME "EXAMPLE_SCORER"

const char *extentionName = "EXAMPLE_EXTENSION";

int RS_ExtensionInit(RSExtensionCtx *ctx);

#endif