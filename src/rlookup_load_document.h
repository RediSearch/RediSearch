/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_LOAD_DOCUMENT_H
#define RLOOKUP_LOAD_DOCUMENT_H
#include "hiredis/sds.h"

#ifdef __cplusplus
extern "C" {
#endif

// added as entry point for the rust code
// Required from Rust therefore exposed as a non-"inline static" function here.
size_t sdslen_rust(const sds s);

#ifdef __cplusplus
}
#endif

#endif
