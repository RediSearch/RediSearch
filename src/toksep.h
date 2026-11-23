/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef TOKSEP_H
#define TOKSEP_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Function reads string pointed to by `s` and indicates the length of the next
 * token in `tokLen`. `s` is set to NULL if this is the last token.
 */
char *toksep(char **s, size_t *tokLen);

/** Classifies the low byte of `c` as a token separator. */
int istoksep(int c);

#ifdef __cplusplus
}
#endif

#endif
