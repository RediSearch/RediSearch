/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef MINMAX_H
#define MINMAX_H

#define Min(a, b) (a) < (b) ? (a) : (b)
#define Max(a, b) (a) > (b) ? (a) : (b)
#ifndef MIN
#define MIN Min
#endif
#ifndef MAX
#define MAX Max
#endif

#endif