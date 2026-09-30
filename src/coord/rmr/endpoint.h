/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
  bool isTls;
  char *unixSock;
  char *password;
} MREndpoint;

// The functions over `MREndpoint` are implemented in Rust and declared in `rmr_ffi.h`.

#ifdef __cplusplus
}
#endif
