/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#pragma once

/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
  char *unixSock;
  char *password;
} MREndpoint;

/* Parse a TCP address into an endpoint, in the format of host:port */
int MREndpoint_Parse(const char *addr, MREndpoint *ep);

/* Copy the endpoint's internal strings so freeing it will not hurt another copy of it */
void MREndpoint_Copy(MREndpoint *dst, const MREndpoint *src);

/* Free the endpoint's internal string, doesn't actually free the endpoint object, which is usually
 * allocated on the stack or as part of a value array */
void MREndpoint_Free(MREndpoint *ep);
