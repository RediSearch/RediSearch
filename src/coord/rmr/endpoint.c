/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <string.h>
#include <stdlib.h>
#include "endpoint.h"
#include "hiredis/hiredis.h"
#include "rmalloc.h"

int MREndpoint_Parse(const char *addr, MREndpoint *ep) {
  // zero out the endpoint, assuming it's uninitialized. This is important for freeing it later.
  memset(ep, 0, sizeof(*ep));

  // see if we have an auth password
  char *at = strchr(addr, '@');
  if (at) {
    ep->password = rm_strndup(addr, at - addr);
    addr = at + 1;
  }

  int has_opener = 0;
  if (addr[0] == '[') {
      has_opener = 1;
      ++addr; // skip the ipv6 opener '['
  }

  char *colon = strrchr(addr, ':'); // look for the last colon
  if (!colon) {
    MREndpoint_Free(ep);
    return REDIS_ERR;
  }

  size_t s = colon - addr;
  if (has_opener) {
      if (addr[s - 1] != ']') {
          MREndpoint_Free(ep);
          return REDIS_ERR;
      }
      --s; // skip the ipv6 closer ']'
  }

  ep->host = rm_strndup(addr, s);
  ep->port = atoi(colon + 1);

  if (ep->port <= 0 || ep->port > 0xFFFF) {
    MREndpoint_Free(ep);
    return REDIS_ERR;
  }
  return REDIS_OK;
}

/* Copy the endpoint's internal strings so freeing it will not hurt another copy of it */
void MREndpoint_Copy(MREndpoint *dst, const MREndpoint *src) {
  *dst = *src;
  if (src->host) {
    dst->host = rm_strdup(src->host);
  }

  if (src->unixSock) {
    dst->unixSock = rm_strdup(src->unixSock);
  }

  if (src->password) {
    dst->password = rm_strdup(src->password);
  }
}

void MREndpoint_Free(MREndpoint *ep) {
  if (ep->host) {
    rm_free(ep->host);
    ep->host = NULL;
  }
  if (ep->unixSock) {
    rm_free(ep->unixSock);
    ep->unixSock = NULL;
  }
  if (ep->password) {
    rm_free(ep->password);
    ep->password = NULL;
  }
}
