/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <string.h>
#include <stdlib.h>
#include "endpoint.h"
#include "hiredis/hiredis.h"
#include "rmalloc.h"

int MREndpoint_Parse(const char *addr, MREndpoint *ep) {

  ep->host = NULL;
  ep->auth = NULL;

  // see if we have an auth password
  char *at = strchr(addr, '@');
  if (at) {
    ep->auth = rm_strndup(addr, at - addr);
    addr = at + 1;
  }

  int has_opener = 0;
  if (addr[0] == '[') {
      has_opener = 1;
      ++addr; // skip the ipv6 opener '['
  }

  char *colon = strrchr(addr, ':');

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

  if (src->auth) {
    dst->auth = rm_strdup(src->auth);
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
  if (ep->auth) {
    rm_free(ep->auth);
    ep->auth = NULL;
  }
}
