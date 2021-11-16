#include <string.h>
#include <stdlib.h>
#include "endpoint.h"
#include "hiredis/hiredis.h"

int MREndpoint_Parse(const char *addr, MREndpoint *ep) {

  ep->host = NULL;
  ep->unixSock = NULL;
  ep->auth = NULL;

  // see if we have an auth password
  char *at = strchr(addr, '@');
  if (at) {
    ep->auth = strndup(addr, at - addr);
    addr = at + 1;
  }

  char *colon = strchr(addr, ':');
  if (!colon || colon == addr) {
    MREndpoint_Free(ep);
    return REDIS_ERR;
  }

  ep->host = strndup(addr, colon - addr);
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
    dst->host = strdup(src->host);
  }

  if (src->unixSock) {
    dst->unixSock = strdup(src->unixSock);
  }

  if (src->auth) {
    dst->auth = strdup(src->auth);
  }
}

void MREndpoint_Free(MREndpoint *ep) {
  if (ep->host) {
    free(ep->host);
    ep->host = NULL;
  }
  if (ep->unixSock) {
    free(ep->unixSock);
    ep->unixSock = NULL;
  }
  if (ep->auth) {
    free(ep->auth);
    ep->auth = NULL;
  }
}
