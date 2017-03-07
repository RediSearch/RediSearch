#ifndef __QUERY_EXPANDER_H__
#define __QUERY_EXPANDER_H__
#include <stdlib.h>
#include "redisearch.h"
#include "query_node.h"
#include "query.h"

/* A QueryExpander is a callback that, given a query node, can expand it, e.g.
* stem it, or replace it.
*
* An expander receives a query node, that can be a token, a phrase, etc.
* It returns a new query node if the node was expanded. This can be the original
* node if it just added a token to a phrase, or a new UNION node if we want to
* add a stem for example.
*
* If the expander wishes not to do anything with the node, it should return
* NULL.
* If it returned anything other than NULL, it is NOT called recursively for any
* node below the returned one.
*/
typedef struct QueryExpander {
  /* The expand method - receive a query node, and optionally return a new node
   * replacing it */
  QueryNode *(*Expand)(void *ctx, Query *q, QueryNode *);
  /* Free method - free the expander's context. If set to NULL we just call
   * free(ctx) if ctx is not NULL */
  void (*Free)(void *ctx);

  /* Private context, e.g. stemmer instance */
  void *ctx;
} QueryExpander;

/* Register a query expander by name (case insensitive) */
void RegisterQueryExpander(const char *name, QueryExpander exp);

/* Get a query expander by name (case insensitive). If the expander does not
 * exist we return NULL */
QueryExpander *GetQueryExpander(const char *name);

#endif