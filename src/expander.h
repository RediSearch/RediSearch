#ifndef __QUERY_EXPANDER_H__
#define __QUERY_EXPANDER_H__
#include <stdlib.h>
#include "types.h"
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
  QueryNode *(*Expand)(void *ctx, Query *q, QueryNode *);
  void *ctx;
} QueryExpander;

#endif