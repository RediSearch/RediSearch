#ifndef __REDISEARCH_EXTN_H__
#define __REDISEARCH_EXTN_H__

#include "redisearch.h"

/* Initialize the extensions mechanism, create registries, etc */
void Extensions_Init();

typedef struct {
  RSScoringFunction sf;
  void *privdata;
} ExtensionsScoringFunctionCtx;

typedef struct {
  RSQueryTokenExpander exp;
  void *privdata;
} ExtensionsQueryExpanderCtx;

ExtensionsScoringFunctionCtx *Extensions_GetScoringFunction(const char *name);
ExtensionsQueryExpanderCtx *Extensions_GetQueryExpander(const char *name);

int Extension_Load(const char *name, RSExtensionInitFunc func);

#endif