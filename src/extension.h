#ifndef __REDISEARCH_EXTN_H__
#define __REDISEARCH_EXTN_H__

#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif
/* Initialize the extensions mechanism, create registries, etc */
void Extensions_Init();
/* clear the extensions list */
void Extensions_Free();

/* Context for saving a scoring function and its private data and free */
typedef struct {
  RSScoringFunction sf;
  RSFreeFunction ff;
  void *privdata;
} ExtScoringFunctionCtx;

/* Context for saving the a token expander and its free / privdata */
typedef struct {
  RSQueryTokenExpander exp;
  RSFreeFunction ff;
  void *privdata;
} ExtQueryExpanderCtx;

/* Get a scoring function by name. Returns NULL if no such scoring function exists */
ExtScoringFunctionCtx *Extensions_GetScoringFunction(ScoringFunctionArgs *fnargs, const char *name);

/* Get a query expander function by name. Returns NULL if no such function exists */
ExtQueryExpanderCtx *Extensions_GetQueryExpander(RSQueryExpanderCtx *ctx, const char *name);

/* Load an extension explicitly with its name and an init function */
int Extension_Load(const char *name, RSExtensionInitFunc func);

/* Dynamically load a RediSearch extension by .so file path. Returns REDISMODULE_OK or ERR. errMsg
 * is set to NULL on success or an error message on failure */
int Extension_LoadDynamic(const char *path, char **errMsg);

#ifdef __cplusplus
}
#endif
#endif