#include "project.h"
#include <ctype.h>

static int upper_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                     pc->properties->keys[0]);
  if (RSValue_IsString(v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = toupper(p[i]);
    }
    // we set the value again, in case it was in the table or the alias is not the same as the key
    RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], *v);
  }

  return RS_RESULT_OK;
}

static int lower_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue *v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                     pc->properties->keys[0]);
  if (v && RSValue_IsString(v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = tolower(p[i]);
    }
  }
  // we set the value again, in case it was in the table or the alias is not the same as the key
  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], *v);
  return RS_RESULT_OK;
}

ResultProcessor *NewLowerArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err) {
  return NewProjectorGeneric(lower_Next, upstream, alias, args, NULL, 1, 1, err);
}

ResultProcessor *NewUpperArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err) {
  return NewProjectorGeneric(upper_Next, upstream, alias, args, NULL, 1, 1, err);
}
