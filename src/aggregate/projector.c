#ifndef RS_PROJECTOR_H_
#define RS_PROJECTOR_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>
#include <math.h>
#include <ctype.h>

typedef ResultProcessor *(*ProjectorFactory)(ResultProcessor *upstream, const char *alias,
                                             CmdArg *args, char **err);

/***
 * Math projections:
 *  * ABS x
 *  * FLOOR x
 *  * CEIL x
 *  * SQRT x
 *  * MOD x y
 *  * MUL x y
 *  * ADD x y
 *  * LOG x
 *
 * String projections:
 *  * LOWER x
 *  * UPPER x
 *  * FORMAT fmt ...
 *  * SUBSTR x
 *  * REPLACE x
 *  * CONCAT x,y,z
 *
 * General projections:
 *  * COALESCE ...
 */

typedef struct {
  RSMultiKey *properties;
  const char *alias;
  void *privdata;
} ProjectorCtx;

ProjectorCtx *NewProjectorCtx(RSMultiKey *props, const char *alias, void *privdata) {
  ProjectorCtx *ret = malloc(sizeof(*ret));
  ret->properties = props;
  ret->alias = alias;
  ret->privdata = privdata;
  return ret;
}

void ProjectorCtx_GenericFree(ResultProcessor *p) {
  ProjectorCtx *pc = p->ctx.privdata;
  if (pc->properties) {
    RSMultiKey_Free(pc->properties);
  }
  free(pc);
  free(p);
}

#define RETURN_ERROR(err, fmt, ...)  \
  {                                  \
    asprintf(err, fmt, __VA_ARGS__); \
    return NULL;                     \
  }

static int abs_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                    pc->properties->keys[0]);
  if (v.t == RSValue_Number) {
    RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0],
                   RS_NumVal(fabs(v.numval)));
  } else {
    RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], RS_NullVal());
  }
  return RS_RESULT_OK;
}

static int upper_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                    pc->properties->keys[0]);
  if (RSValue_IsString(&v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(&v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = toupper(p[i]);
    }
  }
  // we set the value again, in case it was in the table or the alias is not the same as the key
  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], v);

  return RS_RESULT_OK;
}

static int lower_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  ProjectorCtx *pc = ctx->privdata;
  int rc;

  // this will return EOF if needed
  ResultProcessor_ReadOrEOF(ctx->upstream, res, 0);

  RSValue v = SearchResult_GetValue(res, QueryProcessingCtx_GetSortingTable(ctx->qxc),
                                    pc->properties->keys[0]);
  if (RSValue_IsString(&v)) {
    size_t sz;
    char *p = (char *)RSValue_StringPtrLen(&v, &sz);
    for (size_t i = 0; i < sz; i++) {
      p[i] = tolower(p[i]);
    }
  }
  // we set the value again, in case it was in the table or the alias is not the same as the key
  RSFieldMap_Set(&res->fields, pc->alias ? pc->alias : pc->properties->keys[0], v);
  return RS_RESULT_OK;
}

ResultProcessor *NewProjectorGeneric(int (*NextFunc)(ResultProcessorCtx *ctx, SearchResult *res),
                                     ResultProcessor *upstream, const char *alias, CmdArg *args,
                                     void *privdata, int minArgs, int maxArgs, char **err) {

  if (minArgs != 0) {
    if (CMDARG_ARRLEN(args) < minArgs || (maxArgs > 0 && CMDARG_ARRLEN(args) > maxArgs)) {
      RETURN_ERROR(err, "Invalid or missing arguments for projection%s", "");
    }
  }

  RSMultiKey *props = RS_NewMultiKeyFromArgs(&CMDARG_ARR(args));
  ProjectorCtx *ctx = NewProjectorCtx(props, alias, privdata);

  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = NextFunc;
  proc->Free = ProjectorCtx_GenericFree;
  return proc;
}

ResultProcessor *NewAbsArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                            char **err) {
  return NewProjectorGeneric(abs_Next, upstream, alias, args, NULL, 1, 1, err);
}

ResultProcessor *NewFloorArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err);
ResultProcessor *NewCeilArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                             char **err);

ResultProcessor *NewLowerArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err);

ResultProcessor *NewUpperArgs(ResultProcessor *upstream, const char *alias, CmdArg *args,
                              char **err) {
  return NewProjectorGeneric(upper_Next, upstream, alias, args, NULL, 1, 1, err);
}

static struct {
  const char *k;
  ProjectorFactory f;
} projectors_g[] = {
    {"abs", NewAbsArgs},
    {"upper", NewUpperArgs},

    {NULL, NULL},
};

/* Projectors are result processors that have 1:1 conversion of values, without aggregation. I.e.
 * they have no accumulation stage and just shape results based on the parameters */
ResultProcessor *GetProjector(ResultProcessor *upstream, const char *name, const char *alias,
                              CmdArg *args, char **err) {
  for (int i = 0; projectors_g[i].k != NULL; i++) {
    if (!strcasecmp(projectors_g[i].k, name)) {
      return projectors_g[i].f(upstream, alias, args, err);
    }
  }

  RETURN_ERROR(err, "Could not find reducer '%s'", name);
}
#endif