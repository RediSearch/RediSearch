#ifndef RS_PROJECTOR_H_
#define RS_PROJECTOR_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>
#include <math.h>
#include <ctype.h>
#include "project.h"

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
 *  * LOG2 x
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
  if (pc->privdata) {
    free(pc->privdata);
  }
  free(pc);
  free(p);
}

ResultProcessor *NewProjectorGeneric(int (*NextFunc)(ResultProcessorCtx *ctx, SearchResult *res),
                                     ResultProcessor *upstream, const char *alias, CmdArg *args,
                                     void *privdata, int minArgs, int maxArgs, char **err) {

  if (minArgs != 0) {
    if (CMDARG_ARRLEN(args) < minArgs || (maxArgs > 0 && CMDARG_ARRLEN(args) > maxArgs)) {
      RETURN_ERROR(err, "Invalid or missing arguments for projection%s", "");
    }
  }

  RSMultiKey *props = RS_NewMultiKeyFromArgs(&CMDARG_ARR(args), 1);
  ProjectorCtx *ctx = NewProjectorCtx(props, alias, privdata);

  ResultProcessor *proc = NewResultProcessor(upstream, ctx);
  proc->Next = NextFunc;
  proc->Free = ProjectorCtx_GenericFree;
  return proc;
}

typedef ResultProcessor *(*ProjectorFactory)(ResultProcessor *upstream, const char *alias,
                                             CmdArg *args, char **err);

#define PROJECTOR_FACTORY(f) \
  ResultProcessor *f(ResultProcessor *upstream, const char *alias, CmdArg *args, char **err);

// Math projections
PROJECTOR_FACTORY(NewFloorArgs);
PROJECTOR_FACTORY(NewAbsArgs);
PROJECTOR_FACTORY(NewCeilArgs);
PROJECTOR_FACTORY(NewLogArgs);
PROJECTOR_FACTORY(NewLog2Args);
PROJECTOR_FACTORY(NewSqrtArgs);

// String projections
PROJECTOR_FACTORY(NewLowerArgs);
PROJECTOR_FACTORY(NewUpperArgs);
ResultProcessor *NewAddProjection(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                  char **err);
ResultProcessor *NewMulProjection(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                  char **err);
ResultProcessor *NewDivProjection(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                  char **err);
ResultProcessor *NewModProjection(ResultProcessor *upstream, const char *alias, CmdArg *args,
                                  char **err);

static struct {
  const char *k;
  ProjectorFactory f;
} projectors_g[] = {
    {"abs", NewAbsArgs},
    {"floor", NewFloorArgs},
    {"ceil", NewCeilArgs},
    {"upper", NewUpperArgs},
    {"lower", NewLowerArgs},
    {"sqrt", NewSqrtArgs},
    {"log", NewLogArgs},
    {"log2", NewLog2Args},
    {"sum", NewAddProjection},
    {"mul", NewMulProjection},
    {"div", NewDivProjection},
    {"mod", NewModProjection},
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