#ifndef PROJECT_H__
#define PROJECT_H__

#include <result_processor.h>

typedef struct {
  RSMultiKey *properties;
  const char *alias;
  void *privdata;
} ProjectorCtx;

ProjectorCtx *NewProjectorCtx(RSMultiKey *props, const char *alias, void *privdata);

ResultProcessor *NewProjectorGeneric(int (*NextFunc)(ResultProcessorCtx *ctx, SearchResult *res),
                                     ResultProcessor *upstream, const char *alias, CmdArg *args,
                                     void *privdata, int minArgs, int maxArgs, char **err);

void ProjectorCtx_GenericFree(ResultProcessor *p);

#define RETURN_ERROR(err, fmt, ...)  \
  {                                  \
    asprintf(err, fmt, __VA_ARGS__); \
    return NULL;                     \
  }

#endif
