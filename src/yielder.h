#ifndef YIELDER_H
#define YIELDER_H
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef union {
  void *p;
  uint64_t u;
} YielderArg;

typedef int (*YielderCallback)(IndexSpec *, YielderArg *, void *);
typedef void (*YielderFreeCallback)(YielderArg *, void *);
typedef struct YielderData YielderData;

typedef struct {
  IndexSpec *spec;
  struct YielderData {
    void *idx;
    YielderArg arg;
    YielderCallback cb;
    YielderFreeCallback freecb;
  } * cbs;
} Yielder;

void YLD_Init(Yielder *y, IndexSpec *sp);
void YLD_Cleanup(Yielder *y);
void YLD_Add(Yielder *y, YielderCallback cb, YielderFreeCallback freecb, YielderArg a, void *idx);
int YLD_Continue(Yielder *y);

#ifdef __cplusplus
}
#endif
#endif