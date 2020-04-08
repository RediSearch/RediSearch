#include "yielder.h"
#include "util/arr.h"
void YLD_Init(Yielder *y, IndexSpec *sp) {
  if (y->cbs) {
    array_clear(y->cbs);
  } else {
    y->cbs = array_new(YielderData, 8);
  }
}

void YLD_Add(Yielder *y, YielderCallback cb, YielderFreeCallback freecb, YielderArg a, void *idx) {
  YielderData *yd = array_ensure_tail(&y->cbs, YielderData);
  yd->cb = cb;
  yd->arg = a;
  yd->idx = idx;
  yd->freecb = freecb;
}

int YLD_Continue(Yielder *y) {
  size_t n = array_len(y->cbs);
  for (size_t ii = 0; ii < n; ++ii) {
    if (!y->cbs[ii].cb(y->spec, &y->cbs[ii].arg, y->cbs[ii].idx)) {
      return 0;
    }
  }
  return 1;
}

void YLD_Cleanup(Yielder *y) {
  size_t n = array_len(y->cbs);
  for (size_t ii = 0; ii < n; ++ii) {
    if (y->cbs[ii].freecb) {
      y->cbs[ii].freecb(&y->cbs[ii].arg, y->cbs[ii].idx);
    }
  }
}