#include "param.h"
#include "rmalloc.h"

void Param_Free(Param *param) {
  if (param->type != PARAM_NONE)
    rm_free((void*)param->name);
}



