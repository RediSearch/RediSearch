#include <assert.h>
#include "param.h"
#include "rmalloc.h"

void Param_FreeInternal(Param *param) {
  if (param->name) {
    //assert(param->type != PARAM_NONE);
    rm_free((void *)param->name);
    param->name = NULL;
  }
}



