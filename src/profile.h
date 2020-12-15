#pragma once


#include "value.h"
#include "aggregate/aggregate.h"

#define CLOCKS_PER_MILLISEC  ((__clock_t) 1000)

#define IsProfile(r) ((r)->reqflags & QEXEC_F_PROFILE)

int Profile_Print(RedisModuleCtx *ctx, AREQ *req);