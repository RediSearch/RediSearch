#ifndef SUMMARIZE_SPEC_H
#define SUMMARIZE_SPEC_H

#include <stdlib.h>
#include "redismodule.h"
#include "search_request.h"

int ParseSummarizeSpecSimple(RedisModuleString **argv, int argc, size_t *offset, FieldList *fields);

int ParseSummarizeSpecDetailed(RedisModuleString **argv, int argc, size_t *offset,
                               FieldList *fields);

#endif