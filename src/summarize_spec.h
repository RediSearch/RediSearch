#ifndef SUMMARIZE_SPEC_H
#define SUMMARIZE_SPEC_H

#include <stdlib.h>
#include "redismodule.h"
#include "search_request.h"

int ParseSummarize(ArgsCursor *ac, FieldList *fields);
int ParseHighlight(ArgsCursor *ac, FieldList *fields);

#endif