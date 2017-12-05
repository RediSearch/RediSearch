#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include "reducer.h"

ResultProcessor *NewGrouper(ResultProcessor *upstream, const char *property, Reducer *reducer,
                            RSSortingTable *stbl);

#endif