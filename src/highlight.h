#ifndef HIGHLIGHT_H_
#define HIGHLIGHT_H_

#include "result_processor.h"
#include "search_request.h"

ResultProcessor *NewHighlightProcessor(ResultProcessor *upstream, RSSearchRequest *req);

#endif