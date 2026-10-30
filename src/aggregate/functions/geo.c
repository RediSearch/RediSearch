/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "function.h"
#include "value_ffi.h"
#include "aggregate/expr/expression.h"
#include "geo_ffi.h"

static int geofunc_distance(ExprEval *ctx, RSValue **argv, size_t argc, RSValue *result) {
  RSValue_SetNumber(result, GeoDistance_Evaluate(argv, argc, ctx->err));
  return EXPR_EVAL_OK;
}

void RegisterGeoFunctions() {
  RSFunctionRegistry_RegisterFunction("geodistance", geofunc_distance, RSValueType_String, 2, 4);
}
