/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "function.h"
#include "aggregate/expr/expression.h"
#include "rs_geo.h"

// parse "x,y"
static int parseField(RSValue *argv, double *geo, QueryError *status) {
  int rv = REDISMODULE_OK;
  RSValue *val = RSValue_Dereference(argv);

  if (RSValue_IsString(val)) {
    size_t len;
    const char *p = RSValue_StringPtrLen(val, &len);
    rv = parseGeo(p, len, &geo[0], &geo[1], status);
  } else if (val && val->t == RSValue_Number) {
    double dbl;
    RSValue_ToNumber(val, &dbl);
    if (decodeGeo(dbl, geo) == 0) {
      rv = REDISMODULE_ERR;
    }
  } else {
    rv = REDISEARCH_ERR;
  }

  return rv;
}

// parse x,y
static int parseLonLat(RSValue *arg1, RSValue *arg2, double *geo) {
  if (!RSValue_ToNumber(arg1, &geo[0]) || !RSValue_ToNumber(arg2, &geo[1])) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

/* distance() */
static int geofunc_distance(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {
  int rv;
  double geo[2][2], dummy;

  switch (argc) {
  case 2:
    for (int i = 0; i < 2; i++) {
      rv = parseField(&argv[i], geo[i], ctx->err);
      if (rv != REDISMODULE_OK) goto error;
    }
    break;

  case 4:
    for (int i = 0, j = 0; i < 2; i++, j += 2) {
      rv = parseLonLat(&argv[j], &argv[j + 1], geo[i]);
      if (rv != REDISMODULE_OK) goto error;
    }
    break;

  case 3:
    if (RSValue_ToNumber(&argv[0], &dummy)) {
      // lon,lat,"lon,lat"
      rv = parseLonLat(&argv[0], &argv[1], geo[0]);
      if (rv != REDISMODULE_OK) goto error;
      rv = parseField(&argv[2], geo[1], ctx->err);
      if (rv != REDISMODULE_OK) goto error;
    } else {
      // "lon,lat",lon,lat
      rv = parseField(&argv[0], geo[0], ctx->err);
      if (rv != REDISMODULE_OK) goto error;
      rv = parseLonLat(&argv[1], &argv[2], geo[1]);
      if (rv != REDISMODULE_OK) goto error;
    }
    break;
  }

  double distance = geohashGetDistance(geo[0][0], geo[0][1], geo[1][0], geo[1][1]);
  distance = round(distance * 100) / 100;
  RSValue_SetNumber(result, distance);
  return EXPR_EVAL_OK;

error:
  RSValue_SetNumber(result, NAN);
  return EXPR_EVAL_OK;
}

void RegisterGeoFunctions() {
  RSFunctionRegistry_RegisterFunction("geodistance", geofunc_distance, RSValue_String, 2, 4);
}
