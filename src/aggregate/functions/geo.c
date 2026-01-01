/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "function.h"
#include "aggregate/expr/expression.h"
#include "rs_geo.h"

#include <err.h>

// parse "x,y"
static int parseField(RSValue *argv, double *geo, QueryError *status) {
  int rv = REDISMODULE_OK;
  RSValue *val = RSValue_Dereference(argv);

  if (RSValue_IsString(val)) {
    size_t len;
    char *p = (char *)RSValue_StringPtrLen(val, &len); 
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
static int geofunc_distance(ExprEval *ctx, RSValue *result,
                            RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("geodistance", 2, 4, err);
  int rv;
  char *p, *end;
  double geo[2][2], dummy;

  for (int i = 0, j = 0; i < 2; i++) {
    if (RSValue_ToNumber(argv[j], &dummy)) {
      rv = parseLonLat(argv[j], argv[j + 1], geo[i]);
      j += 2;
    } else {
      rv = parseField(argv[j], geo[i], err);
      j += 1;
    }
    if (rv != REDISMODULE_OK) goto error;
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
  RSFunctionRegistry_RegisterFunction("geodistance", geofunc_distance, RSValue_String);
}