#include "function.h"
#include "aggregate/expr/expression.h"
#include "dep/geo/rs_geo.h"
#include "dep/geo/geohash_helper.h"
#include <err.h>

// TODO: remove when integrated with geo-out-of-keyspace
static int parseGeo(const char *c, size_t len, double *lon, double *lat) {
  char str[len + 1];
  memcpy(str, c, len + 1);
  char *pos = strpbrk(str, " ,");
  if (!pos) {
    return REDISMODULE_ERR;
  }
  *pos = '\0';
  pos++;

  char *end1 = NULL, *end2 = NULL;
  *lon = strtod(str, &end1);
  *lat = strtod(pos, &end2);
  if (*end1 || *end2) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

// parse "x,y"
static int parseField(RSValue *argv, double *geo) {
  RSValue *val = RSValue_Dereference(argv);
  if (!RSValue_IsString(val)) {
    return REDISEARCH_ERR;
  }
  size_t len;
  char *p = (char *)RSValue_StringPtrLen(val, &len); 
  return parseGeo(p, len, &geo[0], &geo[1]);
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
      rv = parseField(argv[j], geo[i]);
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