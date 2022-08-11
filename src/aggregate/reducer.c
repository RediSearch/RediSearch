#include "reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

static UnorderedMap<String, ReducerFactory> g_reducersMap;

//---------------------------------------------------------------------------------------------

template <class T>
void RegisterReducer(const char *name) {
  g_reducersMap[name] = [](const ReducerOptions *opt) {
      return new T{opt};
    };
}

//---------------------------------------------------------------------------------------------

static bool isBuiltinsRegistered = false;

ReducerFactory RDCR_GetFactory(const char *name) {
  String ucname{name};
  std::for_each(ucname.begin(), ucname.end(), [](char & c){ c = ::toupper(c); });

  if (!isBuiltinsRegistered) {
    isBuiltinsRegistered = true;
    RDCR_RegisterBuiltins();
  }
  auto reducer = g_reducersMap.find(name);
  if (reducer != g_reducersMap.end()) {
    return reducer->second;
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

#define RDCR_XBUILTIN(X)                       \
  X(RDCRCount, "COUNT")                        \
  X(RDCRSum, "SUM")                            \
  X(RDCRToList, "TOLIST")                      \
  X(RDCRMin, "MIN")                            \
  X(RDCRMax, "MAX")                            \
  X(RDCRAvg, "AVG")                            \
  X(RDCRCountDistinct, "COUNT_DISTINCT")       \
  X(RDCRCountDistinctish, "COUNT_DISTINCTISH") \
  X(RDCRQuantile, "QUANTILE")                  \
  X(RDCRStdDev, "STDDEV")                      \
  X(RDCRFirstValue, "FIRST_VALUE")             \
  X(RDCRRandomSample, "RANDOM_SAMPLE")         \
  X(RDCRHLLCommon, "HLL")                      \
  X(RDCRHLLSum, "HLL_SUM")

//---------------------------------------------------------------------------------------------

void RDCR_RegisterBuiltins() {
#define X(T, name) RegisterReducer<T>(name);
  RDCR_XBUILTIN(X);
#undef X
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Utility function to read the next argument as a lookup key.
// This advances the args variable (ReducerOptions::options) by one.
// If the lookup fails, the appropriate error code is stored in the status within the options.

bool ReducerOptions::GetKey(const RLookupKey **out) const {
  ArgsCursor *ac = args;
  const char *s;
  if (ac->GetString(&s, NULL, 0) != AC_OK) {
    QERR_MKBADARGS_FMT(status, "Missing arguments for %s", name);
    return false;
  }

  // Get the input key
  if (*s == '@') {
    s++;
  }
  *out = srclookup->GetKey(s, RLOOKUP_F_HIDDEN);
  if (!*out) {
    status->SetErrorFmt(QUERY_ENOPROPKEY, "Property `%s` not present in document or pipeline", s);
    return false;
  }
  return true;
}

//---------------------------------------------------------------------------------------------

bool ReducerOptions::EnsureArgsConsumed() const {
  if (args->NumRemaining()) {
    status->FmtUnknownArg(args, name);
    return false;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
