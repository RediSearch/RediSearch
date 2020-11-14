
#pragma once

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdexcept>

#include "rmutil/args.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define QUERY_XERRS(X)                                                          \
  X(QUERY_EGENERIC, "Generic error evaluating the query")                       \
  X(QUERY_ESYNTAX, "Parsing/Syntax error for query string")                     \
  X(QUERY_EPARSEARGS, "Error parsing query/aggregation arguments")              \
  X(QUERY_EADDARGS, "Error parsing document indexing arguments")                \
  X(QUERY_EEXPR, "Parsing/Evaluating dynamic expression failed")                \
  X(QUERY_EKEYWORD, "Could not handle query keyword")                           \
  X(QUERY_ENORESULTS, "Query matches no results")                               \
  X(QUERY_EBADATTR, "Attribute not supported for term")                         \
  X(QUERY_EINVAL, "Could not validate the query nodes (bad attribute?)")        \
  X(QUERY_EBUILDPLAN, "Could not build plan from query")                        \
  X(QUERY_ECONSTRUCT_PIPELINE, "Could not construct query pipeline")            \
  X(QUERY_ENOREDUCER, "Missing reducer")                                        \
  X(QUERY_EREDUCER_GENERIC, "Generic reducer error")                            \
  X(QUERY_EAGGPLAN, "Could not plan aggregation request")                       \
  X(QUERY_ECURSORALLOC, "Could not allocate a cursor")                          \
  X(QUERY_EREDUCERINIT, "Could not initialize reducer")                         \
  X(QUERY_EQSTRING, "Bad query string")                                         \
  X(QUERY_ENOPROPKEY, "Property does not exist in schema")                      \
  X(QUERY_ENOPROPVAL, "Value was not found in result (not a hard error)")       \
  X(QUERY_ENODOC, "Document does not exist")                                    \
  X(QUERY_ENOOPTION, "Invalid option")                                          \
  X(QUERY_EREDISKEYTYPE, "Invalid Redis key")                                   \
  X(QUERY_EINDEXEXISTS, "Index already exists")                                 \
  X(QUERY_EBADOPTION, "Option not supported for current mode")                  \
  X(QUERY_ELIMIT, "Limit exceeded")                                             \
  X(QUERY_ENOINDEX, "Index not found")                                          \
  X(QUERY_EDOCEXISTS, "Document already exists")                                \
  X(QUERY_EDOCNOTADDED, "Document was not added because condition was unmet")   \
  X(QUERY_EDUPFIELD, "Field was specified twice")                               \
  X(QUERY_EGEOFORMAT, "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"") \
  X(QUERY_ENODISTRIBUTE, "Could not distribute the operation")                  \
  X(QUERY_EUNSUPPTYPE, "Unsupported index type")                                \
  X(QUERY_ENOTNUMERIC, "Could not convert value to a number")

enum QueryErrorCode {
  QUERY_OK = 0,

#define X(N, msg) N,
  QUERY_XERRS(X)
#undef X
};

//---------------------------------------------------------------------------------------------

struct QueryError {
  QueryErrorCode code;
  char *detail;

  QueryError();

  static const char *Strerror(QueryErrorCode code);

  const char *GetError() const;

  void SetError(QueryErrorCode code, const char *err);
  void SetCode(QueryErrorCode code);
  void MaybeSetCode(QueryErrorCode code);
  void SetErrorFmt(QueryErrorCode code, const char *fmt, ...);
  void ClearError();

  void FmtUnknownArg(ArgsCursor *ac, const char *name);

  // Return true if the object has an error set
  bool HasError() const { return code != QUERY_OK; }
};

//---------------------------------------------------------------------------------------------

// Convenience macro to set an error of a 'bad argument' with the name of the argument
#define QERR_MKBADARGS_FMT(status, fmt, ...)  \
  (status)->SetErrorFmt(QUERY_EPARSEARGS, (fmt), ##__VA_ARGS__)

// Convenience macro to extract the error string of the argument parser
#define QERR_MKBADARGS_AC(status, name, rv)  \
  (status)->SetErrorFmt(QUERY_EPARSEARGS, "Bad arguments for %s: %s", (name), AC_Strerror(rv))

#define QERR_MKSYNTAXERR(status, ...) (status)->SetErrorFmt(QUERY_ESYNTAX, ##__VA_ARGS__)

// Convenience macro to reply the error string to redis and clear the error code.
// I'm making this into a macro so I don't need to include redismodule.h
#define QueryError_ReplyAndClear(rctx, qerr)               \
  ({                                                       \
    RedisModule_ReplyWithError(rctx, (qerr)->GetError());  \
    (qerr)->ClearError();                                  \
    REDISMODULE_OK;                                        \
  })

#define QueryError_ReplyNoIndex(rctx, ixname)                           \
  {                                                                     \
    QueryError qidx__tmp;                                               \
    qidx__tmp.SetErrorFmt(QUERY_ENOINDEX, "%s: No such index", ixname); \
    qidx__tmp.ReplyAndClear(rctx, &qidx__tmp);                          \
  }

//---------------------------------------------------------------------------------------------

class Error : public std::runtime_error {
public:
  Error(const char *fmt, ...);
  Error(QueryError *err) : std::runtime_error(err ? err->detail : "Query error") {}
};

///////////////////////////////////////////////////////////////////////////////////////////////
