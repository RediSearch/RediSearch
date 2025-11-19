/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "src/query.h"
#include "src/stopwords.h"
#include "src/extension.h"
#include "src/ext/default.h"

#include <cstring>

/**
 * C++ wrapper for RSSearchOptions with default initialization
 */
struct SearchOptionsCXX : RSSearchOptions {
  SearchOptionsCXX() {
    memset(this, 0, sizeof(*this));
    flags = RS_DEFAULT_QUERY_FLAGS;
    fieldmask = RS_FIELDMASK_ALL;
    language = DEFAULT_LANGUAGE;
    stopwords = DefaultStopWordList();
  }
};

/**
 * C++ wrapper for QueryAST with convenient parsing and error handling methods
 * This class provides a RAII-style interface for query parsing and validation
 */
class QASTCXX : public QueryAST {
  SearchOptionsCXX m_opts;
  QueryError m_status = QueryError_Default();
  RedisSearchCtx *sctx = NULL;

 public:
  QASTCXX() {
    memset(static_cast<QueryAST *>(this), 0, sizeof(QueryAST));
  }

  QASTCXX(RedisSearchCtx &sctx) : QASTCXX() {
    setContext(&sctx);
  }

  void setContext(RedisSearchCtx *sctx) {
    this->sctx = sctx;
  }

  /**
   * Parse a query string using version 1 parser
   */
  bool parse(const char *s) {
    return parse(s, 1);
  }

  /**
   * Parse a query string using specified parser version
   */
  bool parse(const char *s, int ver) {
    QueryError_ClearError(&m_status);
    QAST_Destroy(this);

    int rc = QAST_Parse(this, sctx, &m_opts, s, strlen(s), ver, &m_status);
    return rc == REDISMODULE_OK && !QueryError_HasError(&m_status) && root != NULL;
  }

  /**
   * Check if a query string is valid with the specified validation flags
   * This is a generic validation method that can be used with any validation flags
   */
  bool isValidQuery(const char *s, QAST_ValidationFlags validationFlags) {
    // Parse the query using version 2 parser
    if (!parse(s, 2)) {
      return false;
    }
    QueryError_ClearError(&m_status);
    this->validationFlags = validationFlags;
    int rc = QAST_CheckIsValid(this, sctx->spec, &m_opts, &m_status);
    return rc == REDISMODULE_OK && !QueryError_HasError(&m_status);
  }

  /**
   * Print the parsed query AST for debugging
   */
  void print() const {
    QAST_Print(this, sctx->spec);
  }

  /**
   * Get the last error message if any
   */
  const char *getError() const {
    return QueryError_GetUserError(&m_status);
  }

  /**
   * Get the last error code if any
   */
  QueryErrorCode getErrorCode() const {
    return QueryError_GetCode(&m_status);
  }

  /**
   * Destructor - cleans up resources
   */
  ~QASTCXX() {
    QueryError_ClearError(&m_status);
    QAST_Destroy(this);
  }
};
