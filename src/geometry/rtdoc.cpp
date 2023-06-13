/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtdoc.hpp"

#define X(variant)                                                                         \
  RTDoc_##variant *RTDoc_##variant##_From_WKT(const char *wkt, size_t len, t_docId id,     \
                                              RedisModuleString **err_msg) {               \
    return RTDoc_##variant::from_wkt(wkt, len, id, err_msg);                               \
  }                                                                                        \
  RTDoc_##variant *RTDoc_##variant_Copy(RTDoc_##variant const *other) {                    \
    return new RTDoc_##variant{*other};                                                    \
  }                                                                                        \
  void RTDoc_##variant##_Free(RTDoc_ #variant *doc) noexcept {                             \
    delete doc;                                                                            \
  }                                                                                        \
  t_docId RTDoc_##variant##_GetID(RTDoc_ #variant const *doc) noexcept {                   \
    return doc->id();                                                                      \
  }                                                                                        \
  bool RTDoc_##variant##_IsEqual(RTDoc_##variant const *lhs, RTDoc_##variant const *rhs) { \
    return *lhs == *rhs;                                                                   \
  }                                                                                        \
  RedisModuleString *RTDoc_##variant##_ToString(RTDoc_##variant const *doc) {              \
    return doc->to_RMString();                                                             \
  }                                                                                        \
  void RTDoc_##variant##_Print(RTDoc_##variant const *doc) {                               \
    std::cout << *doc << '\n';                                                             \
  }

GEO_VARIANTS(X)

#undef X
