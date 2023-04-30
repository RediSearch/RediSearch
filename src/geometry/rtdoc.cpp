/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "rtdoc.hpp"

// TODO: GEOMETRY - remove this function if not used by tests
RTDoc *From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  try {
    auto geometry = Polygon::from_wkt(std::string_view{wkt, len});
    return new RTDoc{geometry, id};
  } catch (const std::exception &e) {
    if (err_msg)
      *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
    return nullptr;
  }
}

RTDoc *RTDoc_Copy(RTDoc const *other) {
  return new RTDoc{*other};
}

void RTDoc_Free(RTDoc *doc) noexcept {
  delete doc;
}

t_docId RTDoc_GetID(RTDoc const *doc) noexcept {
  return doc->id();
}

bool RTDoc_IsEqual(RTDoc const *lhs, RTDoc const *rhs) {
  return *lhs == *rhs;
}

RedisModuleString *RTDoc_ToString(struct RTDoc const *doc) {
  if (RedisModule_CreateString) {
    string s = doc->rect_to_string();
    return RedisModule_CreateString(nullptr, s.c_str(), s.length());
  } else {
    return nullptr;
  }
}

void RTDoc_Print(struct RTDoc const *doc) {
  std::cout << *doc << '\n';
}
