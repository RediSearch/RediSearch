/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "rtdoc.hpp"

// TODO: GEOMETRY - remove this function if not used by tests
RTDoc_Cartesian *RTDoc_Cartesian_From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  return RTDoc_Cartesian::from_wkt(wkt, len, id, err_msg);
}
RTDoc_Geographic *RTDoc_Geographic_From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
  return RTDoc_Geographic::from_wkt(wkt, len, id, err_msg);
}

RTDoc_Cartesian *RTDoc_Cartesian_Copy(RTDoc_Cartesian const *other) {
  return new RTDoc_Cartesian{*other};
}
RTDoc_Geographic *RTDoc_Geographic_Copy(RTDoc_Geographic const *other) {
  return new RTDoc_Geographic{*other};
}

void RTDoc_Cartesian_Free(RTDoc_Cartesian *doc) noexcept {
  delete doc;
}
void RTDoc_Geographic_Free(RTDoc_Geographic *doc) noexcept {
  delete doc;
}

t_docId RTDoc_Cartesian_GetID(RTDoc_Cartesian const *doc) noexcept {
  return doc->id();
}
t_docId RTDoc_Geographic_GetID(RTDoc_Geographic const *doc) noexcept {
  return doc->id();
}

bool RTDoc_Cartesian_IsEqual(RTDoc_Cartesian const *lhs, RTDoc_Cartesian const *rhs) {
  return *lhs == *rhs;
}
bool RTDoc_Geographic_IsEqual(RTDoc_Geographic const *lhs, RTDoc_Geographic const *rhs) {
  return *lhs == *rhs;
}

RedisModuleString *RTDoc_Cartesian_ToString(RTDoc_Cartesian const *doc) {
  return doc->to_RMString();
}
RedisModuleString *RTDoc_Geographic_ToString(RTDoc_Geographic const *doc) {
  return doc->to_RMString();
}

void RTDoc_Cartesian_Print(RTDoc_Cartesian const *doc) {
  std::cout << *doc << '\n';
}
void RTDoc_Geographic_Print(RTDoc_Geographic const *doc) {
  std::cout << *doc << '\n';
}
