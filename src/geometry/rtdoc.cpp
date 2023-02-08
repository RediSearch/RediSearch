
#include "rtdoc.hpp"

[[nodiscard]] RTDoc *From_WKT(const char *wkt, size_t len, docID_t id) {
  try {
    return new RTDoc{std::string_view{wkt, len}, id};
  } catch (...) {
    return nullptr;
  }
}

[[nodiscard]] RTDoc *RTDoc_Copy(RTDoc const *other) {
  return new RTDoc{*other};
}

void RTDoc_Free(RTDoc *doc) noexcept {
  delete doc;
}

[[nodiscard]] docID_t RTDoc_GetID(RTDoc const *doc) noexcept {
  return doc->id();
}

[[nodiscard]] bool RTDoc_IsEqual(RTDoc const *lhs, RTDoc const *rhs) {
  return *lhs == *rhs;
}

[[nodiscard]] RedisModuleString *RTDoc_ToString(struct RTDoc const *doc) {
  if (RedisModule_CreateString) {
    string s = doc->to_string();
    return RedisModule_CreateString(nullptr, s.c_str(), s.length());
  } else {
    return nullptr;
  }
}

void RTDoc_Print(struct RTDoc const *doc) {
  std::cout << *doc << '\n';
}
