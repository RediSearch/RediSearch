#include "function.h"

RSFunction RSFunctionRegistry_Get(RSFunctionRegistry *reg, const char *name, size_t len) {
  for (size_t i = 0; i < reg->len; i++) {
    if (len == strlen(reg->funcs[i].name) && !strncasecmp(reg->funcs[i].name, name, len)) {
      return reg->funcs[i].f;
    }
  }
  return NULL;
}

int RSFunctionRegistry_RegisterFunction(RSFunctionRegistry *ret, const char *name, RSFunction f) {
  if (ret->len + 1 >= ret->cap) {
    ret->cap += ret->cap ? ret->cap : 2;
    ret->funcs = realloc(ret->funcs, ret->cap * sizeof(*ret->funcs));
  }
  ret->funcs[ret->len].f = f;
  ret->funcs[ret->len].name = name;
  ret->len++;
  return 1;
}
