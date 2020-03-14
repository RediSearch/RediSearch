#include "attribute.h"
#include "util/arr.h"

typedef struct {
  char* name;
  ExprAttributeCallback cb;
} AttrRegistryEntry;

static AttrRegistryEntry* registry_g = NULL;

void Expr_AttributesInit(void) {
  registry_g = array_new(AttrRegistryEntry, 10);
}
void Expr_AttributedDestroy(void) {
  size_t n = array_len(registry_g);
  for (size_t ii = 0; ii < n; ++ii) {
    rm_free(registry_g[ii].name);
  }
  array_free(registry_g);
  registry_g = NULL;
}

int Expr_FindAttributeByName(const char* name, size_t n) {
  size_t alen = array_len(registry_g);
  for (size_t ii = 0; ii < alen; ++ii) {
    if (!strncasecmp(registry_g[ii].name, name, n)) {
      return ii;
    }
  }
  return -1;
}

const char* Expr_FindAttributeByCode(int code) {
  if (code < 0 || code >= array_len(registry_g)) {
    return "<unknown>";
  }
  return registry_g[code].name;
}

ExprAttributeCallback Expr_GetAttributeCallback(int code) {
  assert(code >= 0 && code < array_len(registry_g));
  return registry_g[code].cb;
}

int Expr_RegisterAttribute(const char* name, ExprAttributeCallback cb) {
  int existing = Expr_FindAttributeByName(name, strlen(name));
  if (existing >= 0) {
    return -1;
  }
  size_t n = array_len(registry_g);
  char* s = rm_strdup(name);
  AttrRegistryEntry ent = {.name = s, .cb = cb};
  registry_g = array_append(registry_g, ent);
  return n;
}