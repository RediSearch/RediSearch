#include "expander.h"

/* A KV for query expander.
*  TODO: move this to a real map
*/
struct __qxkv {
  const char *key;
  QueryExpander expander;
};

/* the query expanders registry */
struct __qxkv *__expanders = NULL;
int __numExpanders = 0;

void RegisterQueryExpander(const char *name, QueryExpander ex) {
  // TODO: check for duplicates once this thing is really dynamic
  __expanders = realloc(__expanders, (__numExpanders + 1) * sizeof(struct __qxkv));
  __expanders[__numExpanders++] = (struct __qxkv){.key = name, .expander = ex};
}

QueryExpander *GetQueryExpander(const char *name) {

  for (int i = 0; i < __numExpanders; i++) {
    if (!strcasecmp(name, __expanders[i].key)) {
      return &__expanders[i].expander;
    }
  }

  return NULL;
}