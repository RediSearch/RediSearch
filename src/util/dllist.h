#ifndef DLLIST_H
#define DLLIST_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DLLIST_node {
  struct DLLIST_node *next;
  struct DLLIST_node *prev;
} DLLIST_node, DLLIST;

static inline void dllist_init(DLLIST *l) {
  l->prev = l;
  l->next = l;
}

static inline void dllist_insert(DLLIST_node *prev, DLLIST_node *next, DLLIST_node *item) {
  item->next = next;
  item->prev = prev;
  next->prev = item;
  prev->next = item;
}

static inline void dllist_prepend(DLLIST *list, DLLIST_node *item) {
  dllist_insert(list, list->next, item);
}

static inline void dllist_append(DLLIST *list, DLLIST_node *item) {
  dllist_insert(list->prev, list, item);
}

static inline void dllist_squeeze(DLLIST_node *prev, DLLIST_node *next) {
  next->prev = prev;
  prev->next = next;
}

static inline void dllist_delete(DLLIST_node *item) {
  dllist_squeeze(item->prev, item->next);
  item->next = item->prev = NULL;
}

#define DLLIST_ITEM(L, T, mname) ((T *)(void *)((char *)(L)-offsetof(T, mname)))
#define DLLIST_FOREACH(it, ll) for (DLLIST_node *it = (ll)->next; it != (ll); it = it->next)

#ifdef __cplusplus
}
#endif

#endif