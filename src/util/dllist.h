#ifndef DLLIST_H
#define DLLIST_H

#include <stdlib.h>
#include <stddef.h>

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

#define DLLIST_IS_EMPTY(l) (l)->prev == (l)
#define DLLIST_IS_FIRST(l, itm) ((itm)->prev == l)
#define DLLIST_IS_LAST(l, itm) ((itm)->next == l)
#define DLLIST_IS_END(l, itm) ((l) == (itm))

#define DLLIST_ITEM(L, T, mname) ((T *)(void *)((char *)(L)-offsetof(T, mname)))
#define DLLIST_FOREACH(it, ll) for (DLLIST_node *it = (ll)->next; it != (ll); it = it->next)

static inline DLLIST_node *dllist_pop_tail(DLLIST *list) {
  DLLIST_node *item;
  if (DLLIST_IS_EMPTY(list)) {
    return NULL;
  }
  item = list->prev;
  dllist_delete(item);
  return item;
}

/**
 * DLLIST2 API
 * This API allows the node to be relocated in memory, as opposed to
 * the normal dllist api
 */
struct DLLIST2_node;
typedef struct {
  struct DLLIST2_node *head, *tail;
} DLLIST2;

typedef struct DLLIST2_node {
  struct DLLIST2_node *prev, *next;
} DLLIST2_node;

#define DLLIST2_ITEM DLLIST_ITEM
#define DLLIST2_FOREACH(it, ll) for (DLLIST2_node *it = (ll)->head; it; it = it->next)
#define DLLIST2_IS_EMPTY(ll) ((ll)->head == NULL)

static inline void dllist2_append(DLLIST2 *l, DLLIST2_node *c) {
  if (DLLIST2_IS_EMPTY(l)) {
    l->head = l->tail = c;
    c->prev = c->next = NULL;
  } else {
    l->tail->next = c;
    c->prev = l->tail;
    c->next = NULL;
    l->tail = c;
  }
}

static inline void dllist2_delete(DLLIST2 *l, DLLIST2_node *c) {
  if (l->head == c) {
    l->head = c->next;
  }

  if (l->tail == c) {
    l->tail = c->prev;
  }

  if (c->prev) {
    c->prev->next = c->next;
  }
  if (c->next) {
    c->next->prev = c->prev;
  }
  c->prev = NULL;
  c->next = NULL;
}

#ifdef __cplusplus
}
#endif

#endif