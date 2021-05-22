#pragma once

#include <stdlib.h>
#include <stddef.h>

///////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
struct List {
  struct Node {
    T *prev, *next;
    Node() : prev(NULL), next(NULL) {}
  };

  Node node;

  List() {}

  T *head() { return node.next; }
  T *tail() { return node.prev; }
  T *first() { return node.next; }
  T *last() { return node.prev; }

  void prepend(T *new_item) {
    Node &new_n = (Node &) *new_item;
    new_n.next = head();
    new_n.prev = NULL;
    if (!isEmpty()) {
      Node &n_head = (Node &) *head();
      n_head.prev = new_item;
    } else {
      node.prev = new_item;
    }
    node.next = new_item;
  }

  void prepend(T *item, T *new_item) {
    Node &n = (Node &) *item;
    Node &new_n = (Node &) *new_item;

    new_n.prev = n.prev;
    new_n.next = item;
    if (n.prev) {
      Node &n_prev = (Node &) *n.prev;
      n_prev.next = new_item;
    } else {
      node.next = new_item;
    }
    n.prev = new_item;
  }

  void append(T *new_item) {
    Node &new_n = (Node &) *new_item;
    new_n.prev = tail();
    new_n.next = NULL;
    if (!isEmpty()) {
      Node &n_tail = (Node &) *tail();
      n_tail.next = new_item;
    } else {
      node.next = new_item;
    }
    node.prev = new_item;
  }

  void append(T *item, T *new_item) {
    Node &n = (Node &) *item;
    Node &new_n = (Node &) *new_item;

    new_n.next = n.next;
    new_n.prev = item;
    if (n.next) {
      Node &n_next = (Node &) *n.next;
      n_next.prev = new_item;
    } else {
      node.prev = new_item;
    }
    n.next = new_item;
  }

  void remove(T *item) {
    Node &n = (Node &) *item;

    T *prev_item = n.prev, *next_item = n.next;
    if (next_item) {
      Node &n_next = (Node &) *n.next;
      n_next.prev = prev_item;
    } else {
      node.prev = prev_item;
    }
    if (prev_item) {
      Node &n_prev = (Node &) *n.prev;
      n_prev.next = next_item;
    } else {
      node.next = next_item;
    }
  
    n.next = n.prev = NULL;
  }

  bool isEmpty() const { return !node.next; }
  //bool isFirst(Node *itm) const { return itm->prev == this; }
  //bool isLast(Node *itm) const { return itm->next == this; }
  //bool isEnd(Node *itm) const { return this == itm; }

  //#define ITEM(L, T, mname) ((T *)(void *)((char *)(L)-offsetof(T, mname)))

  T *pop_tail() {
    if (isEmpty()) {
      return NULL;
    }
    Node *item = tail();
    remove(item);
    return item;
  }
};

#define List_foreach(v, T, it) for (T *it = (v).head(); it != NULL; it = ((List<T>::Node &) *it).next)

//---------------------------------------------------------------------------------------------

struct DLLIST_node {
  struct DLLIST_node *next;
  struct DLLIST_node *prev;
};

typedef DLLIST_node DLLIST;

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

///////////////////////////////////////////////////////////////////////////////////////////////

// DLLIST2 API
// This API allows the node to be relocated in memory, as opposed to the normal dllist api

struct DLLIST2_node;

struct DLLIST2 {
  struct DLLIST2_node *head, *tail;
};

struct DLLIST2_node {
  struct DLLIST2_node *prev, *next;
};

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

///////////////////////////////////////////////////////////////////////////////////////////////
