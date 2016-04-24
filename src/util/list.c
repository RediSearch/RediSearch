// manage a list

#include <stdio.h>
#include <stdlib.h>

#include "jhjtypes.h"
#include "list.h"

void jhjDBPrint(char *text, long line, char *file);

void ListInitialise(LIST *l, void *(*GetNext)(void *),
                    void (*SetNext)(void *, void *)) {
  l->Head = NULL;
  l->Tail = NULL;

  l->GetNext = GetNext;
  l->SetNext = SetNext;
}

// join a list
void ListPush(LIST *l, void *item) {
  void *OldTail;

  // if empty
  if (l->Head == NULL) {
    l->Head = l->Tail = item;
    l->SetNext(item, NULL);

    return;
  }

  // else
  OldTail = l->Tail;

  l->Tail = item;
  l->SetNext(item, NULL);

  l->SetNext(OldTail, item);
}

// get pointer to given item and remove it from the list IF found
void *ListPop(LIST *l, void *item, int (*compare)(void *, void *)) {
  void *n;
  void *prev = NULL;
  void *next;

  n = l->Head;

  while (n != NULL) {
    if (compare(item, n) == 0) {
      // found it

      // is head?
      if (l->Head == n) {
        next = l->GetNext(n);

        if (next == NULL) {
          l->Tail = NULL;
        }

        l->Head = next;

        l->SetNext(n, NULL);

        return n;
      } else {
        // fix previous node to point at next
        next = l->GetNext(n);
        l->SetNext(prev, next);

        // fix tail if this was tail
        if (l->Tail == n) {
          l->Tail = prev;
        }

        l->SetNext(n, NULL);

        return n;
      }
    }

    prev = n;

    n = l->GetNext(n);
  }

  return NULL;
}

// find whether an item is in the queue

void *ListFind(LIST *l, void *item, int (*compare)(void *, void *)) {
  void *n;

  n = l->Head;

  while (n != NULL) {
    if (compare(item, n) == 0) return n;

    n = l->GetNext(n);
  }

  return NULL;
}

// todo : just store the count in list header
u_int32_t ListLength(LIST *l) {
  void *n;
  u_int32_t c = 0;

  n = l->Head;

  while (n != NULL) {
    c++;
    n = l->GetNext(n);
  }

  return c;
}
