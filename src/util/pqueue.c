// manage a priority queue as a heap
// the heap is implemented as a fixed size array of pointers to your data

#include <stdio.h>
#include <stdlib.h>

#include "jhjtypes.h"

#include "pqueue.h"
#include "list.h"

// initialise the priority queue with a maximum size of maxelements. maxrating
// is the highest or lowest value of an
// entry in the pqueue depending on whether it is ascending or descending
// respectively. Finally the int tells you whether
// the list is sorted ascending or descending...

void PQueueInitialise(PQUEUE *pq, size_t MaxElements, u_int32_t MaxRating,
                      int bIsAscending) {
  pq->IsAscendingHeap = bIsAscending;

  pq->MaxSize = MaxElements;

  pq->CurrentSize = 0;

  pq->Elements = (void **)malloc(sizeof(void *) * (MaxElements + 1));

  if (pq->Elements == NULL) {
    printf("Memory alloc failed!\n");
  }

  pq->MaxRating = MaxRating;
}

// join a priority queue
// returns TRUE if succesful, FALSE if fails. (You fail by filling the pqueue.)
// PGetRating is a function which returns the rating of the item you're adding
// for sorting purposes

int8 PQueuePush(PQUEUE *pq, void *item, u_int32_t (*PGetRating)(void *)) {
  u_int32_t i;
  u_int32_t r;

  while (PQueueIsFull(pq) == TRUE) {
    PQueuePop(pq, PGetRating);
  }

  // set i to the first unused element and increment CurrentSize

  i = (pq->CurrentSize += 1);

  // get the rating for this item using the provided rating function
  r = PGetRating(item);

  // while the parent of the space we're putting the new node into is worse than
  // our new node, swap the space with the worse node. We keep doing that until
  // we
  // get to a worse node or until we get to the top

  // note that we also can sort so that the minimum elements bubble up so we
  // need to loops
  // with the comparison operator flipped...

  if (pq->IsAscendingHeap == TRUE) {
    while ((i == PQ_FIRST_ENTRY
                ? (pq->MaxRating)  // return biggest possible rating if first
                                   // element
                : (PGetRating(pq->Elements[PQ_PARENT_INDEX(i)]))) < r) {
      pq->Elements[i] = pq->Elements[PQ_PARENT_INDEX(i)];

      i = PQ_PARENT_INDEX(i);
    }
  } else {
    while ((i == PQ_FIRST_ENTRY
                ? (pq->MaxRating)  // return biggest possible rating if first
                                   // element
                : (PGetRating(pq->Elements[PQ_PARENT_INDEX(i)]))) > r) {
      pq->Elements[i] = pq->Elements[PQ_PARENT_INDEX(i)];

      i = PQ_PARENT_INDEX(i);
    }
  }

  // then add the element at the space we created.
  pq->Elements[i] = item;

  return TRUE;
}

// Decide whether or not a Priority Queue is full...
int PQueueIsFull(PQUEUE *pq) {
  // todo assert if size > maxsize

  if (pq->CurrentSize == pq->MaxSize) {
    return TRUE;
  }

  return FALSE;
}

// Decide whether or not a Priority Queue is full...
int PQueueIsEmpty(PQUEUE *pq) {
  // todo assert if size > maxsize

  if (pq->CurrentSize == 0) {
    return TRUE;
  }

  return FALSE;
}

// free up memory for pqueue
void PQueueFree(PQUEUE *pq) { free(pq->Elements); }

// remove the first node from the pqueue and provide a pointer to it

void *PQueuePop(PQUEUE *pq, u_int32_t (*PGetRating)(void *)) {
  u_int32_t i;
  u_int32_t child;

  void *pMaxElement;
  void *pLastElement;

  if (PQueueIsEmpty(pq)) {
    return NULL;
  }

  pMaxElement = pq->Elements[PQ_FIRST_ENTRY];

  // get pointer to last element in tree
  pLastElement = pq->Elements[pq->CurrentSize--];

  if (pq->IsAscendingHeap == TRUE) {
    // code to pop an element from an ascending (top to bottom) pqueue

    // UNTESTED

    for (i = PQ_FIRST_ENTRY; PQ_LEFT_CHILD_INDEX(i) <= pq->CurrentSize;
         i = child) {
      // set child to the smaller of the two children...

      child = PQ_LEFT_CHILD_INDEX(i);

      if ((child != pq->CurrentSize) && (PGetRating(pq->Elements[child + 1]) >
                                         PGetRating(pq->Elements[child]))) {
        child++;
      }

      if (PGetRating(pLastElement) < PGetRating(pq->Elements[child])) {
        pq->Elements[i] = pq->Elements[child];
      } else {
        break;
      }
    }
  } else {
    // code to pop an element from a descending (top to bottom) pqueue

    for (i = PQ_FIRST_ENTRY; PQ_LEFT_CHILD_INDEX(i) <= pq->CurrentSize;
         i = child) {
      // set child to the larger of the two children...

      child = PQ_LEFT_CHILD_INDEX(i);

      if ((child != pq->CurrentSize) && (PGetRating(pq->Elements[child + 1]) <
                                         PGetRating(pq->Elements[child]))) {
        child++;
      }

      if (PGetRating(pLastElement) > PGetRating(pq->Elements[child])) {
        pq->Elements[i] = pq->Elements[child];
      }

      else {
        break;
      }
    }
  }

  pq->Elements[i] = pLastElement;

  return pMaxElement;
}
