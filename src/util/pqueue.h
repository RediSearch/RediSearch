// pqueue header
#include <stdlib.h>
typedef struct _PQUEUE
{
	size_t MaxSize;
	size_t CurrentSize;
	void **Elements; // pointer to void pointers
	u_int32_t MaxRating; // biggest element possible
	int IsAscendingHeap; // true if the heap should be sorted with the maximum scoring elements first
} PQUEUE;

// given an index to any element in a binary tree stored in a linear array with the root at 1 and 
// a "sentinel" value at 0 these macros are useful in making the code clearer

// the parent is always given by index/2
#define PQ_PARENT_INDEX(i) (i/2)
#define PQ_FIRST_ENTRY (1)

// left and right children are index * 2 and (index * 2) +1 respectively
#define PQ_LEFT_CHILD_INDEX(i) (i*2)
#define PQ_RIGHT_CHILD_INDEX(i) ((i*2)+1)

void PQueueInitialise( PQUEUE *pq, size_t MaxElements, u_int32_t MaxRating, int bIsAscending );

void PQueueFree( PQUEUE *pq );

int8_t PQueuePush( PQUEUE *pq, void *item,  u_int32_t (*PGetRating) ( void * ) );

int PQueueIsFull( PQUEUE *pq );
int PQueueIsEmpty( PQUEUE *pq );

void *PQueuePop( PQUEUE *pq, u_int32_t (*PGetRating) ( void * ) );
