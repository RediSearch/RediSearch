// List header

typedef struct _LIST
{
	void *Head; // pointer to head of list
	void *Tail; // pointer to head of list

	void *(* GetNext) ( void * ); // user function to get address of next element
	void (* SetNext) ( void *, void * ); // user function to set next element 

} LIST;

void ListInitialise( LIST *l, void *(* GetNext) ( void * ), void (* SetNext) ( void *, void * ) );

void ListPush( LIST *q, void *item );

void *ListPop( LIST *q, void *item, int (* compare) ( void  *, void * ) );

void *ListFind( LIST *l, void *item, int (* compare) ( void  *, void * ) );

u_int32_t ListLength( LIST *l );