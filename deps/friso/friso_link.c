/*
 * link list implemented functions
 *    defined in header file "friso_API.h".
 * when the link_node is being deleted, here we just free
 *    the allocation of the node, not the allcation of it's value.
 *
 * @author    chenxin <chenxin619315@gmail.com>
 */
#include "friso_API.h"
#include <stdlib.h>

//create a new link list node.
__STATIC_API__ link_node_t new_node_entry( 
    void * value, 
    link_node_t prev, 
    link_node_t next ) 
{
    link_node_t node = ( link_node_t ) 
        FRISO_MALLOC( sizeof( link_node_entry ) );
    if ( node == NULL ) {
        ___ALLOCATION_ERROR___
    }

    node->value = value;
    node->prev  = prev;
    node->next  = next;

    return node;
}

//create a new link list
FRISO_API friso_link_t new_link_list( void ) 
{
    friso_link_t e = ( friso_link_t ) 
        FRISO_MALLOC( sizeof( friso_link_entry ) );
    if ( e == NULL ) {
        ___ALLOCATION_ERROR___
    }

    //initialize the entry
    e->head = new_node_entry( NULL, NULL, NULL );    
    e->tail = new_node_entry( NULL, e->head, NULL );
    e->head->next = e->tail;
    e->size = 0;

    return e;
}

//free the given link list
FRISO_API void free_link_list( friso_link_t link ) 
{
    link_node_t node, next;
    for ( node = link->head; node != NULL; ) {
        next = node->next;
        FRISO_FREE( node );
        node = next;
    }

    FRISO_FREE( link );
}

//clear all nodes in the link list.
FRISO_API friso_link_t link_list_clear( 
    friso_link_t link ) 
{
    link_node_t node, next;
    //free all the middle nodes.
    for ( node = link->head->next; node != link->tail; ) 
    {
        next = node->next;
        FRISO_FREE( node );
        node = next;
    }

    link->head->next = link->tail;
    link->tail->prev = link->head;
    link->size = 0;

    return link;
}

//get the size of the link list.
//FRISO_API uint_t link_list_size( friso_link_t link ) {
//    return link->size;
//}

//check if the link list is empty
//FRISO_API int link_list_empty( friso_link_t link ) {
//    return ( link->size == 0 );
//}


/*
 * find the node at a specified position.
 * static
 */
__STATIC_API__ link_node_t get_node( 
    friso_link_t link, uint_t idx ) 
{
    link_node_t p = NULL;
    register uint_t t;

    if ( idx >= 0 && idx < link->size ) 
    {
        if ( idx < link->size / 2 ) {        //find from the head.
            p = link->head;
            for ( t = 0; t <= idx; t++ )
            p = p->next; 
        } else {                            //find from the tail.
            p = link->tail;
            for ( t = link->size; t > idx; t-- )
            p = p->prev;
        }
    }

    return p;
}

/*
 * insert a node before the given node.
 * static
 */
//__STATIC_API__ void insert_before( 
//    friso_link_t link, 
//    link_node_t node, 
//    void * value ) 
//{
//    link_node_t e = new_node_entry( value, node->prev, node );
//    e->prev->next = e;
//    e->next->prev = e;
//    //node->prev = e;
//
//    link->size++;
//}
#define insert_before( link, node, value ) \
{ \
    link_node_t e = new_node_entry( value, node->prev, node );    \
    e->prev->next = e;                        \
    e->next->prev = e;                        \
    link->size++;                        \
}

/*
 * static function:
 * remove the given node, the allocation of the value will not free,
 * but we return it to you, you will free it youself when there is a necessary.
 *
 * @return the value of the removed node.
 */
__STATIC_API__ void * remove_node( 
    friso_link_t link, link_node_t node ) 
{
    void * _value = node->value;

    node->prev->next = node->next;
    node->next->prev = node->prev;
    link->size--;

    FRISO_FREE( node );

    return _value;
}


//add a new node to the link list.(insert just before the tail)
FRISO_API void link_list_add( 
    friso_link_t link, void * value ) 
{
    insert_before( link, link->tail, value );
}

//add a new node before the given index.
FRISO_API void link_list_insert_before( 
    friso_link_t link, uint_t idx, void * value  ) 
{
    link_node_t node = get_node( link, idx );
    if ( node != NULL ) {
        insert_before( link, node, value );
    }
}

/*
 * get the value with the specified node.
 * 
 * @return the value of the node.
 */
FRISO_API void * link_list_get( 
    friso_link_t link, uint_t idx ) 
{
    link_node_t node = get_node( link, idx );
    if ( node != NULL ) {
        return node->value;
    }
    return NULL;
}

/*
 * set the value of the node that located in the specified position.
 *  we did't free the allocation of the old value, we return it to you.
 *    free it yourself when it is necessary.
 * 
 * @return the old value.
 */
FRISO_API void *link_list_set( 
    friso_link_t link, 
    uint_t idx, void * value ) 
{
    link_node_t node = get_node( link, idx );
    void * _value = NULL;

    if ( node != NULL ) {
        _value = node->value;
        node->value = value;
    }

    return _value;
}

/*
 * remove the node located in the specified position.
 *
 * @see remove_node
 * @return the value of the node removed.
 */
FRISO_API void *link_list_remove( 
    friso_link_t link, uint_t idx ) 
{
    link_node_t node = get_node( link, idx );

    if ( node != NULL ) {
        //printf("idx=%d, node->value=%s\n", idx, (string) node->value );
        return remove_node( link, node );
    }

    return NULL;
}

/*
 * remove the given node from the given link list.
 * 
 * @see remove_node.
 * @return the value of the node removed.
 */
FRISO_API void *link_list_remove_node( 
    friso_link_t link, 
    link_node_t node ) 
{
    return remove_node( link, node );
}

//remove the first node after the head
FRISO_API void *link_list_remove_first( 
    friso_link_t link ) 
{
    if ( link->size > 0 ) {
        return remove_node( link, link->head->next );
    }
    return NULL;
}

//remove the last node just before the tail.
FRISO_API void *link_list_remove_last( 
    friso_link_t link ) 
{
    if ( link->size > 0 ) {
        return remove_node( link, link->tail->prev );
    }
    return NULL;
}

//append a node from the tail.
FRISO_API void link_list_add_last( 
    friso_link_t link, 
    void *value ) 
{
    insert_before( link, link->tail, value );
}

//append a note just after the head.
FRISO_API void link_list_add_first( 
    friso_link_t link, void *value ) 
{
    insert_before( link, link->head->next, value );
}
