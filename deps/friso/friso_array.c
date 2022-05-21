/*
 * friso dynamaic interface implemented functions file
 *        that defined in header file "friso_API.h".
 *    never use it for commercial use.
 *
 * @author    chenxini <chenxin619315@gmail.com>
 */

#include "friso_API.h"
#include <stdlib.h>

/* ********************************************
 * friso array list static functions block    *
 **********************************************/
__STATIC_API__ void **create_array_entries( uint_t __blocks ) 
{
    register uint_t t;
    void **block = ( void ** ) FRISO_CALLOC( sizeof( void * ), __blocks );
    if ( block == NULL ) {
        ___ALLOCATION_ERROR___
    }

    //initialize
    for ( t = 0; t < __blocks; t++ ) {
        block[t] = NULL;
    }

    return block;
}

//resize the array. (the opacity should not be smaller than array->length)
__STATIC_API__ friso_array_t resize_array_list( 
        friso_array_t array, 
        uint_t opacity ) 
{
    register uint_t t;
    void **block = create_array_entries( opacity );

    for ( t = 0; t < array->length ; t++ ) {
        block[t] = array->items[t];
    }

    FRISO_FREE( array->items );
    array->items = block;
    array->allocs = opacity;

    return array;
}


/* ********************************************
 * friso array list FRISO_API functions block    *
 **********************************************/
//create a new array list. (A macro define has replace this.)
//FRISO_API friso_array_t new_array_list( void ) {
//    return new_array_list_with_opacity( __DEFAULT_ARRAY_LIST_OPACITY__ );
//}

//create a new array list with a given opacity.
FRISO_API friso_array_t new_array_list_with_opacity( uint_t opacity ) 
{
    friso_array_t array = ( friso_array_t ) 
        FRISO_MALLOC( sizeof( friso_array_entry ) );
    if ( array == NULL ) {
        ___ALLOCATION_ERROR___
    }

    //initialize
    array->items  = create_array_entries( opacity );
    array->allocs = opacity;
    array->length = 0;

    return array;
}

/*
 * free the given friso array.
 *    and its items, but never where its items item pointed to . 
 */
FRISO_API void free_array_list( friso_array_t array ) 
{
    //free the allocation that all the items pointed to
    //register int t;
    //if ( flag == 1 ) {
    //    for ( t = 0; t < array->length; t++ ) {
    //        if ( array->items[t] == NULL ) continue;
    //        FRISO_FREE( array->items[t] );
    //        array->items[t] = NULL;
    //    }
    //}

    FRISO_FREE( array->items );
    FRISO_FREE( array );
}

//add a new item to the array.
FRISO_API void array_list_add( friso_array_t array, void *value ) 
{
    //check the condition to resize.
    if ( array->length == array->allocs ) {
        resize_array_list( array, array->length * 2 + 1 );
    }
    array->items[array->length++] = value;
}

//insert a new item at a specified position.
FRISO_API void array_list_insert( 
        friso_array_t array, 
        uint_t idx, 
        void *value ) 
{
    register uint_t t;

    if ( idx <= array->length ) {
        //check the condition to resize the array.
        if ( array->length == array->allocs ) {
            resize_array_list( array, array->length * 2 + 1 );
        }

        //move the elements after idx.
        //for ( t = idx; t < array->length; t++ ) {
        //    array->items[t+1] = array->items[t];
        //}
        for ( t = array->length - 1; t >= idx; t-- ) {
            array->items[t+1] = array->items[t];
        }

        array->items[idx] = value;
        array->length++;
    }
}

//get the item at a specified position.
FRISO_API void *array_list_get( friso_array_t array, uint_t idx ) 
{
    if ( idx < array->length ) {
        return array->items[idx];
    }
    return NULL;
}

//set the value of the item at a specified position.
//this will return the old value.
FRISO_API void * array_list_set( 
        friso_array_t array, 
        uint_t idx, 
        void * value ) 
{
    void * oval = NULL;
    if ( idx < array->length ) {
        oval = array->items[idx];
        array->items[idx] = value;
    }
    return oval;
}

//remove the item at a specified position.
//this will return the value of the removed item.
FRISO_API void * array_list_remove( 
        friso_array_t array, uint_t idx ) 
{
    register uint_t t;
    void *oval = NULL;

    if ( idx < array->length ) {
        oval = array->items[idx];
        //move the elements after idx.
        for ( t = idx; t < array->length - 1; t++ ) {
            array->items[t] = array->items[ t + 1 ];
        }
        array->items[array->length - 1] = NULL;
        array->length--;
    }

    return oval;
}

/*trim the array list*/
FRISO_API friso_array_t array_list_trim( friso_array_t array ) 
{
    if ( array->length < array->allocs ) {
        return resize_array_list( array, array->length );
    }    
    return array;
}

/*
 * clear the array list.
 *     this function will free all the allocations that the pointer pointed.
 *        but will not free the point array allocations,
 *        and will reset the length of it.
 */
FRISO_API friso_array_t array_list_clear( friso_array_t array ) 
{
    register uint_t t;
    //free all the allocations that the array->length's pointer pointed.
    for ( t = 0; t < array->length; t++ ) {
        /*if ( array->items[t] == NULL ) continue;
          FRISO_FREE( array->items[t] ); */
        array->items[t] = NULL; 
    }
    //attribute reset.
    array->length = 0;

    return array;
} 

//get the size of the array list. (A macro define has replace this.)
//FRISO_API uint_t array_list_size( friso_array_t array ) {
//    return array->length;
//}

//return the allocations of the array list.(A macro define has replace this)
//FRISO_API uint_t array_list_allocs( friso_array_t array ) {
//    return array->allocs;
//}

//check if the array is empty.(A macro define has replace this.)
//FRISO_API int array_list_empty( friso_array_t array ) 
//{
//    return ( array->length == 0 );
//}
