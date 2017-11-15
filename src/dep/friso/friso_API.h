/*
 * friso ADT application interface header source file.
 * 1. string bufffer interface.
 * 2. hashmap interface.
 * 3. dynamaic array interface.
 * 4. double link list interface.
 * 
 * @author chenxin <chenxin619315@gmail.com>
 */

#ifndef _friso_api_h
#define _friso_api_h

#include <stdio.h>
#include <stdlib.h>

//yat, just take it as this way, 99 percent you will find no problem
#if ( defined(_WIN32) || defined(_WINDOWS_) || defined(__WINDOWS_) )
#    define FRISO_WINNT
#else
#    define FRISO_LINUX
#endif

#ifdef FRISO_WINNT
#    define FRISO_API extern __declspec(dllexport)
#    define __STATIC_API__ static
#else
/*platform shared library statement :: unix*/
#    define FRISO_API extern
#    define __STATIC_API__ static inline
#endif

#define ___ALLOCATION_ERROR___                             \
    printf("Unable to do the memory allocation, program will now exit\n" );    \
exit(1);

#define print(str) printf("%s", str )
#define println(str) printf("%s\n", str )

/*
 * memory allocation macro definition.
 *         cause we should use emalloc,ecalloc .ege. in php.
 * so you could make it better apdat the php environment.
 */
#define FRISO_CALLOC(_bytes, _blocks)     calloc(_bytes, _blocks)
#define FRISO_MALLOC(_bytes)         malloc(_bytes)
#define FRISO_FREE( _ptr )        free( _ptr )

typedef unsigned short ushort_t;
typedef unsigned char uchar_t;
typedef unsigned int uint_t;
typedef char * fstring;




/* {{{ fstring handle interface define::start. */
#define __CHAR_BYTES__ 8
#define __BUFFER_DEFAULT_LENGTH__ 16

typedef struct {
    fstring buffer;
    uint_t length;
    uint_t allocs;
} string_buffer_entry;

typedef string_buffer_entry * string_buffer_t;

//FRISO_API string_buffer_t new_string_buffer( void );
#define new_string_buffer() \
    new_string_buffer_with_opacity( __DEFAULT_ARRAY_LIST_OPACITY__ );
FRISO_API string_buffer_t new_string_buffer_with_opacity( uint_t );
FRISO_API string_buffer_t new_string_buffer_with_string( fstring str );

/*
 * this function will copy the chars that the fstring pointed.
 *        to the buffer.
 * this may cause the resize action of the buffer.
 */
FRISO_API void string_buffer_append( string_buffer_t, fstring );
FRISO_API void string_buffer_append_char( string_buffer_t, char );

//insert the given fstring from the specified position.
FRISO_API void string_buffer_insert( string_buffer_t, uint_t idx, fstring );

//remove the char in the specified position.
FRISO_API fstring string_buffer_remove( string_buffer_t, uint_t idx, uint_t );

/*
 * turn the string_buffer to a string.
 *        or return the buffer of the string_buffer.
 */
FRISO_API string_buffer_t string_buffer_trim( string_buffer_t );

/*
 * free the given fstring buffer.
 *        and this function will not free the allocations of the 
 *        the string_buffer_t->buffer, we return it to you, if there is
 *     a necessary you could free it youself by calling free();
 */
FRISO_API fstring string_buffer_devote( string_buffer_t );

/*
 * clear the given fstring buffer.
 *        reset its buffer with 0 and reset its length to 0.
 */
FRISO_API void string_buffer_clear( string_buffer_t );

//free the fstring buffer include the buffer.
FRISO_API void free_string_buffer( string_buffer_t );

/**
 * fstring specified chars tokenizer functions
 *
 * @date 2013-06-08
 */
typedef struct {
    fstring source;
    uint_t srcLen;
    fstring delimiter;
    uint_t delLen;
    uint_t idx;
} string_split_entry;
typedef string_split_entry * string_split_t;

/**
 * create a new string_split_entry.
 *
 * @param    source
 * @return    string_split_t;    
 */
FRISO_API string_split_t new_string_split( fstring, fstring );

FRISO_API void string_split_reset( string_split_t, fstring, fstring );

FRISO_API void string_split_set_source( string_split_t, fstring );

FRISO_API void string_split_set_delimiter( string_split_t, fstring );

FRISO_API void free_string_split( string_split_t );

/**
 * get the next split fstring, and copy the 
 *     splited fstring into the __dst buffer . 
 *
 * @param    string_split_t
 * @param    __dst
 * @return    fstring (NULL if reach the end of the source 
 *         or there is no more segmentation)
 */
FRISO_API fstring string_split_next( string_split_t, fstring );
/* }}} */




/* {{{ dynamaic array interface define::start*/
#define __DEFAULT_ARRAY_LIST_OPACITY__ 8

/*friso array list entry struct*/
typedef struct {
    void **items;
    uint_t allocs;
    uint_t length;
} friso_array_entry;

typedef friso_array_entry * friso_array_t;

//create a new friso dynamic array.
//FRISO_API friso_array_t new_array_list( void );
#define new_array_list() new_array_list_with_opacity(__DEFAULT_ARRAY_LIST_OPACITY__)

//create a new friso dynamic array with the given opacity
FRISO_API friso_array_t new_array_list_with_opacity( uint_t );

/*
 * free the given friso array.
 *     and its items, but never where the items's item to pointed to . 
 */
FRISO_API void free_array_list( friso_array_t );

//add a new item to the array.
FRISO_API void array_list_add( friso_array_t, void * );

//insert a new item at a specifed position.
FRISO_API void array_list_insert( friso_array_t, uint_t, void * );

//get a item at a specified position.
FRISO_API void *array_list_get( friso_array_t, uint_t );

/*
 * set the item at a specified position.
 *     this will return the old value.
 */
FRISO_API void *array_list_set( friso_array_t, uint_t, void * );

/*
 * remove the given item at a specified position.
 *    this will return the value of the removed item.
 */
FRISO_API void *array_list_remove( friso_array_t, uint_t );

/*trim the array list for final use.*/
FRISO_API friso_array_t array_list_trim( friso_array_t );

/*
 * clear the array list.
 *     this function will free all the allocations that the pointer pointed.
 *        but will not free the point array allocations,
 *        and will reset the length of it.
 */
FRISO_API friso_array_t array_list_clear( friso_array_t );

//return the size of the array.
//FRISO_API uint_t array_list_size( friso_array_t );
#define array_list_size( array ) array->length

//return the allocations of the array.
//FRISO_API uint_t array_list_allocs( friso_array_t );
#define array_list_allocs( array ) array->allocs

//check if the array is empty.
//FRISO_API int array_list_empty( friso_array_t );
#define array_list_empty( array ) ( array->length == 0 )
/* }}} dynamaic array interface define::end*/




/* {{{ link list interface define::start*/
struct friso_link_node {
    void *value;
    struct friso_link_node *prev;
    struct friso_link_node *next;
};
typedef struct friso_link_node link_node_entry;
typedef link_node_entry * link_node_t;

/*
 * link list adt
 */
typedef struct {
    link_node_t head;
    link_node_t tail;
    uint_t size;
} friso_link_entry;

typedef friso_link_entry * friso_link_t;

//create a new link list
FRISO_API friso_link_t new_link_list( void );

//free the specified link list
FRISO_API void free_link_list( friso_link_t );

//return the size of the current link list.
//FRISO_API uint_t link_list_size( friso_link_t );
#define link_list_size( link ) link->size

//check the given link is empty or not.
//FRISO_API int link_list_empty( friso_link_t );
#define link_list_empty( link ) (link->size == 0)

//clear all the nodes in the link list( except the head and the tail ).
FRISO_API friso_link_t link_list_clear( friso_link_t link );

//add a new node to the link list.(append from the tail)
FRISO_API void link_list_add( friso_link_t, void * );

//add a new node before the specified node
FRISO_API void link_list_insert_before( friso_link_t, uint_t, void * );

//get the node in the current index.
FRISO_API void *link_list_get( friso_link_t, uint_t );

//modify the node in the current index.
FRISO_API void *link_list_set( friso_link_t, uint_t, void * );

//remove the specified link node
FRISO_API void *link_list_remove( friso_link_t, uint_t );

//remove the given node
FRISO_API void *link_list_remove_node( friso_link_t, link_node_t );

//remove the node from the frist.
FRISO_API void *link_list_remove_first( friso_link_t );

//remove the last node from the link list
FRISO_API void *link_list_remove_last( friso_link_t );

//append a node from the end.
FRISO_API void link_list_add_last( friso_link_t, void * );

//add a node at the begining of the link list.
FRISO_API void link_list_add_first( friso_link_t, void * );
/* }}} link list interface define::end*/




/* {{{ hashtable interface define :: start*/
struct hash_entry {
    fstring _key;                    //the node key
    void * _val;                    //the node value
    struct hash_entry * _next;
};
typedef struct hash_entry friso_hash_entry;
typedef friso_hash_entry * hash_entry_t;
typedef void (*fhash_callback_fn_t)( hash_entry_t ); 

typedef struct {
    uint_t length;
    uint_t size;
    float factor;
    uint_t threshold;
    hash_entry_t *table;
} friso_hash_cdt;

typedef friso_hash_cdt * friso_hash_t;

//default value for friso_hash_cdt
#define DEFAULT_LENGTH     31
#define DEFAULT_FACTOR    0.85f

/*
 * Function: new_hash_table
 * Usage: table = new_hash_table();
 * -------------------------------- 
 * this function allocates a new symbol table with no entries.
 */
FRISO_API friso_hash_t new_hash_table( void );

/*
 * Function: free_hash_table
 * Usage: free_hash_table( table );
 * --------------------------------------
 * this function will free all the allocation for memory.
 */
FRISO_API void free_hash_table( friso_hash_t, fhash_callback_fn_t );

/*
 * Function: put_new_mapping
 * Usage: put_mapping( table, key, value );
 * ----------------------------------------
 * the function associates the specified key with the given value.
 */
FRISO_API void *hash_put_mapping( friso_hash_t, fstring, void * );

/*
 * Function: is_mapping_exists
 * Usage: bool = is_mapping_exists( table, key );
 * ----------------------------------------------
 * this function check the given key mapping is exists or not.
 */
FRISO_API int hash_exist_mapping( friso_hash_t, fstring );

/*
 * Function: get_mapping_value
 * Usage: value = get_mapping_value( table, key );
 * -----------------------------------------------
 * this function return the value associated with the given key.
 *         UNDEFINED will be return if the mapping is not exists.
 */
FRISO_API void * hash_get_value( friso_hash_t, fstring );

/*
 * Function: remove_mapping
 * Usage: remove_mapping( table, key );
 * ------------------------------------
 * This function is used to remove the mapping associated with the given key.
 */
FRISO_API hash_entry_t hash_remove_mapping( friso_hash_t, fstring );

/*
 * Function: get_table_size
 * Usage: size = get_table_size( table );
 * --------------------------------------
 * This function is used to count the size of the specified table. 
 */
//FRISO_API uint_t hash_get_size( friso_hash_t );
#define hash_get_size( hash ) hash->size
/* }}} hashtable interface define :: end*/


/* {{{ utf8 string interface define :: start*/

/*
 * Function: get_utf8_bytes
 *
 * */
FRISO_API int get_utf8_bytes(char);


/*
 * Function: get_utf8_unicode
 *
 * */
FRISO_API int get_utf8_unicode(const fstring);


/*
 * Function: unicode_to_utf8
 *
 * */
FRISO_API int unicode_to_utf8(uint_t, fstring);


/* }}} utf8 string interface define :: start*/

#endif /*end ifndef*/

