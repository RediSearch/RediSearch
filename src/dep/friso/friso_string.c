/*
 * utf-8 handle function implements.
 *         you could modify it or re-release it but never for commercial use.
 * 
 * @author    chenxin <chenxin619315@gmail.com>
 */
#include "friso_API.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ******************************************
 * fstring buffer functions implements.        *
 ********************************************/
/**
 * create a new buffer
 * @Note:
 * 1. it's real length is 1 byte greater than the specifield value
 * 2. we did not do any optimization for the memory allocation to ...
 *     avoid the memory defragmentation.
 *
 * @date: 2014-10-16
 */
__STATIC_API__ fstring create_buffer( uint_t length ) 
{
    fstring buffer = ( fstring ) FRISO_MALLOC( length + 1 );
    if ( buffer == NULL ) {
        ___ALLOCATION_ERROR___
    }

    memset( buffer, 0x00, length + 1 );

    return buffer;
}

//the __allocs should not be smaller than sb->length
__STATIC_API__ string_buffer_t resize_buffer( 
    string_buffer_t sb, uint_t __allocs ) 
{
    //create a new buffer.
    //if ( __allocs < sb->length ) __allocs = sb->length + 1; 
    fstring str = create_buffer( __allocs );

    //register uint_t t;
    //for ( t = 0; t < sb->length; t++ ) {
    //    str[t] = sb->buffer[t];
    //}
    memcpy( str, sb->buffer, sb->length );
    FRISO_FREE( sb->buffer );

    sb->buffer = str;
    sb->allocs = __allocs;

    return sb;
}

//create a new fstring buffer with a default opacity.
//FRISO_API string_buffer_t new_string_buffer( void ) 
//{
//    return new_string_buffer_with_opacity( __BUFFER_DEFAULT_LENGTH__ );
//}

//create a new fstring buffer with the given opacity.
FRISO_API string_buffer_t new_string_buffer_with_opacity( uint_t opacity ) 
{
    string_buffer_t sb = ( string_buffer_t ) 
    FRISO_MALLOC( sizeof( string_buffer_entry ) );
    if ( sb == NULL ) {
        ___ALLOCATION_ERROR___
    } 

    sb->buffer = create_buffer( opacity );
    sb->length = 0;
    sb->allocs = opacity;

    return sb;
}

//create a buffer with the given string.
FRISO_API string_buffer_t new_string_buffer_with_string( fstring str ) 
{
    //buffer allocations.
    string_buffer_t sb = ( string_buffer_t ) 
    FRISO_MALLOC( sizeof( string_buffer_entry ) );
    if ( sb == NULL ) {
        ___ALLOCATION_ERROR___
    }

    //initialize
    sb->length = strlen( str );
    sb->buffer = create_buffer( sb->length + __BUFFER_DEFAULT_LENGTH__ );
    sb->allocs = sb->length + __BUFFER_DEFAULT_LENGTH__;

    //register uint_t t;
    //copy the str to the buffer.
    //for ( t = 0; t < sb->length; t++ ) {
    //    sb->buffer[t] = str[t];
    //}
    memcpy( sb->buffer, str, sb->length );

    return sb;
}

FRISO_API void string_buffer_append( 
    string_buffer_t sb, fstring __str ) 
{
    register uint_t __len__ = strlen( __str );

    //check the necessity to resize the buffer.
    if ( sb->length + __len__ > sb->allocs ) {
        sb = resize_buffer( sb, ( sb->length + __len__ ) * 2 + 1 );
    }

    //register uint_t t;
    ////copy the __str to the buffer.
    //for ( t = 0; t < __len__; t++ ) {
    //    sb->buffer[ sb->length++ ] = __str[t];
    //}
    memcpy( sb->buffer + sb->length, __str, __len__ );
    sb->length += __len__;
}

FRISO_API void string_buffer_append_char( 
    string_buffer_t sb, char ch ) 
{
    //check the necessity to resize the buffer.
    if ( sb->length + 1 > sb->allocs ) {
        sb = resize_buffer( sb, sb->length * 2 + 1 );
    }

    sb->buffer[sb->length++] = ch;
}

FRISO_API void string_buffer_insert( 
    string_buffer_t sb, 
    uint_t idx, 
    fstring __str ) 
{
}

/*
 * remove the given bytes from the buffer start from idx.
 *        this will cause the byte move after the idx+length.
 *
 * @return the new string.
 */
FRISO_API fstring string_buffer_remove( 
    string_buffer_t sb, 
    uint_t idx, 
    uint_t length ) 
{
    uint_t t;
    //move the bytes after the idx + length
    for ( t = idx + length; t < sb->length; t++ ) {
        sb->buffer[t - length] = sb->buffer[t];
    }
    sb->buffer[t] = '\0';
    //memcpy( sb->buffer + idx, 
    //        sb->buffer + idx + length, 
    //        sb->length - idx - length );

    t = sb->length - idx;
    if ( t > 0 ) {
        sb->length -= ( t > length ) ? length : t;
    }
    sb->buffer[sb->length-1] = '\0';

    return sb->buffer;
}

/*
 * turn the string_buffer to a string.
 *        or return the buffer of the string_buffer.
 */
FRISO_API string_buffer_t string_buffer_trim( string_buffer_t sb ) 
{
    //resize the buffer.
    if ( sb->length < sb->allocs - 1 ) {
        sb = resize_buffer( sb, sb->length + 1 );
    }
    return sb;
}

/*
 * free the given fstring buffer.
 * and this function will not free the allocations of the 
 *     string_buffer_t->buffer, we return it to you, if there is
 *     a necessary you could free it youself by calling free();
 */
FRISO_API fstring string_buffer_devote( string_buffer_t sb ) 
{
    fstring buffer = sb->buffer;
    FRISO_FREE( sb );
    return buffer;
}

/*
 * clear the given fstring buffer.
 *        reset its buffer with 0 and reset its length to 0.
 */
FRISO_API void string_buffer_clear( string_buffer_t sb ) 
{
    memset( sb->buffer, 0x00, sb->length );
    sb->length = 0;
}

//free everything of the fstring buffer.
FRISO_API void free_string_buffer( string_buffer_t sb ) 
{
    FRISO_FREE( sb->buffer );
    FRISO_FREE( sb );
}


/**
 * create a new string_split_entry.
 *
 * @param    source
 * @return    string_split_t;    
 */
FRISO_API string_split_t new_string_split( 
    fstring delimiter, 
    fstring source ) 
{
    string_split_t e = ( string_split_t ) 
    FRISO_MALLOC( sizeof( string_split_entry ) );
    if ( e == NULL ) {
        ___ALLOCATION_ERROR___;
    }

    e->delimiter = delimiter;
    e->delLen = strlen(delimiter);
    e->source = source;
    e->srcLen = strlen(source);
    e->idx = 0;

    return e;
}

FRISO_API void string_split_reset( 
    string_split_t sst, 
    fstring delimiter, 
    fstring source ) 
{
    sst->delimiter = delimiter;
    sst->delLen = strlen(delimiter);
    sst->source = source;
    sst->srcLen = strlen(source);    
    sst->idx = 0;
}

FRISO_API void string_split_set_source( 
    string_split_t sst, fstring source ) 
{
    sst->source = source;
    sst->srcLen = strlen(source);
    sst->idx = 0;
}

FRISO_API void string_split_set_delimiter( 
    string_split_t sst, fstring delimiter ) 
{
    sst->delimiter = delimiter;
    sst->delLen = strlen( delimiter );
    sst->idx = 0;
}

FRISO_API void free_string_split( string_split_t sst ) 
{
    FRISO_FREE(sst);
}

/**
 * get the next split fstring, and copy the 
 *     splited fstring into the __dst buffer . 
 *
 * @param    string_split_t
 * @param    __dst
 * @return    fstring (NULL if reach the end of the source 
 *         or there is no more segmentation)
 */
FRISO_API fstring string_split_next( 
    string_split_t sst, fstring __dst) 
{
    uint_t i, _ok;
    fstring _dst = __dst;

    //check if reach the end of the fstring
    if ( sst->idx >= sst->srcLen ) return NULL;

    while ( 1 ) {
        _ok = 1;
        for ( i = 0; i < sst->delLen 
            && (sst->idx + i < sst->srcLen); i++ ) {
            if ( sst->source[sst->idx+i] != sst->delimiter[i] ) {
                _ok = 0;
                break;
            }
        }    

        //find the delimiter here,
        //break the loop and self plus the sst->idx, then return the buffer . 
        if ( _ok == 1 ) {
            sst->idx += sst->delLen;
            break;
        }

        //coy the char to the buffer
        *_dst++ = sst->source[sst->idx++];
        //check if reach the end of the fstring
        if ( sst->idx >= sst->srcLen ) break;
    }

    *_dst = '\0';
    return _dst;
}
