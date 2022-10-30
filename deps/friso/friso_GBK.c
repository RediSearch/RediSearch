/**
 * Friso GBK about function implements source file.
 *     @package src/friso_GBK.c .
 *
 * @author chenxin <chenxin619315@gmail.com>
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "friso_API.h"
#include "friso_ctype.h"

/* read the next GBK word from the specified position.
 *
 * @return int    the bytes of the current readed word.
 */
FRISO_API int gbk_next_word( 
    friso_task_t task, 
    uint_t *idx, 
    fstring __word )
{
    int c;
    if ( *idx >= task->length ) return 0;

    c = (uchar_t)task->text[*idx];
    if ( c <= 0x80 ) {
        task->bytes = 1;
    } else {
        task->bytes = 2;
    }

    //copy the word to the buffer.
    memcpy(__word, task->text + (*idx), task->bytes);
    (*idx) += task->bytes;
    __word[task->bytes] = '\0';

    return task->bytes;
}

//get the bytes of a gbk char.
//FRISO_API int get_gbk_bytes( char c )
//{
//    return 1;
//}

//check if the given buffer is a gbk word (ANSII string).
//    included the simplified and traditional words.
FRISO_API int gbk_cn_string(char *str) 
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    //GBK/2: gb2312 chinese word.
    return ( ((c1 >= 0xb0 && c1 <= 0xf7) 
        && (c2 >= 0xa1 && c2 <= 0xfe))
    //GBK/3: extend chinese words.
      || ((c1 >= 0x81 && c1 <= 0xa0) 
          && ( (c2 >= 0x40 && c2 <= 0x7e) 
          || (c2 >= 0x80 && c2 <= 0xfe) )) 
    //GBK/4: extend chinese words.
      || ((c1 >= 0xaa && c1 <= 0xfe) 
          && ( (c2 >= 0x40 && c2 <= 0xfe) 
          || (c2 >= 0x80 && c2 <= 0xa0) )) );
}

/*check if the given char is a ASCII letter
 *     include all the arabic number, letters and english puntuations.*/
FRISO_API int gbk_halfwidth_en_char( char c ) 
{
    int u = (uchar_t) c;
    return ( u >= 32 && u <= 126 );
}

/*
 * check if the given char is a full-width latain.
 *    include the full-width arabic numeber, letters.
 *        but not the full-width puntuations.
 */
FRISO_API int gbk_fullwidth_en_char( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    return ( (c1 == 0xA3) 
        && ( (c2 >= 0xB0 && c2 <= 0xB9)         //arabic numbers.
           || ( c2 >= 0xC1 && c2 <= 0xDA )         //uppercase letters.
           || ( c2 >= 0xE1 && c2 <= 0xFA) ) );    //lowercase letters.
}

//check if the given char is a upper case english letter.
//    included the full-width and half-width letters.
FRISO_API int gbk_uppercase_letter( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    if ( c1 <= 0x80 ) { //half-width
        return ( c1 >= 65 && c1 <= 90 );
    } else {            //full-width
        return ( c1 == 0xa3 && ( c2 >= 0xc1 && c2 <= 0xda ) );
    }
}

//check if the given char is a lower case char.
//    included the full-width and half-width letters.
FRISO_API int gbk_lowercase_letter( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    if ( c1 <= 0x80 ) { //half-width
        return ( c1 >= 97 && c1 <= 122 );
    } else {           //full-width
        return ( c1 == 0xa3 && ( c2 >= 0xe1 && c2 <= 0xfa ) );
    }
}

//check if the given char is a arabic numeric.
//    included the full-width and half-width arabic numeric.
FRISO_API int gbk_numeric_letter( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    if ( c1 <= 0x80 ) { //half-width
        return ( c1 >= 48 && c1 <= 57 );
    } else {            //full-width
        return ( ( c1 == 0xa3 ) && ( c2 >= 0xb0 && c2 <= 0xb9 ) );
    }
}

/*
 * check if the given fstring is make up with numeric chars.
 *     both full-width,half-width numeric is ok.
 */
FRISO_API int gbk_numeric_string( char *str )
{
    char *s = str;
    int c1 = 0;
    int c2 = 0;

    while ( *s != '\0' ) {
        c1 = (uchar_t) (*s++);
        if ( c1 <= 0x80 ) {     //half-width
            if ( c1 < 48 || c2 > 57 ) return 0;
        } else {            //full-width
            if ( c1 != 0xa3 ) return 0;
            c2 = (uchar_t) (*s++);
            if ( c2 < 0xb0 || c2 > 0xb9 ) return 0;
        }
    }

    return 1;
}

FRISO_API int gbk_decimal_string( char *str )
{
    int c1 = 0;
    int c2 = 0;
    int len = strlen(str), i, p = 0;

    //point header check.
    if ( str[0] == '.' || str[len - 1] == '.' ) return 0;

    for ( i = 0; i < len; ) {
        c1 = (uchar_t) str[i++];
        //count the number of the points.
        if ( c1 == 46 ) {
            p++;
            continue;
        }

        if ( c1 <= 0x80 ) { //half-width
            if ( c1 < 48 || c1 > 57 ) return 0;
        } else {            //full-width
            if ( c1 != 0xa3 ) return 0;
            c2 = (uchar_t) str[i++];
            if ( c2 < 0xb0 || c2 > 0xb9 ) return 0;
        }
    }

    return (p == 1);
}

//check if the given char is a english(ASCII) letter.
//    (full-width and half-width), not the punctuation/arabic of course.
FRISO_API int gbk_en_letter( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    if ( c1 <= 0x80 ) {
        return ( (c1 >= 65 && c1 <= 90)         //lowercase
            || (c1 >= 97 && c1 <= 122));        //uppercase
    } else {
        return ( (c1 == 0xa3) 
            && ( ( c2 >= 0xc1 && c2 <= 0xda )     //lowercase
              || ( c2 >= 0xe1 && c2 <= 0xfa ) ) );    //uppercase
    }

    return 0;
}

//check the given char is a whitespace or not.
//    included full-width and half-width whitespace.
FRISO_API int gbk_whitespace( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    if ( c1 <= 0x80 ) {
        return (c1 == 32);
    } else {
        return ( c1 == 0xa3 && c2 == 0xa0 );
    }
}

/* check if the given char is a letter number like 'ⅠⅡ'
 */
FRISO_API int gbk_letter_number( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    return ( (c1 == 0xa2) 
        && ( ( c2 >= 0xa1 && c2 <= 0xb0 )         //lowercase
        || ( c2 >= 0xf0 && c2 <= 0xfe ) ) );    //uppercase
}

/*
 * check if the given char is a other number like '①⑩⑽㈩'
 */
FRISO_API int gbk_other_number( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    return ( ( c1 == 0xa2 ) && ( c2 >= 0xc5 && c2 <= 0xee ) );
}

//check if the given char is a english punctuation.
FRISO_API int gbk_en_punctuation( char c )
{
    int u = (uchar_t) c;
    return ( (u > 32 && u < 48)
        || ( u > 57 && u < 65 )
        || ( u > 90 && u < 97 )
        || ( u > 122 && u < 127 ) );
}

//check the given char is a chinese punctuation.
FRISO_API int gbk_cn_punctuation( char *str )
{
    int c1 = (uchar_t) str[0];
    int c2 = (uchar_t) str[1];
    //full-width en punctuation.
    return ( (c1 == 0xa3 && (( c2 >= 0xa1 && c2 <= 0xaf ) 
            || ( c2 >= 0xba && c2 <= 0xc0 ) 
          || ( c2 >= 0xdb && c2 <= 0xe0 ) 
          || ( c2 >= 0xfb && c2 <= 0xfe ) )) 
    //chinese punctuation.
        || (c1 == 0xa1 && ( (c2 >= 0xa1 && c2 <= 0xae) 
            || ( c2 >= 0xb0 && c2 <= 0xbf ) )) 
    //A6 area special punctuations:" "
        || (c1 == 0xa6 && (c2 >= 0xf9 && c2 <= 0xfe)) 
    //A8 area special punctuations: " ˊˋ˙–―‥‵℅ "
        || (c1 == 0xa8 && (c2 >= 0x40 && c2 <= 0x47)) );
}

/* {{{
   '@', '$','%', '^', '&', '-', ':', '.', '/', '\'', '#', '+'
   */
//cause it it the same as utf-8, we use utf8's interface instead.
//@see the friso_ctype.h#gbk_keep_punctuation macro defined.

//static friso_hash_t __keep_punctuations_hash__ = NULL;

/* @Deprecated
 * check the given char is an english keep punctuation.*/
//FRISO_API int gbk_keep_punctuation( char *str )
//{
//    if ( __keep_punctuations_hash__ == NULL ) {
//    __keep_punctuations_hash__ = new_hash_table();
//    hash_put_mapping( __keep_punctuations_hash__, "@", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "$", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "%", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "^", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "&", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "-", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, ":", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, ".", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "/", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "'", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "#", NULL );
//    hash_put_mapping( __keep_punctuations_hash__, "+", NULL );
//    }
//    //check the hash.
//    return hash_exist_mapping( __keep_punctuations_hash__, str );
//}
/* }}} */

//check if the given english char is a full-width char or not.
//FRISO_API int gbk_fullwidth_char( char *str ) 
//{
//    return 1;
//}
