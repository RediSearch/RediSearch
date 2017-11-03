/**
 * Friso charset about function interface header file.
 *     @package src/friso_charset.h .
 * Available charset for now:
 * 1. UTF8    - function start with utf8
 * 2. GBK    - function start with gbk
 *
 * @author chenxin <chenxin619315@gmail.com>
 */
#ifndef _friso_charset_h
#define _friso_charset_h

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "friso.h"
#include "friso_API.h"

/** {{{ wrap interface */
/* check if the specified string is a cn string.
 * 
 * @return int (true for cn string or false)
 * */
FRISO_API int friso_cn_string( friso_charset_t, friso_task_t );

//check if the specified word is a whitespace.
FRISO_API int friso_whitespace( friso_charset_t, friso_task_t );

//check if the specifiled word is a numeric letter.
FRISO_API int friso_numeric_letter(friso_charset_t, friso_task_t);

//check if the specified word is a english letter.
FRISO_API int friso_en_letter( friso_charset_t, friso_task_t );

//check if the specified word is a half-width letter.
//    punctuations are inclued.
FRISO_API int friso_halfwidth_en_char( friso_charset_t, friso_task_t );

//check if the specified word is a full-width letter.
//    full-width punctuations are not included.
FRISO_API int friso_fullwidth_en_char( friso_charset_t, friso_task_t );

//check if the specified word is an english punctuations.
FRISO_API int friso_en_punctuation( friso_charset_t, friso_task_t );

//check if the specified word ia sn chinese punctuation.
FRISO_API int friso_cn_punctuation( friso_charset_t, friso_task_t );

FRISO_API int friso_letter_number( friso_charset_t, friso_task_t );
FRISO_API int friso_other_number( friso_charset_t, friso_task_t );

//check if the word is a keep punctuation.
//@Deprecated
//FRISO_API int friso_keep_punctuation( friso_charset_t, friso_task_t );

//check the specified string is numeric string.
FRISO_API int friso_numeric_string( friso_charset_t, char * );

//check the specified string is a decimal string.
FRISO_API int friso_decimal_string( friso_charset_t, char * );

//check if the specified char is english uppercase letter.
//    included full-width and half-width letters.
FRISO_API int friso_uppercase_letter( friso_charset_t, friso_task_t );


//en char type.
//#define FRISO_EN_LETTER     0     //a-z && A-Z
//#define FRISO_EN_NUMERIC    1    //0-9
//#define FRISO_EN_PUNCTUATION    2    //english punctuations
//#define FRISO_EN_WHITESPACE    3    //whitespace
//#define FRISO_EN_UNKNOW        -1    //beyond 32-122
typedef enum {
    FRISO_EN_LETTER        = 0,    //A-Z, a-z
    FRISO_EN_NUMERIC        = 1,    //0-9
    FRISO_EN_PUNCTUATION    = 2,    //english punctuations
    FRISO_EN_WHITESPACE        = 3,    //whitespace
    FRISO_EN_UNKNOW        = -1    //unkow(beyond 32-126)
} friso_enchar_t;

/* get the type of the specified char.
 *     the type will be the constants defined above.
 * (include the fullwidth english char.)
 */
FRISO_API friso_enchar_t friso_enchar_type( friso_charset_t, friso_task_t );

/* get the type of the specified en char.
 *     the type will be the constants defined above.
 * (the char should be half-width english char only)
 */
FRISO_API friso_enchar_t get_enchar_type( char );

/* }}} */




/** {{{ UTF8 interface*/

/* read the next utf-8 word from the specified position.
 *
 * @return int    the bytes of the current readed word.
 */
FRISO_API int utf8_next_word( friso_task_t, uint_t *, fstring );

//get the bytes of a utf-8 char.
FRISO_API int get_utf8_bytes( char );

//return the unicode serial number of a given string.
FRISO_API int get_utf8_unicode( const fstring );

//convert the unicode serial to a utf-8 string.
FRISO_API int unicode_to_utf8( uint_t, fstring );

//check if the given char is a CJK.
FRISO_API int utf8_cjk_string( uint_t ) ;

/*check the given char is a Basic Latin letter or not.
 *         include all the letters and english puntuations.*/
FRISO_API int utf8_halfwidth_en_char( uint_t );

/*
 * check the given char is a full-width latain or not.
 *    include the full-width arabic numeber, letters.
 *        but not the full-width puntuations.
 */
FRISO_API int utf8_fullwidth_en_char( uint_t );

//check the given char is a upper case letter or not.
//    included all the full-width and half-width letters.
FRISO_API int utf8_uppercase_letter( uint_t );

//check the given char is a lower case letter or not.
//    included all the full-width and half-width letters.
FRISO_API int utf8_lowercase_letter( uint_t );

//check the given char is a numeric.
//    included the full-width and half-width arabic numeric.
FRISO_API int utf8_numeric_letter( uint_t );

/*
 * check if the given fstring is make up with numeric chars.
 *     both full-width,half-width numeric is ok.
 */
FRISO_API int utf8_numeric_string( char * );

FRISO_API int utf8_decimal_string( char * );

//check the given char is a english char.
//(full-width and half-width)
//not the punctuation of course.
FRISO_API int utf8_en_letter( uint_t );

//check the given char is a whitespace or not.
FRISO_API int utf8_whitespace( uint_t );

/* check if the given char is a letter number like 'ⅠⅡ'
 */
FRISO_API int utf8_letter_number( uint_t );

/*
 * check if the given char is a other number like '①⑩⑽㈩'
 */
FRISO_API int utf8_other_number( uint_t );

//check if the given char is a english punctuation.
FRISO_API int utf8_en_punctuation( uint_t ) ;

//check if the given char is a chinese punctuation.
FRISO_API int utf8_cn_punctuation( uint_t u ); 

FRISO_API int is_en_punctuation( friso_charset_t, char );
//#define is_en_punctuation( c ) utf8_en_punctuation((uint_t) c) 

//@Deprecated
//FRISO_API int utf8_keep_punctuation( fstring );
/* }}} */




/** {{{ GBK interface */

/* read the next GBK word from the specified position.
 *
 * @return int    the bytes of the current readed word.
 */
FRISO_API int gbk_next_word( friso_task_t, uint_t *, fstring );

//get the bytes of a utf-8 char.
FRISO_API int get_gbk_bytes( char );

//check if the given char is a gbk char (ANSII string).
FRISO_API int gbk_cn_string( char * ) ;

/*check if the given char is a ASCII letter
 *     include all the letters and english puntuations.*/
FRISO_API int gbk_halfwidth_en_char( char );

/*
 * check if the given char is a full-width latain.
 *    include the full-width arabic numeber, letters.
 *        but not the full-width puntuations.
 */
FRISO_API int gbk_fullwidth_en_char( char * );

//check if the given char is a upper case char.
//    included all the full-width and half-width letters.
FRISO_API int gbk_uppercase_letter( char * );

//check if the given char is a lower case char.
//    included all the full-width and half-width letters.
FRISO_API int gbk_lowercase_letter( char * );

//check if the given char is a numeric.
//    included the full-width and half-width arabic numeric.
FRISO_API int gbk_numeric_letter( char * );

/*
 * check if the given fstring is make up with numeric chars.
 *     both full-width,half-width numeric is ok.
 */
FRISO_API int gbk_numeric_string( char * );

FRISO_API int gbk_decimal_string( char * );

//check if the given char is a english(ASCII) char.
//(full-width and half-width)
//not the punctuation of course.
FRISO_API int gbk_en_letter( char * );

//check the specified char is a whitespace or not.
FRISO_API int gbk_whitespace( char * );

/* check if the given char is a letter number like 'ⅠⅡ'
 */
FRISO_API int gbk_letter_number( char * );

/*
 * check if the given char is a other number like '①⑩⑽㈩'
 */
FRISO_API int gbk_other_number( char * );

//check if the given char is a english punctuation.
FRISO_API int gbk_en_punctuation( char ) ;

//check the given char is a chinese punctuation.
FRISO_API int gbk_cn_punctuation( char * ); 

//cause the logic handle is the same as the utf8.
//    here invoke the utf8 interface directly.
//FRISO_API int gbk_keep_punctuation( char * );
//@Deprecated
//#define gbk_keep_punctuation( str ) utf8_keep_punctuation(str)

//check if the given english char is a full-width char or not.
//FRISO_API int gbk_fullwidth_char( char * ) ;
/* }}}*/

#endif    /*end _friso_charset_h*/
