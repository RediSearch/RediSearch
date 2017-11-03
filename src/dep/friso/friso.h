/*
 * main interface file for friso - free soul.
 *         you could modify it and re-release it but never for commercial use.
 * 
 * @author    chenxin <chenxin619315@gmail.com>
 */
#ifndef _friso_h
#define _friso_h

#include "friso_API.h"
#include <stdio.h>

/* {{{ friso main interface define :: start*/
#define FRISO_VERSION "1.6.2"
#define friso_version() FRISO_VERSION


#define DEFAULT_SEGMENT_LENGTH     5
#define DEFAULT_MIX_LENGTH    2
#define DEFAULT_LNA_LENGTH     1
#define DEFAULT_NTHRESHOLD     1000000
#define DEFAULT_SEGMENT_MODE     2

/*
 * Type: friso_lex_t
 * -----------
 * This type used to represent the type of the lexicon. 
 */
typedef enum {
    __LEX_CJK_WORDS__ = 0,
    __LEX_CJK_UNITS__ = 1,
    __LEX_ECM_WORDS__ = 2,    //english and chinese mixed words.
    __LEX_CEM_WORDS__ = 3,    //chinese and english mixed words.
    __LEX_CN_LNAME__ = 4,
    __LEX_CN_SNAME__ = 5,
    __LEX_CN_DNAME1__ = 6,
    __LEX_CN_DNAME2__ = 7,
    __LEX_CN_LNA__ = 8,
    __LEX_STOPWORDS__ = 9,
    __LEX_ENPUN_WORDS__ = 10,
    __LEX_EN_WORDS__ = 11,
    __LEX_OTHER_WORDS__ = 15,
    __LEX_NCSYN_WORDS__ = 16,
    __LEX_PUNC_WORDS__ = 17,        //punctuations
    __LEX_UNKNOW_WORDS__ = 18        //unrecognized words.
} friso_lex_t;

typedef friso_hash_t * friso_dic_t;
#define __FRISO_LEXICON_LENGTH__ 12 


//charset that Friso now support.
typedef enum {
    FRISO_UTF8    = 0,        //UTF-8
    FRISO_GBK    = 1        //GBK
} friso_charset_t;

/*
 * Type: friso_mode_t
 * ------------------
 * use to identidy the mode that the friso use. 
 */
typedef enum {
    __FRISO_SIMPLE_MODE__   = 1,
    __FRISO_COMPLEX_MODE__  = 2,
    __FRISO_DETECT_MODE__   = 3
} friso_mode_t;

/* friso entry.*/
typedef struct {
    friso_dic_t dic;        //friso dictionary
    friso_charset_t charset;    //project charset.
} friso_entry;
typedef friso_entry * friso_t;



/*
 * Type: lex_entry_cdt
 * -------------------
 * This type used to represent the lexicon entry struct. 
 */
#define _LEX_APPENSYN_MASK (1 << 0)    //append synoyums words.
#define lex_appensyn_open(e)    e->ctrlMask |= _LEX_APPENSYN_MASK
#define lex_appensyn_close(e)   e->ctrlMask &= ~_LEX_APPENSYN_MASK
#define lex_appensyn_check(e)   ((e->ctrlMask & _LEX_APPENSYN_MASK) != 0)
typedef struct {
    /*
     * the type of the lexicon item.
     * available value is all the elements in friso_lex_t enum.
     *    and if it is __LEX_OTHER_WORDS__, we need to free it after use it.
     */
    uchar_t length;     //the length of the token.(after the convertor of Friso.)
    uchar_t rlen;       //the real length of the token.(before any convert)
    uchar_t type;
    uchar_t ctrlMask;   //function control mask, like append the synoyums words.
    uint_t offset;      //offset index.
    fstring word;
    //fstring py;       //pinyin of the word.(invalid)
    friso_array_t syn;  //synoyums words.
    friso_array_t pos;  //part of speech.
    uint_t fre;         //single word frequency.
} lex_entry_cdt;
typedef lex_entry_cdt * lex_entry_t;


/*the segmentation token entry.*/
#define __HITS_WORD_LENGTH__ 64

typedef struct {
    uchar_t type;    //type of the word. (item of friso_lex_t)
    uchar_t length;  //length of the token.
    uchar_t rlen;    //the real length of the token.(in orgin strng)
    char pos;        //part of speech.
    int offset;     //start offset of the word.
    char word[__HITS_WORD_LENGTH__];
    //char py[0];
} friso_token_entry;
typedef friso_token_entry * friso_token_t;


/*
 * Type: friso_task_entry
 * This type used to represent the current segmentation content.
 *         like the text to split, and the current index, token buffer eg.... 
 */
//action control mask for #FRISO_TASK_T#.
#define _TASK_CHECK_CF_MASK (1 << 0)     //Wether to check the chinese fraction.
#define _TASK_START_SS_MASK (1 << 1)    //Wether to start the secondary segmentation.
#define task_ssseg_open(task)   task->ctrlMask |= _TASK_START_SS_MASK
#define task_ssseg_close(task)  task->ctrlMask &= ~_TASK_START_SS_MASK
#define task_ssseg_check(task)  ((task->ctrlMask & _TASK_START_SS_MASK) != 0)
typedef struct {
    fstring text;           //text to tokenize
    uint_t idx;             //start offset index.
    uint_t length;          //length of the text.
    uint_t bytes;           //latest word bytes in C.
    uint_t unicode;         //latest word unicode number.
    uint_t ctrlMask;        //action control mask.
    friso_link_t pool;      //task pool.
    string_buffer_t sbuf;   //string buffer.
    friso_token_t token;    //token result token;
    char buffer[7];         //word buffer. (1-6 bytes for an utf-8 word in C).
} friso_task_entry;
typedef friso_task_entry * friso_task_t;


/* task configuration entry.*/
#define _FRISO_KEEP_PUNC_LEN 13
#define friso_en_kpunc(config, ch) (strchr(config->kpuncs, ch) != 0)
//typedef friso_token_t ( * friso_next_hit_fn ) ( friso_t, void *, friso_task_t );
//typedef lex_entry_t  ( * friso_next_lex_fn ) ( friso_t, void *, friso_task_t );
struct friso_config_struct {
    ushort_t max_len;            //the max match length (4 - 7).
    ushort_t r_name;            //1 for open chinese name recognition 0 for close it.
    ushort_t mix_len;            //the max length for the CJK words in a mix string.
    ushort_t lna_len;            //the max length for the chinese last name adron.
    ushort_t add_syn;            //append synonyms tokenizer words.
    ushort_t clr_stw;            //clear the stopwords.
    ushort_t keep_urec;         //keep the unrecongnized words.
    ushort_t spx_out;            //use sphinx output customize.
    ushort_t en_sseg;            //start the secondary segmentation.
    ushort_t st_minl;            //min length of the secondary segmentation token.
    uint_t nthreshold;            //the threshold value for a char to make up a chinese name.
    friso_mode_t mode;            //Complex mode or simple mode

    //pointer to the function to get the next token
    friso_token_t (*next_token) (friso_t, struct friso_config_struct *, friso_task_t);
    //pointer to the function to get the next cjk lex_entry_t
    lex_entry_t   (*next_cjk  ) (friso_t, struct friso_config_struct *, friso_task_t);        

    char kpuncs[_FRISO_KEEP_PUNC_LEN]; //keep punctuations buffer.
};
typedef struct friso_config_struct friso_config_entry;
typedef friso_config_entry * friso_config_t;



/*
 * Function: friso_new;
 * Usage: vars = friso_new( void );
 * --------------------------------
 * This function used to create a new empty friso friso_t; 
 *        with default value.
 */
FRISO_API friso_t friso_new( void );

//creat a friso entry with a default value from a configuratile file.
//@return 1 for successfully and 0 for failed.
FRISO_API int friso_init_from_ifile( friso_t, friso_config_t, fstring );

/*
 * Function: friso_free_vars;
 * Usage: friso_free( vars );
 * --------------------------
 * This function is used to free the allocation of the given vars. 
 */
FRISO_API void friso_free( friso_t );

/*
 * Function: friso_set_dic
 * Usage: dic = friso_set_dic( vars, dic );
 * ----------------------------------------
 * This function is used to set the dictionary for friso. 
 *         and firso_dic_t is the pointer of a hash table array.
 */
//FRISO_API void friso_set_dic( friso_t, friso_dic_t );
#define friso_set_dic(friso, dic)\
do {\
    friso->dic = dic;\
} while (0)

/*
 * Function: friso_set_mode
 * Usage: friso_set_mode( vars, mode );
 * ------------------------------------
 * This function is used to set the mode(complex or simple) that you want to friso to use.
 */
FRISO_API void friso_set_mode( friso_config_t, friso_mode_t );

/*create a new friso configuration entry and initialize 
  it with the default value.*/
FRISO_API friso_config_t friso_new_config( void );

//initialize the specified friso config entry with default value.
FRISO_API void friso_init_config( friso_config_t );

//free the specified friso configuration entry.
//FRISO_API void friso_free_config( friso_config_t );
#define friso_free_config(cfg) FRISO_FREE(cfg)

/*
 * Function: friso_new_task;
 * Usage: segment = friso_new_task( void );
 * ----------------------------------------
 * This function is used to create a new friso segment type; 
 */
FRISO_API friso_task_t friso_new_task( void );

/*
 * Function: friso_free_task;
 * Usage: friso_free_task( task ); 
 * -------------------------------
 * This function is used to free the allocation of function friso_new_segment();
 */
FRISO_API void friso_free_task( friso_task_t );

//create a new friso token
FRISO_API friso_token_t friso_new_token( void );

//free the given friso token
//FRISO_API void friso_free_token( friso_token_t );
#define friso_free_token(token) FRISO_FREE(token)

/*
 * Function: friso_set_text
 * Usage: friso_set_text( task, text );
 * ------------------------------------
 * This function is used to set the text that is going to segment. 
 */
FRISO_API void friso_set_text( friso_task_t, fstring );


//get the next cjk word with mmseg simple mode
FRISO_API lex_entry_t next_simple_cjk( friso_t, friso_config_t, friso_task_t );

//get the next cjk word with mmseg complex mode(mmseg core algorithm)
FRISO_API lex_entry_t next_complex_cjk( friso_t, friso_config_t, friso_task_t );

/*
 * Function: next_mmseg_token
 * Usage: word = next_mmseg_token( vars, seg );
 * --------------------------------------
 * This function is used to get next word that friso segmented
 *     with a split mode of __FRISO_SIMPLE_MODE__ or __FRISO_COMPLEX_MODE__
 */
FRISO_API friso_token_t next_mmseg_token( friso_t, friso_config_t, friso_task_t );

//__FRISO_DETECT_MODE__
FRISO_API friso_token_t next_detect_token( friso_t, friso_config_t, friso_task_t );
/* }}} friso main interface define :: end*/

/* {{{ lexicon interface define :: start*/

/*
 * Function: friso_dic_new
 * Usage: dic = friso_new_dic();
 * -----------------------------
 * This function used to create a new dictionary.(memory allocation).
 */
FRISO_API friso_dic_t friso_dic_new( void );

FRISO_API fstring file_get_line( fstring, FILE * );

/*
 * Function: friso_dic_free
 * Usage: friso_dic_free( void );
 * ------------------------------
 * This function is used to free all the allocation of friso_dic_new. 
 */
FRISO_API void friso_dic_free( friso_dic_t );

//create a new lexicon entry.
FRISO_API lex_entry_t new_lex_entry( fstring, friso_array_t, uint_t, uint_t, uint_t );

//free the given lexicon entry.
//free all the allocations that its synonyms word's items pointed to
//when the second arguments is 1
FRISO_API void free_lex_entry_full( lex_entry_t );
FRISO_API void free_lex_entry( lex_entry_t );

/*
 * Function: friso_dic_load
 * Usage: friso_dic_load( friso, friso_lex_t, path, length ); 
 * --------------------------------------------------
 * This function is used to load dictionary from a given path.
 *         no length limit when length less than 0.
 */
FRISO_API void friso_dic_load( friso_t, friso_config_t, 
        friso_lex_t, fstring, uint_t );

/*
 * load the lexicon configuration file.
 *    and load all the valid lexicon from the conf file.
 */
FRISO_API void friso_dic_load_from_ifile( friso_t, friso_config_t, fstring, uint_t );

/*
 * Function: friso_dic_match
 * Usage: friso_dic_add( dic, friso_lex_t, word, syn );
 * ----------------------------------------------
 * This function used to put new word into the dictionary.
 */
FRISO_API void friso_dic_add( friso_dic_t, friso_lex_t, fstring, friso_array_t );

/*
 * Function: friso_dic_add_with_fre
 * Usage: friso_dic_add_with_fre( dic, friso_lex_t, word, value, syn, fre );
 * -------------------------------------------------------------------
 * This function used to put new word width frequency into the dictionary.
 */
FRISO_API void friso_dic_add_with_fre( friso_dic_t, friso_lex_t, fstring, friso_array_t, uint_t );

/*
 * Function: friso_dic_match
 * Usage: result = friso_dic_match( dic, friso_lex_t, word );
 * ----------------------------------------------------
 * This function is used to check the given word is in the dictionary or not. 
 */
FRISO_API int friso_dic_match( friso_dic_t, friso_lex_t, fstring );

/*
 * Function: friso_dic_get
 * Usage: friso_dic_get( dic, friso_lex_t, word );
 * -----------------------------------------
 * This function is used to search the specified lex_entry_t.
 */
FRISO_API lex_entry_t friso_dic_get( friso_dic_t, friso_lex_t, fstring );

/*
 * Function: friso_spec_dic_size
 * Usage: friso_spec_dic_size( dic, friso_lex_t )
 * This function is used to get the size of the dictionary with a specified type. 
 */
FRISO_API uint_t friso_spec_dic_size( friso_dic_t, friso_lex_t );
FRISO_API uint_t friso_all_dic_size( friso_dic_t );
/* }}} lexicon interface define :: end*/

#endif /*end ifndef*/
