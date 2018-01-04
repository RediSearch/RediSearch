/*
** 2000-05-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Driver template for the LEMON parser generator.
**
** The "lemon" program processes an LALR(1) input grammar file, then uses
** this template to construct a parser.  The "lemon" program inserts text
** at each "%%" line.  Also, any "P-a-r-s-e" identifer prefix (without the
** interstitial "-" characters) contained in this template is changed into
** the value of the %name directive from the grammar.  Otherwise, the content
** of this template is copied straight through into the generate parser
** source file.
**
** The following is the concatenation of all %include directives from the
** input grammar file:
*/
#include <stdio.h>
/************ Begin %include sections from the grammar ************************/
#line 35 "parser.y"
   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../rmutil/vector.h"
#include "../query_node.h"

// strndup + lowercase in one pass!
char *strdupcase(const char *s, size_t len) {
  char *ret = strndup(s, len);
  char *dst = ret;
  char *src = dst;
  while (*src) {
      // unescape 
      if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
          ++src;
          continue;
      }
      *dst = tolower(*src);
      ++dst;
      ++src;

  }
  *dst = '\0';
  
  return ret;
}

// unescape a string (non null terminated) and return the new length (may be shorter than the original. This manipulates the string itself 
size_t unescapen(char *s, size_t sz) {
  
  char *dst = s;
  char *src = dst;
  char *end = s + sz;
  while (src < end) {
      // unescape 
      if (*src == '\\' && src + 1 < end &&
         (ispunct(*(src+1)) || isspace(*(src+1)))) {
          ++src;
          continue;
      }
      *dst++ = *src++;
  }
 
  return (size_t)(dst - s);
}
   
#line 79 "parser.c"
/**************** End of %include directives **********************************/
/* These constants specify the various numeric values for terminal symbols
** in a format understandable to "makeheaders".  This section is blank unless
** "lemon" is run with the "-m" command-line option.
***************** Begin makeheaders token definitions *************************/
/**************** End makeheaders token definitions ***************************/

/* The next sections is a series of control #defines.
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used to store the integer codes
**                       that represent terminal and non-terminal symbols.
**                       "unsigned char" is used if there are fewer than
**                       256 symbols.  Larger types otherwise.
**    YYNOCODE           is a number of type YYCODETYPE that is not used for
**                       any terminal or nonterminal symbol.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       (also known as: "terminal symbols") have fall-back
**                       values which should be used if the original symbol
**                       would not parse.  This permits keywords to sometimes
**                       be used as identifiers, for example.
**    YYACTIONTYPE       is the data type used for "action codes" - numbers
**                       that indicate what to do in response to the next
**                       token.
**    ParseTOKENTYPE     is the data type used for minor type for terminal
**                       symbols.  Background: A "minor type" is a semantic
**                       value associated with a terminal or non-terminal
**                       symbols.  For example, for an "ID" terminal symbol,
**                       the minor type might be the name of the identifier.
**                       Each non-terminal can have a different minor type.
**                       Terminal symbols all have the same minor type, though.
**                       This macros defines the minor type for terminal 
**                       symbols.
**    YYMINORTYPE        is the data type used for all minor types.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_MIN_REDUCE      Maximum value for reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned char
#define YYNOCODE 36
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  QueryNode * yy7;
  GeoFilter * yy8;
  Vector* yy36;
  NumericFilter * yy50;
  RangeNumber yy69;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  QueryParseCtx *ctx ;
#define ParseARG_PDECL , QueryParseCtx *ctx 
#define ParseARG_FETCH  QueryParseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             40
#define YYNRULE              39
#define YY_MAX_SHIFT         39
#define YY_MIN_SHIFTREDUCE   63
#define YY_MAX_SHIFTREDUCE   101
#define YY_MIN_REDUCE        102
#define YY_MAX_REDUCE        140
#define YY_ERROR_ACTION      141
#define YY_ACCEPT_ACTION     142
#define YY_NO_ACTION         143
/************* End control #defines *******************************************/

/* Define the yytestcase() macro to be a no-op if is not already defined
** otherwise.
**
** Applications can choose to define yytestcase() in the %include section
** to a macro that can assist in verifying code coverage.  For production
** code the yytestcase() macro should be turned off.  But it is useful
** for testing.
*/
#ifndef yytestcase
# define yytestcase(X)
#endif


/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.  
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N <= YY_MAX_SHIFT             Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   N between YY_MIN_SHIFTREDUCE       Shift to an arbitrary state then
**     and YY_MAX_SHIFTREDUCE           reduce by rule N-YY_MIN_SHIFTREDUCE.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE
**
**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as either:
**
**    (A)   N = yy_action[ yy_shift_ofst[S] + X ]
**    (B)   N = yy_default[S]
**
** The (A) formula is preferred.  The B formula is used instead if:
**    (1)  The yy_shift_ofst[S]+X value is out of range, or
**    (2)  yy_lookahead[yy_shift_ofst[S]+X] is not equal to X, or
**    (3)  yy_shift_ofst[S] equal YY_SHIFT_USE_DFLT.
** (Implementation note: YY_SHIFT_USE_DFLT is chosen so that
** YY_SHIFT_USE_DFLT+X will be out of range for all possible lookaheads X.
** Hence only tests (1) and (2) need to be evaluated.)
**
** The formulas above are for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and YY_REDUCE_USE_DFLT is used in place of
** YY_SHIFT_USE_DFLT.
**
** The following are the tables generated in this section:
**
**  yy_action[]        A single table containing all actions.
**  yy_lookahead[]     A table containing the lookahead for each entry in
**                     yy_action.  Used to detect hash collisions.
**  yy_shift_ofst[]    For each state, the offset into yy_action for
**                     shifting terminals.
**  yy_reduce_ofst[]   For each state, the offset into yy_action for
**                     shifting non-terminals after a reduce.
**  yy_default[]       Default action for each state.
**
*********** Begin parsing tables **********************************************/
#define YY_ACTTAB_COUNT (211)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     5,    9,   19,   39,    6,  101,   77,   99,  100,   83,
 /*    10 */     7,   18,   84,   26,   92,    5,   17,   19,   25,    6,
 /*    20 */   101,   77,   30,  100,   83,    7,   71,   84,  102,    9,
 /*    30 */     5,   79,   19,   98,    6,  101,   77,   20,  100,   83,
 /*    40 */     7,  103,   84,    5,    9,   19,   24,    6,  101,   77,
 /*    50 */   100,  100,   83,    7,   27,   84,   13,   75,   34,   66,
 /*    60 */    35,   95,   36,   31,   93,  101,   37,   33,  100,   19,
 /*    70 */    78,    6,  101,   77,    8,  100,   83,    7,    5,   84,
 /*    80 */    19,    9,    6,  101,   77,   32,  100,   83,    7,   85,
 /*    90 */    84,    3,   75,   34,   66,  101,   80,   36,  100,   21,
 /*   100 */   142,   37,   33,   14,   75,   34,   66,   86,   28,   36,
 /*   110 */    96,    1,   38,   37,   33,    4,   75,   34,   66,  104,
 /*   120 */   104,   36,   29,  104,  104,   37,   33,   11,   75,   34,
 /*   130 */    66,  104,  104,   36,  104,  104,  104,   37,   33,    2,
 /*   140 */    75,   34,   66,  104,  104,   36,  104,  104,  104,   37,
 /*   150 */    33,   12,   75,   34,   66,  104,  104,   36,  104,  104,
 /*   160 */   104,   37,   33,   15,   75,   34,   66,  104,  104,   36,
 /*   170 */   104,  104,  104,   37,   33,   16,   75,   34,   66,  104,
 /*   180 */   104,   36,   22,   97,  104,   37,   33,  104,   23,  101,
 /*   190 */    77,  104,  100,   83,    7,  104,   84,   94,    9,   72,
 /*   200 */    22,   97,  101,   80,   73,  100,   23,  101,  104,  104,
 /*   210 */   100,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     2,   16,    4,   30,    6,    7,    8,   30,   10,   11,
 /*    10 */    12,   16,   14,   25,   19,    2,   18,    4,   20,    6,
 /*    20 */     7,    8,   34,   10,   11,   12,   13,   14,    0,   16,
 /*    30 */     2,   34,    4,   30,    6,    7,    8,   30,   10,   11,
 /*    40 */    12,    0,   14,    2,   16,    4,   30,    6,    7,    8,
 /*    50 */    10,   10,   11,   12,   25,   14,   23,   24,   25,   26,
 /*    60 */    27,   28,   29,   34,   31,    7,   33,   34,   10,    4,
 /*    70 */    34,    6,    7,    8,    5,   10,   11,   12,    2,   14,
 /*    80 */     4,   16,    6,    7,    8,   16,   10,   11,   12,   34,
 /*    90 */    14,   23,   24,   25,   26,    7,    8,   29,   10,   25,
 /*   100 */    32,   33,   34,   23,   24,   25,   26,   34,   34,   29,
 /*   110 */    21,    5,   10,   33,   34,   23,   24,   25,   26,   35,
 /*   120 */    35,   29,   16,   35,   35,   33,   34,   23,   24,   25,
 /*   130 */    26,   35,   35,   29,   35,   35,   35,   33,   34,   23,
 /*   140 */    24,   25,   26,   35,   35,   29,   35,   35,   35,   33,
 /*   150 */    34,   23,   24,   25,   26,   35,   35,   29,   35,   35,
 /*   160 */    35,   33,   34,   23,   24,   25,   26,   35,   35,   29,
 /*   170 */    35,   35,   35,   33,   34,   23,   24,   25,   26,   35,
 /*   180 */    35,   29,    6,    7,   35,   33,   34,   35,   12,    7,
 /*   190 */     8,   35,   10,   11,   12,   35,   14,   21,   16,    4,
 /*   200 */     6,    7,    7,    8,    4,   10,   12,    7,   35,   35,
 /*   210 */    10,
};
#define YY_SHIFT_USE_DFLT (211)
#define YY_SHIFT_COUNT    (39)
#define YY_SHIFT_MIN      (-15)
#define YY_SHIFT_MAX      (200)
static const short yy_shift_ofst[] = {
 /*     0 */    41,   -2,   13,   28,   65,   76,   76,   76,   76,   76,
 /*    10 */    76,  182,  -15,  -15,  -15,  211,  211,   58,   58,   58,
 /*    20 */   176,  195,  194,  194,  194,  194,   88,   88,  200,   58,
 /*    30 */    58,   58,   58,   58,   40,   -5,   69,  106,   89,  102,
};
#define YY_REDUCE_USE_DFLT (-28)
#define YY_REDUCE_COUNT (34)
#define YY_REDUCE_MIN   (-27)
#define YY_REDUCE_MAX   (152)
static const short yy_reduce_ofst[] = {
 /*     0 */    68,   33,   80,   80,   80,   92,  104,  116,  128,  140,
 /*    10 */   152,   80,   80,   80,   80,   80,   80,  -12,   29,   74,
 /*    20 */   -27,   -3,  -23,    3,    7,   16,   -3,   -3,   36,   55,
 /*    30 */    36,   36,   73,   36,   -3,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   141,  141,  141,  141,  121,  141,  141,  141,  141,  141,
 /*    10 */   141,  120,  109,  108,  104,  106,  107,  141,  141,  141,
 /*    20 */   141,  141,  141,  141,  141,  141,  128,  130,  141,  141,
 /*    30 */   127,  129,  141,  113,  115,  126,  141,  141,  141,  141,
};
/********** End of lemon-generated parsing tables *****************************/

/* The next table maps tokens (terminal symbols) into fallback tokens.  
** If a construct like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
**
** This feature can be used, for example, to cause some keywords in a language
** to revert to identifiers if they keyword does not apply in the context where
** it appears.
*/
#ifdef YYFALLBACK
static const YYCODETYPE yyFallback[] = {
};
#endif /* YYFALLBACK */

/* The following structure represents a single element of the
** parser's stack.  Information stored includes:
**
**   +  The state number for the parser at this level of the stack.
**
**   +  The value of the token stored at this level of the stack.
**      (In other words, the "major" token.)
**
**   +  The semantic value stored at this level of the stack.  This is
**      the information used by the action routines in the grammar.
**      It is sometimes called the "minor" token.
**
** After the "shift" half of a SHIFTREDUCE action, the stateno field
** actually contains the reduce action for the second half of the
** SHIFTREDUCE.
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number, or reduce action in SHIFTREDUCE */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  yyStackEntry *yytos;          /* Pointer to top element of the stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyhwm;                    /* High-water mark of the stack */
#endif
#ifndef YYNOERRORRECOVERY
  int yyerrcnt;                 /* Shifts left before out of the error */
#endif
  ParseARG_SDECL                /* A place to hold %extra_argument */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
  yyStackEntry yystk0;          /* First stack entry */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
#endif
};
typedef struct yyParser yyParser;

#ifndef NDEBUG
#include <stdio.h>
static FILE *yyTraceFILE = 0;
static char *yyTracePrompt = 0;
#endif /* NDEBUG */

#ifndef NDEBUG
/* 
** Turn parser tracing on by giving a stream to which to write the trace
** and a prompt to preface each trace message.  Tracing is turned off
** by making either argument NULL 
**
** Inputs:
** <ul>
** <li> A FILE* to which trace output should be written.
**      If NULL, then tracing is turned off.
** <li> A prefix string written at the beginning of every
**      line of trace output.  If NULL, then tracing is
**      turned off.
** </ul>
**
** Outputs:
** None.
*/
void RSQuery_ParseTrace(FILE *TraceFILE, char *zTracePrompt){
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if( yyTraceFILE==0 ) yyTracePrompt = 0;
  else if( yyTracePrompt==0 ) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  "$",             "LOWEST",        "TILDE",         "TAGLIST",     
  "QUOTE",         "COLON",         "MINUS",         "NUMBER",      
  "STOPWORD",      "TERMLIST",      "TERM",          "PREFIX",      
  "LP",            "RP",            "MODIFIER",      "AND",         
  "OR",            "ORX",           "LB",            "RB",          
  "LSQB",          "RSQB",          "error",         "expr",        
  "prefix",        "termlist",      "union",         "tag_list",    
  "geo_filter",    "modifierlist",  "num",           "numeric_range",
  "query",         "modifier",      "term",        
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "query ::= expr",
 /*   1 */ "query ::=",
 /*   2 */ "expr ::= expr expr",
 /*   3 */ "expr ::= union",
 /*   4 */ "union ::= expr OR expr",
 /*   5 */ "union ::= union OR expr",
 /*   6 */ "expr ::= modifier COLON expr",
 /*   7 */ "expr ::= modifierlist COLON expr",
 /*   8 */ "expr ::= LP expr RP",
 /*   9 */ "expr ::= QUOTE termlist QUOTE",
 /*  10 */ "expr ::= QUOTE term QUOTE",
 /*  11 */ "expr ::= term",
 /*  12 */ "expr ::= prefix",
 /*  13 */ "expr ::= termlist",
 /*  14 */ "expr ::= STOPWORD",
 /*  15 */ "termlist ::= term term",
 /*  16 */ "termlist ::= termlist term",
 /*  17 */ "termlist ::= termlist STOPWORD",
 /*  18 */ "expr ::= MINUS expr",
 /*  19 */ "expr ::= TILDE expr",
 /*  20 */ "prefix ::= PREFIX",
 /*  21 */ "modifier ::= MODIFIER",
 /*  22 */ "modifierlist ::= modifier OR term",
 /*  23 */ "modifierlist ::= modifierlist OR term",
 /*  24 */ "expr ::= modifier COLON tag_list",
 /*  25 */ "tag_list ::= LB term",
 /*  26 */ "tag_list ::= LB termlist",
 /*  27 */ "tag_list ::= tag_list OR term",
 /*  28 */ "tag_list ::= tag_list OR termlist",
 /*  29 */ "tag_list ::= tag_list RB",
 /*  30 */ "expr ::= modifier COLON numeric_range",
 /*  31 */ "numeric_range ::= LSQB num num RSQB",
 /*  32 */ "expr ::= modifier COLON geo_filter",
 /*  33 */ "geo_filter ::= LSQB num num num TERM RSQB",
 /*  34 */ "num ::= NUMBER",
 /*  35 */ "num ::= LP num",
 /*  36 */ "num ::= MINUS num",
 /*  37 */ "term ::= TERM",
 /*  38 */ "term ::= NUMBER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.  Return the number
** of errors.  Return 0 on success.
*/
static int yyGrowStack(yyParser *p){
  int newSize;
  int idx;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  idx = p->yytos ? (int)(p->yytos - p->yystack) : 0;
  if( p->yystack==&p->yystk0 ){
    pNew = malloc(newSize*sizeof(pNew[0]));
    if( pNew ) pNew[0] = p->yystk0;
  }else{
    pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  }
  if( pNew ){
    p->yystack = pNew;
    p->yytos = &p->yystack[idx];
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows from %d to %d entries.\n",
              yyTracePrompt, p->yystksz, newSize);
    }
#endif
    p->yystksz = newSize;
  }
  return pNew==0; 
}
#endif

/* Datatype of the argument to the memory allocated passed as the
** second argument to ParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void RSQuery_ParseInit(void *yypParser){
  yyParser *pParser = (yyParser*)yypParser;
#ifdef YYTRACKMAXSTACKDEPTH
  pParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  pParser->yytos = NULL;
  pParser->yystack = NULL;
  pParser->yystksz = 0;
  if( yyGrowStack(pParser) ){
    pParser->yystack = &pParser->yystk0;
    pParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  pParser->yyerrcnt = -1;
#endif
  pParser->yytos = pParser->yystack;
  pParser->yystack[0].stateno = 0;
  pParser->yystack[0].major = 0;
}

#ifndef Parse_ENGINEALWAYSONSTACK
/* 
** This function allocates a new parser.
** The only argument is a pointer to a function which works like
** malloc.
**
** Inputs:
** A pointer to the function used to allocate memory.
**
** Outputs:
** A pointer to a parser.  This pointer is used in subsequent calls
** to Parse and ParseFree.
*/
void *RSQuery_ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( pParser ) RSQuery_ParseInit(pParser);
  return pParser;
}
#endif /* Parse_ENGINEALWAYSONSTACK */


/* The following function deletes the "minor type" or semantic value
** associated with a symbol.  The symbol can be either a terminal
** or nonterminal. "yymajor" is the symbol code, and "yypminor" is
** a pointer to the value to be deleted.  The code used to do the 
** deletions is derived from the %destructor and/or %token_destructor
** directives of the input grammar.
*/
static void yy_destructor(
  yyParser *yypParser,    /* The parser */
  YYCODETYPE yymajor,     /* Type code for object to destroy */
  YYMINORTYPE *yypminor   /* The object to be destroyed */
){
  ParseARG_FETCH;
  switch( yymajor ){
    /* Here is inserted the actions which take place when a
    ** terminal or non-terminal is destroyed.  This can happen
    ** when the symbol is popped from the stack during a
    ** reduce or during error processing or when a parser is 
    ** being destroyed before it is finished parsing.
    **
    ** Note: during a reduce, the only symbols destroyed are those
    ** which appear on the RHS of the rule, but which are *not* used
    ** inside the C code.
    */
/********* Begin destructor definitions ***************************************/
      /* Default NON-TERMINAL Destructor */
    case 22: /* error */
    case 30: /* num */
    case 32: /* query */
    case 33: /* modifier */
    case 34: /* term */
{
#line 89 "parser.y"
 
#line 598 "parser.c"
}
      break;
    case 23: /* expr */
    case 24: /* prefix */
    case 25: /* termlist */
    case 26: /* union */
    case 27: /* tag_list */
{
#line 92 "parser.y"
 QueryNode_Free((yypminor->yy7)); 
#line 609 "parser.c"
}
      break;
    case 28: /* geo_filter */
{
#line 107 "parser.y"
 GeoFilter_Free((yypminor->yy8)); 
#line 616 "parser.c"
}
      break;
    case 29: /* modifierlist */
{
#line 110 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy36)); i++) {
        char *s;
        Vector_Get((yypminor->yy36), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy36)); 

#line 630 "parser.c"
}
      break;
    case 31: /* numeric_range */
{
#line 122 "parser.y"

    NumericFilter_Free((yypminor->yy50));

#line 639 "parser.c"
}
      break;
/********* End destructor definitions *****************************************/
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
*/
static void yy_pop_parser_stack(yyParser *pParser){
  yyStackEntry *yytos;
  assert( pParser->yytos!=0 );
  assert( pParser->yytos > pParser->yystack );
  yytos = pParser->yytos--;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yy_destructor(pParser, yytos->major, &yytos->minor);
}

/*
** Clear all secondary memory allocations from the parser
*/
void RSQuery_ParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef Parse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void RSQuery_ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  RSQuery_ParseFinalize(p);
  (*freeProc)(p);
}
#endif /* Parse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int RSQuery_ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyhwm;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static unsigned int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yytos->stateno;
 
  if( stateno>=YY_MIN_REDUCE ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
  do{
    i = yy_shift_ofst[stateno];
    assert( iLookAhead!=YYNOCODE );
    i += iLookAhead;
    if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      if( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0])
             && (iFallback = yyFallback[iLookAhead])!=0 ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE, "%sFALLBACK %s => %s\n",
             yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[iFallback]);
        }
#endif
        assert( yyFallback[iFallback]==0 ); /* Fallback loop must terminate */
        iLookAhead = iFallback;
        continue;
      }
#endif
#ifdef YYWILDCARD
      {
        int j = i - iLookAhead + YYWILDCARD;
        if( 
#if YY_SHIFT_MIN+YYWILDCARD<0
          j>=0 &&
#endif
#if YY_SHIFT_MAX+YYWILDCARD>=YY_ACTTAB_COUNT
          j<YY_ACTTAB_COUNT &&
#endif
          yy_lookahead[j]==YYWILDCARD && iLookAhead>0
        ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead],
               yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
      return yy_default[stateno];
    }else{
      return yy_action[i];
    }
  }while(1);
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
static int yy_find_reduce_action(
  int stateno,              /* Current state number */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
#ifdef YYERRORSYMBOL
  if( stateno>YY_REDUCE_COUNT ){
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_COUNT );
#endif
  i = yy_reduce_ofst[stateno];
  assert( i!=YY_REDUCE_USE_DFLT );
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    return yy_default[stateno];
  }
#else
  assert( i>=0 && i<YY_ACTTAB_COUNT );
  assert( yy_lookahead[i]==iLookAhead );
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser){
   ParseARG_FETCH;
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
/******** Begin %stack_overflow code ******************************************/
/******** End %stack_overflow code ********************************************/
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
}

/*
** Print tracing information for a SHIFT action
*/
#ifndef NDEBUG
static void yyTraceShift(yyParser *yypParser, int yyNewState){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%sShift '%s', go to state %d\n",
         yyTracePrompt,yyTokenName[yypParser->yytos->major],
         yyNewState);
    }else{
      fprintf(yyTraceFILE,"%sShift '%s'\n",
         yyTracePrompt,yyTokenName[yypParser->yytos->major]);
    }
  }
}
#else
# define yyTraceShift(X,Y)
#endif

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
  ParseTOKENTYPE yyMinor        /* The minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yytos++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
    yypParser->yyhwm++;
    assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack) );
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yytos>=&yypParser->yystack[YYSTACKDEPTH] ){
    yypParser->yytos--;
    yyStackOverflow(yypParser);
    return;
  }
#else
  if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz] ){
    if( yyGrowStack(yypParser) ){
      yypParser->yytos--;
      yyStackOverflow(yypParser);
      return;
    }
  }
#endif
  if( yyNewState > YY_MAX_SHIFT ){
    yyNewState += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
  }
  yytos = yypParser->yytos;
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState);
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 32, 1 },
  { 32, 0 },
  { 23, 2 },
  { 23, 1 },
  { 26, 3 },
  { 26, 3 },
  { 23, 3 },
  { 23, 3 },
  { 23, 3 },
  { 23, 3 },
  { 23, 3 },
  { 23, 1 },
  { 23, 1 },
  { 23, 1 },
  { 23, 1 },
  { 25, 2 },
  { 25, 2 },
  { 25, 2 },
  { 23, 2 },
  { 23, 2 },
  { 24, 1 },
  { 33, 1 },
  { 29, 3 },
  { 29, 3 },
  { 23, 3 },
  { 27, 2 },
  { 27, 2 },
  { 27, 3 },
  { 27, 3 },
  { 27, 2 },
  { 23, 3 },
  { 31, 4 },
  { 23, 3 },
  { 28, 6 },
  { 30, 1 },
  { 30, 2 },
  { 30, 2 },
  { 34, 1 },
  { 34, 1 },
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno        /* Number of the rule by which to reduce */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfo[yyruleno].nrhs;
    fprintf(yyTraceFILE, "%sReduce [%s], go to state %d.\n", yyTracePrompt,
      yyRuleName[yyruleno], yymsp[-yysize].stateno);
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfo[yyruleno].nrhs==0 ){
#ifdef YYTRACKMAXSTACKDEPTH
    if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
      yypParser->yyhwm++;
      assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack));
    }
#endif
#if YYSTACKDEPTH>0 
    if( yypParser->yytos>=&yypParser->yystack[YYSTACKDEPTH-1] ){
      yyStackOverflow(yypParser);
      return;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        return;
      }
      yymsp = yypParser->yytos;
    }
#endif
  }

  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
/********** Begin reduce actions **********************************************/
        YYMINORTYPE yylhsminor;
      case 0: /* query ::= expr */
#line 126 "parser.y"
{ 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = yymsp[0].minor.yy7;
 
}
#line 1001 "parser.c"
        break;
      case 1: /* query ::= */
#line 132 "parser.y"
{
    ctx->root = NULL;
}
#line 1008 "parser.c"
        break;
      case 2: /* expr ::= expr expr */
#line 140 "parser.y"
{

    // if both yymsp[-1].minor.yy7 and yymsp[0].minor.yy7 are null we return null
    if (yymsp[-1].minor.yy7 == NULL && yymsp[0].minor.yy7 == NULL) {
        yylhsminor.yy7 = NULL;
    } else {

        if (yymsp[-1].minor.yy7 && yymsp[-1].minor.yy7->type == QN_PHRASE && yymsp[-1].minor.yy7->pn.exact == 0 && 
            yymsp[-1].minor.yy7->fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy7 = yymsp[-1].minor.yy7;
        } else {     
            yylhsminor.yy7 = NewPhraseNode(0);
            QueryPhraseNode_AddChild(yylhsminor.yy7, yymsp[-1].minor.yy7);
        }
        QueryPhraseNode_AddChild(yylhsminor.yy7, yymsp[0].minor.yy7);
    }
}
#line 1029 "parser.c"
  yymsp[-1].minor.yy7 = yylhsminor.yy7;
        break;
      case 3: /* expr ::= union */
#line 163 "parser.y"
{
    yylhsminor.yy7 = yymsp[0].minor.yy7;
}
#line 1037 "parser.c"
  yymsp[0].minor.yy7 = yylhsminor.yy7;
        break;
      case 4: /* union ::= expr OR expr */
#line 167 "parser.y"
{
    if (yymsp[-2].minor.yy7 == NULL && yymsp[0].minor.yy7 == NULL) {
        yylhsminor.yy7 = NULL;
    } else if (yymsp[-2].minor.yy7 && yymsp[-2].minor.yy7->type == QN_UNION && yymsp[-2].minor.yy7->fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy7 = yymsp[-2].minor.yy7;
    } else {
        yylhsminor.yy7 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy7, yymsp[-2].minor.yy7);
        if (yymsp[-2].minor.yy7) 
         yylhsminor.yy7->fieldMask |= yymsp[-2].minor.yy7->fieldMask;

    } 
    if (yymsp[0].minor.yy7) {

        QueryUnionNode_AddChild(yylhsminor.yy7, yymsp[0].minor.yy7);
        yylhsminor.yy7->fieldMask |= yymsp[0].minor.yy7->fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy7, yylhsminor.yy7->fieldMask);
    }
    
}
#line 1062 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 5: /* union ::= union OR expr */
#line 188 "parser.y"
{
    
    yylhsminor.yy7 = yymsp[-2].minor.yy7;
    QueryUnionNode_AddChild(yylhsminor.yy7, yymsp[0].minor.yy7); 
    yylhsminor.yy7->fieldMask |= yymsp[0].minor.yy7->fieldMask;
    QueryNode_SetFieldMask(yymsp[0].minor.yy7, yylhsminor.yy7->fieldMask);


}
#line 1076 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 6: /* expr ::= modifier COLON expr */
#line 202 "parser.y"
{
    if (yymsp[0].minor.yy7 == NULL) {
        yylhsminor.yy7 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy7, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy7 = yymsp[0].minor.yy7; 
    }
}
#line 1091 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 7: /* expr ::= modifierlist COLON expr */
#line 222 "parser.y"
{
    
    if (yymsp[0].minor.yy7 == NULL) {
        yylhsminor.yy7 = NULL;
    } else {
        //yymsp[0].minor.yy7->fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy36); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy36, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy7, mask);
        Vector_Free(yymsp[-2].minor.yy36);
        yylhsminor.yy7=yymsp[0].minor.yy7;
    }
}
#line 1116 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 8: /* expr ::= LP expr RP */
#line 243 "parser.y"
{
    yymsp[-2].minor.yy7 = yymsp[-1].minor.yy7;
}
#line 1124 "parser.c"
        break;
      case 9: /* expr ::= QUOTE termlist QUOTE */
#line 251 "parser.y"
{
    yymsp[-1].minor.yy7->pn.exact =1;
    yymsp[-1].minor.yy7->flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy7 = yymsp[-1].minor.yy7;
}
#line 1134 "parser.c"
        break;
      case 10: /* expr ::= QUOTE term QUOTE */
#line 258 "parser.y"
{
    yymsp[-2].minor.yy7 = NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
    yymsp[-2].minor.yy7->flags |= QueryNode_Verbatim;
    
}
#line 1143 "parser.c"
        break;
      case 11: /* expr ::= term */
#line 264 "parser.y"
{
   yylhsminor.yy7 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
#line 1150 "parser.c"
  yymsp[0].minor.yy7 = yylhsminor.yy7;
        break;
      case 12: /* expr ::= prefix */
#line 268 "parser.y"
{
    yylhsminor.yy7= yymsp[0].minor.yy7;
}
#line 1158 "parser.c"
  yymsp[0].minor.yy7 = yylhsminor.yy7;
        break;
      case 13: /* expr ::= termlist */
#line 272 "parser.y"
{
        yylhsminor.yy7 = yymsp[0].minor.yy7;
}
#line 1166 "parser.c"
  yymsp[0].minor.yy7 = yylhsminor.yy7;
        break;
      case 14: /* expr ::= STOPWORD */
#line 276 "parser.y"
{
    yymsp[0].minor.yy7 = NULL;
}
#line 1174 "parser.c"
        break;
      case 15: /* termlist ::= term term */
#line 280 "parser.y"
{
    yylhsminor.yy7 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy7, NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1));
    QueryPhraseNode_AddChild(yylhsminor.yy7, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1183 "parser.c"
  yymsp[-1].minor.yy7 = yylhsminor.yy7;
        break;
      case 16: /* termlist ::= termlist term */
#line 286 "parser.y"
{
    yylhsminor.yy7 = yymsp[-1].minor.yy7;
    QueryPhraseNode_AddChild(yylhsminor.yy7, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1192 "parser.c"
  yymsp[-1].minor.yy7 = yylhsminor.yy7;
        break;
      case 17: /* termlist ::= termlist STOPWORD */
      case 29: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==29);
#line 291 "parser.y"
{
    yylhsminor.yy7 = yymsp[-1].minor.yy7;
}
#line 1201 "parser.c"
  yymsp[-1].minor.yy7 = yylhsminor.yy7;
        break;
      case 18: /* expr ::= MINUS expr */
#line 299 "parser.y"
{ 
    yymsp[-1].minor.yy7 = NewNotNode(yymsp[0].minor.yy7);
}
#line 1209 "parser.c"
        break;
      case 19: /* expr ::= TILDE expr */
#line 307 "parser.y"
{ 
    yymsp[-1].minor.yy7 = NewOptionalNode(yymsp[0].minor.yy7);
}
#line 1216 "parser.c"
        break;
      case 20: /* prefix ::= PREFIX */
#line 315 "parser.y"
{
    yylhsminor.yy7 = NewPrefixNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len);
}
#line 1223 "parser.c"
  yymsp[0].minor.yy7 = yylhsminor.yy7;
        break;
      case 21: /* modifier ::= MODIFIER */
#line 323 "parser.y"
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1232 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 22: /* modifierlist ::= modifier OR term */
#line 328 "parser.y"
{
    yylhsminor.yy36 = NewVector(char *, 2);
    char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy36, s);
    s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy36, s);
}
#line 1244 "parser.c"
  yymsp[-2].minor.yy36 = yylhsminor.yy36;
        break;
      case 23: /* modifierlist ::= modifierlist OR term */
#line 336 "parser.y"
{
    char *s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy36, s);
    yylhsminor.yy36 = yymsp[-2].minor.yy36;
}
#line 1254 "parser.c"
  yymsp[-2].minor.yy36 = yylhsminor.yy36;
        break;
      case 24: /* expr ::= modifier COLON tag_list */
#line 346 "parser.y"
{
    if (!yymsp[0].minor.yy7) {
        yylhsminor.yy7= NULL;
    } else {
        yylhsminor.yy7 = NewTagNode(strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len), yymsp[-2].minor.yy0.len);
        QueryTagNode_AddChildren(yylhsminor.yy7, yymsp[0].minor.yy7->pn.children, yymsp[0].minor.yy7->pn.numChildren);
        
        // Set the children count on yymsp[0].minor.yy7 to 0 so they won't get recursively free'd
        yymsp[0].minor.yy7->pn.numChildren = 0;
        QueryNode_Free(yymsp[0].minor.yy7);
    }
}
#line 1271 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 25: /* tag_list ::= LB term */
#line 359 "parser.y"
{
    yymsp[-1].minor.yy7 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy7, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1280 "parser.c"
        break;
      case 26: /* tag_list ::= LB termlist */
#line 365 "parser.y"
{
    yymsp[-1].minor.yy7 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy7, yymsp[0].minor.yy7);
}
#line 1288 "parser.c"
        break;
      case 27: /* tag_list ::= tag_list OR term */
#line 370 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy7, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy7 = yymsp[-2].minor.yy7;
}
#line 1296 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 28: /* tag_list ::= tag_list OR termlist */
#line 374 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy7, yymsp[0].minor.yy7);
    yylhsminor.yy7 = yymsp[-2].minor.yy7;
}
#line 1305 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 30: /* expr ::= modifier COLON numeric_range */
#line 388 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy50->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy7 = NewNumericNode(yymsp[0].minor.yy50);
}
#line 1315 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 31: /* numeric_range ::= LSQB num num RSQB */
#line 394 "parser.y"
{
    yymsp[-3].minor.yy50 = NewNumericFilter(yymsp[-2].minor.yy69.num, yymsp[-1].minor.yy69.num, yymsp[-2].minor.yy69.inclusive, yymsp[-1].minor.yy69.inclusive);
}
#line 1323 "parser.c"
        break;
      case 32: /* expr ::= modifier COLON geo_filter */
#line 402 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy8->property = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy7 = NewGeofilterNode(yymsp[0].minor.yy8);
}
#line 1332 "parser.c"
  yymsp[-2].minor.yy7 = yylhsminor.yy7;
        break;
      case 33: /* geo_filter ::= LSQB num num num TERM RSQB */
#line 408 "parser.y"
{
    yymsp[-5].minor.yy8 = NewGeoFilter(yymsp[-4].minor.yy69.num, yymsp[-3].minor.yy69.num, yymsp[-2].minor.yy69.num, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(yymsp[-5].minor.yy8, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}
#line 1345 "parser.c"
        break;
      case 34: /* num ::= NUMBER */
#line 421 "parser.y"
{
    yylhsminor.yy69.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy69.inclusive = 1;
}
#line 1353 "parser.c"
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 35: /* num ::= LP num */
#line 426 "parser.y"
{
    yymsp[-1].minor.yy69=yymsp[0].minor.yy69;
    yymsp[-1].minor.yy69.inclusive = 0;
}
#line 1362 "parser.c"
        break;
      case 36: /* num ::= MINUS num */
#line 431 "parser.y"
{
    yymsp[0].minor.yy69.num = -yymsp[0].minor.yy69.num;
    yymsp[-1].minor.yy69 = yymsp[0].minor.yy69;
}
#line 1370 "parser.c"
        break;
      case 37: /* term ::= TERM */
      case 38: /* term ::= NUMBER */ yytestcase(yyruleno==38);
#line 436 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1378 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact <= YY_MAX_SHIFTREDUCE ){
    if( yyact>YY_MAX_SHIFT ){
      yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
    }
    yymsp -= yysize-1;
    yypParser->yytos = yymsp;
    yymsp->stateno = (YYACTIONTYPE)yyact;
    yymsp->major = (YYCODETYPE)yygoto;
    yyTraceShift(yypParser, yyact);
  }else{
    assert( yyact == YY_ACCEPT_ACTION );
    yypParser->yytos -= yysize;
    yy_accept(yypParser);
  }
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sFail!\n",yyTracePrompt);
  }
#endif
  while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
/************ Begin %parse_failure code ***************************************/
/************ End %parse_failure code *****************************************/
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  ParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
#line 25 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1447 "parser.c"
/************ End %syntax_error code ******************************************/
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sAccept!\n",yyTracePrompt);
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  assert( yypParser->yytos==yypParser->yystack );
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */
/*********** Begin %parse_accept code *****************************************/
/*********** End %parse_accept code *******************************************/
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "ParseAlloc" which describes the current state of the parser.
** The second argument is the major token number.  The third is
** the minor token.  The fourth optional argument is whatever the
** user wants (and specified in the grammar) and is available for
** use by the action routines.
**
** Inputs:
** <ul>
** <li> A pointer to the parser (an opaque structure.)
** <li> The major token number.
** <li> The minor token number.
** <li> An option argument of a grammar-specified type.
** </ul>
**
** Outputs:
** None.
*/
void RSQuery_Parse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  ParseTOKENTYPE yyminor       /* The value for the token */
  ParseARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  unsigned int yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  yypParser = (yyParser*)yyp;
  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput '%s'\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      yymajor = YYNOCODE;
    }else if( yyact <= YY_MAX_REDUCE ){
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE);
    }else{
      assert( yyact == YY_ERROR_ACTION );
      yyminorunion.yy0 = yyminor;
#ifdef YYERRORSYMBOL
      int yymx;
#endif
#ifndef NDEBUG
      if( yyTraceFILE ){
        fprintf(yyTraceFILE,"%sSyntax Error!\n",yyTracePrompt);
      }
#endif
#ifdef YYERRORSYMBOL
      /* A syntax error has occurred.
      ** The response to an error depends upon whether or not the
      ** grammar defines an error token "ERROR".  
      **
      ** This is what we do if the grammar does define ERROR:
      **
      **  * Call the %syntax_error function.
      **
      **  * Begin popping the stack until we enter a state where
      **    it is legal to shift the error symbol, then shift
      **    the error symbol.
      **
      **  * Set the error count to three.
      **
      **  * Begin accepting and shifting new tokens.  No new error
      **    processing will occur until three tokens have been
      **    shifted successfully.
      **
      */
      if( yypParser->yyerrcnt<0 ){
        yy_syntax_error(yypParser,yymajor,yyminor);
      }
      yymx = yypParser->yytos->major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor, &yyminorunion);
        yymajor = YYNOCODE;
      }else{
        while( yypParser->yytos >= yypParser->yystack
            && yymx != YYERRORSYMBOL
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yytos < yypParser->yystack || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
          yypParser->yyerrcnt = -1;
#endif
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          yy_shift(yypParser,yyact,YYERRORSYMBOL,yyminor);
        }
      }
      yypParser->yyerrcnt = 3;
      yyerrorhit = 1;
#elif defined(YYNOERRORRECOVERY)
      /* If the YYNOERRORRECOVERY macro is defined, then do not attempt to
      ** do any kind of error recovery.  Instead, simply invoke the syntax
      ** error routine and continue going as if nothing had happened.
      **
      ** Applications can set this macro (for example inside %include) if
      ** they intend to abandon the parse upon the first syntax error seen.
      */
      yy_syntax_error(yypParser,yymajor, yyminor);
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      yymajor = YYNOCODE;
      
#else  /* YYERRORSYMBOL is not defined */
      /* This is what we do if the grammar does not define ERROR:
      **
      **  * Report an error message, and throw away the input token.
      **
      **  * If the input token is $, then fail the parse.
      **
      ** As before, subsequent error messages are suppressed until
      ** three input tokens have been successfully shifted.
      */
      if( yypParser->yyerrcnt<=0 ){
        yy_syntax_error(yypParser,yymajor, yyminor);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
        yypParser->yyerrcnt = -1;
#endif
      }
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yytos>yypParser->yystack );
#ifndef NDEBUG
  if( yyTraceFILE ){
    yyStackEntry *i;
    char cDiv = '[';
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i=&yypParser->yystack[1]; i<=yypParser->yytos; i++){
      fprintf(yyTraceFILE,"%c%s", cDiv, yyTokenName[i->major]);
      cDiv = ' ';
    }
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}