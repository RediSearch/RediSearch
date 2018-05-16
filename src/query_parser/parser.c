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
#line 36 "parser.y"
   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../util/arr.h"
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
   
#line 80 "parser.c"
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
#define YYNOCODE 43
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  QueryNode * yy35;
  NumericFilter * yy36;
  QueryAttribute yy55;
  GeoFilter * yy64;
  QueryAttribute * yy69;
  Vector* yy78;
  RangeNumber yy83;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  QueryParseCtx *ctx ;
#define ParseARG_PDECL , QueryParseCtx *ctx 
#define ParseARG_FETCH  QueryParseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             49
#define YYNRULE              49
#define YY_MAX_SHIFT         48
#define YY_MIN_SHIFTREDUCE   79
#define YY_MAX_SHIFTREDUCE   127
#define YY_MIN_REDUCE        128
#define YY_MAX_REDUCE        176
#define YY_ERROR_ACTION      177
#define YY_ACCEPT_ACTION     178
#define YY_NO_ACTION         179
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
#define YY_ACTTAB_COUNT (242)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     5,    9,   19,   48,    6,  127,  100,   48,  126,  106,
 /*    10 */    46,   22,    7,   88,  108,  128,    9,    5,   48,   19,
 /*    20 */    29,    6,  127,  100,   47,  126,  106,   46,   44,    7,
 /*    30 */   129,  108,    5,    9,   19,   48,    6,  127,  100,   18,
 /*    40 */   126,  106,   46,  102,    7,  118,  108,   23,  123,  127,
 /*    50 */   125,   42,  126,  106,    5,   24,   19,  124,    6,  127,
 /*    60 */   100,   21,  126,  106,   46,   25,    7,  120,  108,  113,
 /*    70 */    27,  127,  103,  101,  126,   17,  109,   26,  127,   31,
 /*    80 */    19,  126,    6,  127,  100,  110,  126,  106,   46,   89,
 /*    90 */     7,   37,  108,   94,    9,   13,   48,  126,   98,   36,
 /*   100 */    83,   38,  121,   40,    8,  119,   91,   41,   34,  130,
 /*   110 */     5,  122,   19,   43,    6,  127,  100,   33,  126,  106,
 /*   120 */    46,   45,    7,    1,  108,   20,  127,  100,   35,  126,
 /*   130 */   106,   46,  107,    7,  130,  108,   30,    9,  130,   48,
 /*   140 */   130,    3,   23,  123,   98,   36,   83,  116,   28,   40,
 /*   150 */    24,  130,  178,   41,   34,   14,  130,   32,   98,   36,
 /*   160 */    83,   90,   39,   40,  130,  130,    4,   41,   34,   98,
 /*   170 */    36,   83,  130,  130,   40,  130,  130,  130,   41,   34,
 /*   180 */   130,   11,  130,  130,   98,   36,   83,    2,  130,   40,
 /*   190 */    98,   36,   83,   41,   34,   40,  130,  130,   12,   41,
 /*   200 */    34,   98,   36,   83,  130,  130,   40,  130,  130,  130,
 /*   210 */    41,   34,  130,   15,  130,  130,   98,   36,   83,   16,
 /*   220 */   130,   40,   98,   36,   83,   41,   34,   40,  130,  130,
 /*   230 */    95,   41,   34,  127,  103,   96,  126,  130,  127,  130,
 /*   240 */   130,  126,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     2,   18,    4,   20,    6,    7,    8,   20,   10,   11,
 /*    10 */    12,   32,   14,   15,   16,    0,   18,    2,   20,    4,
 /*    20 */    41,    6,    7,    8,   13,   10,   11,   12,   37,   14,
 /*    30 */     0,   16,    2,   18,    4,   20,    6,    7,    8,   18,
 /*    40 */    10,   11,   12,   41,   14,   24,   16,    6,    7,    7,
 /*    50 */    37,   21,   10,   11,    2,   14,    4,   37,    6,    7,
 /*    60 */     8,   37,   10,   11,   12,   37,   14,   26,   16,   31,
 /*    70 */    32,    7,    8,   41,   10,   23,   41,   25,    7,   41,
 /*    80 */     4,   10,    6,    7,    8,   41,   10,   11,   12,   41,
 /*    90 */    14,   22,   16,   24,   18,   28,   20,   10,   31,   32,
 /*   100 */    33,   34,   35,   36,    5,   38,   29,   40,   41,    0,
 /*   110 */     2,   26,    4,   10,    6,    7,    8,   18,   10,   11,
 /*   120 */    12,   10,   14,    5,   16,   23,    7,    8,    5,   10,
 /*   130 */    11,   12,   12,   14,   42,   16,   18,   18,   42,   20,
 /*   140 */    42,   28,    6,    7,   31,   32,   33,   31,   32,   36,
 /*   150 */    14,   42,   39,   40,   41,   28,   42,   41,   31,   32,
 /*   160 */    33,   29,   30,   36,   42,   42,   28,   40,   41,   31,
 /*   170 */    32,   33,   42,   42,   36,   42,   42,   42,   40,   41,
 /*   180 */    42,   28,   42,   42,   31,   32,   33,   28,   42,   36,
 /*   190 */    31,   32,   33,   40,   41,   36,   42,   42,   28,   40,
 /*   200 */    41,   31,   32,   33,   42,   42,   36,   42,   42,   42,
 /*   210 */    40,   41,   42,   28,   42,   42,   31,   32,   33,   28,
 /*   220 */    42,   36,   31,   32,   33,   40,   41,   36,   42,   42,
 /*   230 */     4,   40,   41,    7,    8,    4,   10,   42,    7,   42,
 /*   240 */    42,   10,
};
#define YY_SHIFT_USE_DFLT (242)
#define YY_SHIFT_COUNT    (48)
#define YY_SHIFT_MIN      (-17)
#define YY_SHIFT_MAX      (231)
static const short yy_shift_ofst[] = {
 /*     0 */    30,   52,   -2,   15,   76,  108,  108,  108,  108,  108,
 /*    10 */   108,  119,  -17,  -17,  -17,  -13,  -13,   42,   42,   71,
 /*    20 */    11,   41,  226,  136,  136,  136,  136,   64,   64,  231,
 /*    30 */    71,   71,   71,   71,   71,   71,   87,   11,   21,   69,
 /*    40 */    99,  118,  109,   85,  103,  120,  111,  123,  102,
};
#define YY_REDUCE_USE_DFLT (-22)
#define YY_REDUCE_COUNT (37)
#define YY_REDUCE_MIN   (-21)
#define YY_REDUCE_MAX   (191)
static const short yy_reduce_ofst[] = {
 /*     0 */   113,   67,  127,  127,  127,  138,  153,  159,  170,  185,
 /*    10 */   191,  127,  127,  127,  127,  127,  127,   38,  116,  -21,
 /*    20 */   132,   -9,    2,   13,   20,   24,   28,    2,    2,   32,
 /*    30 */    35,   32,   32,   44,   32,   48,    2,   77,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   177,  177,  177,  177,  154,  177,  177,  177,  177,  177,
 /*    10 */   177,  153,  136,  135,  131,  133,  134,  177,  177,  177,
 /*    20 */   142,  177,  177,  177,  177,  177,  177,  163,  166,  177,
 /*    30 */   177,  161,  164,  177,  146,  177,  148,  141,  160,  177,
 /*    40 */   177,  177,  177,  177,  177,  177,  177,  177,  177,
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
  "PERCENT",       "ATTRIBUTE",     "LP",            "RP",          
  "MODIFIER",      "AND",           "OR",            "ORX",         
  "ARROW",         "STAR",          "SEMICOLON",     "LB",          
  "RB",            "LSQB",          "RSQB",          "error",       
  "expr",          "attribute",     "attribute_list",  "prefix",      
  "termlist",      "union",         "tag_list",      "geo_filter",  
  "modifierlist",  "num",           "numeric_range",  "query",       
  "modifier",      "term",        
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "query ::= expr",
 /*   1 */ "query ::=",
 /*   2 */ "query ::= STAR",
 /*   3 */ "expr ::= expr expr",
 /*   4 */ "expr ::= union",
 /*   5 */ "union ::= expr OR expr",
 /*   6 */ "union ::= union OR expr",
 /*   7 */ "expr ::= modifier COLON expr",
 /*   8 */ "expr ::= modifierlist COLON expr",
 /*   9 */ "expr ::= LP expr RP",
 /*  10 */ "attribute ::= ATTRIBUTE COLON term",
 /*  11 */ "attribute_list ::= attribute",
 /*  12 */ "attribute_list ::= attribute_list SEMICOLON attribute",
 /*  13 */ "attribute_list ::= attribute_list SEMICOLON",
 /*  14 */ "attribute_list ::=",
 /*  15 */ "expr ::= expr ARROW LB attribute_list RB",
 /*  16 */ "expr ::= QUOTE termlist QUOTE",
 /*  17 */ "expr ::= QUOTE term QUOTE",
 /*  18 */ "expr ::= term",
 /*  19 */ "expr ::= prefix",
 /*  20 */ "expr ::= termlist",
 /*  21 */ "expr ::= STOPWORD",
 /*  22 */ "termlist ::= term term",
 /*  23 */ "termlist ::= termlist term",
 /*  24 */ "termlist ::= termlist STOPWORD",
 /*  25 */ "expr ::= MINUS expr",
 /*  26 */ "expr ::= TILDE expr",
 /*  27 */ "prefix ::= PREFIX",
 /*  28 */ "expr ::= PERCENT TERM PERCENT",
 /*  29 */ "modifier ::= MODIFIER",
 /*  30 */ "modifierlist ::= modifier OR term",
 /*  31 */ "modifierlist ::= modifierlist OR term",
 /*  32 */ "expr ::= modifier COLON tag_list",
 /*  33 */ "tag_list ::= LB term",
 /*  34 */ "tag_list ::= LB prefix",
 /*  35 */ "tag_list ::= LB termlist",
 /*  36 */ "tag_list ::= tag_list OR term",
 /*  37 */ "tag_list ::= tag_list OR prefix",
 /*  38 */ "tag_list ::= tag_list OR termlist",
 /*  39 */ "tag_list ::= tag_list RB",
 /*  40 */ "expr ::= modifier COLON numeric_range",
 /*  41 */ "numeric_range ::= LSQB num num RSQB",
 /*  42 */ "expr ::= modifier COLON geo_filter",
 /*  43 */ "geo_filter ::= LSQB num num num TERM RSQB",
 /*  44 */ "num ::= NUMBER",
 /*  45 */ "num ::= LP num",
 /*  46 */ "num ::= MINUS num",
 /*  47 */ "term ::= TERM",
 /*  48 */ "term ::= NUMBER",
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
    case 27: /* error */
    case 37: /* num */
    case 39: /* query */
    case 40: /* modifier */
    case 41: /* term */
{
#line 91 "parser.y"
 
#line 621 "parser.c"
}
      break;
    case 28: /* expr */
    case 31: /* prefix */
    case 32: /* termlist */
    case 33: /* union */
    case 34: /* tag_list */
{
#line 94 "parser.y"
 QueryNode_Free((yypminor->yy35)); 
#line 632 "parser.c"
}
      break;
    case 29: /* attribute */
{
#line 97 "parser.y"
 free((char*)(yypminor->yy55).value); 
#line 639 "parser.c"
}
      break;
    case 30: /* attribute_list */
{
#line 100 "parser.y"
 array_free_ex((yypminor->yy69), free((char*)((QueryAttribute*)ptr )->value)); 
#line 646 "parser.c"
}
      break;
    case 35: /* geo_filter */
{
#line 116 "parser.y"
 GeoFilter_Free((yypminor->yy64)); 
#line 653 "parser.c"
}
      break;
    case 36: /* modifierlist */
{
#line 119 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy78)); i++) {
        char *s;
        Vector_Get((yypminor->yy78), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy78)); 

#line 667 "parser.c"
}
      break;
    case 38: /* numeric_range */
{
#line 131 "parser.y"

    NumericFilter_Free((yypminor->yy36));

#line 676 "parser.c"
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
  { 39, 1 },
  { 39, 0 },
  { 39, 1 },
  { 28, 2 },
  { 28, 1 },
  { 33, 3 },
  { 33, 3 },
  { 28, 3 },
  { 28, 3 },
  { 28, 3 },
  { 29, 3 },
  { 30, 1 },
  { 30, 3 },
  { 30, 2 },
  { 30, 0 },
  { 28, 5 },
  { 28, 3 },
  { 28, 3 },
  { 28, 1 },
  { 28, 1 },
  { 28, 1 },
  { 28, 1 },
  { 32, 2 },
  { 32, 2 },
  { 32, 2 },
  { 28, 2 },
  { 28, 2 },
  { 31, 1 },
  { 28, 3 },
  { 40, 1 },
  { 36, 3 },
  { 36, 3 },
  { 28, 3 },
  { 34, 2 },
  { 34, 2 },
  { 34, 2 },
  { 34, 3 },
  { 34, 3 },
  { 34, 3 },
  { 34, 2 },
  { 28, 3 },
  { 38, 4 },
  { 28, 3 },
  { 35, 6 },
  { 37, 1 },
  { 37, 2 },
  { 37, 2 },
  { 41, 1 },
  { 41, 1 },
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
#line 135 "parser.y"
{ 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = yymsp[0].minor.yy35;
 
}
#line 1048 "parser.c"
        break;
      case 1: /* query ::= */
#line 141 "parser.y"
{
    ctx->root = NULL;
}
#line 1055 "parser.c"
        break;
      case 2: /* query ::= STAR */
#line 145 "parser.y"
{
    ctx->root = NewWildcardNode();
}
#line 1062 "parser.c"
        break;
      case 3: /* expr ::= expr expr */
#line 153 "parser.y"
{

    // if both yymsp[-1].minor.yy35 and yymsp[0].minor.yy35 are null we return null
    if (yymsp[-1].minor.yy35 == NULL && yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else {

        if (yymsp[-1].minor.yy35 && yymsp[-1].minor.yy35->type == QN_PHRASE && yymsp[-1].minor.yy35->pn.exact == 0 && 
            yymsp[-1].minor.yy35->opts.fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy35 = yymsp[-1].minor.yy35;
        } else {     
            yylhsminor.yy35 = NewPhraseNode(0);
            QueryPhraseNode_AddChild(yylhsminor.yy35, yymsp[-1].minor.yy35);
        }
        QueryPhraseNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35);
    }
}
#line 1083 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 4: /* expr ::= union */
#line 176 "parser.y"
{
    yylhsminor.yy35 = yymsp[0].minor.yy35;
}
#line 1091 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 5: /* union ::= expr OR expr */
#line 180 "parser.y"
{
    if (yymsp[-2].minor.yy35 == NULL && yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else if (yymsp[-2].minor.yy35 && yymsp[-2].minor.yy35->type == QN_UNION && yymsp[-2].minor.yy35->opts.fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy35 = yymsp[-2].minor.yy35;
    } else {
        yylhsminor.yy35 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[-2].minor.yy35);
        if (yymsp[-2].minor.yy35) 
         yylhsminor.yy35->opts.fieldMask |= yymsp[-2].minor.yy35->opts.fieldMask;

    } 
    if (yymsp[0].minor.yy35) {

        QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35);
        yylhsminor.yy35->opts.fieldMask |= yymsp[0].minor.yy35->opts.fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy35, yylhsminor.yy35->opts.fieldMask);
    }
    
}
#line 1116 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 6: /* union ::= union OR expr */
#line 201 "parser.y"
{
    
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
    QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35); 
    yylhsminor.yy35->opts.fieldMask |= yymsp[0].minor.yy35->opts.fieldMask;
    QueryNode_SetFieldMask(yymsp[0].minor.yy35, yylhsminor.yy35->opts.fieldMask);


}
#line 1130 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 7: /* expr ::= modifier COLON expr */
#line 215 "parser.y"
{
    if (yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy35, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy35 = yymsp[0].minor.yy35; 
    }
}
#line 1145 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
#line 227 "parser.y"
{
    
    if (yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else {
        //yymsp[0].minor.yy35->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy78); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy78, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy35, mask);
        Vector_Free(yymsp[-2].minor.yy78);
        yylhsminor.yy35=yymsp[0].minor.yy35;
    }
}
#line 1170 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 9: /* expr ::= LP expr RP */
#line 248 "parser.y"
{
    yymsp[-2].minor.yy35 = yymsp[-1].minor.yy35;
}
#line 1178 "parser.c"
        break;
      case 10: /* attribute ::= ATTRIBUTE COLON term */
#line 256 "parser.y"
{
    
    yylhsminor.yy55 = (QueryAttribute){ .name = yymsp[-2].minor.yy0.s, .namelen = yymsp[-2].minor.yy0.len, .value = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), .vallen = yymsp[0].minor.yy0.len };
}
#line 1186 "parser.c"
  yymsp[-2].minor.yy55 = yylhsminor.yy55;
        break;
      case 11: /* attribute_list ::= attribute */
#line 261 "parser.y"
{
    yylhsminor.yy69 = array_new(QueryAttribute, 2);
    yylhsminor.yy69 = array_append(yylhsminor.yy69, yymsp[0].minor.yy55);
}
#line 1195 "parser.c"
  yymsp[0].minor.yy69 = yylhsminor.yy69;
        break;
      case 12: /* attribute_list ::= attribute_list SEMICOLON attribute */
#line 266 "parser.y"
{
    yylhsminor.yy69 = array_append(yymsp[-2].minor.yy69, yymsp[0].minor.yy55);
}
#line 1203 "parser.c"
  yymsp[-2].minor.yy69 = yylhsminor.yy69;
        break;
      case 13: /* attribute_list ::= attribute_list SEMICOLON */
#line 270 "parser.y"
{
    yylhsminor.yy69 = yymsp[-1].minor.yy69;
}
#line 1211 "parser.c"
  yymsp[-1].minor.yy69 = yylhsminor.yy69;
        break;
      case 14: /* attribute_list ::= */
#line 274 "parser.y"
{
    yymsp[1].minor.yy69 = NULL;
}
#line 1219 "parser.c"
        break;
      case 15: /* expr ::= expr ARROW LB attribute_list RB */
#line 278 "parser.y"
{

    if (yymsp[-1].minor.yy69) {
        char *err = NULL;
        if (!QueryNode_ApplyAttributes(yymsp[-4].minor.yy35, yymsp[-1].minor.yy69, array_len(yymsp[-1].minor.yy69), &err)) {
            ctx->ok = 0;
            ctx->errorMsg = err;
        }
    }
    array_free_ex(yymsp[-1].minor.yy69, free((char*)((QueryAttribute*)ptr )->value));
    yylhsminor.yy35 = yymsp[-4].minor.yy35;
}
#line 1235 "parser.c"
  yymsp[-4].minor.yy35 = yylhsminor.yy35;
        break;
      case 16: /* expr ::= QUOTE termlist QUOTE */
#line 295 "parser.y"
{
    yymsp[-1].minor.yy35->pn.exact =1;
    yymsp[-1].minor.yy35->opts.flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy35 = yymsp[-1].minor.yy35;
}
#line 1246 "parser.c"
        break;
      case 17: /* expr ::= QUOTE term QUOTE */
#line 302 "parser.y"
{
    yymsp[-2].minor.yy35 = NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
    yymsp[-2].minor.yy35->opts.flags |= QueryNode_Verbatim;
    
}
#line 1255 "parser.c"
        break;
      case 18: /* expr ::= term */
#line 308 "parser.y"
{
   yylhsminor.yy35 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
#line 1262 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 19: /* expr ::= prefix */
#line 312 "parser.y"
{
    yylhsminor.yy35= yymsp[0].minor.yy35;
}
#line 1270 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 20: /* expr ::= termlist */
#line 316 "parser.y"
{
        yylhsminor.yy35 = yymsp[0].minor.yy35;
}
#line 1278 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 21: /* expr ::= STOPWORD */
#line 320 "parser.y"
{
    yymsp[0].minor.yy35 = NULL;
}
#line 1286 "parser.c"
        break;
      case 22: /* termlist ::= term term */
#line 324 "parser.y"
{
    yylhsminor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1));
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1295 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 23: /* termlist ::= termlist term */
#line 330 "parser.y"
{
    yylhsminor.yy35 = yymsp[-1].minor.yy35;
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1304 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 24: /* termlist ::= termlist STOPWORD */
      case 39: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==39);
#line 335 "parser.y"
{
    yylhsminor.yy35 = yymsp[-1].minor.yy35;
}
#line 1313 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 25: /* expr ::= MINUS expr */
#line 343 "parser.y"
{ 
    yymsp[-1].minor.yy35 = NewNotNode(yymsp[0].minor.yy35);
}
#line 1321 "parser.c"
        break;
      case 26: /* expr ::= TILDE expr */
#line 351 "parser.y"
{ 
    yymsp[-1].minor.yy35 = NewOptionalNode(yymsp[0].minor.yy35);
}
#line 1328 "parser.c"
        break;
      case 27: /* prefix ::= PREFIX */
#line 359 "parser.y"
{
    yymsp[0].minor.yy0.s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy35 = NewPrefixNode(ctx, yymsp[0].minor.yy0.s, strlen(yymsp[0].minor.yy0.s));
}
#line 1336 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 28: /* expr ::= PERCENT TERM PERCENT */
#line 368 "parser.y"
{
    yymsp[-1].minor.yy0.s = strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
    yymsp[-2].minor.yy35 = NewFuzzyNode(ctx, yymsp[-1].minor.yy0.s, strlen(yymsp[-1].minor.yy0.s), 2);
}
#line 1345 "parser.c"
        break;
      case 29: /* modifier ::= MODIFIER */
#line 378 "parser.y"
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1353 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 30: /* modifierlist ::= modifier OR term */
#line 383 "parser.y"
{
    yylhsminor.yy78 = NewVector(char *, 2);
    char *s = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy78, s);
    s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy78, s);
}
#line 1365 "parser.c"
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 31: /* modifierlist ::= modifierlist OR term */
#line 391 "parser.y"
{
    char *s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy78, s);
    yylhsminor.yy78 = yymsp[-2].minor.yy78;
}
#line 1375 "parser.c"
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 32: /* expr ::= modifier COLON tag_list */
#line 401 "parser.y"
{
    if (!yymsp[0].minor.yy35) {
        yylhsminor.yy35= NULL;
    } else {
        // Tag field names must be case sensitive, we we can't do strdupcase
        char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        size_t slen = unescapen((char*)s, yymsp[-2].minor.yy0.len);

        yylhsminor.yy35 = NewTagNode(s, slen);
        QueryTagNode_AddChildren(yylhsminor.yy35, yymsp[0].minor.yy35->pn.children, yymsp[0].minor.yy35->pn.numChildren);
        
        // Set the children count on yymsp[0].minor.yy35 to 0 so they won't get recursively free'd
        yymsp[0].minor.yy35->pn.numChildren = 0;
        QueryNode_Free(yymsp[0].minor.yy35);
    }
}
#line 1396 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 33: /* tag_list ::= LB term */
#line 418 "parser.y"
{
    yymsp[-1].minor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1405 "parser.c"
        break;
      case 34: /* tag_list ::= LB prefix */
      case 35: /* tag_list ::= LB termlist */ yytestcase(yyruleno==35);
#line 423 "parser.y"
{
    yymsp[-1].minor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy35, yymsp[0].minor.yy35);
}
#line 1414 "parser.c"
        break;
      case 36: /* tag_list ::= tag_list OR term */
#line 433 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
}
#line 1422 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 37: /* tag_list ::= tag_list OR prefix */
      case 38: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==38);
#line 438 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy35, yymsp[0].minor.yy35);
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
}
#line 1432 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 40: /* expr ::= modifier COLON numeric_range */
#line 457 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy36->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy35 = NewNumericNode(yymsp[0].minor.yy36);
}
#line 1442 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 41: /* numeric_range ::= LSQB num num RSQB */
#line 463 "parser.y"
{
    yymsp[-3].minor.yy36 = NewNumericFilter(yymsp[-2].minor.yy83.num, yymsp[-1].minor.yy83.num, yymsp[-2].minor.yy83.inclusive, yymsp[-1].minor.yy83.inclusive);
}
#line 1450 "parser.c"
        break;
      case 42: /* expr ::= modifier COLON geo_filter */
#line 471 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy64->property = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy35 = NewGeofilterNode(yymsp[0].minor.yy64);
}
#line 1459 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 43: /* geo_filter ::= LSQB num num num TERM RSQB */
#line 477 "parser.y"
{
    yymsp[-5].minor.yy64 = NewGeoFilter(yymsp[-4].minor.yy83.num, yymsp[-3].minor.yy83.num, yymsp[-2].minor.yy83.num, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(yymsp[-5].minor.yy64, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}
#line 1472 "parser.c"
        break;
      case 44: /* num ::= NUMBER */
#line 492 "parser.y"
{
    yylhsminor.yy83.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy83.inclusive = 1;
}
#line 1480 "parser.c"
  yymsp[0].minor.yy83 = yylhsminor.yy83;
        break;
      case 45: /* num ::= LP num */
#line 497 "parser.y"
{
    yymsp[-1].minor.yy83=yymsp[0].minor.yy83;
    yymsp[-1].minor.yy83.inclusive = 0;
}
#line 1489 "parser.c"
        break;
      case 46: /* num ::= MINUS num */
#line 502 "parser.y"
{
    yymsp[0].minor.yy83.num = -yymsp[0].minor.yy83.num;
    yymsp[-1].minor.yy83 = yymsp[0].minor.yy83;
}
#line 1497 "parser.c"
        break;
      case 47: /* term ::= TERM */
      case 48: /* term ::= NUMBER */ yytestcase(yyruleno==48);
#line 507 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1505 "parser.c"
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
#line 26 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1574 "parser.c"
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