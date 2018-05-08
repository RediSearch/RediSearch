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
#define YYNOCODE 42
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  Vector* yy10;
  QueryAttribute yy17;
  QueryNode * yy37;
  NumericFilter * yy40;
  QueryAttribute * yy61;
  RangeNumber yy62;
  GeoFilter * yy68;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  QueryParseCtx *ctx ;
#define ParseARG_PDECL , QueryParseCtx *ctx 
#define ParseARG_FETCH  QueryParseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             47
#define YYNRULE              48
#define YY_MAX_SHIFT         46
#define YY_MIN_SHIFTREDUCE   76
#define YY_MAX_SHIFTREDUCE   123
#define YY_MIN_REDUCE        124
#define YY_MAX_REDUCE        171
#define YY_ERROR_ACTION      172
#define YY_ACCEPT_ACTION     173
#define YY_NO_ACTION         174
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
#define YY_ACTTAB_COUNT (216)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     5,    9,   19,   46,    6,  123,   97,  123,  122,  103,
 /*    10 */   122,    7,   85,  104,  124,    9,    5,   46,   19,   45,
 /*    20 */     6,  123,   97,   46,  122,  103,   44,    7,  125,  104,
 /*    30 */     5,    9,   19,   46,    6,  123,   97,   99,  122,  103,
 /*    40 */    37,    7,   91,  104,  123,  100,   92,  122,   42,  123,
 /*    50 */   100,    5,  122,   19,  121,    6,  123,   97,  120,  122,
 /*    60 */   103,   21,    7,  123,  104,   22,  122,  103,   25,   23,
 /*    70 */   119,   17,   93,   26,   29,  123,   24,  122,  122,   13,
 /*    80 */   109,   27,   95,   36,   80,   38,  117,   40,  116,  115,
 /*    90 */    31,   41,   34,   18,   19,   98,    6,  123,   97,  114,
 /*   100 */   122,  103,  105,    7,    8,  104,    5,    9,   19,   46,
 /*   110 */     6,  123,   97,  106,  122,  103,   33,    7,    1,  104,
 /*   120 */     3,   86,   88,   95,   36,   80,   87,   39,   40,   43,
 /*   130 */    30,  173,   41,   34,  126,  118,  123,   97,   35,  122,
 /*   140 */   103,  126,    7,   20,  104,  126,    9,   14,   46,  126,
 /*   150 */    95,   36,   80,    4,  126,   40,   95,   36,   80,   41,
 /*   160 */    34,   40,  126,  126,   11,   41,   34,   95,   36,   80,
 /*   170 */     2,  126,   40,   95,   36,   80,   41,   34,   40,  126,
 /*   180 */   126,   12,   41,   34,   95,   36,   80,   15,  126,   40,
 /*   190 */    95,   36,   80,   41,   34,   40,  126,  126,   16,   41,
 /*   200 */    34,   95,   36,   80,  112,   28,   40,  126,   23,  119,
 /*   210 */    41,   34,  126,  126,   32,   24,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     2,   17,    4,   19,    6,    7,    8,    7,   10,   11,
 /*    10 */    10,   13,   14,   15,    0,   17,    2,   19,    4,   12,
 /*    20 */     6,    7,    8,   19,   10,   11,   36,   13,    0,   15,
 /*    30 */     2,   17,    4,   19,    6,    7,    8,   40,   10,   11,
 /*    40 */    21,   13,   23,   15,    7,    8,    4,   10,   20,    7,
 /*    50 */     8,    2,   10,    4,   36,    6,    7,    8,   36,   10,
 /*    60 */    11,   36,   13,    7,   15,   31,   10,   11,   36,    6,
 /*    70 */     7,   22,    4,   24,   40,    7,   13,   10,   10,   27,
 /*    80 */    30,   31,   30,   31,   32,   33,   34,   35,   25,   37,
 /*    90 */    40,   39,   40,   17,    4,   40,    6,    7,    8,   23,
 /*   100 */    10,   11,   40,   13,    5,   15,    2,   17,    4,   19,
 /*   110 */     6,    7,    8,   40,   10,   11,   17,   13,    5,   15,
 /*   120 */    27,   40,   28,   30,   31,   32,   28,   29,   35,   10,
 /*   130 */    17,   38,   39,   40,    0,   25,    7,    8,    5,   10,
 /*   140 */    11,   41,   13,   22,   15,   41,   17,   27,   19,   41,
 /*   150 */    30,   31,   32,   27,   41,   35,   30,   31,   32,   39,
 /*   160 */    40,   35,   41,   41,   27,   39,   40,   30,   31,   32,
 /*   170 */    27,   41,   35,   30,   31,   32,   39,   40,   35,   41,
 /*   180 */    41,   27,   39,   40,   30,   31,   32,   27,   41,   35,
 /*   190 */    30,   31,   32,   39,   40,   35,   41,   41,   27,   39,
 /*   200 */    40,   30,   31,   32,   30,   31,   35,   41,    6,    7,
 /*   210 */    39,   40,   41,   41,   40,   13,
};
#define YY_SHIFT_USE_DFLT (216)
#define YY_SHIFT_COUNT    (46)
#define YY_SHIFT_MIN      (-16)
#define YY_SHIFT_MAX      (202)
static const short yy_shift_ofst[] = {
 /*     0 */    28,   49,   -2,   14,   90,  104,  104,  104,  104,  104,
 /*    10 */   104,  129,  -16,  -16,  -16,    4,    4,   56,   56,    0,
 /*    20 */     7,   63,   42,  202,  202,  202,  202,   37,   37,   68,
 /*    30 */     0,    0,    0,    0,    0,    0,   67,    7,   76,   19,
 /*    40 */    99,  113,  134,  110,  119,  133,  121,
};
#define YY_REDUCE_USE_DFLT (-11)
#define YY_REDUCE_COUNT (37)
#define YY_REDUCE_MIN   (-10)
#define YY_REDUCE_MAX   (174)
static const short yy_reduce_ofst[] = {
 /*     0 */    93,   52,  120,  120,  120,  126,  137,  143,  154,  160,
 /*    10 */   171,  120,  120,  120,  120,  120,  120,   50,  174,   34,
 /*    20 */    98,  -10,   -3,   18,   22,   25,   32,   -3,   -3,   55,
 /*    30 */    62,   55,   55,   73,   55,   81,   -3,   94,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   172,  172,  172,  172,  150,  172,  172,  172,  172,  172,
 /*    10 */   172,  149,  132,  131,  127,  129,  130,  172,  172,  172,
 /*    20 */   138,  172,  172,  172,  172,  172,  172,  158,  161,  172,
 /*    30 */   172,  156,  159,  172,  142,  172,  144,  137,  155,  172,
 /*    40 */   172,  172,  172,  172,  172,  172,  172,
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
  "ATTRIBUTE",     "LP",            "RP",            "MODIFIER",    
  "AND",           "OR",            "ORX",           "ARROW",       
  "STAR",          "SEMICOLON",     "LB",            "RB",          
  "LSQB",          "RSQB",          "error",         "expr",        
  "attribute",     "attribute_list",  "prefix",        "termlist",    
  "union",         "tag_list",      "geo_filter",    "modifierlist",
  "num",           "numeric_range",  "query",         "modifier",    
  "term",        
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
 /*  28 */ "modifier ::= MODIFIER",
 /*  29 */ "modifierlist ::= modifier OR term",
 /*  30 */ "modifierlist ::= modifierlist OR term",
 /*  31 */ "expr ::= modifier COLON tag_list",
 /*  32 */ "tag_list ::= LB term",
 /*  33 */ "tag_list ::= LB prefix",
 /*  34 */ "tag_list ::= LB termlist",
 /*  35 */ "tag_list ::= tag_list OR term",
 /*  36 */ "tag_list ::= tag_list OR prefix",
 /*  37 */ "tag_list ::= tag_list OR termlist",
 /*  38 */ "tag_list ::= tag_list RB",
 /*  39 */ "expr ::= modifier COLON numeric_range",
 /*  40 */ "numeric_range ::= LSQB num num RSQB",
 /*  41 */ "expr ::= modifier COLON geo_filter",
 /*  42 */ "geo_filter ::= LSQB num num num TERM RSQB",
 /*  43 */ "num ::= NUMBER",
 /*  44 */ "num ::= LP num",
 /*  45 */ "num ::= MINUS num",
 /*  46 */ "term ::= TERM",
 /*  47 */ "term ::= NUMBER",
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
    case 26: /* error */
    case 36: /* num */
    case 38: /* query */
    case 39: /* modifier */
    case 40: /* term */
{
#line 90 "parser.y"
 
#line 614 "parser.c"
}
      break;
    case 27: /* expr */
    case 30: /* prefix */
    case 31: /* termlist */
    case 32: /* union */
    case 33: /* tag_list */
{
#line 93 "parser.y"
 QueryNode_Free((yypminor->yy37)); 
#line 625 "parser.c"
}
      break;
    case 28: /* attribute */
{
#line 96 "parser.y"
 free((char*)(yypminor->yy17).value); 
#line 632 "parser.c"
}
      break;
    case 29: /* attribute_list */
{
#line 99 "parser.y"
 array_free_ex((yypminor->yy61), free((char*)((QueryAttribute*)ptr )->value)); 
#line 639 "parser.c"
}
      break;
    case 34: /* geo_filter */
{
#line 115 "parser.y"
 GeoFilter_Free((yypminor->yy68)); 
#line 646 "parser.c"
}
      break;
    case 35: /* modifierlist */
{
#line 118 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy10)); i++) {
        char *s;
        Vector_Get((yypminor->yy10), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy10)); 

#line 660 "parser.c"
}
      break;
    case 37: /* numeric_range */
{
#line 130 "parser.y"

    NumericFilter_Free((yypminor->yy40));

#line 669 "parser.c"
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
  { 38, 1 },
  { 38, 0 },
  { 38, 1 },
  { 27, 2 },
  { 27, 1 },
  { 32, 3 },
  { 32, 3 },
  { 27, 3 },
  { 27, 3 },
  { 27, 3 },
  { 28, 3 },
  { 29, 1 },
  { 29, 3 },
  { 29, 2 },
  { 29, 0 },
  { 27, 5 },
  { 27, 3 },
  { 27, 3 },
  { 27, 1 },
  { 27, 1 },
  { 27, 1 },
  { 27, 1 },
  { 31, 2 },
  { 31, 2 },
  { 31, 2 },
  { 27, 2 },
  { 27, 2 },
  { 30, 1 },
  { 39, 1 },
  { 35, 3 },
  { 35, 3 },
  { 27, 3 },
  { 33, 2 },
  { 33, 2 },
  { 33, 2 },
  { 33, 3 },
  { 33, 3 },
  { 33, 3 },
  { 33, 2 },
  { 27, 3 },
  { 37, 4 },
  { 27, 3 },
  { 34, 6 },
  { 36, 1 },
  { 36, 2 },
  { 36, 2 },
  { 40, 1 },
  { 40, 1 },
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
#line 134 "parser.y"
{ 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = yymsp[0].minor.yy37;
 
}
#line 1040 "parser.c"
        break;
      case 1: /* query ::= */
#line 140 "parser.y"
{
    ctx->root = NULL;
}
#line 1047 "parser.c"
        break;
      case 2: /* query ::= STAR */
#line 144 "parser.y"
{
    ctx->root = NewWildcardNode();
}
#line 1054 "parser.c"
        break;
      case 3: /* expr ::= expr expr */
#line 152 "parser.y"
{

    // if both yymsp[-1].minor.yy37 and yymsp[0].minor.yy37 are null we return null
    if (yymsp[-1].minor.yy37 == NULL && yymsp[0].minor.yy37 == NULL) {
        yylhsminor.yy37 = NULL;
    } else {

        if (yymsp[-1].minor.yy37 && yymsp[-1].minor.yy37->type == QN_PHRASE && yymsp[-1].minor.yy37->pn.exact == 0 && 
            yymsp[-1].minor.yy37->opts.fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy37 = yymsp[-1].minor.yy37;
        } else {     
            yylhsminor.yy37 = NewPhraseNode(0);
            QueryPhraseNode_AddChild(yylhsminor.yy37, yymsp[-1].minor.yy37);
        }
        QueryPhraseNode_AddChild(yylhsminor.yy37, yymsp[0].minor.yy37);
    }
}
#line 1075 "parser.c"
  yymsp[-1].minor.yy37 = yylhsminor.yy37;
        break;
      case 4: /* expr ::= union */
#line 175 "parser.y"
{
    yylhsminor.yy37 = yymsp[0].minor.yy37;
}
#line 1083 "parser.c"
  yymsp[0].minor.yy37 = yylhsminor.yy37;
        break;
      case 5: /* union ::= expr OR expr */
#line 179 "parser.y"
{
    if (yymsp[-2].minor.yy37 == NULL && yymsp[0].minor.yy37 == NULL) {
        yylhsminor.yy37 = NULL;
    } else if (yymsp[-2].minor.yy37 && yymsp[-2].minor.yy37->type == QN_UNION && yymsp[-2].minor.yy37->opts.fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy37 = yymsp[-2].minor.yy37;
    } else {
        yylhsminor.yy37 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy37, yymsp[-2].minor.yy37);
        if (yymsp[-2].minor.yy37) 
         yylhsminor.yy37->opts.fieldMask |= yymsp[-2].minor.yy37->opts.fieldMask;

    } 
    if (yymsp[0].minor.yy37) {

        QueryUnionNode_AddChild(yylhsminor.yy37, yymsp[0].minor.yy37);
        yylhsminor.yy37->opts.fieldMask |= yymsp[0].minor.yy37->opts.fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy37, yylhsminor.yy37->opts.fieldMask);
    }
    
}
#line 1108 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 6: /* union ::= union OR expr */
#line 200 "parser.y"
{
    
    yylhsminor.yy37 = yymsp[-2].minor.yy37;
    QueryUnionNode_AddChild(yylhsminor.yy37, yymsp[0].minor.yy37); 
    yylhsminor.yy37->opts.fieldMask |= yymsp[0].minor.yy37->opts.fieldMask;
    QueryNode_SetFieldMask(yymsp[0].minor.yy37, yylhsminor.yy37->opts.fieldMask);


}
#line 1122 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 7: /* expr ::= modifier COLON expr */
#line 214 "parser.y"
{
    if (yymsp[0].minor.yy37 == NULL) {
        yylhsminor.yy37 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy37, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy37 = yymsp[0].minor.yy37; 
    }
}
#line 1137 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
#line 234 "parser.y"
{
    
    if (yymsp[0].minor.yy37 == NULL) {
        yylhsminor.yy37 = NULL;
    } else {
        //yymsp[0].minor.yy37->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy10); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy10, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy37, mask);
        Vector_Free(yymsp[-2].minor.yy10);
        yylhsminor.yy37=yymsp[0].minor.yy37;
    }
}
#line 1162 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 9: /* expr ::= LP expr RP */
#line 255 "parser.y"
{
    yymsp[-2].minor.yy37 = yymsp[-1].minor.yy37;
}
#line 1170 "parser.c"
        break;
      case 10: /* attribute ::= ATTRIBUTE COLON term */
#line 263 "parser.y"
{
    
    yylhsminor.yy17 = (QueryAttribute){ .name = yymsp[-2].minor.yy0.s, .namelen = yymsp[-2].minor.yy0.len, .value = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), .vallen = yymsp[0].minor.yy0.len };
}
#line 1178 "parser.c"
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 11: /* attribute_list ::= attribute */
#line 268 "parser.y"
{
    yylhsminor.yy61 = array_new(QueryAttribute, 2);
    yylhsminor.yy61 = array_append(yylhsminor.yy61, yymsp[0].minor.yy17);
}
#line 1187 "parser.c"
  yymsp[0].minor.yy61 = yylhsminor.yy61;
        break;
      case 12: /* attribute_list ::= attribute_list SEMICOLON attribute */
#line 273 "parser.y"
{
    yylhsminor.yy61 = array_append(yymsp[-2].minor.yy61, yymsp[0].minor.yy17);
}
#line 1195 "parser.c"
  yymsp[-2].minor.yy61 = yylhsminor.yy61;
        break;
      case 13: /* attribute_list ::= attribute_list SEMICOLON */
#line 277 "parser.y"
{
    yylhsminor.yy61 = yymsp[-1].minor.yy61;
}
#line 1203 "parser.c"
  yymsp[-1].minor.yy61 = yylhsminor.yy61;
        break;
      case 14: /* attribute_list ::= */
#line 281 "parser.y"
{
    yymsp[1].minor.yy61 = NULL;
}
#line 1211 "parser.c"
        break;
      case 15: /* expr ::= expr ARROW LB attribute_list RB */
#line 285 "parser.y"
{

    if (yymsp[-1].minor.yy61) {
        char *err = NULL;
        if (!QueryNode_ApplyAttributes(yymsp[-4].minor.yy37, yymsp[-1].minor.yy61, array_len(yymsp[-1].minor.yy61), &err)) {
            ctx->ok = 0;
            ctx->errorMsg = err;
        }
    }
    array_free_ex(yymsp[-1].minor.yy61, free((char*)((QueryAttribute*)ptr )->value));
    yylhsminor.yy37 = yymsp[-4].minor.yy37;
}
#line 1227 "parser.c"
  yymsp[-4].minor.yy37 = yylhsminor.yy37;
        break;
      case 16: /* expr ::= QUOTE termlist QUOTE */
#line 302 "parser.y"
{
    yymsp[-1].minor.yy37->pn.exact =1;
    yymsp[-1].minor.yy37->opts.flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy37 = yymsp[-1].minor.yy37;
}
#line 1238 "parser.c"
        break;
      case 17: /* expr ::= QUOTE term QUOTE */
#line 309 "parser.y"
{
    yymsp[-2].minor.yy37 = NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
    yymsp[-2].minor.yy37->opts.flags |= QueryNode_Verbatim;
    
}
#line 1247 "parser.c"
        break;
      case 18: /* expr ::= term */
#line 315 "parser.y"
{
   yylhsminor.yy37 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
#line 1254 "parser.c"
  yymsp[0].minor.yy37 = yylhsminor.yy37;
        break;
      case 19: /* expr ::= prefix */
#line 319 "parser.y"
{
    yylhsminor.yy37= yymsp[0].minor.yy37;
}
#line 1262 "parser.c"
  yymsp[0].minor.yy37 = yylhsminor.yy37;
        break;
      case 20: /* expr ::= termlist */
#line 323 "parser.y"
{
        yylhsminor.yy37 = yymsp[0].minor.yy37;
}
#line 1270 "parser.c"
  yymsp[0].minor.yy37 = yylhsminor.yy37;
        break;
      case 21: /* expr ::= STOPWORD */
#line 327 "parser.y"
{
    yymsp[0].minor.yy37 = NULL;
}
#line 1278 "parser.c"
        break;
      case 22: /* termlist ::= term term */
#line 331 "parser.y"
{
    yylhsminor.yy37 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy37, NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1));
    QueryPhraseNode_AddChild(yylhsminor.yy37, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1287 "parser.c"
  yymsp[-1].minor.yy37 = yylhsminor.yy37;
        break;
      case 23: /* termlist ::= termlist term */
#line 337 "parser.y"
{
    yylhsminor.yy37 = yymsp[-1].minor.yy37;
    QueryPhraseNode_AddChild(yylhsminor.yy37, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1296 "parser.c"
  yymsp[-1].minor.yy37 = yylhsminor.yy37;
        break;
      case 24: /* termlist ::= termlist STOPWORD */
      case 38: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==38);
#line 342 "parser.y"
{
    yylhsminor.yy37 = yymsp[-1].minor.yy37;
}
#line 1305 "parser.c"
  yymsp[-1].minor.yy37 = yylhsminor.yy37;
        break;
      case 25: /* expr ::= MINUS expr */
#line 350 "parser.y"
{ 
    yymsp[-1].minor.yy37 = NewNotNode(yymsp[0].minor.yy37);
}
#line 1313 "parser.c"
        break;
      case 26: /* expr ::= TILDE expr */
#line 358 "parser.y"
{ 
    yymsp[-1].minor.yy37 = NewOptionalNode(yymsp[0].minor.yy37);
}
#line 1320 "parser.c"
        break;
      case 27: /* prefix ::= PREFIX */
#line 366 "parser.y"
{
    yymsp[0].minor.yy0.s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy37 = NewPrefixNode(ctx, yymsp[0].minor.yy0.s, strlen(yymsp[0].minor.yy0.s));
}
#line 1328 "parser.c"
  yymsp[0].minor.yy37 = yylhsminor.yy37;
        break;
      case 28: /* modifier ::= MODIFIER */
#line 375 "parser.y"
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1337 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 29: /* modifierlist ::= modifier OR term */
#line 380 "parser.y"
{
    yylhsminor.yy10 = NewVector(char *, 2);
    char *s = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy10, s);
    s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy10, s);
}
#line 1349 "parser.c"
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 30: /* modifierlist ::= modifierlist OR term */
#line 388 "parser.y"
{
    char *s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy10, s);
    yylhsminor.yy10 = yymsp[-2].minor.yy10;
}
#line 1359 "parser.c"
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 31: /* expr ::= modifier COLON tag_list */
#line 398 "parser.y"
{
    if (!yymsp[0].minor.yy37) {
        yylhsminor.yy37= NULL;
    } else {
        // Tag field names must be case sensitive, we we can't do strdupcase
        char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        size_t slen = unescapen((char*)s, yymsp[-2].minor.yy0.len);

        yylhsminor.yy37 = NewTagNode(s, slen);
        QueryTagNode_AddChildren(yylhsminor.yy37, yymsp[0].minor.yy37->pn.children, yymsp[0].minor.yy37->pn.numChildren);
        
        // Set the children count on yymsp[0].minor.yy37 to 0 so they won't get recursively free'd
        yymsp[0].minor.yy37->pn.numChildren = 0;
        QueryNode_Free(yymsp[0].minor.yy37);
    }
}
#line 1380 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 32: /* tag_list ::= LB term */
#line 415 "parser.y"
{
    yymsp[-1].minor.yy37 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy37, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1389 "parser.c"
        break;
      case 33: /* tag_list ::= LB prefix */
      case 34: /* tag_list ::= LB termlist */ yytestcase(yyruleno==34);
#line 420 "parser.y"
{
    yymsp[-1].minor.yy37 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy37, yymsp[0].minor.yy37);
}
#line 1398 "parser.c"
        break;
      case 35: /* tag_list ::= tag_list OR term */
#line 430 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy37, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy37 = yymsp[-2].minor.yy37;
}
#line 1406 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 36: /* tag_list ::= tag_list OR prefix */
      case 37: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==37);
#line 435 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy37, yymsp[0].minor.yy37);
    yylhsminor.yy37 = yymsp[-2].minor.yy37;
}
#line 1416 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 39: /* expr ::= modifier COLON numeric_range */
#line 454 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy40->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy37 = NewNumericNode(yymsp[0].minor.yy40);
}
#line 1426 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 40: /* numeric_range ::= LSQB num num RSQB */
#line 460 "parser.y"
{
    yymsp[-3].minor.yy40 = NewNumericFilter(yymsp[-2].minor.yy62.num, yymsp[-1].minor.yy62.num, yymsp[-2].minor.yy62.inclusive, yymsp[-1].minor.yy62.inclusive);
}
#line 1434 "parser.c"
        break;
      case 41: /* expr ::= modifier COLON geo_filter */
#line 468 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy68->property = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy37 = NewGeofilterNode(yymsp[0].minor.yy68);
}
#line 1443 "parser.c"
  yymsp[-2].minor.yy37 = yylhsminor.yy37;
        break;
      case 42: /* geo_filter ::= LSQB num num num TERM RSQB */
#line 474 "parser.y"
{
    yymsp[-5].minor.yy68 = NewGeoFilter(yymsp[-4].minor.yy62.num, yymsp[-3].minor.yy62.num, yymsp[-2].minor.yy62.num, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(yymsp[-5].minor.yy68, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}
#line 1456 "parser.c"
        break;
      case 43: /* num ::= NUMBER */
#line 489 "parser.y"
{
    yylhsminor.yy62.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy62.inclusive = 1;
}
#line 1464 "parser.c"
  yymsp[0].minor.yy62 = yylhsminor.yy62;
        break;
      case 44: /* num ::= LP num */
#line 494 "parser.y"
{
    yymsp[-1].minor.yy62=yymsp[0].minor.yy62;
    yymsp[-1].minor.yy62.inclusive = 0;
}
#line 1473 "parser.c"
        break;
      case 45: /* num ::= MINUS num */
#line 499 "parser.y"
{
    yymsp[0].minor.yy62.num = -yymsp[0].minor.yy62.num;
    yymsp[-1].minor.yy62 = yymsp[0].minor.yy62;
}
#line 1481 "parser.c"
        break;
      case 46: /* term ::= TERM */
      case 47: /* term ::= NUMBER */ yytestcase(yyruleno==47);
#line 504 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1489 "parser.c"
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
#line 1558 "parser.c"
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