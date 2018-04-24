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
#define YYNOCODE 37
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  GeoFilter * yy28;
  QueryNode * yy35;
  NumericFilter * yy36;
  RangeNumber yy47;
  Vector* yy66;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  QueryParseCtx *ctx ;
#define ParseARG_PDECL , QueryParseCtx *ctx 
#define ParseARG_FETCH  QueryParseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             41
#define YYNRULE              42
#define YY_MAX_SHIFT         40
#define YY_MIN_SHIFTREDUCE   66
#define YY_MAX_SHIFTREDUCE   107
#define YY_MIN_REDUCE        108
#define YY_MAX_REDUCE        149
#define YY_ERROR_ACTION      150
#define YY_ACCEPT_ACTION     151
#define YY_NO_ACTION         152
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
#define YY_ACTTAB_COUNT (225)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   109,   18,    5,   40,   19,   98,    6,  107,   81,    9,
 /*    10 */   106,   87,    7,   21,   88,    5,   83,   19,   38,    6,
 /*    20 */   107,   81,   28,  106,   87,    7,  107,   88,  107,  106,
 /*    30 */    87,  106,   17,  105,   25,    5,  104,   19,   20,    6,
 /*    40 */   107,   81,   24,  106,   87,    7,   75,   88,  108,    9,
 /*    50 */     5,   82,   19,   89,    6,  107,   81,   90,  106,   87,
 /*    60 */     7,    8,   88,  106,    9,   13,   79,   34,   70,   35,
 /*    70 */   101,   36,   32,   99,  102,   37,   33,  110,   19,   39,
 /*    80 */     6,  107,   81,  110,  106,   87,    7,    5,   88,   19,
 /*    90 */     9,    6,  107,   81,    1,  106,   87,    7,  110,   88,
 /*   100 */     3,   79,   34,   70,  110,   29,   36,   93,   26,  151,
 /*   110 */    37,   33,   14,   79,   34,   70,  110,   30,   36,   96,
 /*   120 */    27,  110,   37,   33,    4,   79,   34,   70,  110,   31,
 /*   130 */    36,  110,  110,  110,   37,   33,   11,   79,   34,   70,
 /*   140 */   110,  110,   36,  110,  110,  110,   37,   33,    2,   79,
 /*   150 */    34,   70,  110,  110,   36,  110,  110,  110,   37,   33,
 /*   160 */    12,   79,   34,   70,  110,  110,   36,  110,  110,  110,
 /*   170 */    37,   33,   15,   79,   34,   70,  110,  110,   36,  110,
 /*   180 */   110,  110,   37,   33,   16,   79,   34,   70,  110,  110,
 /*   190 */    36,   22,  103,  110,   37,   33,  110,   23,  107,   81,
 /*   200 */   110,  106,   87,    7,  110,   88,  110,    9,   22,  103,
 /*   210 */   107,   84,   76,  106,   23,  107,   84,   77,  106,  110,
 /*   220 */   107,  110,  110,  106,  100,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,   16,    2,   31,    4,   20,    6,    7,    8,   16,
 /*    10 */    10,   11,   12,   26,   14,    2,   35,    4,   18,    6,
 /*    20 */     7,    8,   35,   10,   11,   12,    7,   14,    7,   10,
 /*    30 */    11,   10,   19,   31,   21,    2,   31,    4,   31,    6,
 /*    40 */     7,    8,   31,   10,   11,   12,   13,   14,    0,   16,
 /*    50 */     2,   35,    4,   35,    6,    7,    8,   35,   10,   11,
 /*    60 */    12,    5,   14,   10,   16,   24,   25,   26,   27,   28,
 /*    70 */    29,   30,   16,   32,   22,   34,   35,    0,    4,   10,
 /*    80 */     6,    7,    8,   36,   10,   11,   12,    2,   14,    4,
 /*    90 */    16,    6,    7,    8,    5,   10,   11,   12,   36,   14,
 /*   100 */    24,   25,   26,   27,   36,   16,   30,   25,   26,   33,
 /*   110 */    34,   35,   24,   25,   26,   27,   36,   35,   30,   25,
 /*   120 */    26,   36,   34,   35,   24,   25,   26,   27,   36,   35,
 /*   130 */    30,   36,   36,   36,   34,   35,   24,   25,   26,   27,
 /*   140 */    36,   36,   30,   36,   36,   36,   34,   35,   24,   25,
 /*   150 */    26,   27,   36,   36,   30,   36,   36,   36,   34,   35,
 /*   160 */    24,   25,   26,   27,   36,   36,   30,   36,   36,   36,
 /*   170 */    34,   35,   24,   25,   26,   27,   36,   36,   30,   36,
 /*   180 */    36,   36,   34,   35,   24,   25,   26,   27,   36,   36,
 /*   190 */    30,    6,    7,   36,   34,   35,   36,   12,    7,    8,
 /*   200 */    36,   10,   11,   12,   36,   14,   36,   16,    6,    7,
 /*   210 */     7,    8,    4,   10,   12,    7,    8,    4,   10,   36,
 /*   220 */     7,   36,   36,   10,   22,
};
#define YY_SHIFT_USE_DFLT (225)
#define YY_SHIFT_COUNT    (40)
#define YY_SHIFT_MIN      (-15)
#define YY_SHIFT_MAX      (213)
static const short yy_shift_ofst[] = {
 /*     0 */     0,   13,   33,   48,   74,   85,   85,   85,   85,   85,
 /*    10 */    85,  191,   -7,   -7,   -7,  225,  225,   19,   19,   21,
 /*    20 */   202,  208,  185,  185,  185,  185,  203,  203,  213,   21,
 /*    30 */    21,   21,   21,   21,   53,  -15,   56,   89,   77,   52,
 /*    40 */    69,
};
#define YY_REDUCE_USE_DFLT (-29)
#define YY_REDUCE_COUNT (34)
#define YY_REDUCE_MIN   (-28)
#define YY_REDUCE_MAX   (160)
static const short yy_reduce_ofst[] = {
 /*     0 */    76,   41,   88,   88,   88,  100,  112,  124,  136,  148,
 /*    10 */   160,   88,   88,   88,   88,   88,   88,   82,   94,  -13,
 /*    20 */   -28,  -19,    2,    5,    7,   11,  -19,  -19,   16,   18,
 /*    30 */    16,   16,   22,   16,  -19,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   150,  150,  150,  150,  128,  150,  150,  150,  150,  150,
 /*    10 */   150,  127,  116,  115,  111,  113,  114,  150,  150,  150,
 /*    20 */   150,  150,  150,  150,  150,  150,  136,  139,  150,  150,
 /*    30 */   134,  137,  150,  120,  122,  133,  150,  150,  150,  150,
 /*    40 */   150,
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
  "OR",            "ORX",           "STAR",          "LB",          
  "RB",            "LSQB",          "RSQB",          "error",       
  "expr",          "prefix",        "termlist",      "union",       
  "tag_list",      "geo_filter",    "modifierlist",  "num",         
  "numeric_range",  "query",         "modifier",      "term",        
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
 /*  10 */ "expr ::= QUOTE termlist QUOTE",
 /*  11 */ "expr ::= QUOTE term QUOTE",
 /*  12 */ "expr ::= term",
 /*  13 */ "expr ::= prefix",
 /*  14 */ "expr ::= termlist",
 /*  15 */ "expr ::= STOPWORD",
 /*  16 */ "termlist ::= term term",
 /*  17 */ "termlist ::= termlist term",
 /*  18 */ "termlist ::= termlist STOPWORD",
 /*  19 */ "expr ::= MINUS expr",
 /*  20 */ "expr ::= TILDE expr",
 /*  21 */ "prefix ::= PREFIX",
 /*  22 */ "modifier ::= MODIFIER",
 /*  23 */ "modifierlist ::= modifier OR term",
 /*  24 */ "modifierlist ::= modifierlist OR term",
 /*  25 */ "expr ::= modifier COLON tag_list",
 /*  26 */ "tag_list ::= LB term",
 /*  27 */ "tag_list ::= LB prefix",
 /*  28 */ "tag_list ::= LB termlist",
 /*  29 */ "tag_list ::= tag_list OR term",
 /*  30 */ "tag_list ::= tag_list OR prefix",
 /*  31 */ "tag_list ::= tag_list OR termlist",
 /*  32 */ "tag_list ::= tag_list RB",
 /*  33 */ "expr ::= modifier COLON numeric_range",
 /*  34 */ "numeric_range ::= LSQB num num RSQB",
 /*  35 */ "expr ::= modifier COLON geo_filter",
 /*  36 */ "geo_filter ::= LSQB num num num TERM RSQB",
 /*  37 */ "num ::= NUMBER",
 /*  38 */ "num ::= LP num",
 /*  39 */ "num ::= MINUS num",
 /*  40 */ "term ::= TERM",
 /*  41 */ "term ::= NUMBER",
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
    case 23: /* error */
    case 31: /* num */
    case 33: /* query */
    case 34: /* modifier */
    case 35: /* term */
{
#line 89 "parser.y"
 
#line 605 "parser.c"
}
      break;
    case 24: /* expr */
    case 25: /* prefix */
    case 26: /* termlist */
    case 27: /* union */
    case 28: /* tag_list */
{
#line 92 "parser.y"
 QueryNode_Free((yypminor->yy35)); 
#line 616 "parser.c"
}
      break;
    case 29: /* geo_filter */
{
#line 107 "parser.y"
 GeoFilter_Free((yypminor->yy28)); 
#line 623 "parser.c"
}
      break;
    case 30: /* modifierlist */
{
#line 110 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy66)); i++) {
        char *s;
        Vector_Get((yypminor->yy66), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy66)); 

#line 637 "parser.c"
}
      break;
    case 32: /* numeric_range */
{
#line 122 "parser.y"

    NumericFilter_Free((yypminor->yy36));

#line 646 "parser.c"
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
  { 33, 1 },
  { 33, 0 },
  { 33, 1 },
  { 24, 2 },
  { 24, 1 },
  { 27, 3 },
  { 27, 3 },
  { 24, 3 },
  { 24, 3 },
  { 24, 3 },
  { 24, 3 },
  { 24, 3 },
  { 24, 1 },
  { 24, 1 },
  { 24, 1 },
  { 24, 1 },
  { 26, 2 },
  { 26, 2 },
  { 26, 2 },
  { 24, 2 },
  { 24, 2 },
  { 25, 1 },
  { 34, 1 },
  { 30, 3 },
  { 30, 3 },
  { 24, 3 },
  { 28, 2 },
  { 28, 2 },
  { 28, 2 },
  { 28, 3 },
  { 28, 3 },
  { 28, 3 },
  { 28, 2 },
  { 24, 3 },
  { 32, 4 },
  { 24, 3 },
  { 29, 6 },
  { 31, 1 },
  { 31, 2 },
  { 31, 2 },
  { 35, 1 },
  { 35, 1 },
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
 
    ctx->root = yymsp[0].minor.yy35;
 
}
#line 1011 "parser.c"
        break;
      case 1: /* query ::= */
#line 132 "parser.y"
{
    ctx->root = NULL;
}
#line 1018 "parser.c"
        break;
      case 2: /* query ::= STAR */
#line 136 "parser.y"
{
    ctx->root = NewWildcardNode();
}
#line 1025 "parser.c"
        break;
      case 3: /* expr ::= expr expr */
#line 144 "parser.y"
{

    // if both yymsp[-1].minor.yy35 and yymsp[0].minor.yy35 are null we return null
    if (yymsp[-1].minor.yy35 == NULL && yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else {

        if (yymsp[-1].minor.yy35 && yymsp[-1].minor.yy35->type == QN_PHRASE && yymsp[-1].minor.yy35->pn.exact == 0 && 
            yymsp[-1].minor.yy35->fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy35 = yymsp[-1].minor.yy35;
        } else {     
            yylhsminor.yy35 = NewPhraseNode(0);
            QueryPhraseNode_AddChild(yylhsminor.yy35, yymsp[-1].minor.yy35);
        }
        QueryPhraseNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35);
    }
}
#line 1046 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 4: /* expr ::= union */
#line 167 "parser.y"
{
    yylhsminor.yy35 = yymsp[0].minor.yy35;
}
#line 1054 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 5: /* union ::= expr OR expr */
#line 171 "parser.y"
{
    if (yymsp[-2].minor.yy35 == NULL && yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else if (yymsp[-2].minor.yy35 && yymsp[-2].minor.yy35->type == QN_UNION && yymsp[-2].minor.yy35->fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy35 = yymsp[-2].minor.yy35;
    } else {
        yylhsminor.yy35 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[-2].minor.yy35);
        if (yymsp[-2].minor.yy35) 
         yylhsminor.yy35->fieldMask |= yymsp[-2].minor.yy35->fieldMask;

    } 
    if (yymsp[0].minor.yy35) {

        QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35);
        yylhsminor.yy35->fieldMask |= yymsp[0].minor.yy35->fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy35, yylhsminor.yy35->fieldMask);
    }
    
}
#line 1079 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 6: /* union ::= union OR expr */
#line 192 "parser.y"
{
    
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
    QueryUnionNode_AddChild(yylhsminor.yy35, yymsp[0].minor.yy35); 
    yylhsminor.yy35->fieldMask |= yymsp[0].minor.yy35->fieldMask;
    QueryNode_SetFieldMask(yymsp[0].minor.yy35, yylhsminor.yy35->fieldMask);


}
#line 1093 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 7: /* expr ::= modifier COLON expr */
#line 206 "parser.y"
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
#line 1108 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
#line 226 "parser.y"
{
    
    if (yymsp[0].minor.yy35 == NULL) {
        yylhsminor.yy35 = NULL;
    } else {
        //yymsp[0].minor.yy35->fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy66); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy66, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy35, mask);
        Vector_Free(yymsp[-2].minor.yy66);
        yylhsminor.yy35=yymsp[0].minor.yy35;
    }
}
#line 1133 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 9: /* expr ::= LP expr RP */
#line 247 "parser.y"
{
    yymsp[-2].minor.yy35 = yymsp[-1].minor.yy35;
}
#line 1141 "parser.c"
        break;
      case 10: /* expr ::= QUOTE termlist QUOTE */
#line 255 "parser.y"
{
    yymsp[-1].minor.yy35->pn.exact =1;
    yymsp[-1].minor.yy35->flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy35 = yymsp[-1].minor.yy35;
}
#line 1151 "parser.c"
        break;
      case 11: /* expr ::= QUOTE term QUOTE */
#line 262 "parser.y"
{
    yymsp[-2].minor.yy35 = NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
    yymsp[-2].minor.yy35->flags |= QueryNode_Verbatim;
    
}
#line 1160 "parser.c"
        break;
      case 12: /* expr ::= term */
#line 268 "parser.y"
{
   yylhsminor.yy35 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
#line 1167 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 13: /* expr ::= prefix */
#line 272 "parser.y"
{
    yylhsminor.yy35= yymsp[0].minor.yy35;
}
#line 1175 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 14: /* expr ::= termlist */
#line 276 "parser.y"
{
        yylhsminor.yy35 = yymsp[0].minor.yy35;
}
#line 1183 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 15: /* expr ::= STOPWORD */
#line 280 "parser.y"
{
    yymsp[0].minor.yy35 = NULL;
}
#line 1191 "parser.c"
        break;
      case 16: /* termlist ::= term term */
#line 284 "parser.y"
{
    yylhsminor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1));
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1200 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 17: /* termlist ::= termlist term */
#line 290 "parser.y"
{
    yylhsminor.yy35 = yymsp[-1].minor.yy35;
    QueryPhraseNode_AddChild(yylhsminor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1209 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 18: /* termlist ::= termlist STOPWORD */
      case 32: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==32);
#line 295 "parser.y"
{
    yylhsminor.yy35 = yymsp[-1].minor.yy35;
}
#line 1218 "parser.c"
  yymsp[-1].minor.yy35 = yylhsminor.yy35;
        break;
      case 19: /* expr ::= MINUS expr */
#line 303 "parser.y"
{ 
    yymsp[-1].minor.yy35 = NewNotNode(yymsp[0].minor.yy35);
}
#line 1226 "parser.c"
        break;
      case 20: /* expr ::= TILDE expr */
#line 311 "parser.y"
{ 
    yymsp[-1].minor.yy35 = NewOptionalNode(yymsp[0].minor.yy35);
}
#line 1233 "parser.c"
        break;
      case 21: /* prefix ::= PREFIX */
#line 319 "parser.y"
{
    yymsp[0].minor.yy0.s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy35 = NewPrefixNode(ctx, yymsp[0].minor.yy0.s, strlen(yymsp[0].minor.yy0.s));
}
#line 1241 "parser.c"
  yymsp[0].minor.yy35 = yylhsminor.yy35;
        break;
      case 22: /* modifier ::= MODIFIER */
#line 328 "parser.y"
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1250 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 23: /* modifierlist ::= modifier OR term */
#line 333 "parser.y"
{
    yylhsminor.yy66 = NewVector(char *, 2);
    char *s = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
    s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
}
#line 1262 "parser.c"
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 24: /* modifierlist ::= modifierlist OR term */
#line 341 "parser.y"
{
    char *s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy66, s);
    yylhsminor.yy66 = yymsp[-2].minor.yy66;
}
#line 1272 "parser.c"
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 25: /* expr ::= modifier COLON tag_list */
#line 351 "parser.y"
{
    if (!yymsp[0].minor.yy35) {
        yylhsminor.yy35= NULL;
    } else {
        char *s = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        yylhsminor.yy35 = NewTagNode(s, strlen(s));
        QueryTagNode_AddChildren(yylhsminor.yy35, yymsp[0].minor.yy35->pn.children, yymsp[0].minor.yy35->pn.numChildren);
        
        // Set the children count on yymsp[0].minor.yy35 to 0 so they won't get recursively free'd
        yymsp[0].minor.yy35->pn.numChildren = 0;
        QueryNode_Free(yymsp[0].minor.yy35);
    }
}
#line 1290 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 26: /* tag_list ::= LB term */
#line 365 "parser.y"
{
    yymsp[-1].minor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1299 "parser.c"
        break;
      case 27: /* tag_list ::= LB prefix */
      case 28: /* tag_list ::= LB termlist */ yytestcase(yyruleno==28);
#line 370 "parser.y"
{
    yymsp[-1].minor.yy35 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy35, yymsp[0].minor.yy35);
}
#line 1308 "parser.c"
        break;
      case 29: /* tag_list ::= tag_list OR term */
#line 380 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy35, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
}
#line 1316 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 30: /* tag_list ::= tag_list OR prefix */
      case 31: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==31);
#line 385 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy35, yymsp[0].minor.yy35);
    yylhsminor.yy35 = yymsp[-2].minor.yy35;
}
#line 1326 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 33: /* expr ::= modifier COLON numeric_range */
#line 404 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy36->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy35 = NewNumericNode(yymsp[0].minor.yy36);
}
#line 1336 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 34: /* numeric_range ::= LSQB num num RSQB */
#line 410 "parser.y"
{
    yymsp[-3].minor.yy36 = NewNumericFilter(yymsp[-2].minor.yy47.num, yymsp[-1].minor.yy47.num, yymsp[-2].minor.yy47.inclusive, yymsp[-1].minor.yy47.inclusive);
}
#line 1344 "parser.c"
        break;
      case 35: /* expr ::= modifier COLON geo_filter */
#line 418 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy28->property = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy35 = NewGeofilterNode(yymsp[0].minor.yy28);
}
#line 1353 "parser.c"
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 36: /* geo_filter ::= LSQB num num num TERM RSQB */
#line 424 "parser.y"
{
    yymsp[-5].minor.yy28 = NewGeoFilter(yymsp[-4].minor.yy47.num, yymsp[-3].minor.yy47.num, yymsp[-2].minor.yy47.num, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(yymsp[-5].minor.yy28, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}
#line 1366 "parser.c"
        break;
      case 37: /* num ::= NUMBER */
#line 437 "parser.y"
{
    yylhsminor.yy47.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy47.inclusive = 1;
}
#line 1374 "parser.c"
  yymsp[0].minor.yy47 = yylhsminor.yy47;
        break;
      case 38: /* num ::= LP num */
#line 442 "parser.y"
{
    yymsp[-1].minor.yy47=yymsp[0].minor.yy47;
    yymsp[-1].minor.yy47.inclusive = 0;
}
#line 1383 "parser.c"
        break;
      case 39: /* num ::= MINUS num */
#line 447 "parser.y"
{
    yymsp[0].minor.yy47.num = -yymsp[0].minor.yy47.num;
    yymsp[-1].minor.yy47 = yymsp[0].minor.yy47;
}
#line 1391 "parser.c"
        break;
      case 40: /* term ::= TERM */
      case 41: /* term ::= NUMBER */ yytestcase(yyruleno==41);
#line 452 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1399 "parser.c"
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
#line 1468 "parser.c"
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