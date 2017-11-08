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
#line 31 "parser.y"
   

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
   
#line 60 "parser.c"
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
#define YYNOCODE 34
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  GeoFilter * yy10;
  Vector* yy42;
  RangeNumber yy47;
  NumericFilter * yy48;
  QueryNode * yy53;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  QueryParseCtx *ctx ;
#define ParseARG_PDECL , QueryParseCtx *ctx 
#define ParseARG_FETCH  QueryParseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             44
#define YYNRULE              38
#define YY_MAX_SHIFT         43
#define YY_MIN_SHIFTREDUCE   65
#define YY_MAX_SHIFTREDUCE   102
#define YY_MIN_REDUCE        103
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
#define YY_ACTTAB_COUNT (179)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     5,    9,   41,   19,   21,    6,  102,   85,   78,   80,
 /*    10 */     7,  101,   33,   29,   40,  102,   17,   81,   28,  100,
 /*    20 */     5,   86,  101,   19,   99,    6,  102,   85,   78,   20,
 /*    30 */     7,   74,   18,    9,  103,    5,   93,  101,   19,   27,
 /*    40 */     6,  102,   85,   78,   79,    7,  104,    5,    9,  101,
 /*    50 */    19,  102,    6,  102,   85,   78,   87,    7,   13,   43,
 /*    60 */    68,   36,   96,   37,   39,   94,  139,   38,   42,  101,
 /*    70 */    19,   97,    6,  102,   85,   78,    5,    7,  101,   19,
 /*    80 */     9,    6,  102,   85,   78,    3,    7,   68,  101,   33,
 /*    90 */    37,   76,  102,  142,   38,   42,   14,   84,   68,  101,
 /*   100 */    34,   37,    4,  102,   68,   38,   42,   37,   11,  105,
 /*   110 */    68,   38,   42,   37,   22,  105,  105,   38,   42,    2,
 /*   120 */   105,   68,  105,   30,   37,   12,  105,   68,   38,   42,
 /*   130 */    37,   15,  105,   68,   38,   42,   37,   23,  105,  105,
 /*   140 */    38,   42,   16,   78,   68,    7,   32,   37,    9,   25,
 /*   150 */    98,   38,   42,  105,   26,  102,   85,   78,  105,    7,
 /*   160 */   101,   35,    9,   95,  102,    8,   81,   25,   98,    1,
 /*   170 */   105,  105,   26,  105,   31,  105,  105,  105,   24,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   14,    3,    4,   23,    6,    7,    8,    9,   32,
 /*    10 */    11,    3,    4,   32,   28,    7,   17,    9,   19,   28,
 /*    20 */     1,   32,    3,    4,   28,    6,    7,    8,    9,   28,
 /*    30 */    11,   12,   14,   14,    0,    1,   18,    3,    4,   28,
 /*    40 */     6,    7,    8,    9,   32,   11,    0,    1,   14,    3,
 /*    50 */     4,    7,    6,    7,    8,    9,   32,   11,   22,   32,
 /*    60 */    24,   25,   26,   27,    3,   29,   16,   31,   32,    3,
 /*    70 */     4,   20,    6,    7,    8,    9,    1,   11,    3,    4,
 /*    80 */    14,    6,    7,    8,    9,   22,   11,   24,    3,    4,
 /*    90 */    27,    4,    7,   30,   31,   32,   22,   16,   24,    3,
 /*   100 */     4,   27,   22,    7,   24,   31,   32,   27,   22,   33,
 /*   110 */    24,   31,   32,   27,   23,   33,   33,   31,   32,   22,
 /*   120 */    33,   24,   33,   32,   27,   22,   33,   24,   31,   32,
 /*   130 */    27,   22,   33,   24,   31,   32,   27,   23,   33,   33,
 /*   140 */    31,   32,   22,    9,   24,   11,   32,   27,   14,    6,
 /*   150 */     7,   31,   32,   33,   11,    7,    8,    9,   33,   11,
 /*   160 */     3,    4,   14,   20,    7,    5,    9,    6,    7,    5,
 /*   170 */    33,   33,   11,   33,   14,   33,   33,   33,   14,
};
#define YY_SHIFT_USE_DFLT (179)
#define YY_SHIFT_COUNT    (43)
#define YY_SHIFT_MIN      (-13)
#define YY_SHIFT_MAX      (164)
static const short yy_shift_ofst[] = {
 /*     0 */    46,   -1,   19,   34,   66,   75,   75,   75,   75,   75,
 /*    10 */    75,  148,  134,  134,  -13,  179,  179,   85,   85,   85,
 /*    20 */   143,    8,    8,  157,   85,  161,  161,  161,  161,   85,
 /*    30 */    85,   85,   96,   85,   44,   44,   18,  160,  164,   51,
 /*    40 */    61,   50,   81,   87,
};
#define YY_REDUCE_USE_DFLT (-24)
#define YY_REDUCE_COUNT (35)
#define YY_REDUCE_MIN   (-23)
#define YY_REDUCE_MAX   (120)
static const signed char yy_reduce_ofst[] = {
 /*     0 */    63,   36,   74,   74,   74,   80,   86,   97,  103,  109,
 /*    10 */   120,   74,   74,   74,   74,   74,   74,  -19,   91,  114,
 /*    20 */   -14,  -23,  -23,  -23,  -11,   -9,   -4,    1,   11,   12,
 /*    30 */    12,   24,   12,   27,   27,   27,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   141,  141,  141,  141,  121,  141,  141,  141,  141,  141,
 /*    10 */   141,  120,  111,  109,  105,  107,  108,  141,  141,  141,
 /*    20 */   141,  128,  130,  141,  141,  141,  141,  141,  141,  127,
 /*    30 */   129,  141,  141,  141,  114,  113,  126,  141,  141,  141,
 /*    40 */   141,  110,  115,  141,
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
  "$",             "TILDE",         "TAGLIST",       "TERM",        
  "QUOTE",         "COLON",         "MINUS",         "NUMBER",      
  "MODIFIER",      "STOPWORD",      "TERMLIST",      "LP",          
  "RP",            "AND",           "OR",            "ORX",         
  "STAR",          "LB",            "RB",            "LSQB",        
  "RSQB",          "error",         "expr",          "termlist",    
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
 /*   2 */ "expr ::= expr expr",
 /*   3 */ "expr ::= union",
 /*   4 */ "union ::= expr OR expr",
 /*   5 */ "union ::= union OR expr",
 /*   6 */ "expr ::= modifier COLON expr",
 /*   7 */ "expr ::= modifier COLON TERM",
 /*   8 */ "expr ::= modifierlist COLON expr",
 /*   9 */ "expr ::= LP expr RP",
 /*  10 */ "expr ::= QUOTE termlist QUOTE",
 /*  11 */ "term ::= QUOTE term QUOTE",
 /*  12 */ "expr ::= term",
 /*  13 */ "expr ::= STOPWORD",
 /*  14 */ "termlist ::= term term",
 /*  15 */ "termlist ::= termlist term",
 /*  16 */ "termlist ::= termlist STOPWORD",
 /*  17 */ "expr ::= MINUS expr",
 /*  18 */ "expr ::= TILDE expr",
 /*  19 */ "expr ::= term STAR",
 /*  20 */ "modifier ::= MODIFIER",
 /*  21 */ "modifierlist ::= modifier OR term",
 /*  22 */ "modifierlist ::= modifierlist OR term",
 /*  23 */ "expr ::= modifier COLON tag_list",
 /*  24 */ "tag_list ::= LB term",
 /*  25 */ "tag_list ::= LB termlist",
 /*  26 */ "tag_list ::= tag_list OR term",
 /*  27 */ "tag_list ::= tag_list OR termlist",
 /*  28 */ "tag_list ::= tag_list RB",
 /*  29 */ "expr ::= modifier COLON numeric_range",
 /*  30 */ "numeric_range ::= LSQB num num RSQB",
 /*  31 */ "expr ::= modifier COLON geo_filter",
 /*  32 */ "geo_filter ::= LSQB num num num TERM RSQB",
 /*  33 */ "num ::= NUMBER",
 /*  34 */ "num ::= LP num",
 /*  35 */ "num ::= MINUS num",
 /*  36 */ "term ::= TERM",
 /*  37 */ "term ::= NUMBER",
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
    case 21: /* error */
    case 28: /* num */
    case 30: /* query */
    case 31: /* modifier */
    case 32: /* term */
{
#line 66 "parser.y"
 
#line 572 "parser.c"
}
      break;
    case 22: /* expr */
    case 23: /* termlist */
    case 24: /* union */
    case 25: /* tag_list */
{
#line 69 "parser.y"
 QueryNode_Free((yypminor->yy53)); 
#line 582 "parser.c"
}
      break;
    case 26: /* geo_filter */
{
#line 81 "parser.y"
 GeoFilter_Free((yypminor->yy10)); 
#line 589 "parser.c"
}
      break;
    case 27: /* modifierlist */
{
#line 84 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy42)); i++) {
        char *s;
        Vector_Get((yypminor->yy42), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy42)); 

#line 603 "parser.c"
}
      break;
    case 29: /* numeric_range */
{
#line 96 "parser.y"

    NumericFilter_Free((yypminor->yy48));

#line 612 "parser.c"
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
  { 30, 1 },
  { 30, 0 },
  { 22, 2 },
  { 22, 1 },
  { 24, 3 },
  { 24, 3 },
  { 22, 3 },
  { 22, 3 },
  { 22, 3 },
  { 22, 3 },
  { 22, 3 },
  { 32, 3 },
  { 22, 1 },
  { 22, 1 },
  { 23, 2 },
  { 23, 2 },
  { 23, 2 },
  { 22, 2 },
  { 22, 2 },
  { 22, 2 },
  { 31, 1 },
  { 27, 3 },
  { 27, 3 },
  { 22, 3 },
  { 25, 2 },
  { 25, 2 },
  { 25, 3 },
  { 25, 3 },
  { 25, 2 },
  { 22, 3 },
  { 29, 4 },
  { 22, 3 },
  { 26, 6 },
  { 28, 1 },
  { 28, 2 },
  { 28, 2 },
  { 32, 1 },
  { 32, 1 },
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
#line 100 "parser.y"
{ 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = yymsp[0].minor.yy53;
 
}
#line 973 "parser.c"
        break;
      case 1: /* query ::= */
#line 106 "parser.y"
{
    ctx->root = NULL;
}
#line 980 "parser.c"
        break;
      case 2: /* expr ::= expr expr */
#line 114 "parser.y"
{

    // if both yymsp[-1].minor.yy53 and yymsp[0].minor.yy53 are null we return null
    if (yymsp[-1].minor.yy53 == NULL && yymsp[0].minor.yy53 == NULL) {
        yylhsminor.yy53 = NULL;
    } else {

        if (yymsp[-1].minor.yy53 && yymsp[-1].minor.yy53->type == QN_PHRASE && yymsp[-1].minor.yy53->pn.exact == 0 && 
            yymsp[-1].minor.yy53->fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy53 = yymsp[-1].minor.yy53;
        } else {
            yylhsminor.yy53 = NewPhraseNode(0);
            QueryPhraseNode_AddChild(yylhsminor.yy53, yymsp[-1].minor.yy53);
        } 
        QueryPhraseNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53);
    }
}
#line 1001 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 3: /* expr ::= union */
#line 137 "parser.y"
{
    yylhsminor.yy53 = yymsp[0].minor.yy53;
}
#line 1009 "parser.c"
  yymsp[0].minor.yy53 = yylhsminor.yy53;
        break;
      case 4: /* union ::= expr OR expr */
#line 141 "parser.y"
{
    
    if (yymsp[-2].minor.yy53 && yymsp[-2].minor.yy53->type == QN_UNION && yymsp[-2].minor.yy53->fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy53 =yymsp[-2].minor.yy53;
    } else {
        yylhsminor.yy53 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[-2].minor.yy53);
    } 
    QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53); 
    
}
#line 1025 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 5: /* union ::= union OR expr */
#line 153 "parser.y"
{
    
    yylhsminor.yy53 = yymsp[-2].minor.yy53;

    QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53); 

}
#line 1037 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 6: /* expr ::= modifier COLON expr */
#line 165 "parser.y"
{
    if (yymsp[0].minor.yy53 == NULL) {
        yylhsminor.yy53 = NULL;
    } else {
        if (ctx->sctx->spec) {
            yymsp[0].minor.yy53->fieldMask = IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
        }
        yylhsminor.yy53 = yymsp[0].minor.yy53; 
    }
}
#line 1052 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 7: /* expr ::= modifier COLON TERM */
#line 176 "parser.y"
{


    yylhsminor.yy53 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
    if (ctx->sctx->spec) {
        yylhsminor.yy53->fieldMask = IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
    }
}
#line 1065 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
#line 185 "parser.y"
{
    
    if (yymsp[0].minor.yy53 == NULL) {
        yylhsminor.yy53 = NULL;
    } else {
        yymsp[0].minor.yy53->fieldMask = 0;
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy42); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy42, i, &p);
                yymsp[0].minor.yy53->fieldMask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        Vector_Free(yymsp[-2].minor.yy42);
        yylhsminor.yy53=yymsp[0].minor.yy53;
    }
}
#line 1088 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 9: /* expr ::= LP expr RP */
#line 204 "parser.y"
{
    yymsp[-2].minor.yy53 = yymsp[-1].minor.yy53;
}
#line 1096 "parser.c"
        break;
      case 10: /* expr ::= QUOTE termlist QUOTE */
#line 212 "parser.y"
{
    yymsp[-1].minor.yy53->pn.exact =1;
    yymsp[-2].minor.yy53 = yymsp[-1].minor.yy53;
}
#line 1104 "parser.c"
        break;
      case 11: /* term ::= QUOTE term QUOTE */
#line 217 "parser.y"
{
    yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
}
#line 1111 "parser.c"
        break;
      case 12: /* expr ::= term */
#line 221 "parser.y"
{
        yylhsminor.yy53 = NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
#line 1118 "parser.c"
  yymsp[0].minor.yy53 = yylhsminor.yy53;
        break;
      case 13: /* expr ::= STOPWORD */
#line 225 "parser.y"
{
    yymsp[0].minor.yy53 = NULL;
}
#line 1126 "parser.c"
        break;
      case 14: /* termlist ::= term term */
#line 229 "parser.y"
{
    
    yylhsminor.yy53 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1));
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));

}
#line 1137 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 15: /* termlist ::= termlist term */
#line 236 "parser.y"
{
    yylhsminor.yy53 = yymsp[-1].minor.yy53;
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1146 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 16: /* termlist ::= termlist STOPWORD */
      case 28: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==28);
#line 241 "parser.y"
{
    yylhsminor.yy53 = yymsp[-1].minor.yy53;
}
#line 1155 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 17: /* expr ::= MINUS expr */
#line 249 "parser.y"
{ 
    yymsp[-1].minor.yy53 = NewNotNode(yymsp[0].minor.yy53);
}
#line 1163 "parser.c"
        break;
      case 18: /* expr ::= TILDE expr */
#line 257 "parser.y"
{ 
    yymsp[-1].minor.yy53 = NewOptionalNode(yymsp[0].minor.yy53);
}
#line 1170 "parser.c"
        break;
      case 19: /* expr ::= term STAR */
#line 265 "parser.y"
{
    yylhsminor.yy53 = NewPrefixNode(ctx, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len);
}
#line 1177 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 20: /* modifier ::= MODIFIER */
#line 273 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1185 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 21: /* modifierlist ::= modifier OR term */
#line 277 "parser.y"
{
    yylhsminor.yy42 = NewVector(char *, 2);
    char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy42, s);
    s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy42, s);
}
#line 1197 "parser.c"
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 22: /* modifierlist ::= modifierlist OR term */
#line 285 "parser.y"
{
    char *s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy42, s);
    yylhsminor.yy42 = yymsp[-2].minor.yy42;
}
#line 1207 "parser.c"
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 23: /* expr ::= modifier COLON tag_list */
#line 295 "parser.y"
{
    if (!yymsp[0].minor.yy53) {
        yylhsminor.yy53= NULL;
    } else {
        yylhsminor.yy53 = NewTagNode(strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len), yymsp[-2].minor.yy0.len);
        QueryTagNode_AddChildren(yylhsminor.yy53, yymsp[0].minor.yy53->pn.children, yymsp[0].minor.yy53->pn.numChildren);
        
        // Set the children count on yymsp[0].minor.yy53 to 0 so they won't get recursively free'd
        yymsp[0].minor.yy53->pn.numChildren = 0;
        QueryNode_Free(yymsp[0].minor.yy53);
    }
}
#line 1224 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 24: /* tag_list ::= LB term */
#line 308 "parser.y"
{
    yymsp[-1].minor.yy53 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy53, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
#line 1233 "parser.c"
        break;
      case 25: /* tag_list ::= LB termlist */
#line 314 "parser.y"
{
    yymsp[-1].minor.yy53 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy53, yymsp[0].minor.yy53);
}
#line 1241 "parser.c"
        break;
      case 26: /* tag_list ::= tag_list OR term */
#line 319 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy53, NewTokenNode(ctx, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy53 = yymsp[-2].minor.yy53;
}
#line 1249 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 27: /* tag_list ::= tag_list OR termlist */
#line 323 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-2].minor.yy53, yymsp[0].minor.yy53);
    yylhsminor.yy53 = yymsp[-2].minor.yy53;
}
#line 1258 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 29: /* expr ::= modifier COLON numeric_range */
#line 337 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy48->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy53 = NewNumericNode(yymsp[0].minor.yy48);
}
#line 1268 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 30: /* numeric_range ::= LSQB num num RSQB */
#line 343 "parser.y"
{
    yymsp[-3].minor.yy48 = NewNumericFilter(yymsp[-2].minor.yy47.num, yymsp[-1].minor.yy47.num, yymsp[-2].minor.yy47.inclusive, yymsp[-1].minor.yy47.inclusive);
}
#line 1276 "parser.c"
        break;
      case 31: /* expr ::= modifier COLON geo_filter */
#line 351 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy10->property = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy53 = NewGeofilterNode(yymsp[0].minor.yy10);
}
#line 1285 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 32: /* geo_filter ::= LSQB num num num TERM RSQB */
#line 357 "parser.y"
{
    yymsp[-5].minor.yy10 = NewGeoFilter(yymsp[-4].minor.yy47.num, yymsp[-3].minor.yy47.num, yymsp[-2].minor.yy47.num, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(yymsp[-5].minor.yy10, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}
#line 1298 "parser.c"
        break;
      case 33: /* num ::= NUMBER */
#line 370 "parser.y"
{
    yylhsminor.yy47.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy47.inclusive = 1;
}
#line 1306 "parser.c"
  yymsp[0].minor.yy47 = yylhsminor.yy47;
        break;
      case 34: /* num ::= LP num */
#line 375 "parser.y"
{
    yymsp[-1].minor.yy47=yymsp[0].minor.yy47;
    yymsp[-1].minor.yy47.inclusive = 0;
}
#line 1315 "parser.c"
        break;
      case 35: /* num ::= MINUS num */
#line 380 "parser.y"
{
    yymsp[0].minor.yy47.num = -yymsp[0].minor.yy47.num;
    yymsp[-1].minor.yy47 = yymsp[0].minor.yy47;
}
#line 1323 "parser.c"
        break;
      case 36: /* term ::= TERM */
      case 37: /* term ::= NUMBER */ yytestcase(yyruleno==37);
#line 385 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1331 "parser.c"
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
#line 21 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1400 "parser.c"
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