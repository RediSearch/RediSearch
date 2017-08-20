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
#line 28 "parser.y"
   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../rmutil/vector.h"
#include "../query_node.h"

char *strdupcase(const char *s, size_t len) {
  char *ret = strndup(s, len);
  for (int i = 0; i < len; i++) {
    ret[i] = tolower(ret[i]);
  }
  return ret;
}
   
#line 47 "parser.c"
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
#define YYNOCODE 28
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  RangeNumber yy11;
  Vector* yy48;
  QueryNode * yy53;
  NumericFilter * yy54;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  parseCtx *ctx ;
#define ParseARG_PDECL , parseCtx *ctx 
#define ParseARG_FETCH  parseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE             35
#define YYNRULE              28
#define YY_MAX_SHIFT         34
#define YY_MIN_SHIFTREDUCE   51
#define YY_MAX_SHIFTREDUCE   78
#define YY_MIN_REDUCE        79
#define YY_MAX_REDUCE        106
#define YY_ERROR_ACTION      107
#define YY_ACCEPT_ACTION     108
#define YY_NO_ACTION         109
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
#define YY_ACTTAB_COUNT (159)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     5,   77,   17,    9,    6,   78,   69,    7,    7,   60,
 /*    10 */     9,    9,   79,    5,   77,   17,   26,    6,   78,   69,
 /*    20 */    70,    7,   76,   24,    9,   80,    5,   77,   17,   75,
 /*    30 */     6,   78,   69,   31,    7,    8,    5,   32,   17,   21,
 /*    40 */     6,   78,   69,   23,    7,   78,   69,    1,    7,   71,
 /*    50 */    22,    9,   64,   77,   17,   18,    6,   78,   69,   34,
 /*    60 */     7,   78,   65,    9,   73,    5,   77,   17,   62,    6,
 /*    70 */    78,   69,    3,    7,   54,   29,  105,   68,  108,   30,
 /*    80 */    33,   77,   25,   81,   13,   78,   54,   29,   81,   72,
 /*    90 */    81,   30,   33,   14,   81,   54,   29,   81,   19,   74,
 /*   100 */    30,   33,   20,    4,   81,   54,   29,   81,   81,   81,
 /*   110 */    30,   33,   11,   81,   54,   29,   81,   77,   27,   30,
 /*   120 */    33,   78,    2,   81,   54,   29,   81,   81,   81,   30,
 /*   130 */    33,   12,   81,   54,   29,   81,   77,   28,   30,   33,
 /*   140 */    78,   15,   81,   54,   29,   81,   81,   81,   30,   33,
 /*   150 */    16,   81,   54,   29,   81,   81,   81,   30,   33,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,    2,    3,   12,    5,    6,    7,    9,    9,   10,
 /*    10 */    12,   12,    0,    1,    2,    3,   19,    5,    6,    7,
 /*    20 */    26,    9,   22,   26,   12,    0,    1,    2,    3,   22,
 /*    30 */     5,    6,    7,   22,    9,    4,    1,    2,    3,   22,
 /*    40 */     5,    6,    7,   12,    9,    6,    7,    4,    9,   26,
 /*    50 */    15,   12,   26,    2,    3,   12,    5,    6,    7,   26,
 /*    60 */     9,    6,   26,   12,   16,    1,    2,    3,    3,    5,
 /*    70 */     6,    7,   18,    9,   20,   21,   14,   14,   24,   25,
 /*    80 */    26,    2,    3,   27,   18,    6,   20,   21,   27,   23,
 /*    90 */    27,   25,   26,   18,   27,   20,   21,   27,    5,    6,
 /*   100 */    25,   26,    9,   18,   27,   20,   21,   27,   27,   27,
 /*   110 */    25,   26,   18,   27,   20,   21,   27,    2,    3,   25,
 /*   120 */    26,    6,   18,   27,   20,   21,   27,   27,   27,   25,
 /*   130 */    26,   18,   27,   20,   21,   27,    2,    3,   25,   26,
 /*   140 */     6,   18,   27,   20,   21,   27,   27,   27,   25,   26,
 /*   150 */    18,   27,   20,   21,   27,   27,   27,   25,   26,
};
#define YY_SHIFT_USE_DFLT (159)
#define YY_SHIFT_COUNT    (34)
#define YY_SHIFT_MIN      (-9)
#define YY_SHIFT_MAX      (134)
static const short yy_shift_ofst[] = {
 /*     0 */    25,   35,   -1,   12,   51,   64,   64,   64,   64,   64,
 /*    10 */    64,   39,   -2,   -2,   -9,  159,  159,   79,   79,   93,
 /*    20 */    93,   93,   93,   79,  115,   79,  134,   55,   55,   31,
 /*    30 */    43,   48,   62,   63,   65,
};
#define YY_REDUCE_USE_DFLT (-7)
#define YY_REDUCE_COUNT (28)
#define YY_REDUCE_MIN   (-6)
#define YY_REDUCE_MAX   (132)
static const short yy_reduce_ofst[] = {
 /*     0 */    54,   66,   75,   75,   75,   85,   94,  104,  113,  123,
 /*    10 */   132,   75,   75,   75,   75,   75,   75,   -3,   -6,    0,
 /*    20 */     7,   11,   17,   23,   26,   33,   36,   33,   33,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   107,  107,  107,  107,   95,  107,  107,  107,  107,  107,
 /*    10 */   107,   94,   87,   85,   81,   83,   84,  107,  107,  107,
 /*    20 */   107,  107,  107,  107,  107,  107,  107,   90,   89,  107,
 /*    30 */   107,  107,   86,   91,  107,
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
  "$",             "TILDE",         "TERM",          "QUOTE",       
  "COLON",         "MINUS",         "NUMBER",        "MODIFIER",    
  "TERMLIST",      "LP",            "RP",            "AND",         
  "OR",            "ORX",           "STAR",          "LSQB",        
  "RSQB",          "error",         "expr",          "termlist",    
  "union",         "modifierlist",  "num",           "numeric_range",
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
 /*   7 */ "expr ::= modifier COLON TERM",
 /*   8 */ "expr ::= modifierlist COLON expr",
 /*   9 */ "expr ::= LP expr RP",
 /*  10 */ "expr ::= QUOTE termlist QUOTE",
 /*  11 */ "term ::= QUOTE term QUOTE",
 /*  12 */ "expr ::= term",
 /*  13 */ "termlist ::= term term",
 /*  14 */ "termlist ::= termlist term",
 /*  15 */ "expr ::= MINUS expr",
 /*  16 */ "expr ::= TILDE expr",
 /*  17 */ "expr ::= term STAR",
 /*  18 */ "modifier ::= MODIFIER",
 /*  19 */ "modifierlist ::= modifier OR term",
 /*  20 */ "modifierlist ::= modifierlist OR term",
 /*  21 */ "expr ::= modifier COLON numeric_range",
 /*  22 */ "numeric_range ::= LSQB num num RSQB",
 /*  23 */ "num ::= NUMBER",
 /*  24 */ "num ::= LP num",
 /*  25 */ "num ::= MINUS num",
 /*  26 */ "term ::= TERM",
 /*  27 */ "term ::= NUMBER",
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
    case 17: /* error */
    case 22: /* num */
    case 24: /* query */
    case 25: /* modifier */
    case 26: /* term */
{
#line 50 "parser.y"
 
#line 539 "parser.c"
}
      break;
    case 18: /* expr */
    case 19: /* termlist */
    case 20: /* union */
{
#line 53 "parser.y"
 QueryNode_Free((yypminor->yy53)); 
#line 548 "parser.c"
}
      break;
    case 21: /* modifierlist */
{
#line 62 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy48)); i++) {
        char *s;
        Vector_Get((yypminor->yy48), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy48)); 

#line 562 "parser.c"
}
      break;
    case 23: /* numeric_range */
{
#line 74 "parser.y"

    NumericFilter_Free((yypminor->yy54));

#line 571 "parser.c"
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
  { 24, 1 },
  { 24, 0 },
  { 18, 2 },
  { 18, 1 },
  { 20, 3 },
  { 20, 3 },
  { 18, 3 },
  { 18, 3 },
  { 18, 3 },
  { 18, 3 },
  { 18, 3 },
  { 26, 3 },
  { 18, 1 },
  { 19, 2 },
  { 19, 2 },
  { 18, 2 },
  { 18, 2 },
  { 18, 2 },
  { 25, 1 },
  { 21, 3 },
  { 21, 3 },
  { 18, 3 },
  { 23, 4 },
  { 22, 1 },
  { 22, 2 },
  { 22, 2 },
  { 26, 1 },
  { 26, 1 },
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
#line 78 "parser.y"
{ 
 /* If the root is a negative node, we intersect it with a wildcard node */
 if (yymsp[0].minor.yy53->type == QN_NOT) {
    ctx->root = NewPhraseNode(0);
    QueryPhraseNode_AddChild(ctx->root, NewWildcardNode());
    QueryPhraseNode_AddChild(ctx->root, yymsp[0].minor.yy53);
 } else {
    ctx->root = yymsp[0].minor.yy53;
 }

}
#line 927 "parser.c"
        break;
      case 1: /* query ::= */
#line 89 "parser.y"
{
 ctx->root = NULL;
}
#line 934 "parser.c"
        break;
      case 2: /* expr ::= expr expr */
#line 93 "parser.y"
{
    if (yymsp[-1].minor.yy53->type == QN_PHRASE && yymsp[-1].minor.yy53->pn.exact == 0 && 
        yymsp[-1].minor.yy53->fieldMask == RS_FIELDMASK_ALL ) {
        yylhsminor.yy53 = yymsp[-1].minor.yy53;
    } else {
        yylhsminor.yy53 = NewPhraseNode(0);
        QueryPhraseNode_AddChild(yylhsminor.yy53, yymsp[-1].minor.yy53);
    } 
    QueryPhraseNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53);
}
#line 948 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 3: /* expr ::= union */
#line 104 "parser.y"
{
    yylhsminor.yy53 = yymsp[0].minor.yy53;
}
#line 956 "parser.c"
  yymsp[0].minor.yy53 = yylhsminor.yy53;
        break;
      case 4: /* union ::= expr OR expr */
#line 109 "parser.y"
{
    
    if (yymsp[-2].minor.yy53->type == QN_UNION && yymsp[-2].minor.yy53->fieldMask == RS_FIELDMASK_ALL) {
        yylhsminor.yy53 =yymsp[-2].minor.yy53;
    } else {
        yylhsminor.yy53 = NewUnionNode();
        QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[-2].minor.yy53);
    }
    QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53); 
}
#line 971 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 5: /* union ::= union OR expr */
#line 122 "parser.y"
{
    yylhsminor.yy53 = yymsp[-2].minor.yy53;
    QueryUnionNode_AddChild(yylhsminor.yy53, yymsp[0].minor.yy53); 
}
#line 980 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 6: /* expr ::= modifier COLON expr */
#line 131 "parser.y"
{
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        yymsp[0].minor.yy53->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
    }
    yylhsminor.yy53 = yymsp[0].minor.yy53; 
}
#line 991 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 7: /* expr ::= modifier COLON TERM */
#line 138 "parser.y"
{
    yylhsminor.yy53 = NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len);
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        yylhsminor.yy53->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
    }
}
#line 1002 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
#line 147 "parser.y"
{
    
    yymsp[0].minor.yy53->fieldMask = 0;
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy48); i++) {
            char *p;
            Vector_Get(yymsp[-2].minor.yy48, i, &p);
            yymsp[0].minor.yy53->fieldMask |= IndexSpec_GetFieldBit(ctx->q->ctx->spec, p, strlen(p)); 
            free(p);
        }
    }
    Vector_Free(yymsp[-2].minor.yy48);
    yylhsminor.yy53=yymsp[0].minor.yy53;
}
#line 1021 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 9: /* expr ::= LP expr RP */
#line 162 "parser.y"
{
    yymsp[-2].minor.yy53 = yymsp[-1].minor.yy53;
}
#line 1029 "parser.c"
        break;
      case 10: /* expr ::= QUOTE termlist QUOTE */
#line 166 "parser.y"
{
    yymsp[-1].minor.yy53->pn.exact =1;
    yymsp[-2].minor.yy53 = yymsp[-1].minor.yy53;
}
#line 1037 "parser.c"
        break;
      case 11: /* term ::= QUOTE term QUOTE */
#line 171 "parser.y"
{
    yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
}
#line 1044 "parser.c"
        break;
      case 12: /* expr ::= term */
#line 175 "parser.y"
{
    yylhsminor.yy53 = NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len);
}
#line 1051 "parser.c"
  yymsp[0].minor.yy53 = yylhsminor.yy53;
        break;
      case 13: /* termlist ::= term term */
#line 179 "parser.y"
{
    
    yylhsminor.yy53 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len));
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));

}
#line 1063 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 14: /* termlist ::= termlist term */
#line 186 "parser.y"
{
    yylhsminor.yy53 = yymsp[-1].minor.yy53;
    QueryPhraseNode_AddChild(yylhsminor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));

}
#line 1073 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 15: /* expr ::= MINUS expr */
#line 193 "parser.y"
{ 
    yymsp[-1].minor.yy53 = NewNotNode(yymsp[0].minor.yy53);
}
#line 1081 "parser.c"
        break;
      case 16: /* expr ::= TILDE expr */
#line 196 "parser.y"
{ 
    yymsp[-1].minor.yy53 = NewOptionalNode(yymsp[0].minor.yy53);
}
#line 1088 "parser.c"
        break;
      case 17: /* expr ::= term STAR */
#line 200 "parser.y"
{
    yylhsminor.yy53 = NewPrefixNode(ctx->q, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len);
}
#line 1095 "parser.c"
  yymsp[-1].minor.yy53 = yylhsminor.yy53;
        break;
      case 18: /* modifier ::= MODIFIER */
#line 204 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
#line 1103 "parser.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* modifierlist ::= modifier OR term */
#line 208 "parser.y"
{
    yylhsminor.yy48 = NewVector(char *, 2);
    char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy48, s);
    s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy48, s);
}
#line 1115 "parser.c"
  yymsp[-2].minor.yy48 = yylhsminor.yy48;
        break;
      case 20: /* modifierlist ::= modifierlist OR term */
#line 216 "parser.y"
{
    char *s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy48, s);
    yylhsminor.yy48 = yymsp[-2].minor.yy48;
}
#line 1125 "parser.c"
  yymsp[-2].minor.yy48 = yylhsminor.yy48;
        break;
      case 21: /* expr ::= modifier COLON numeric_range */
#line 222 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy54->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy53 = NewNumericNode(yymsp[0].minor.yy54);
}
#line 1135 "parser.c"
  yymsp[-2].minor.yy53 = yylhsminor.yy53;
        break;
      case 22: /* numeric_range ::= LSQB num num RSQB */
#line 228 "parser.y"
{
    yymsp[-3].minor.yy54 = NewNumericFilter(yymsp[-2].minor.yy11.num, yymsp[-1].minor.yy11.num, yymsp[-2].minor.yy11.inclusive, yymsp[-1].minor.yy11.inclusive);
}
#line 1143 "parser.c"
        break;
      case 23: /* num ::= NUMBER */
#line 232 "parser.y"
{
    yylhsminor.yy11.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy11.inclusive = 1;
}
#line 1151 "parser.c"
  yymsp[0].minor.yy11 = yylhsminor.yy11;
        break;
      case 24: /* num ::= LP num */
#line 237 "parser.y"
{
    yymsp[-1].minor.yy11=yymsp[0].minor.yy11;
    yymsp[-1].minor.yy11.inclusive = 0;
}
#line 1160 "parser.c"
        break;
      case 25: /* num ::= MINUS num */
#line 242 "parser.y"
{
    yymsp[0].minor.yy11.num = -yymsp[0].minor.yy11.num;
    yymsp[-1].minor.yy11 = yymsp[0].minor.yy11;
}
#line 1168 "parser.c"
        break;
      case 26: /* term ::= TERM */
      case 27: /* term ::= NUMBER */ yytestcase(yyruleno==27);
#line 247 "parser.y"
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
#line 1176 "parser.c"
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
#line 18 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1245 "parser.c"
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