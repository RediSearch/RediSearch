/* Driver template for the LEMON parser generator.
** The author disclaims copyright to this source code.
*/
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */
#include <stdio.h>
#line 22 "parser.y"
   

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
   
#line 27 "parser.c"
/* Next is all token values, in a form suitable for use by makeheaders.
** This section will be null unless lemon is run with the -m switch.
*/
/* 
** These constants (all generated automatically by the parser generator)
** specify the various kinds of tokens (terminals) that the parser
** understands. 
**
** Each symbol here is a terminal symbol in the grammar.
*/
/* Make sure the INTERFACE macro is defined.
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/* The next thing included is series of defines which control
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 terminals
**                       and nonterminals.  "int" is used otherwise.
**    YYNOCODE           is a number of type YYCODETYPE which corresponds
**                       to no legal terminal or nonterminal number.  This
**                       number is used to fill in empty slots of the hash 
**                       table.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       have fall-back values which should be used if the
**                       original value of the token will not parse.
**    YYACTIONTYPE       is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 rules and
**                       states combined.  "int" is used otherwise.
**    ParseTOKENTYPE     is the data type used for minor tokens given 
**                       directly to the parser from the tokenizer.
**    YYMINORTYPE        is the data type used for all minor tokens.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for base tokens is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
*/
#define YYCODETYPE unsigned char
#define YYNOCODE 26
#define YYACTIONTYPE unsigned char
#define ParseTOKENTYPE QueryToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  NumericFilter * yy30;
  Vector* yy36;
  QueryNode * yy47;
  RangeNumber yy49;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  parseCtx *ctx ;
#define ParseARG_PDECL , parseCtx *ctx 
#define ParseARG_FETCH  parseCtx *ctx  = yypParser->ctx 
#define ParseARG_STORE yypParser->ctx  = ctx 
#define YYNSTATE 49
#define YYNRULE 29
#define YY_NO_ACTION      (YYNSTATE+YYNRULE+2)
#define YY_ACCEPT_ACTION  (YYNSTATE+YYNRULE+1)
#define YY_ERROR_ACTION   (YYNSTATE+YYNRULE)

/* The yyzerominor constant is used to initialize instances of
** YYMINORTYPE objects to zero. */
static const YYMINORTYPE yyzerominor = { 0 };

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
**   0 <= N < YYNSTATE                  Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   YYNSTATE <= N < YYNSTATE+YYNRULE   Reduce by rule N-YYNSTATE.
**
**   N == YYNSTATE+YYNRULE              A syntax error has occurred.
**
**   N == YYNSTATE+YYNRULE+1            The parser accepts its input.
**
**   N == YYNSTATE+YYNRULE+2            No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as
**
**      yy_action[ yy_shift_ofst[S] + X ]
**
** If the index value yy_shift_ofst[S]+X is out of range or if the value
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
** is equal to YY_SHIFT_USE_DFLT, it means that the action is not in the table
** and that yy_default[S] should be used instead.  
**
** The formula above is for computing the action when the lookahead is
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
*/
static const YYACTIONTYPE yy_action[] = {
 /*     0 */     2,   48,   15,   28,   36,   29,    9,    8,    2,   46,
 /*    10 */    15,   28,   36,   29,    9,    8,   25,   13,   10,   20,
 /*    20 */    22,   21,   45,   14,   79,    5,    6,   24,   11,   50,
 /*    30 */    28,    2,   29,   15,   28,   36,   29,    9,    8,   49,
 /*    40 */    33,    2,   47,   15,   28,   36,   29,    9,    8,   51,
 /*    50 */    37,    2,   38,   15,   28,   36,   29,    9,    8,    2,
 /*    60 */    31,   15,   28,   36,   29,    9,    8,   20,   22,   21,
 /*    70 */    27,   44,   23,    4,    3,   24,   11,    2,   18,   15,
 /*    80 */    28,   36,   29,    9,    8,   20,   22,   21,    7,   43,
 /*    90 */    19,   17,   42,   24,   11,   20,   22,   21,   20,   22,
 /*   100 */    21,    1,   34,   24,   11,   26,   24,   11,   20,   22,
 /*   110 */    21,   20,   22,   21,   40,   30,   24,   11,   39,   24,
 /*   120 */    11,   20,   22,   21,   41,   28,   16,   29,   35,   24,
 /*   130 */    11,   12,   80,   80,   80,   32,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     2,    3,    4,    5,    6,    7,    8,    9,    2,   18,
 /*    10 */     4,    5,    6,    7,    8,    9,   17,    2,   12,   15,
 /*    20 */    16,   17,    7,    8,   20,   21,   22,   23,   24,    0,
 /*    30 */     5,    2,    7,    4,    5,    6,    7,    8,    9,    0,
 /*    40 */    17,    2,   18,    4,    5,    6,    7,    8,    9,    0,
 /*    50 */    17,    2,   17,    4,    5,    6,    7,    8,    9,    2,
 /*    60 */     3,    4,    5,    6,    7,    8,    9,   15,   16,   17,
 /*    70 */    17,   13,   18,   21,   22,   23,   24,    2,    1,    4,
 /*    80 */     5,    6,    7,    8,    9,   15,   16,   17,   11,   19,
 /*    90 */     1,    1,   22,   23,   24,   15,   16,   17,   15,   16,
 /*   100 */    17,   11,   22,   23,   24,   22,   23,   24,   15,   16,
 /*   110 */    17,   15,   16,   17,   17,   22,   23,   24,   22,   23,
 /*   120 */    24,   15,   16,   17,    4,    5,    1,    7,   22,   23,
 /*   130 */    24,   18,   25,   25,   25,   10,
};
#define YY_SHIFT_USE_DFLT (-3)
#define YY_SHIFT_MAX 24
static const signed char yy_shift_ofst[] = {
 /*     0 */    49,    6,   75,   57,   -2,   39,   29,   75,   75,   75,
 /*    10 */    15,  120,   15,   15,   15,   25,   25,   25,   25,   25,
 /*    20 */    77,  125,   90,   58,   89,
};
#define YY_REDUCE_USE_DFLT (-10)
#define YY_REDUCE_MAX 19
static const signed char yy_reduce_ofst[] = {
 /*     0 */     4,   70,   52,   93,   83,   83,   93,   96,  106,   80,
 /*    10 */   113,   97,   54,   -9,   24,   -1,   23,   33,   35,   53,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */    78,   78,   78,   78,   78,   78,   78,   78,   78,   78,
 /*    10 */    78,   78,   78,   78,   78,   78,   78,   78,   78,   78,
 /*    20 */    78,   57,   78,   78,   54,   66,   53,   69,   76,   77,
 /*    30 */    52,   55,   63,   70,   58,   59,   60,   61,   62,   64,
 /*    40 */    67,   68,   65,   71,   72,   73,   74,   75,   56,
};
#define YY_SZ_ACTTAB (int)(sizeof(yy_action)/sizeof(yy_action[0]))

/* The next table maps tokens into fallback tokens.  If a construct
** like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
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
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  int yyidx;                    /* Index of top element in stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyidxMax;                 /* Maximum value of yyidx */
#endif
  int yyerrcnt;                 /* Shifts left before out of the error */
  ParseARG_SDECL                /* A place to hold %extra_argument */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
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
void ParseTrace(FILE *TraceFILE, char *zTracePrompt){
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
  "$",             "OR",            "LP",            "RP",          
  "QUOTE",         "TERM",          "MODIFIER",      "NUMBER",      
  "MINUS",         "TILDE",         "STAR",          "COLON",       
  "LSQB",          "RSQB",          "error",         "modifierlist",
  "modifier",      "term",          "num",           "numeric_range",
  "query",         "exprlist",      "expr",          "union",       
  "exact",       
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "query ::= exprlist",
 /*   1 */ "query ::= expr",
 /*   2 */ "query ::=",
 /*   3 */ "exprlist ::= expr expr",
 /*   4 */ "exprlist ::= exprlist expr",
 /*   5 */ "expr ::= union",
 /*   6 */ "expr ::= LP expr RP",
 /*   7 */ "expr ::= LP exprlist RP",
 /*   8 */ "expr ::= term",
 /*   9 */ "expr ::= MINUS expr",
 /*  10 */ "expr ::= TILDE expr",
 /*  11 */ "modifier ::= MODIFIER",
 /*  12 */ "modifierlist ::= modifier OR term",
 /*  13 */ "modifierlist ::= modifierlist OR term",
 /*  14 */ "expr ::= term STAR",
 /*  15 */ "expr ::= modifierlist COLON expr",
 /*  16 */ "expr ::= modifier COLON expr",
 /*  17 */ "exact ::= QUOTE term",
 /*  18 */ "exact ::= exact term",
 /*  19 */ "expr ::= exact QUOTE",
 /*  20 */ "union ::= union OR term",
 /*  21 */ "union ::= term OR term",
 /*  22 */ "expr ::= modifier COLON numeric_range",
 /*  23 */ "numeric_range ::= LSQB num num RSQB",
 /*  24 */ "num ::= NUMBER",
 /*  25 */ "num ::= LP num",
 /*  26 */ "num ::= MINUS num",
 /*  27 */ "term ::= TERM",
 /*  28 */ "term ::= NUMBER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.
*/
static void yyGrowStack(yyParser *p){
  int newSize;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  if( pNew ){
    p->yystack = pNew;
    p->yystksz = newSize;
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows to %d entries!\n",
              yyTracePrompt, p->yystksz);
    }
#endif
  }
}
#endif

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
void *ParseAlloc(void *(*mallocProc)(size_t)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (size_t)sizeof(yyParser) );
  if( pParser ){
    pParser->yyidx = -1;
#ifdef YYTRACKMAXSTACKDEPTH
    pParser->yyidxMax = 0;
#endif
#if YYSTACKDEPTH<=0
    pParser->yystack = NULL;
    pParser->yystksz = 0;
    yyGrowStack(pParser);
#endif
  }
  return pParser;
}

/* The following function deletes the value associated with a
** symbol.  The symbol can be either a terminal or nonterminal.
** "yymajor" is the symbol code, and "yypminor" is a pointer to
** the value.
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
    ** which appear on the RHS of the rule, but which are not used
    ** inside the C code.
    */
      /* Default NON-TERMINAL Destructor */
    case 14: /* error */
    case 20: /* query */
    case 21: /* exprlist */
    case 22: /* expr */
    case 23: /* union */
    case 24: /* exact */
{
#line 44 "parser.y"
 QueryNode_Free((yypminor->yy47)); 
#line 441 "parser.c"
}
      break;
    case 15: /* modifierlist */
{
#line 60 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy36)); i++) {
        char *s;
        Vector_Get((yypminor->yy36), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy36)); 

#line 455 "parser.c"
}
      break;
    case 16: /* modifier */
    case 17: /* term */
{
#line 59 "parser.y"
  
#line 463 "parser.c"
}
      break;
    case 18: /* num */
{
#line 54 "parser.y"
 
#line 470 "parser.c"
}
      break;
    case 19: /* numeric_range */
{
#line 68 "parser.y"

    NumericFilter_Free((yypminor->yy30));

#line 479 "parser.c"
}
      break;
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
**
** Return the major token number for the symbol popped.
*/
static int yy_pop_parser_stack(yyParser *pParser){
  YYCODETYPE yymajor;
  yyStackEntry *yytos = &pParser->yystack[pParser->yyidx];

  if( pParser->yyidx<0 ) return 0;
#ifndef NDEBUG
  if( yyTraceFILE && pParser->yyidx>=0 ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yymajor = yytos->major;
  yy_destructor(pParser, yymajor, &yytos->minor);
  pParser->yyidx--;
  return yymajor;
}

/* 
** Deallocate and destroy a parser.  Destructors are all called for
** all stack elements before shutting the parser down.
**
** Inputs:
** <ul>
** <li>  A pointer to the parser.  This should be a pointer
**       obtained from ParseAlloc.
** <li>  A pointer to a function used to reclaim memory obtained
**       from malloc.
** </ul>
*/
void ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
  yyParser *pParser = (yyParser*)p;
  if( pParser==0 ) return;
  while( pParser->yyidx>=0 ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  free(pParser->yystack);
#endif
  (*freeProc)((void*)pParser);
}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyidxMax;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
*/
static int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if( stateno>YY_SHIFT_MAX || (i = yy_shift_ofst[stateno])==YY_SHIFT_USE_DFLT ){
    return yy_default[stateno];
  }
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
  if( i<0 || i>=YY_SZ_ACTTAB || yy_lookahead[i]!=iLookAhead ){
    if( iLookAhead>0 ){
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
        return yy_find_shift_action(pParser, iFallback);
      }
#endif
#ifdef YYWILDCARD
      {
        int j = i - iLookAhead + YYWILDCARD;
        if( j>=0 && j<YY_SZ_ACTTAB && yy_lookahead[j]==YYWILDCARD ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
    }
    return yy_default[stateno];
  }else{
    return yy_action[i];
  }
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
*/
static int yy_find_reduce_action(
  int stateno,              /* Current state number */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
#ifdef YYERRORSYMBOL
  if( stateno>YY_REDUCE_MAX ){
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_MAX );
#endif
  i = yy_reduce_ofst[stateno];
  assert( i!=YY_REDUCE_USE_DFLT );
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if( i<0 || i>=YY_SZ_ACTTAB || yy_lookahead[i]!=iLookAhead ){
    return yy_default[stateno];
  }
#else
  assert( i>=0 && i<YY_SZ_ACTTAB );
  assert( yy_lookahead[i]==iLookAhead );
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser, YYMINORTYPE *yypMinor){
   ParseARG_FETCH;
   yypParser->yyidx--;
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
}

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
  YYMINORTYPE *yypMinor         /* Pointer to the minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yyidx++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( yypParser->yyidx>yypParser->yyidxMax ){
    yypParser->yyidxMax = yypParser->yyidx;
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yyidx>=YYSTACKDEPTH ){
    yyStackOverflow(yypParser, yypMinor);
    return;
  }
#else
  if( yypParser->yyidx>=yypParser->yystksz ){
    yyGrowStack(yypParser);
    if( yypParser->yyidx>=yypParser->yystksz ){
      yyStackOverflow(yypParser, yypMinor);
      return;
    }
  }
#endif
  yytos = &yypParser->yystack[yypParser->yyidx];
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor = *yypMinor;
#ifndef NDEBUG
  if( yyTraceFILE && yypParser->yyidx>0 ){
    int i;
    fprintf(yyTraceFILE,"%sShift %d\n",yyTracePrompt,yyNewState);
    fprintf(yyTraceFILE,"%sStack:",yyTracePrompt);
    for(i=1; i<=yypParser->yyidx; i++)
      fprintf(yyTraceFILE," %s",yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"\n");
  }
#endif
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 20, 1 },
  { 20, 1 },
  { 20, 0 },
  { 21, 2 },
  { 21, 2 },
  { 22, 1 },
  { 22, 3 },
  { 22, 3 },
  { 22, 1 },
  { 22, 2 },
  { 22, 2 },
  { 16, 1 },
  { 15, 3 },
  { 15, 3 },
  { 22, 2 },
  { 22, 3 },
  { 22, 3 },
  { 24, 2 },
  { 24, 2 },
  { 22, 2 },
  { 23, 3 },
  { 23, 3 },
  { 22, 3 },
  { 19, 4 },
  { 18, 1 },
  { 18, 2 },
  { 18, 2 },
  { 17, 1 },
  { 17, 1 },
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  int yyruleno                 /* Number of the rule by which to reduce */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  YYMINORTYPE yygotominor;        /* The LHS of the rule reduced */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = &yypParser->yystack[yypParser->yyidx];
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno>=0 
        && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    fprintf(yyTraceFILE, "%sReduce [%s].\n", yyTracePrompt,
      yyRuleName[yyruleno]);
  }
#endif /* NDEBUG */

  /* Silence complaints from purify about yygotominor being uninitialized
  ** in some cases when it is copied into the stack after the following
  ** switch.  yygotominor is uninitialized when a rule reduces that does
  ** not set the value of its left-hand side nonterminal.  Leaving the
  ** value of the nonterminal uninitialized is utterly harmless as long
  ** as the value is never used.  So really the only thing this code
  ** accomplishes is to quieten purify.  
  **
  ** 2007-01-16:  The wireshark project (www.wireshark.org) reports that
  ** without this code, their parser segfaults.  I'm not sure what there
  ** parser is doing to make this happen.  This is the second bug report
  ** from wireshark this week.  Clearly they are stressing Lemon in ways
  ** that it has not been previously stressed...  (SQLite ticket #2172)
  */
  /*memset(&yygotominor, 0, sizeof(yygotominor));*/
  yygotominor = yyzerominor;


  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
      case 0: /* query ::= exprlist */
      case 1: /* query ::= expr */ yytestcase(yyruleno==1);
#line 71 "parser.y"
{ ctx->root = yymsp[0].minor.yy47; }
#line 795 "parser.c"
        break;
      case 2: /* query ::= */
#line 75 "parser.y"
{ ctx->root = NULL; }
#line 800 "parser.c"
        break;
      case 3: /* exprlist ::= expr expr */
#line 77 "parser.y"
{
    yygotominor.yy47 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yygotominor.yy47, yymsp[-1].minor.yy47);
    QueryPhraseNode_AddChild(yygotominor.yy47, yymsp[0].minor.yy47);
}
#line 809 "parser.c"
        break;
      case 4: /* exprlist ::= exprlist expr */
#line 83 "parser.y"
{
    yygotominor.yy47 = yymsp[-1].minor.yy47;
    QueryPhraseNode_AddChild(yygotominor.yy47, yymsp[0].minor.yy47);
}
#line 817 "parser.c"
        break;
      case 5: /* expr ::= union */
#line 89 "parser.y"
{  yygotominor.yy47 = yymsp[0].minor.yy47;}
#line 822 "parser.c"
        break;
      case 6: /* expr ::= LP expr RP */
      case 7: /* expr ::= LP exprlist RP */ yytestcase(yyruleno==7);
#line 90 "parser.y"
{ yygotominor.yy47 = yymsp[-1].minor.yy47; }
#line 828 "parser.c"
        break;
      case 8: /* expr ::= term */
#line 92 "parser.y"
{ 
 yygotominor.yy47 = NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len);  
}
#line 835 "parser.c"
        break;
      case 9: /* expr ::= MINUS expr */
#line 96 "parser.y"
{
    yygotominor.yy47 = NewNotNode(yymsp[0].minor.yy47);
}
#line 842 "parser.c"
        break;
      case 10: /* expr ::= TILDE expr */
#line 100 "parser.y"
{
    yygotominor.yy47 = NewOptionalNode(yymsp[0].minor.yy47);
}
#line 849 "parser.c"
        break;
      case 11: /* modifier ::= MODIFIER */
#line 105 "parser.y"
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
#line 854 "parser.c"
        break;
      case 12: /* modifierlist ::= modifier OR term */
#line 107 "parser.y"
{ 
    yygotominor.yy36 = NewVector(char *, 2);
    char *s = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yygotominor.yy36, s);
    s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yygotominor.yy36, s);
}
#line 865 "parser.c"
        break;
      case 13: /* modifierlist ::= modifierlist OR term */
#line 115 "parser.y"
{
    char *s = strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy36, s);
    yygotominor.yy36 = yymsp[-2].minor.yy36;
}
#line 874 "parser.c"
        break;
      case 14: /* expr ::= term STAR */
#line 121 "parser.y"
{
    yygotominor.yy47 = NewPrefixNode(ctx->q, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len);
}
#line 881 "parser.c"
        break;
      case 15: /* expr ::= modifierlist COLON expr */
#line 125 "parser.y"
{
    yymsp[0].minor.yy47->fieldMask = 0;
    for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy36); i++) {
        char *p;
        Vector_Get(yymsp[-2].minor.yy36, i, &p);

        if (ctx->q->ctx && ctx->q->ctx->spec) {
            yymsp[0].minor.yy47->fieldMask |= IndexSpec_GetFieldBit(ctx->q->ctx->spec, p, strlen(p)); 
        }
        free(p);
    }
    Vector_Free(yymsp[-2].minor.yy36);
    yygotominor.yy47=yymsp[0].minor.yy47;
    
}
#line 900 "parser.c"
        break;
      case 16: /* expr ::= modifier COLON expr */
#line 141 "parser.y"
{
    // gets the field mask from the query's spec. 
    // TODO: Avoid leaky abstraction here
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        yymsp[0].minor.yy47->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
    }
    //free((char *)yymsp[-2].minor.yy0.s);
    yygotominor.yy47 = yymsp[0].minor.yy47; 
}
#line 913 "parser.c"
        break;
      case 17: /* exact ::= QUOTE term */
#line 151 "parser.y"
{
    yygotominor.yy47 = NewPhraseNode(1);
    QueryPhraseNode_AddChild(yygotominor.yy47, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));
}
#line 921 "parser.c"
        break;
      case 18: /* exact ::= exact term */
#line 156 "parser.y"
{
    QueryPhraseNode_AddChild(yymsp[-1].minor.yy47, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));
    yygotominor.yy47 = yymsp[-1].minor.yy47;
}
#line 929 "parser.c"
        break;
      case 19: /* expr ::= exact QUOTE */
#line 161 "parser.y"
{
    yygotominor.yy47 = yymsp[-1].minor.yy47;
}
#line 936 "parser.c"
        break;
      case 20: /* union ::= union OR term */
#line 165 "parser.y"
{
    QueryUnionNode_AddChild(yymsp[-2].minor.yy47, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));
    yygotominor.yy47 = yymsp[-2].minor.yy47;
}
#line 944 "parser.c"
        break;
      case 21: /* union ::= term OR term */
#line 171 "parser.y"
{
    yygotominor.yy47 = NewUnionNode();
    QueryUnionNode_AddChild(yygotominor.yy47, NewTokenNode(ctx->q, strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len), yymsp[-2].minor.yy0.len));
    QueryUnionNode_AddChild(yygotominor.yy47, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));
}
#line 953 "parser.c"
        break;
      case 22: /* expr ::= modifier COLON numeric_range */
#line 178 "parser.y"
{
    yymsp[0].minor.yy30->fieldName = strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yygotominor.yy47 = NewNumericNode(yymsp[0].minor.yy30);
}
#line 961 "parser.c"
        break;
      case 23: /* numeric_range ::= LSQB num num RSQB */
#line 183 "parser.y"
{
    yygotominor.yy30 = NewNumericFilter(yymsp[-2].minor.yy49.num, yymsp[-1].minor.yy49.num, yymsp[-2].minor.yy49.inclusive, yymsp[-1].minor.yy49.inclusive);
}
#line 968 "parser.c"
        break;
      case 24: /* num ::= NUMBER */
#line 187 "parser.y"
{
    yygotominor.yy49.num = yymsp[0].minor.yy0.numval;
    yygotominor.yy49.inclusive = 1;
}
#line 976 "parser.c"
        break;
      case 25: /* num ::= LP num */
#line 192 "parser.y"
{
    yygotominor.yy49=yymsp[0].minor.yy49;
    yygotominor.yy49.inclusive = 0;
}
#line 984 "parser.c"
        break;
      case 26: /* num ::= MINUS num */
#line 196 "parser.y"
{
    yymsp[0].minor.yy49.num = -yymsp[0].minor.yy49.num;
    yygotominor.yy49 = yymsp[0].minor.yy49;
}
#line 992 "parser.c"
        break;
      case 27: /* term ::= TERM */
      case 28: /* term ::= NUMBER */ yytestcase(yyruleno==28);
#line 201 "parser.y"
{
    yygotominor.yy0 = yymsp[0].minor.yy0;
}
#line 1000 "parser.c"
        break;
      default:
        break;
  };
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yypParser->yyidx -= yysize;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact < YYNSTATE ){
#ifdef NDEBUG
    /* If we are not debugging and the reduce action popped at least
    ** one element off the stack, then we can push the new element back
    ** onto the stack here, and skip the stack overflow test in yy_shift().
    ** That gives a significant speed improvement. */
    if( yysize ){
      yypParser->yyidx++;
      yymsp -= yysize-1;
      yymsp->stateno = (YYACTIONTYPE)yyact;
      yymsp->major = (YYCODETYPE)yygoto;
      yymsp->minor = yygotominor;
    }else
#endif
    {
      yy_shift(yypParser,yyact,yygoto,&yygotominor);
    }
  }else{
    assert( yyact == YYNSTATE + YYNRULE + 1 );
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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  YYMINORTYPE yyminor            /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN (yyminor.yy0)
#line 12 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1071 "parser.c"
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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */
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
void Parse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  ParseTOKENTYPE yyminor       /* The value for the token */
  ParseARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  int yyact;            /* The parser action. */
  int yyendofinput;     /* True if we are at the end of input */
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  /* (re)initialize the parser, if necessary */
  yypParser = (yyParser*)yyp;
  if( yypParser->yyidx<0 ){
#if YYSTACKDEPTH<=0
    if( yypParser->yystksz <=0 ){
      /*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
      yyminorunion = yyzerominor;
      yyStackOverflow(yypParser, &yyminorunion);
      return;
    }
#endif
    yypParser->yyidx = 0;
    yypParser->yyerrcnt = -1;
    yypParser->yystack[0].stateno = 0;
    yypParser->yystack[0].major = 0;
  }
  yyminorunion.yy0 = yyminor;
  yyendofinput = (yymajor==0);
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput %s\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact<YYNSTATE ){
      assert( !yyendofinput );  /* Impossible to shift the $ token */
      yy_shift(yypParser,yyact,yymajor,&yyminorunion);
      yypParser->yyerrcnt--;
      yymajor = YYNOCODE;
    }else if( yyact < YYNSTATE + YYNRULE ){
      yy_reduce(yypParser,yyact-YYNSTATE);
    }else{
      assert( yyact == YY_ERROR_ACTION );
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yymx = yypParser->yystack[yypParser->yyidx].major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor,&yyminorunion);
        yymajor = YYNOCODE;
      }else{
         while(
          yypParser->yyidx >= 0 &&
          yymx != YYERRORSYMBOL &&
          (yyact = yy_find_reduce_action(
                        yypParser->yystack[yypParser->yyidx].stateno,
                        YYERRORSYMBOL)) >= YYNSTATE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yyidx < 0 || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          YYMINORTYPE u2;
          u2.YYERRSYMDT = 0;
          yy_shift(yypParser,yyact,YYERRORSYMBOL,&u2);
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
      yy_syntax_error(yypParser,yymajor,yyminorunion);
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
      }
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yyidx>=0 );
  return;
}
