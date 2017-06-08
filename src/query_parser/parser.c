/* Driver template for the LEMON parser generator.
** The author disclaims copyright to this source code.
*/
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */
#include <stdio.h>
#line 29 "parser.y"
   

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
#define YYNSTATE 50
#define YYNRULE 27
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
 /*     0 */    50,    4,   49,   17,   26,    8,   34,   35,   44,    7,
 /*    10 */     7,   24,    5,    5,    4,   49,   17,   32,    8,   34,
 /*    20 */    35,   25,    7,   40,   37,    5,   51,    4,   49,   17,
 /*    30 */    41,    8,   34,   35,   45,    7,    1,    4,   49,   17,
 /*    40 */    33,    8,   34,   35,   22,    7,   34,   35,   10,    7,
 /*    50 */    48,   20,    5,    4,   49,   17,   21,    8,   34,   35,
 /*    60 */    34,    7,   49,   17,   46,    8,   34,   35,   36,    7,
 /*    70 */    49,   19,    5,   13,   34,   39,   29,   42,   38,    5,
 /*    80 */    30,   31,    3,   79,   39,   29,   79,   79,   78,   30,
 /*    90 */    31,   14,   47,   39,   29,   79,   79,   79,   30,   31,
 /*   100 */     6,   79,   39,   29,   79,   79,   79,   30,   31,   16,
 /*   110 */    79,   39,   29,   79,   79,   79,   30,   31,    2,   79,
 /*   120 */    39,   29,   79,   79,   79,   30,   31,   11,   79,   39,
 /*   130 */    29,   79,   79,   79,   30,   31,   15,   79,   39,   29,
 /*   140 */    79,   79,   79,   30,   31,   12,   79,   39,   29,   79,
 /*   150 */    18,   43,   30,   31,   23,   49,   28,   49,   27,   34,
 /*   160 */    79,   34,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,    1,    2,    3,   19,    5,    6,    7,   26,    9,
 /*    10 */     9,   26,   12,   12,    1,    2,    3,   26,    5,    6,
 /*    20 */     7,   22,    9,   10,   26,   12,    0,    1,    2,    3,
 /*    30 */    26,    5,    6,    7,   22,    9,    4,    1,    2,    3,
 /*    40 */    22,    5,    6,    7,   12,    9,    6,    7,    4,    9,
 /*    50 */    26,   15,   12,    1,    2,    3,   12,    5,    6,    7,
 /*    60 */     6,    9,    2,    3,   14,    5,    6,    7,    3,    9,
 /*    70 */     2,    3,   12,   18,    6,   20,   21,   16,   23,   12,
 /*    80 */    25,   26,   18,   27,   20,   21,   27,   27,   24,   25,
 /*    90 */    26,   18,   22,   20,   21,   27,   27,   27,   25,   26,
 /*   100 */    18,   27,   20,   21,   27,   27,   27,   25,   26,   18,
 /*   110 */    27,   20,   21,   27,   27,   27,   25,   26,   18,   27,
 /*   120 */    20,   21,   27,   27,   27,   25,   26,   18,   27,   20,
 /*   130 */    21,   27,   27,   27,   25,   26,   18,   27,   20,   21,
 /*   140 */    27,   27,   27,   25,   26,   18,   27,   20,   21,   27,
 /*   150 */     5,    6,   25,   26,    9,    2,    3,    2,    3,    6,
 /*   160 */    27,    6,
};
#define YY_SHIFT_USE_DFLT (-1)
#define YY_SHIFT_MAX 33
static const short yy_shift_ofst[] = {
 /*     0 */    26,   36,   13,    0,   52,   52,   60,   52,   52,   52,
 /*    10 */    52,   40,    1,    1,   67,   -1,   -1,   68,  145,   68,
 /*    20 */   145,   68,   68,  145,  153,  145,  155,   54,   54,   44,
 /*    30 */    32,   50,   65,   61,
};
#define YY_REDUCE_USE_DFLT (-19)
#define YY_REDUCE_MAX 28
static const signed char yy_reduce_ofst[] = {
 /*     0 */    64,   55,   73,   73,   82,   91,   73,  100,  109,  118,
 /*    10 */   127,   73,   73,   73,   73,   73,   73,  -15,   70,   -9,
 /*    20 */    -1,   -2,    4,   12,  -18,   18,   24,   -9,   -9,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */    77,   77,   77,   77,   77,   77,   65,   77,   77,   77,
 /*    10 */    77,   64,   57,   56,   52,   55,   54,   77,   77,   77,
 /*    20 */    77,   77,   77,   77,   77,   77,   77,   59,   60,   77,
 /*    30 */    77,   61,   77,   77,   76,   67,   60,   69,   70,   53,
 /*    40 */    58,   68,   71,   72,   62,   73,   66,   74,   63,   75,
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
 /*   7 */ "expr ::= modifierlist COLON expr",
 /*   8 */ "expr ::= LP expr RP",
 /*   9 */ "expr ::= QUOTE termlist QUOTE",
 /*  10 */ "term ::= QUOTE term QUOTE",
 /*  11 */ "expr ::= term",
 /*  12 */ "termlist ::= term term",
 /*  13 */ "termlist ::= termlist term",
 /*  14 */ "expr ::= MINUS expr",
 /*  15 */ "expr ::= TILDE expr",
 /*  16 */ "expr ::= term STAR",
 /*  17 */ "modifier ::= MODIFIER",
 /*  18 */ "modifierlist ::= modifier OR term",
 /*  19 */ "modifierlist ::= modifierlist OR term",
 /*  20 */ "expr ::= modifier COLON numeric_range",
 /*  21 */ "numeric_range ::= LSQB num num RSQB",
 /*  22 */ "num ::= NUMBER",
 /*  23 */ "num ::= LP num",
 /*  24 */ "num ::= MINUS num",
 /*  25 */ "term ::= TERM",
 /*  26 */ "term ::= NUMBER",
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
    case 17: /* error */
    case 22: /* num */
    case 24: /* query */
    case 25: /* modifier */
    case 26: /* term */
{
#line 51 "parser.y"
 
#line 446 "parser.c"
}
      break;
    case 18: /* expr */
    case 19: /* termlist */
    case 20: /* union */
{
#line 54 "parser.y"
 QueryNode_Free((yypminor->yy53)); 
#line 455 "parser.c"
}
      break;
    case 21: /* modifierlist */
{
#line 63 "parser.y"
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy48)); i++) {
        char *s;
        Vector_Get((yypminor->yy48), i, &s);
        free(s);
    }
    Vector_Free((yypminor->yy48)); 

#line 469 "parser.c"
}
      break;
    case 23: /* numeric_range */
{
#line 75 "parser.y"

    NumericFilter_Free((yypminor->yy54));

#line 478 "parser.c"
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
      case 0: /* query ::= expr */
#line 79 "parser.y"
{ 
 ctx->root = yymsp[0].minor.yy53;
}
#line 793 "parser.c"
        break;
      case 1: /* query ::= */
#line 82 "parser.y"
{
 ctx->root = NULL;
}
#line 800 "parser.c"
        break;
      case 2: /* expr ::= expr expr */
#line 86 "parser.y"
{
    if (yymsp[-1].minor.yy53->type == QN_PHRASE && yymsp[-1].minor.yy53->pn.exact == 0 && 
        yymsp[-1].minor.yy53->fieldMask == RS_FIELDMASK_ALL ) {
        yygotominor.yy53 = yymsp[-1].minor.yy53;
    } else {
        yygotominor.yy53 = NewPhraseNode(0);
        QueryPhraseNode_AddChild(yygotominor.yy53, yymsp[-1].minor.yy53);
    } 
    QueryPhraseNode_AddChild(yygotominor.yy53, yymsp[0].minor.yy53);
}
#line 814 "parser.c"
        break;
      case 3: /* expr ::= union */
#line 97 "parser.y"
{
    yygotominor.yy53 = yymsp[0].minor.yy53;
}
#line 821 "parser.c"
        break;
      case 4: /* union ::= expr OR expr */
#line 102 "parser.y"
{
    
    if (yymsp[-2].minor.yy53->type == QN_UNION && yymsp[-2].minor.yy53->fieldMask == RS_FIELDMASK_ALL) {
        yygotominor.yy53 =yymsp[-2].minor.yy53;
    } else {
        yygotominor.yy53 = NewUnionNode();
        QueryUnionNode_AddChild(yygotominor.yy53, yymsp[-2].minor.yy53);
    }
    QueryUnionNode_AddChild(yygotominor.yy53, yymsp[0].minor.yy53); 
}
#line 835 "parser.c"
        break;
      case 5: /* union ::= union OR expr */
#line 115 "parser.y"
{
    yygotominor.yy53 = yymsp[-2].minor.yy53;
    QueryUnionNode_AddChild(yygotominor.yy53, yymsp[0].minor.yy53); 
}
#line 843 "parser.c"
        break;
      case 6: /* expr ::= modifier COLON expr */
#line 124 "parser.y"
{
    if (ctx->q->ctx && ctx->q->ctx->spec) {
        yymsp[0].minor.yy53->fieldMask = IndexSpec_GetFieldBit(ctx->q->ctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len); 
    }
    yygotominor.yy53 = yymsp[0].minor.yy53; 
}
#line 853 "parser.c"
        break;
      case 7: /* expr ::= modifierlist COLON expr */
#line 132 "parser.y"
{
    yymsp[0].minor.yy53->fieldMask = 0;
    for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy48); i++) {
        char *p;
        Vector_Get(yymsp[-2].minor.yy48, i, &p);

        if (ctx->q->ctx && ctx->q->ctx->spec) {
            yymsp[0].minor.yy53->fieldMask |= IndexSpec_GetFieldBit(ctx->q->ctx->spec, p, strlen(p)); 
        }
        free(p);
    }
    Vector_Free(yymsp[-2].minor.yy48);
    yygotominor.yy53=yymsp[0].minor.yy53;
}
#line 871 "parser.c"
        break;
      case 8: /* expr ::= LP expr RP */
#line 147 "parser.y"
{
    yygotominor.yy53 = yymsp[-1].minor.yy53;
}
#line 878 "parser.c"
        break;
      case 9: /* expr ::= QUOTE termlist QUOTE */
#line 151 "parser.y"
{
    yymsp[-1].minor.yy53->pn.exact =1;
    yygotominor.yy53 = yymsp[-1].minor.yy53;
}
#line 886 "parser.c"
        break;
      case 10: /* term ::= QUOTE term QUOTE */
#line 156 "parser.y"
{
    yygotominor.yy0 = yymsp[-1].minor.yy0;
}
#line 893 "parser.c"
        break;
      case 11: /* expr ::= term */
#line 160 "parser.y"
{
    yygotominor.yy53 = NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len);
}
#line 900 "parser.c"
        break;
      case 12: /* termlist ::= term term */
#line 164 "parser.y"
{
    
    yygotominor.yy53 = NewPhraseNode(0);
    QueryPhraseNode_AddChild(yygotominor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len));
    QueryPhraseNode_AddChild(yygotominor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));

}
#line 911 "parser.c"
        break;
      case 13: /* termlist ::= termlist term */
#line 171 "parser.y"
{
    yygotominor.yy53 = yymsp[-1].minor.yy53;
    QueryPhraseNode_AddChild(yygotominor.yy53, NewTokenNode(ctx->q, strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), yymsp[0].minor.yy0.len));

}
#line 920 "parser.c"
        break;
      case 14: /* expr ::= MINUS expr */
#line 178 "parser.y"
{ 
    yygotominor.yy53 = NewNotNode(yymsp[0].minor.yy53);
}
#line 927 "parser.c"
        break;
      case 15: /* expr ::= TILDE expr */
#line 181 "parser.y"
{ 
    yygotominor.yy53 = NewOptionalNode(yymsp[0].minor.yy53);
}
#line 934 "parser.c"
        break;
      case 16: /* expr ::= term STAR */
#line 185 "parser.y"
{
    yygotominor.yy53 = NewPrefixNode(ctx->q, strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), yymsp[-1].minor.yy0.len);
}
#line 941 "parser.c"
        break;
      case 17: /* modifier ::= MODIFIER */
#line 189 "parser.y"
{
    yygotominor.yy0 = yymsp[0].minor.yy0;
 }
#line 948 "parser.c"
        break;
      case 18: /* modifierlist ::= modifier OR term */
#line 193 "parser.y"
{
    yygotominor.yy48 = NewVector(char *, 2);
    char *s = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yygotominor.yy48, s);
    s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yygotominor.yy48, s);
}
#line 959 "parser.c"
        break;
      case 19: /* modifierlist ::= modifierlist OR term */
#line 201 "parser.y"
{
    char *s = strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy48, s);
    yygotominor.yy48 = yymsp[-2].minor.yy48;
}
#line 968 "parser.c"
        break;
      case 20: /* expr ::= modifier COLON numeric_range */
#line 207 "parser.y"
{
    // we keep the capitalization as is
    yymsp[0].minor.yy54->fieldName = strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yygotominor.yy53 = NewNumericNode(yymsp[0].minor.yy54);
}
#line 977 "parser.c"
        break;
      case 21: /* numeric_range ::= LSQB num num RSQB */
#line 213 "parser.y"
{
    yygotominor.yy54 = NewNumericFilter(yymsp[-2].minor.yy11.num, yymsp[-1].minor.yy11.num, yymsp[-2].minor.yy11.inclusive, yymsp[-1].minor.yy11.inclusive);
}
#line 984 "parser.c"
        break;
      case 22: /* num ::= NUMBER */
#line 217 "parser.y"
{
    yygotominor.yy11.num = yymsp[0].minor.yy0.numval;
    yygotominor.yy11.inclusive = 1;
}
#line 992 "parser.c"
        break;
      case 23: /* num ::= LP num */
#line 222 "parser.y"
{
    yygotominor.yy11=yymsp[0].minor.yy11;
    yygotominor.yy11.inclusive = 0;
}
#line 1000 "parser.c"
        break;
      case 24: /* num ::= MINUS num */
#line 227 "parser.y"
{
    yymsp[0].minor.yy11.num = -yymsp[0].minor.yy11.num;
    yygotominor.yy11 = yymsp[0].minor.yy11;
}
#line 1008 "parser.c"
        break;
      case 25: /* term ::= TERM */
      case 26: /* term ::= NUMBER */ yytestcase(yyruleno==26);
#line 232 "parser.y"
{
    yygotominor.yy0 = yymsp[0].minor.yy0; 
}
#line 1016 "parser.c"
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
#line 19 "parser.y"
  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
#line 1087 "parser.c"
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
