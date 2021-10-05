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
   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../util/arr.h"
#include "../rmutil/vector.h"
#include "../query_node.h"
#include "vector_index.h"
#include "../query_param.h"
#include "../query_internal.h"
#include "../util/strconv.h"

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

#define NODENN_BOTH_VALID 0
#define NODENN_BOTH_INVALID -1
#define NODENN_ONE_NULL 1 
// Returns:
// 0 if a && b
// -1 if !a && !b
// 1 if a ^ b (i.e. !(a&&b||!a||!b)). The result is stored in `out` 
static int one_not_null(void *a, void *b, void *out) {
    if (a && b) {
        return NODENN_BOTH_VALID;
    } else if (a == NULL && b == NULL) {
        return NODENN_BOTH_INVALID;
    } if (a) {
        *(void **)out = a;
        return NODENN_ONE_NULL;
    } else {
        *(void **)out = b;
        return NODENN_ONE_NULL;
    }
}
   
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
**    RSQueryParser_TOKENTYPE     is the data type used for minor type for terminal
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
**                       which is RSQueryParser_TOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    RSQueryParser_ARG_SDECL     A static variable declaration for the %extra_argument
**    RSQueryParser_ARG_PDECL     A parameter declaration for the %extra_argument
**    RSQueryParser_ARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    RSQueryParser_ARG_STORE     Code to store %extra_argument into yypParser
**    RSQueryParser_ARG_FETCH     Code to extract %extra_argument from yypParser
**    RSQueryParser_CTX_*         As RSQueryParser_ARG_ except for %extra_context
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYNTOKEN           Number of terminal symbols
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
**    YY_MIN_REDUCE      Minimum value for reduce actions
**    YY_MAX_REDUCE      Maximum value for reduce actions
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned char
#define YYNOCODE 46
#define YYACTIONTYPE unsigned char
#define RSQueryParser_TOKENTYPE QueryToken
typedef union {
  int yyinit;
  RSQueryParser_TOKENTYPE yy0;
  QueryAttribute * yy9;
  QueryNode * yy19;
  Vector* yy62;
  QueryAttribute yy71;
  QueryParam * yy86;
  RangeNumber yy91;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define RSQueryParser_ARG_SDECL  QueryParseCtx *ctx ;
#define RSQueryParser_ARG_PDECL , QueryParseCtx *ctx 
#define RSQueryParser_ARG_PARAM ,ctx 
#define RSQueryParser_ARG_FETCH  QueryParseCtx *ctx =yypParser->ctx ;
#define RSQueryParser_ARG_STORE yypParser->ctx =ctx ;
#define RSQueryParser_CTX_SDECL
#define RSQueryParser_CTX_PDECL
#define RSQueryParser_CTX_PARAM
#define RSQueryParser_CTX_FETCH
#define RSQueryParser_CTX_STORE
#define YYNSTATE             68
#define YYNRULE              64
#define YYNTOKEN             28
#define YY_MAX_SHIFT         67
#define YY_MIN_SHIFTREDUCE   113
#define YY_MAX_SHIFTREDUCE   176
#define YY_ERROR_ACTION      177
#define YY_ACCEPT_ACTION     178
#define YY_NO_ACTION         179
#define YY_MIN_REDUCE        180
#define YY_MAX_REDUCE        243
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
**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as either:
**
**    (A)   N = yy_action[ yy_shift_ofst[S] + X ]
**    (B)   N = yy_default[S]
**
** The (A) formula is preferred.  The B formula is used instead if
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X.
**
** The formulas above are for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array.
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
#define YY_ACTTAB_COUNT (293)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   220,   27,    5,  241,   23,  203,    6,  172,  134,   35,
 /*    10 */   171,  140,   30,   36,  173,    7,  122,  147,  180,    9,
 /*    20 */     5,   67,   23,  171,    6,  172,  134,  173,  171,  140,
 /*    30 */    30,   25,  173,    7,  181,  147,    5,    9,   23,   67,
 /*    40 */     6,  172,  134,   31,  171,  140,   30,  172,  173,    7,
 /*    50 */   171,  147,  170,   56,  173,  169,   51,   38,    9,    5,
 /*    60 */    67,   23,   67,    6,  172,  134,  242,  171,  140,   30,
 /*    70 */    66,  173,    7,  233,  147,  242,   33,   46,  224,   28,
 /*    80 */   171,   18,  233,   17,   45,   26,   23,  234,    6,  172,
 /*    90 */   134,   37,  171,  140,   30,   64,  173,    7,   65,  147,
 /*   100 */   241,    9,  202,   67,  172,  152,   52,  171,  140,  191,
 /*   110 */    48,  173,   13,   21,  235,  199,   42,  184,    8,   47,
 /*   120 */   229,  231,   49,   53,  227,    5,   50,   23,   39,    6,
 /*   130 */   172,  134,   41,  171,  140,   30,  170,  173,    7,  169,
 /*   140 */   147,  172,  134,   62,  171,  140,   30,  215,  173,    7,
 /*   150 */    44,  147,  128,    9,    3,   67,  241,  199,   42,  184,
 /*   160 */   241,   32,   22,  216,   49,   19,   20,  178,   50,   14,
 /*   170 */    39,  159,  199,   42,  184,   33,  166,    4,  190,   49,
 /*   180 */   199,   42,  184,   50,   34,   39,  192,   49,  165,  170,
 /*   190 */    59,   50,  169,   39,   33,  166,   11,  163,  144,  199,
 /*   200 */    42,  184,  175,   26,  170,   54,   49,  169,  182,   29,
 /*   210 */    50,    2,   39,  145,  199,   42,  184,   55,  146,  172,
 /*   220 */   156,   49,  171,  140,   43,   50,  173,   39,   12,   57,
 /*   230 */    58,  199,   42,  184,  172,  137,  143,  171,   49,  179,
 /*   240 */     1,  173,   50,   15,   39,   60,  199,   42,  184,   61,
 /*   250 */   142,   63,  129,   49,   40,  172,  137,   50,  171,   39,
 /*   260 */    16,  141,  173,  199,   42,  184,   24,  179,  179,  179,
 /*   270 */    49,   33,  166,  179,   50,  179,   39,   33,  166,  175,
 /*   280 */    26,  130,  179,  179,  172,  176,   34,  171,  179,  179,
 /*   290 */   179,  173,  161,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    31,   32,    2,   39,    4,   44,    6,    7,    8,   45,
 /*    10 */    10,   11,   12,   44,   14,   15,   16,   17,    0,   19,
 /*    20 */     2,   21,    4,   10,    6,    7,    8,   14,   10,   11,
 /*    30 */    12,   32,   14,   15,    0,   17,    2,   19,    4,   21,
 /*    40 */     6,    7,    8,   44,   10,   11,   12,    7,   14,   15,
 /*    50 */    10,   17,    7,    8,   14,   10,   22,   12,   19,    2,
 /*    60 */    21,    4,   21,    6,    7,    8,    6,   10,   11,   12,
 /*    70 */    14,   14,   15,    6,   17,   15,    6,    7,   31,   32,
 /*    80 */    10,   24,   15,   26,   14,   15,    4,   39,    6,    7,
 /*    90 */     8,   44,   10,   11,   12,   43,   14,   15,   43,   17,
 /*   100 */    39,   19,   44,   21,    7,    8,   45,   10,   11,   29,
 /*   110 */    30,   14,   28,   44,   39,   31,   32,   33,    5,   35,
 /*   120 */    36,   37,   38,   44,   40,    2,   42,    4,   44,    6,
 /*   130 */     7,    8,   19,   10,   11,   12,    7,   14,   15,   10,
 /*   140 */    17,    7,    8,   43,   10,   11,   12,   43,   14,   15,
 /*   150 */    23,   17,   25,   19,   28,   21,   39,   31,   32,   33,
 /*   160 */    39,   44,   45,   43,   38,   19,   45,   41,   42,   28,
 /*   170 */    44,   25,   31,   32,   33,    6,    7,   28,   43,   38,
 /*   180 */    31,   32,   33,   42,   15,   44,   29,   38,   27,    7,
 /*   190 */     8,   42,   10,   44,    6,    7,   28,   27,   12,   31,
 /*   200 */    32,   33,   14,   15,    7,    8,   38,   10,    0,   12,
 /*   210 */    42,   28,   44,   12,   31,   32,   33,   12,   12,    7,
 /*   220 */     8,   38,   10,   11,    5,   42,   14,   44,   28,   12,
 /*   230 */    12,   31,   32,   33,    7,    8,   12,   10,   38,   46,
 /*   240 */     5,   14,   42,   28,   44,   12,   31,   32,   33,   12,
 /*   250 */    12,   12,    4,   38,   19,    7,    8,   42,   10,   44,
 /*   260 */    28,   12,   14,   31,   32,   33,   24,   46,   46,   46,
 /*   270 */    38,    6,    7,   46,   42,   46,   44,    6,    7,   14,
 /*   280 */    15,    4,   46,   46,    7,   14,   15,   10,   46,   46,
 /*   290 */    46,   14,   27,   46,   46,   46,   46,   46,   46,   46,
 /*   300 */    46,   46,   46,   46,   46,   46,   46,   46,   46,   46,
};
#define YY_SHIFT_COUNT    (67)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (277)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    34,   57,    0,   18,   82,  123,  123,  123,  123,  123,
 /*    10 */   123,  134,   39,   39,   39,   41,   41,   70,   97,  212,
 /*    20 */   265,  188,  188,   40,   56,  248,  271,  227,  227,   45,
 /*    30 */   197,  277,   40,  169,  169,   40,   40,   40,  182,   40,
 /*    40 */   129,  129,   13,  129,   56,   60,   67,  146,  127,  113,
 /*    50 */   235,  208,  161,  170,  186,  201,  205,  206,  217,  218,
 /*    60 */   224,  233,  237,  238,  239,  249,  219,  242,
};
#define YY_REDUCE_COUNT (44)
#define YY_REDUCE_MIN   (-39)
#define YY_REDUCE_MAX   (232)
static const short yy_reduce_ofst[] = {
 /*     0 */   126,   84,  141,  141,  141,  149,  168,  183,  200,  215,
 /*    10 */   232,  141,  141,  141,  141,  141,  141,  117,  -31,   47,
 /*    20 */   -36,   61,  121,   -1,   80,  -39,   48,  -39,  -39,   52,
 /*    30 */    55,   58,   69,   75,   48,   79,   58,   58,  100,   58,
 /*    40 */   104,  120,  -39,  135,  157,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   177,  177,  177,  177,  206,  177,  177,  177,  177,  177,
 /*    10 */   177,  205,  188,  187,  183,  185,  186,  177,  177,  177,
 /*    20 */   177,  177,  177,  177,  194,  177,  177,  221,  225,  177,
 /*    30 */   177,  177,  177,  177,  177,  177,  218,  222,  177,  198,
 /*    40 */   177,  177,  200,  177,  193,  240,  239,  217,  177,  177,
 /*    50 */   177,  177,  177,  177,  177,  177,  177,  177,  177,  177,
 /*    60 */   177,  177,  177,  177,  177,  177,  177,  177,
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
  RSQueryParser_ARG_SDECL                /* A place to hold %extra_argument */
  RSQueryParser_CTX_SDECL                /* A place to hold %extra_context */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
  yyStackEntry yystk0;          /* First stack entry */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
  yyStackEntry *yystackEnd;            /* Last entry in the stack */
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
void RSQueryParser_Trace(FILE *TraceFILE, char *zTracePrompt){
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if( yyTraceFILE==0 ) yyTracePrompt = 0;
  else if( yyTracePrompt==0 ) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#if defined(YYCOVERAGE) || !defined(NDEBUG)
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  /*    0 */ "$",
  /*    1 */ "LOWEST",
  /*    2 */ "TILDE",
  /*    3 */ "TAGLIST",
  /*    4 */ "QUOTE",
  /*    5 */ "COLON",
  /*    6 */ "MINUS",
  /*    7 */ "NUMBER",
  /*    8 */ "STOPWORD",
  /*    9 */ "TERMLIST",
  /*   10 */ "TERM",
  /*   11 */ "PREFIX",
  /*   12 */ "PERCENT",
  /*   13 */ "PARAM",
  /*   14 */ "ATTRIBUTE",
  /*   15 */ "LP",
  /*   16 */ "RP",
  /*   17 */ "MODIFIER",
  /*   18 */ "AND",
  /*   19 */ "OR",
  /*   20 */ "ORX",
  /*   21 */ "ARROW",
  /*   22 */ "STAR",
  /*   23 */ "SEMICOLON",
  /*   24 */ "LB",
  /*   25 */ "RB",
  /*   26 */ "LSQB",
  /*   27 */ "RSQB",
  /*   28 */ "expr",
  /*   29 */ "attribute",
  /*   30 */ "attribute_list",
  /*   31 */ "prefix",
  /*   32 */ "termlist",
  /*   33 */ "union",
  /*   34 */ "fuzzy",
  /*   35 */ "tag_list",
  /*   36 */ "geo_filter",
  /*   37 */ "vector_filter",
  /*   38 */ "modifierlist",
  /*   39 */ "num",
  /*   40 */ "numeric_range",
  /*   41 */ "query",
  /*   42 */ "modifier",
  /*   43 */ "term",
  /*   44 */ "param_term",
  /*   45 */ "param_num",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

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
 /*  17 */ "expr ::= QUOTE param_term QUOTE",
 /*  18 */ "expr ::= param_term",
 /*  19 */ "expr ::= prefix",
 /*  20 */ "expr ::= termlist",
 /*  21 */ "expr ::= STOPWORD",
 /*  22 */ "termlist ::= param_term param_term",
 /*  23 */ "termlist ::= termlist param_term",
 /*  24 */ "termlist ::= termlist STOPWORD",
 /*  25 */ "expr ::= MINUS expr",
 /*  26 */ "expr ::= TILDE expr",
 /*  27 */ "prefix ::= PREFIX",
 /*  28 */ "expr ::= PERCENT term PERCENT",
 /*  29 */ "expr ::= PERCENT PERCENT term PERCENT PERCENT",
 /*  30 */ "expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT",
 /*  31 */ "expr ::= PERCENT STOPWORD PERCENT",
 /*  32 */ "expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT",
 /*  33 */ "expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT",
 /*  34 */ "modifier ::= MODIFIER",
 /*  35 */ "modifierlist ::= modifier OR term",
 /*  36 */ "modifierlist ::= modifierlist OR term",
 /*  37 */ "expr ::= modifier COLON tag_list",
 /*  38 */ "tag_list ::= LB param_term",
 /*  39 */ "tag_list ::= LB STOPWORD",
 /*  40 */ "tag_list ::= LB prefix",
 /*  41 */ "tag_list ::= LB termlist",
 /*  42 */ "tag_list ::= tag_list OR param_term",
 /*  43 */ "tag_list ::= tag_list OR STOPWORD",
 /*  44 */ "tag_list ::= tag_list OR prefix",
 /*  45 */ "tag_list ::= tag_list OR termlist",
 /*  46 */ "tag_list ::= tag_list RB",
 /*  47 */ "expr ::= modifier COLON numeric_range",
 /*  48 */ "numeric_range ::= LSQB param_num param_num RSQB",
 /*  49 */ "expr ::= modifier COLON geo_filter",
 /*  50 */ "geo_filter ::= LSQB param_num param_num param_num param_term RSQB",
 /*  51 */ "expr ::= modifier COLON vector_filter",
 /*  52 */ "vector_filter ::= LSQB param_term param_term param_num RSQB",
 /*  53 */ "num ::= NUMBER",
 /*  54 */ "num ::= LP num",
 /*  55 */ "num ::= MINUS num",
 /*  56 */ "term ::= TERM",
 /*  57 */ "term ::= NUMBER",
 /*  58 */ "param_term ::= TERM",
 /*  59 */ "param_term ::= NUMBER",
 /*  60 */ "param_term ::= ATTRIBUTE",
 /*  61 */ "param_num ::= num",
 /*  62 */ "param_num ::= ATTRIBUTE",
 /*  63 */ "param_num ::= LP ATTRIBUTE",
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
** second argument to RSQueryParser_Alloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void RSQueryParser_Init(void *yypRawParser RSQueryParser_CTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  RSQueryParser_CTX_STORE
#ifdef YYTRACKMAXSTACKDEPTH
  yypParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  yypParser->yytos = NULL;
  yypParser->yystack = NULL;
  yypParser->yystksz = 0;
  if( yyGrowStack(yypParser) ){
    yypParser->yystack = &yypParser->yystk0;
    yypParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  yypParser->yytos = yypParser->yystack;
  yypParser->yystack[0].stateno = 0;
  yypParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  yypParser->yystackEnd = &yypParser->yystack[YYSTACKDEPTH-1];
#endif
}

#ifndef RSQueryParser__ENGINEALWAYSONSTACK
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
** to RSQueryParser_ and RSQueryParser_Free.
*/
void *RSQueryParser_Alloc(void *(*mallocProc)(YYMALLOCARGTYPE) RSQueryParser_CTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    RSQueryParser_CTX_STORE
    RSQueryParser_Init(yypParser RSQueryParser_CTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* RSQueryParser__ENGINEALWAYSONSTACK */


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
  RSQueryParser_ARG_FETCH
  RSQueryParser_CTX_FETCH
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
    case 39: /* num */
    case 41: /* query */
    case 42: /* modifier */
    case 43: /* term */
    case 44: /* param_term */
    case 45: /* param_num */
{
 
}
      break;
    case 28: /* expr */
    case 31: /* prefix */
    case 32: /* termlist */
    case 33: /* union */
    case 34: /* fuzzy */
    case 35: /* tag_list */
{
 QueryNode_Free((yypminor->yy19)); 
}
      break;
    case 29: /* attribute */
{
 rm_free((char*)(yypminor->yy71).value); 
}
      break;
    case 30: /* attribute_list */
{
 array_free_ex((yypminor->yy9), rm_free((char*)((QueryAttribute*)ptr )->value)); 
}
      break;
    case 36: /* geo_filter */
    case 37: /* vector_filter */
{
 QueryParam_Free((yypminor->yy86)); 
}
      break;
    case 38: /* modifierlist */
{
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy62)); i++) {
        char *s;
        Vector_Get((yypminor->yy62), i, &s);
        rm_free(s);
    }
    Vector_Free((yypminor->yy62)); 

}
      break;
    case 40: /* numeric_range */
{

  QueryParam_Free((yypminor->yy86));

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
void RSQueryParser_Finalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef RSQueryParser__ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void RSQueryParser_Free(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  RSQueryParser_Finalize(p);
  (*freeProc)(p);
}
#endif /* RSQueryParser__ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int RSQueryParser_StackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyhwm;
}
#endif

/* This array of booleans keeps track of the parser statement
** coverage.  The element yycoverage[X][Y] is set when the parser
** is in state X and has a lookahead token Y.  In a well-tested
** systems, every element of this matrix should end up being set.
*/
#if defined(YYCOVERAGE)
static unsigned char yycoverage[YYNSTATE][YYNTOKEN];
#endif

/*
** Write into out a description of every state/lookahead combination that
**
**   (1)  has not been used by the parser, and
**   (2)  is not a syntax error.
**
** Return the number of missed state/lookahead combinations.
*/
#if defined(YYCOVERAGE)
int RSQueryParser_Coverage(FILE *out){
  int stateno, iLookAhead, i;
  int nMissed = 0;
  for(stateno=0; stateno<YYNSTATE; stateno++){
    i = yy_shift_ofst[stateno];
    for(iLookAhead=0; iLookAhead<YYNTOKEN; iLookAhead++){
      if( yy_lookahead[i+iLookAhead]!=iLookAhead ) continue;
      if( yycoverage[stateno][iLookAhead]==0 ) nMissed++;
      if( out ){
        fprintf(out,"State %d lookahead %s %s\n", stateno,
                yyTokenName[iLookAhead],
                yycoverage[stateno][iLookAhead] ? "ok" : "missed");
      }
    }
  }
  return nMissed;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_shift_action(
  YYCODETYPE iLookAhead,    /* The look-ahead token */
  YYACTIONTYPE stateno      /* Current state number */
){
  int i;

  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 );
    assert( i+YYNTOKEN<=(int)sizeof(yy_lookahead)/sizeof(yy_lookahead[0]) );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( yy_lookahead[i]!=iLookAhead ){
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
  YYACTIONTYPE stateno,     /* Current state number */
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
   RSQueryParser_ARG_FETCH
   RSQueryParser_CTX_FETCH
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
   RSQueryParser_ARG_STORE /* Suppress warning about unused %extra_argument var */
   RSQueryParser_CTX_STORE
}

/*
** Print tracing information for a SHIFT action
*/
#ifndef NDEBUG
static void yyTraceShift(yyParser *yypParser, int yyNewState, const char *zTag){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%s%s '%s', go to state %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState);
    }else{
      fprintf(yyTraceFILE,"%s%s '%s', pending reduce %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState - YY_MIN_REDUCE);
    }
  }
}
#else
# define yyTraceShift(X,Y,Z)
#endif

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  YYACTIONTYPE yyNewState,      /* The new state to shift in */
  YYCODETYPE yyMajor,           /* The major token to shift in */
  RSQueryParser_TOKENTYPE yyMinor        /* The minor token to shift in */
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
  if( yypParser->yytos>yypParser->yystackEnd ){
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
  yytos->stateno = yyNewState;
  yytos->major = yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState, "Shift");
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;       /* Symbol on the left-hand side of the rule */
  signed char nrhs;     /* Negative of the number of RHS symbols in the rule */
} yyRuleInfo[] = {
  {   41,   -1 }, /* (0) query ::= expr */
  {   41,    0 }, /* (1) query ::= */
  {   41,   -1 }, /* (2) query ::= STAR */
  {   28,   -2 }, /* (3) expr ::= expr expr */
  {   28,   -1 }, /* (4) expr ::= union */
  {   33,   -3 }, /* (5) union ::= expr OR expr */
  {   33,   -3 }, /* (6) union ::= union OR expr */
  {   28,   -3 }, /* (7) expr ::= modifier COLON expr */
  {   28,   -3 }, /* (8) expr ::= modifierlist COLON expr */
  {   28,   -3 }, /* (9) expr ::= LP expr RP */
  {   29,   -3 }, /* (10) attribute ::= ATTRIBUTE COLON term */
  {   30,   -1 }, /* (11) attribute_list ::= attribute */
  {   30,   -3 }, /* (12) attribute_list ::= attribute_list SEMICOLON attribute */
  {   30,   -2 }, /* (13) attribute_list ::= attribute_list SEMICOLON */
  {   30,    0 }, /* (14) attribute_list ::= */
  {   28,   -5 }, /* (15) expr ::= expr ARROW LB attribute_list RB */
  {   28,   -3 }, /* (16) expr ::= QUOTE termlist QUOTE */
  {   28,   -3 }, /* (17) expr ::= QUOTE param_term QUOTE */
  {   28,   -1 }, /* (18) expr ::= param_term */
  {   28,   -1 }, /* (19) expr ::= prefix */
  {   28,   -1 }, /* (20) expr ::= termlist */
  {   28,   -1 }, /* (21) expr ::= STOPWORD */
  {   32,   -2 }, /* (22) termlist ::= param_term param_term */
  {   32,   -2 }, /* (23) termlist ::= termlist param_term */
  {   32,   -2 }, /* (24) termlist ::= termlist STOPWORD */
  {   28,   -2 }, /* (25) expr ::= MINUS expr */
  {   28,   -2 }, /* (26) expr ::= TILDE expr */
  {   31,   -1 }, /* (27) prefix ::= PREFIX */
  {   28,   -3 }, /* (28) expr ::= PERCENT term PERCENT */
  {   28,   -5 }, /* (29) expr ::= PERCENT PERCENT term PERCENT PERCENT */
  {   28,   -7 }, /* (30) expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT */
  {   28,   -3 }, /* (31) expr ::= PERCENT STOPWORD PERCENT */
  {   28,   -5 }, /* (32) expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */
  {   28,   -7 }, /* (33) expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */
  {   42,   -1 }, /* (34) modifier ::= MODIFIER */
  {   38,   -3 }, /* (35) modifierlist ::= modifier OR term */
  {   38,   -3 }, /* (36) modifierlist ::= modifierlist OR term */
  {   28,   -3 }, /* (37) expr ::= modifier COLON tag_list */
  {   35,   -2 }, /* (38) tag_list ::= LB param_term */
  {   35,   -2 }, /* (39) tag_list ::= LB STOPWORD */
  {   35,   -2 }, /* (40) tag_list ::= LB prefix */
  {   35,   -2 }, /* (41) tag_list ::= LB termlist */
  {   35,   -3 }, /* (42) tag_list ::= tag_list OR param_term */
  {   35,   -3 }, /* (43) tag_list ::= tag_list OR STOPWORD */
  {   35,   -3 }, /* (44) tag_list ::= tag_list OR prefix */
  {   35,   -3 }, /* (45) tag_list ::= tag_list OR termlist */
  {   35,   -2 }, /* (46) tag_list ::= tag_list RB */
  {   28,   -3 }, /* (47) expr ::= modifier COLON numeric_range */
  {   40,   -4 }, /* (48) numeric_range ::= LSQB param_num param_num RSQB */
  {   28,   -3 }, /* (49) expr ::= modifier COLON geo_filter */
  {   36,   -6 }, /* (50) geo_filter ::= LSQB param_num param_num param_num param_term RSQB */
  {   28,   -3 }, /* (51) expr ::= modifier COLON vector_filter */
  {   37,   -5 }, /* (52) vector_filter ::= LSQB param_term param_term param_num RSQB */
  {   39,   -1 }, /* (53) num ::= NUMBER */
  {   39,   -2 }, /* (54) num ::= LP num */
  {   39,   -2 }, /* (55) num ::= MINUS num */
  {   43,   -1 }, /* (56) term ::= TERM */
  {   43,   -1 }, /* (57) term ::= NUMBER */
  {   44,   -1 }, /* (58) param_term ::= TERM */
  {   44,   -1 }, /* (59) param_term ::= NUMBER */
  {   44,   -1 }, /* (60) param_term ::= ATTRIBUTE */
  {   45,   -1 }, /* (61) param_num ::= num */
  {   45,   -1 }, /* (62) param_num ::= ATTRIBUTE */
  {   45,   -2 }, /* (63) param_num ::= LP ATTRIBUTE */
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
**
** The yyLookahead and yyLookaheadToken parameters provide reduce actions
** access to the lookahead token (if any).  The yyLookahead will be YYNOCODE
** if the lookahead token has already been consumed.  As this procedure is
** only called from one place, optimizing compilers will in-line it, which
** means that the extra parameters have no performance impact.
*/
static YYACTIONTYPE yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  RSQueryParser_TOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  RSQueryParser_CTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  RSQueryParser_ARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfo[yyruleno].nrhs;
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s], go to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno], yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s].\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno]);
    }
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
    if( yypParser->yytos>=yypParser->yystackEnd ){
      yyStackOverflow(yypParser);
      /* The call to yyStackOverflow() above pops the stack until it is
      ** empty, causing the main parser loop to exit.  So the return value
      ** is never used and does not matter. */
      return 0;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        /* The call to yyStackOverflow() above pops the stack until it is
        ** empty, causing the main parser loop to exit.  So the return value
        ** is never used and does not matter. */
        return 0;
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
{ 
    FILE *f = NULL;
    #ifndef NDEBUG
    f = fopen("/tmp/lemon_query.log", "w");
    RSQueryParser_Trace(f, "tr: ");
    #endif
    ctx->trace_log = f;
    ctx->root = yymsp[0].minor.yy19;
 
}
        break;
      case 1: /* query ::= */
{
    ctx->root = NULL;
}
        break;
      case 2: /* query ::= STAR */
{
    ctx->root = NewWildcardNode();
}
        break;
      case 3: /* expr ::= expr expr */
{
    int rv = one_not_null(yymsp[-1].minor.yy19, yymsp[0].minor.yy19, (void**)&yylhsminor.yy19);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy19 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- `out` is already assigned
    } else {
        if (yymsp[-1].minor.yy19 && yymsp[-1].minor.yy19->type == QN_PHRASE && yymsp[-1].minor.yy19->pn.exact == 0 && 
            yymsp[-1].minor.yy19->opts.fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy19 = yymsp[-1].minor.yy19;
        } else {     
            yylhsminor.yy19 = NewPhraseNode(0);
            QueryNode_AddChild(yylhsminor.yy19, yymsp[-1].minor.yy19);
        }
        QueryNode_AddChild(yylhsminor.yy19, yymsp[0].minor.yy19);
    }
}
  yymsp[-1].minor.yy19 = yylhsminor.yy19;
        break;
      case 4: /* expr ::= union */
      case 19: /* expr ::= prefix */ yytestcase(yyruleno==19);
{
    yylhsminor.yy19 = yymsp[0].minor.yy19;
}
  yymsp[0].minor.yy19 = yylhsminor.yy19;
        break;
      case 5: /* union ::= expr OR expr */
{
    int rv = one_not_null(yymsp[-2].minor.yy19, yymsp[0].minor.yy19, (void**)&yylhsminor.yy19);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy19 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (yymsp[-2].minor.yy19->type == QN_UNION && yymsp[-2].minor.yy19->opts.fieldMask == RS_FIELDMASK_ALL) {
            yylhsminor.yy19 = yymsp[-2].minor.yy19;
        } else {
            yylhsminor.yy19 = NewUnionNode();
            QueryNode_AddChild(yylhsminor.yy19, yymsp[-2].minor.yy19);
            yylhsminor.yy19->opts.fieldMask |= yymsp[-2].minor.yy19->opts.fieldMask;
        }

        // Handle yymsp[0].minor.yy19
        QueryNode_AddChild(yylhsminor.yy19, yymsp[0].minor.yy19);
        yylhsminor.yy19->opts.fieldMask |= yymsp[0].minor.yy19->opts.fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy19, yylhsminor.yy19->opts.fieldMask);
    }
    
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 6: /* union ::= union OR expr */
{
    yylhsminor.yy19 = yymsp[-2].minor.yy19;
    if (yymsp[0].minor.yy19) {
        QueryNode_AddChild(yylhsminor.yy19, yymsp[0].minor.yy19);
        yylhsminor.yy19->opts.fieldMask |= yymsp[0].minor.yy19->opts.fieldMask;
        QueryNode_SetFieldMask(yymsp[0].minor.yy19, yylhsminor.yy19->opts.fieldMask);
    }
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 7: /* expr ::= modifier COLON expr */
{
    if (yymsp[0].minor.yy19 == NULL) {
        yylhsminor.yy19 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy19, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy19 = yymsp[0].minor.yy19; 
    }
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
{
    
    if (yymsp[0].minor.yy19 == NULL) {
        yylhsminor.yy19 = NULL;
    } else {
        //yymsp[0].minor.yy19->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy62); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy62, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                rm_free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy19, mask);
        Vector_Free(yymsp[-2].minor.yy62);
        yylhsminor.yy19=yymsp[0].minor.yy19;
    }
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 9: /* expr ::= LP expr RP */
{
    yymsp[-2].minor.yy19 = yymsp[-1].minor.yy19;
}
        break;
      case 10: /* attribute ::= ATTRIBUTE COLON term */
{
    
    yylhsminor.yy71 = (QueryAttribute){ .name = yymsp[-2].minor.yy0.s, .namelen = yymsp[-2].minor.yy0.len, .value = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), .vallen = yymsp[0].minor.yy0.len };
}
  yymsp[-2].minor.yy71 = yylhsminor.yy71;
        break;
      case 11: /* attribute_list ::= attribute */
{
    yylhsminor.yy9 = array_new(QueryAttribute, 2);
    yylhsminor.yy9 = array_append(yylhsminor.yy9, yymsp[0].minor.yy71);
}
  yymsp[0].minor.yy9 = yylhsminor.yy9;
        break;
      case 12: /* attribute_list ::= attribute_list SEMICOLON attribute */
{
    yylhsminor.yy9 = array_append(yymsp[-2].minor.yy9, yymsp[0].minor.yy71);
}
  yymsp[-2].minor.yy9 = yylhsminor.yy9;
        break;
      case 13: /* attribute_list ::= attribute_list SEMICOLON */
{
    yylhsminor.yy9 = yymsp[-1].minor.yy9;
}
  yymsp[-1].minor.yy9 = yylhsminor.yy9;
        break;
      case 14: /* attribute_list ::= */
{
    yymsp[1].minor.yy9 = NULL;
}
        break;
      case 15: /* expr ::= expr ARROW LB attribute_list RB */
{

    if (yymsp[-4].minor.yy19 && yymsp[-1].minor.yy9) {
        QueryNode_ApplyAttributes(yymsp[-4].minor.yy19, yymsp[-1].minor.yy9, array_len(yymsp[-1].minor.yy9), ctx->status);
    }
    array_free_ex(yymsp[-1].minor.yy9, rm_free((char*)((QueryAttribute*)ptr )->value));
    yylhsminor.yy19 = yymsp[-4].minor.yy19;
}
  yymsp[-4].minor.yy19 = yylhsminor.yy19;
        break;
      case 16: /* expr ::= QUOTE termlist QUOTE */
{
    yymsp[-1].minor.yy19->pn.exact =1;
    yymsp[-1].minor.yy19->opts.flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy19 = yymsp[-1].minor.yy19;
}
        break;
      case 17: /* expr ::= QUOTE param_term QUOTE */
{
  yymsp[-2].minor.yy19 = NewTokenNode_WithParam(ctx, &yymsp[-1].minor.yy0);
  yymsp[-2].minor.yy19->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 18: /* expr ::= param_term */
{
  yylhsminor.yy19 = NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy19 = yylhsminor.yy19;
        break;
      case 20: /* expr ::= termlist */
{
        yylhsminor.yy19 = yymsp[0].minor.yy19;
}
  yymsp[0].minor.yy19 = yylhsminor.yy19;
        break;
      case 21: /* expr ::= STOPWORD */
{
    yymsp[0].minor.yy19 = NULL;
}
        break;
      case 22: /* termlist ::= param_term param_term */
{
  yylhsminor.yy19 = NewPhraseNode(0);
  QueryNode_AddChild(yylhsminor.yy19, NewTokenNode_WithParam(ctx, &yymsp[-1].minor.yy0));
  QueryNode_AddChild(yylhsminor.yy19, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy19 = yylhsminor.yy19;
        break;
      case 23: /* termlist ::= termlist param_term */
{
  yylhsminor.yy19 = yymsp[-1].minor.yy19;
  QueryNode_AddChild(yylhsminor.yy19, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy19 = yylhsminor.yy19;
        break;
      case 24: /* termlist ::= termlist STOPWORD */
      case 46: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==46);
{
    yylhsminor.yy19 = yymsp[-1].minor.yy19;
}
  yymsp[-1].minor.yy19 = yylhsminor.yy19;
        break;
      case 25: /* expr ::= MINUS expr */
{ 
    if (yymsp[0].minor.yy19) {
        yymsp[-1].minor.yy19 = NewNotNode(yymsp[0].minor.yy19);
    } else {
        yymsp[-1].minor.yy19 = NULL;
    }
}
        break;
      case 26: /* expr ::= TILDE expr */
{ 
    if (yymsp[0].minor.yy19) {
        yymsp[-1].minor.yy19 = NewOptionalNode(yymsp[0].minor.yy19);
    } else {
        yymsp[-1].minor.yy19 = NULL;
    }
}
        break;
      case 27: /* prefix ::= PREFIX */
{
    yylhsminor.yy19 = NewPrefixNode_WithParam(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy19 = yylhsminor.yy19;
        break;
      case 28: /* expr ::= PERCENT term PERCENT */
      case 31: /* expr ::= PERCENT STOPWORD PERCENT */ yytestcase(yyruleno==31);
{
  yymsp[-1].minor.yy0.s = rm_strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
    yymsp[-2].minor.yy19 = NewFuzzyNode(ctx, yymsp[-1].minor.yy0.s, strlen(yymsp[-1].minor.yy0.s), 1);
}
        break;
      case 29: /* expr ::= PERCENT PERCENT term PERCENT PERCENT */
      case 32: /* expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */ yytestcase(yyruleno==32);
{
  yymsp[-2].minor.yy0.s = rm_strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yymsp[-4].minor.yy19 = NewFuzzyNode(ctx, yymsp[-2].minor.yy0.s, strlen(yymsp[-2].minor.yy0.s), 2);
}
        break;
      case 30: /* expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT */
      case 33: /* expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */ yytestcase(yyruleno==33);
{
  yymsp[-3].minor.yy0.s = rm_strdupcase(yymsp[-3].minor.yy0.s, yymsp[-3].minor.yy0.len);
    yymsp[-6].minor.yy19 = NewFuzzyNode(ctx, yymsp[-3].minor.yy0.s, strlen(yymsp[-3].minor.yy0.s), 3);
}
        break;
      case 34: /* modifier ::= MODIFIER */
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 35: /* modifierlist ::= modifier OR term */
{
    yylhsminor.yy62 = NewVector(char *, 2);
    char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy62, s);
    s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy62, s);
}
  yymsp[-2].minor.yy62 = yylhsminor.yy62;
        break;
      case 36: /* modifierlist ::= modifierlist OR term */
{
    char *s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy62, s);
    yylhsminor.yy62 = yymsp[-2].minor.yy62;
}
  yymsp[-2].minor.yy62 = yylhsminor.yy62;
        break;
      case 37: /* expr ::= modifier COLON tag_list */
{
    if (!yymsp[0].minor.yy19) {
        yylhsminor.yy19= NULL;
    } else {
      // Tag field names must be case sensitive, we can't do rm_strdupcase
        char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        size_t slen = unescapen((char*)s, yymsp[-2].minor.yy0.len);

        yylhsminor.yy19 = NewTagNode(s, slen);
        QueryNode_AddChildren(yylhsminor.yy19, yymsp[0].minor.yy19->children, QueryNode_NumChildren(yymsp[0].minor.yy19));
        
        // Set the children count on yymsp[0].minor.yy19 to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(yymsp[0].minor.yy19, 0);
        QueryNode_Free(yymsp[0].minor.yy19);
    }
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 38: /* tag_list ::= LB param_term */
{
  yymsp[-1].minor.yy19 = NewPhraseNode(0);
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-1].minor.yy19, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
        break;
      case 39: /* tag_list ::= LB STOPWORD */
{
    yymsp[-1].minor.yy19 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy19, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
        break;
      case 40: /* tag_list ::= LB prefix */
      case 41: /* tag_list ::= LB termlist */ yytestcase(yyruleno==41);
{
    yymsp[-1].minor.yy19 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy19, yymsp[0].minor.yy19);
}
        break;
      case 42: /* tag_list ::= tag_list OR param_term */
{
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-2].minor.yy19, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
  yylhsminor.yy19 = yymsp[-2].minor.yy19;
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 43: /* tag_list ::= tag_list OR STOPWORD */
{
    QueryNode_AddChild(yymsp[-2].minor.yy19, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy19 = yymsp[-2].minor.yy19;
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 44: /* tag_list ::= tag_list OR prefix */
      case 45: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==45);
{
    QueryNode_AddChild(yymsp[-2].minor.yy19, yymsp[0].minor.yy19);
    yylhsminor.yy19 = yymsp[-2].minor.yy19;
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 47: /* expr ::= modifier COLON numeric_range */
{
    // we keep the capitalization as is
    yymsp[0].minor.yy86->nf->fieldName = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy19 = NewNumericNode(yymsp[0].minor.yy86);
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 48: /* numeric_range ::= LSQB param_num param_num RSQB */
{
  // Update token type to be more specific if possible
  if (yymsp[-2].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-2].minor.yy0.type = QT_PARAM_NUMERIC_MIN_RANGE;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-1].minor.yy0.type = QT_PARAM_NUMERIC_MAX_RANGE;
  yymsp[-3].minor.yy86 = NewNumericFilterQueryParam_WithParams(ctx, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, yymsp[-2].minor.yy0.inclusive, yymsp[-1].minor.yy0.inclusive);
}
        break;
      case 49: /* expr ::= modifier COLON geo_filter */
{
    // we keep the capitalization as is
    yymsp[0].minor.yy86->gf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy19 = NewGeofilterNode(yymsp[0].minor.yy86);
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 50: /* geo_filter ::= LSQB param_num param_num param_num param_term RSQB */
{
  // Update token type to be more specific if possible
  if (yymsp[-4].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-4].minor.yy0.type = QT_PARAM_GEO_COORD;
  if (yymsp[-3].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-3].minor.yy0.type = QT_PARAM_GEO_COORD;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_TERM)
    yymsp[-1].minor.yy0.type = QT_PARAM_GEO_UNIT;
  yymsp[-5].minor.yy86 = NewGeoFilterQueryParam_WithParams(ctx, &yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
}
        break;
      case 51: /* expr ::= modifier COLON vector_filter */
{
    // we keep the capitalization as is
    if (yymsp[0].minor.yy86) {
        yymsp[0].minor.yy86->vf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        yylhsminor.yy19 = NewVectorNode(yymsp[0].minor.yy86);
    } else {
        yylhsminor.yy19 = NewQueryNode(QN_NULL);
    }
}
  yymsp[-2].minor.yy19 = yylhsminor.yy19;
        break;
      case 52: /* vector_filter ::= LSQB param_term param_term param_num RSQB */
{
  // FIXME: Remove hack for handling lexer/scanner of terms with trailing equal signs.
  //  Equal signs are currently considered as punct (punctuation) and are not included in a term,
  //  But in base64 encoding, it is used as padding to extend the string to a length which is a multiple of 3.
  size_t len = yymsp[-3].minor.yy0.len;
  int remainder = len % 3;
  if (remainder == 1 && *((yymsp[-3].minor.yy0.s)+len) == '=' && *((yymsp[-3].minor.yy0.s)+len+1) == '=')
    yymsp[-3].minor.yy0.len = len + 2;
  else if (remainder == 2 && *((yymsp[-3].minor.yy0.s)+len) == '=')
    yymsp[-3].minor.yy0.len = len + 1;
  // Update token type to be more specific if possible
  if (yymsp[-3].minor.yy0.type == QT_TERM)
    yymsp[-3].minor.yy0.type = QT_TERM_CASE;
  if (yymsp[-3].minor.yy0.type == QT_PARAM_TERM)
    yymsp[-3].minor.yy0.type = QT_PARAM_TERM_CASE;
  if (yymsp[-2].minor.yy0.type == QT_PARAM_TERM)
    yymsp[-2].minor.yy0.type = QT_PARAM_VEC_SIM_TYPE;
  yymsp[-4].minor.yy86 = NewVectorFilterQueryParam_WithParams(ctx, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
}
        break;
      case 53: /* num ::= NUMBER */
{
    yylhsminor.yy91.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy91.inclusive = 1;
}
  yymsp[0].minor.yy91 = yylhsminor.yy91;
        break;
      case 54: /* num ::= LP num */
{
    yymsp[-1].minor.yy91=yymsp[0].minor.yy91;
    yymsp[-1].minor.yy91.inclusive = 0;
}
        break;
      case 55: /* num ::= MINUS num */
{
    yymsp[0].minor.yy91.num = -yymsp[0].minor.yy91.num;
    yymsp[-1].minor.yy91 = yymsp[0].minor.yy91;
}
        break;
      case 56: /* term ::= TERM */
      case 57: /* term ::= NUMBER */ yytestcase(yyruleno==57);
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 58: /* param_term ::= TERM */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 59: /* param_term ::= NUMBER */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  // Number is treated as a term here
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 60: /* param_term ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 61: /* param_num ::= num */
{
  yylhsminor.yy0.numval = yymsp[0].minor.yy91.num;
  yylhsminor.yy0.inclusive = yymsp[0].minor.yy91.inclusive;
  yylhsminor.yy0.type = QT_NUMERIC;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 62: /* param_num ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_NUMERIC;
  yylhsminor.yy0.inclusive = 1;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 63: /* param_num ::= LP ATTRIBUTE */
{
  yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;
  yymsp[-1].minor.yy0.type = QT_PARAM_NUMERIC;
  yymsp[-1].minor.yy0.inclusive = 0;
}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yyact = yy_find_reduce_action(yymsp[yysize].stateno,(YYCODETYPE)yygoto);

  /* There are no SHIFTREDUCE actions on nonterminals because the table
  ** generator has simplified them to pure REDUCE actions. */
  assert( !(yyact>YY_MAX_SHIFT && yyact<=YY_MAX_SHIFTREDUCE) );

  /* It is not possible for a REDUCE to be followed by an error */
  assert( yyact!=YY_ERROR_ACTION );

  yymsp += yysize+1;
  yypParser->yytos = yymsp;
  yymsp->stateno = (YYACTIONTYPE)yyact;
  yymsp->major = (YYCODETYPE)yygoto;
  yyTraceShift(yypParser, yyact, "... then shift");
  return yyact;
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  RSQueryParser_ARG_FETCH
  RSQueryParser_CTX_FETCH
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
  RSQueryParser_ARG_STORE /* Suppress warning about unused %extra_argument variable */
  RSQueryParser_CTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  RSQueryParser_TOKENTYPE yyminor         /* The minor type of the error token */
){
  RSQueryParser_ARG_FETCH
  RSQueryParser_CTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
  
    QueryError_SetErrorFmt(ctx->status, QUERY_ESYNTAX,
        "Syntax error at offset %d near %.*s",
        TOKEN.pos, TOKEN.len, TOKEN.s);
/************ End %syntax_error code ******************************************/
  RSQueryParser_ARG_STORE /* Suppress warning about unused %extra_argument variable */
  RSQueryParser_CTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  RSQueryParser_ARG_FETCH
  RSQueryParser_CTX_FETCH
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
  RSQueryParser_ARG_STORE /* Suppress warning about unused %extra_argument variable */
  RSQueryParser_CTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "RSQueryParser_Alloc" which describes the current state of the parser.
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
void RSQueryParser_(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  RSQueryParser_TOKENTYPE yyminor       /* The value for the token */
  RSQueryParser_ARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  YYACTIONTYPE yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser = (yyParser*)yyp;  /* The parser */
  RSQueryParser_CTX_FETCH
  RSQueryParser_ARG_STORE

  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif

  yyact = yypParser->yytos->stateno;
#ifndef NDEBUG
  if( yyTraceFILE ){
    if( yyact < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    assert( yyact==yypParser->yytos->stateno );
    yyact = yy_find_shift_action(yymajor,yyact);
    if( yyact >= YY_MIN_REDUCE ){
      yyact = yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,
                        yyminor RSQueryParser_CTX_PARAM);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      break;
    }else if( yyact==YY_ACCEPT_ACTION ){
      yypParser->yytos--;
      yy_accept(yypParser);
      return;
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
      if( yymajor==YYNOCODE ) break;
      yyact = yypParser->yytos->stateno;
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
      break;
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
      break;
#endif
    }
  }while( yypParser->yytos>yypParser->yystack );
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
