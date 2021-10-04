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
#define YYNOCODE 45
#define YYACTIONTYPE unsigned char
#define RSQueryParser_TOKENTYPE QueryToken
typedef union {
  int yyinit;
  RSQueryParser_TOKENTYPE yy0;
  QueryNode * yy17;
  RangeNumber yy29;
  QueryAttribute yy55;
  QueryAttribute * yy63;
  Vector* yy66;
  QueryParam * yy86;
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
#define YYNSTATE             64
#define YYNRULE              64
#define YYNTOKEN             28
#define YY_MAX_SHIFT         63
#define YY_MIN_SHIFTREDUCE   114
#define YY_MAX_SHIFTREDUCE   177
#define YY_ERROR_ACTION      178
#define YY_ACCEPT_ACTION     179
#define YY_NO_ACTION         180
#define YY_MIN_REDUCE        181
#define YY_MAX_REDUCE        244
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
#define YY_ACTTAB_COUNT (400)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   242,   59,    5,   60,   18,   48,    6,  169,  137,  237,
 /*    10 */   172,  144,   34,  238,  176,    7,  123,  151,  181,    9,
 /*    20 */     5,   63,   18,   62,    6,  169,  137,   63,  172,  144,
 /*    30 */    34,   57,  176,    7,  182,  151,    5,    9,   18,   63,
 /*    40 */     6,  169,  137,   19,  172,  144,   34,  242,  176,    7,
 /*    50 */   242,  151,   24,  219,   61,   26,   47,  173,   51,    5,
 /*    60 */   172,   18,   36,    6,  169,  137,  220,  172,  144,   34,
 /*    70 */   173,  176,    7,  172,  151,    9,  130,   63,   37,  169,
 /*    80 */   141,   17,  172,   28,  191,  242,  176,   35,   13,  206,
 /*    90 */   207,  202,   23,  185,   21,   43,  232,  234,   45,  242,
 /*   100 */   230,  242,   46,  200,   31,   18,   30,    6,  169,  137,
 /*   110 */   193,  172,  144,   34,  183,  176,    7,  166,  151,    5,
 /*   120 */     9,   18,   63,    6,  169,  137,  131,  172,  144,   34,
 /*   130 */   148,  176,    7,  242,  151,    5,  149,   18,  205,    6,
 /*   140 */   169,  137,   50,  172,  144,   34,  180,  177,    7,    3,
 /*   150 */   151,  172,  202,   23,  185,  176,   35,   37,  169,   45,
 /*   160 */   242,  150,  179,   46,  200,   31,   38,  169,  137,   52,
 /*   170 */   172,  144,   34,  242,  176,    7,   53,  151,   25,    9,
 /*   180 */   147,   63,   14,  242,   55,  202,   23,  185,   27,   56,
 /*   190 */   192,   44,   45,  242,   22,  146,   46,  200,   31,    4,
 /*   200 */   162,   58,  202,   23,  185,  173,   54,  145,  172,   45,
 /*   210 */   242,    8,   41,   46,  200,   31,   11,   32,  180,  202,
 /*   220 */    23,  185,   42,  180,  129,   40,   45,  238,    1,  180,
 /*   230 */    46,  200,   31,    2,  180,  180,  202,   23,  185,  180,
 /*   240 */   180,  180,   39,   45,  237,  180,  180,   46,  200,   31,
 /*   250 */    12,  180,  180,  202,   23,  185,  180,  180,  180,  180,
 /*   260 */    45,  242,  180,  180,   46,  200,   31,   15,  180,  180,
 /*   270 */   202,   23,  185,  180,  180,  180,  180,   45,  242,  180,
 /*   280 */   180,   46,  200,   31,   16,  180,  180,  202,   23,  185,
 /*   290 */   180,  180,  180,  180,   45,  242,  180,  180,   46,  200,
 /*   300 */    31,   37,  169,  156,  180,  174,  144,  180,  180,  176,
 /*   310 */    35,  180,   37,  169,  141,  180,  172,  180,  180,  180,
 /*   320 */   176,   35,  180,   37,  169,  160,  180,  174,   37,  169,
 /*   330 */   180,  176,   35,  180,   37,  169,  177,   38,  174,  180,
 /*   340 */   180,  180,  176,   35,  180,   37,  169,  180,  180,  174,
 /*   350 */   180,  180,  180,  176,   35,  168,  180,  180,  132,  180,
 /*   360 */    37,  169,  180,  180,  174,  180,  164,  180,  176,   35,
 /*   370 */   180,   37,  169,   37,  169,  172,  180,  174,  180,  176,
 /*   380 */    35,  176,   35,  180,  180,  180,  224,   20,  173,   49,
 /*   390 */   180,  172,  180,   33,  242,  180,  180,  180,  180,   29,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    39,   43,    2,   43,    4,   44,    6,    7,    8,   39,
 /*    10 */    10,   11,   12,   39,   14,   15,   16,   17,    0,   19,
 /*    20 */     2,   21,    4,   14,    6,    7,    8,   21,   10,   11,
 /*    30 */    12,   43,   14,   15,    0,   17,    2,   19,    4,   21,
 /*    40 */     6,    7,    8,   32,   10,   11,   12,   39,   14,   15,
 /*    50 */    39,   17,   44,   43,   43,   44,   22,    7,    8,    2,
 /*    60 */    10,    4,   12,    6,    7,    8,   43,   10,   11,   12,
 /*    70 */     7,   14,   15,   10,   17,   19,    4,   21,    6,    7,
 /*    80 */     8,   24,   10,   26,   43,   39,   14,   15,   28,   43,
 /*    90 */    44,   31,   32,   33,   32,   35,   36,   37,   38,   39,
 /*   100 */    40,   39,   42,   43,   44,    4,   44,    6,    7,    8,
 /*   110 */    29,   10,   11,   12,    0,   14,   15,   27,   17,    2,
 /*   120 */    19,    4,   21,    6,    7,    8,    4,   10,   11,   12,
 /*   130 */    12,   14,   15,   39,   17,    2,   12,    4,   44,    6,
 /*   140 */     7,    8,   12,   10,   11,   12,   45,   14,   15,   28,
 /*   150 */    17,   10,   31,   32,   33,   14,   15,    6,    7,   38,
 /*   160 */    39,   12,   41,   42,   43,   44,   15,    7,    8,   12,
 /*   170 */    10,   11,   12,   39,   14,   15,   12,   17,   44,   19,
 /*   180 */    12,   21,   28,   39,   12,   31,   32,   33,   44,   12,
 /*   190 */    29,   30,   38,   39,   19,   12,   42,   43,   44,   28,
 /*   200 */    25,   12,   31,   32,   33,    7,    8,   12,   10,   38,
 /*   210 */    39,    5,    5,   42,   43,   44,   28,   24,   45,   31,
 /*   220 */    32,   33,   23,   45,   25,   19,   38,   39,    5,   45,
 /*   230 */    42,   43,   44,   28,   45,   45,   31,   32,   33,   45,
 /*   240 */    45,   45,   19,   38,   39,   45,   45,   42,   43,   44,
 /*   250 */    28,   45,   45,   31,   32,   33,   45,   45,   45,   45,
 /*   260 */    38,   39,   45,   45,   42,   43,   44,   28,   45,   45,
 /*   270 */    31,   32,   33,   45,   45,   45,   45,   38,   39,   45,
 /*   280 */    45,   42,   43,   44,   28,   45,   45,   31,   32,   33,
 /*   290 */    45,   45,   45,   45,   38,   39,   45,   45,   42,   43,
 /*   300 */    44,    6,    7,    8,   45,   10,   11,   45,   45,   14,
 /*   310 */    15,   45,    6,    7,    8,   45,   10,   45,   45,   45,
 /*   320 */    14,   15,   45,    6,    7,    8,   45,   10,    6,    7,
 /*   330 */    45,   14,   15,   45,    6,    7,   14,   15,   10,   45,
 /*   340 */    45,   45,   14,   15,   45,    6,    7,   45,   45,   10,
 /*   350 */    45,   45,   45,   14,   15,   27,   45,   45,    4,   45,
 /*   360 */     6,    7,   45,   45,   10,   45,   27,   45,   14,   15,
 /*   370 */    45,    6,    7,    6,    7,   10,   45,   10,   45,   14,
 /*   380 */    15,   14,   15,   45,   45,   45,   31,   32,    7,    8,
 /*   390 */    45,   10,   45,   12,   39,   45,   45,   45,   45,   44,
 /*   400 */    45,   45,   45,   45,   45,   45,   45,   45,   45,   45,
 /*   410 */    45,   45,   45,   45,   45,   45,   45,
};
#define YY_SHIFT_COUNT    (63)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (381)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    34,   57,    0,   18,  101,  117,  117,  133,  117,  117,
 /*    10 */   117,  160,   56,   56,   56,    6,    6,  295,  365,   72,
 /*    20 */   306,  306,  317,  141,  328,  339,  354,  367,  367,  367,
 /*    30 */   367,  367,    9,   50,  381,  322,  198,  151,  151,   63,
 /*    40 */    63,   63,    9,  175,  199,  206,  223,  114,   90,  118,
 /*    50 */   124,  130,  149,  157,  164,  168,  172,  177,  183,  189,
 /*    60 */   195,  122,  207,  193,
};
#define YY_REDUCE_COUNT (42)
#define YY_REDUCE_MIN   (-42)
#define YY_REDUCE_MAX   (355)
static const short yy_reduce_ofst[] = {
 /*     0 */   121,   60,  154,  154,  154,  171,  188,  205,  222,  239,
 /*    10 */   256,  154,  154,  154,  154,  154,  154,  355,   11,   46,
 /*    20 */    46,   46,   62,   46,  -39,    8,   94,  134,  144,   94,
 /*    30 */    94,   94,  161,  -42,  -40,  -30,  -12,  -26,  -30,   10,
 /*    40 */    23,   41,   81,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   178,  178,  178,  178,  210,  178,  178,  178,  178,  178,
 /*    10 */   178,  209,  189,  188,  184,  186,  187,  178,  178,  178,
 /*    20 */   225,  228,  178,  203,  178,  178,  178,  178,  178,  222,
 /*    30 */   226,  201,  195,  178,  178,  178,  178,  178,  178,  178,
 /*    40 */   178,  178,  194,  221,  178,  178,  178,  178,  178,  178,
 /*    50 */   178,  178,  178,  178,  178,  178,  178,  178,  178,  178,
 /*    60 */   178,  178,  178,  178,
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
  /*   44 */ "value_ref",
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
 /*  17 */ "expr ::= QUOTE term QUOTE",
 /*  18 */ "expr ::= QUOTE value_ref QUOTE",
 /*  19 */ "expr ::= term",
 /*  20 */ "expr ::= value_ref",
 /*  21 */ "expr ::= prefix",
 /*  22 */ "expr ::= termlist",
 /*  23 */ "expr ::= STOPWORD",
 /*  24 */ "termlist ::= value_ref value_ref",
 /*  25 */ "termlist ::= termlist term",
 /*  26 */ "termlist ::= termlist value_ref",
 /*  27 */ "termlist ::= termlist STOPWORD",
 /*  28 */ "expr ::= MINUS expr",
 /*  29 */ "expr ::= TILDE expr",
 /*  30 */ "prefix ::= PREFIX",
 /*  31 */ "expr ::= PERCENT term PERCENT",
 /*  32 */ "expr ::= PERCENT PERCENT term PERCENT PERCENT",
 /*  33 */ "expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT",
 /*  34 */ "expr ::= PERCENT STOPWORD PERCENT",
 /*  35 */ "expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT",
 /*  36 */ "expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT",
 /*  37 */ "modifier ::= MODIFIER",
 /*  38 */ "modifierlist ::= modifier OR term",
 /*  39 */ "modifierlist ::= modifierlist OR term",
 /*  40 */ "expr ::= modifier COLON tag_list",
 /*  41 */ "tag_list ::= LB value_ref",
 /*  42 */ "tag_list ::= LB STOPWORD",
 /*  43 */ "tag_list ::= LB prefix",
 /*  44 */ "tag_list ::= LB termlist",
 /*  45 */ "tag_list ::= tag_list OR value_ref",
 /*  46 */ "tag_list ::= tag_list OR STOPWORD",
 /*  47 */ "tag_list ::= tag_list OR termlist",
 /*  48 */ "tag_list ::= tag_list RB",
 /*  49 */ "expr ::= modifier COLON numeric_range",
 /*  50 */ "numeric_range ::= LSQB value_ref value_ref RSQB",
 /*  51 */ "expr ::= modifier COLON geo_filter",
 /*  52 */ "geo_filter ::= LSQB value_ref value_ref value_ref value_ref RSQB",
 /*  53 */ "expr ::= modifier COLON vector_filter",
 /*  54 */ "vector_filter ::= LSQB value_ref value_ref value_ref RSQB",
 /*  55 */ "num ::= NUMBER",
 /*  56 */ "num ::= LP num",
 /*  57 */ "num ::= MINUS num",
 /*  58 */ "term ::= TERM",
 /*  59 */ "term ::= NUMBER",
 /*  60 */ "value_ref ::= TERM",
 /*  61 */ "value_ref ::= num",
 /*  62 */ "value_ref ::= ATTRIBUTE",
 /*  63 */ "value_ref ::= LP ATTRIBUTE",
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
    case 44: /* value_ref */
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
 QueryNode_Free((yypminor->yy17)); 
}
      break;
    case 29: /* attribute */
{
 rm_free((char*)(yypminor->yy55).value); 
}
      break;
    case 30: /* attribute_list */
{
 array_free_ex((yypminor->yy63), rm_free((char*)((QueryAttribute*)ptr )->value)); 
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
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy66)); i++) {
        char *s;
        Vector_Get((yypminor->yy66), i, &s);
        rm_free(s);
    }
    Vector_Free((yypminor->yy66)); 

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
  {   28,   -3 }, /* (17) expr ::= QUOTE term QUOTE */
  {   28,   -3 }, /* (18) expr ::= QUOTE value_ref QUOTE */
  {   28,   -1 }, /* (19) expr ::= term */
  {   28,   -1 }, /* (20) expr ::= value_ref */
  {   28,   -1 }, /* (21) expr ::= prefix */
  {   28,   -1 }, /* (22) expr ::= termlist */
  {   28,   -1 }, /* (23) expr ::= STOPWORD */
  {   32,   -2 }, /* (24) termlist ::= value_ref value_ref */
  {   32,   -2 }, /* (25) termlist ::= termlist term */
  {   32,   -2 }, /* (26) termlist ::= termlist value_ref */
  {   32,   -2 }, /* (27) termlist ::= termlist STOPWORD */
  {   28,   -2 }, /* (28) expr ::= MINUS expr */
  {   28,   -2 }, /* (29) expr ::= TILDE expr */
  {   31,   -1 }, /* (30) prefix ::= PREFIX */
  {   28,   -3 }, /* (31) expr ::= PERCENT term PERCENT */
  {   28,   -5 }, /* (32) expr ::= PERCENT PERCENT term PERCENT PERCENT */
  {   28,   -7 }, /* (33) expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT */
  {   28,   -3 }, /* (34) expr ::= PERCENT STOPWORD PERCENT */
  {   28,   -5 }, /* (35) expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */
  {   28,   -7 }, /* (36) expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */
  {   42,   -1 }, /* (37) modifier ::= MODIFIER */
  {   38,   -3 }, /* (38) modifierlist ::= modifier OR term */
  {   38,   -3 }, /* (39) modifierlist ::= modifierlist OR term */
  {   28,   -3 }, /* (40) expr ::= modifier COLON tag_list */
  {   35,   -2 }, /* (41) tag_list ::= LB value_ref */
  {   35,   -2 }, /* (42) tag_list ::= LB STOPWORD */
  {   35,   -2 }, /* (43) tag_list ::= LB prefix */
  {   35,   -2 }, /* (44) tag_list ::= LB termlist */
  {   35,   -3 }, /* (45) tag_list ::= tag_list OR value_ref */
  {   35,   -3 }, /* (46) tag_list ::= tag_list OR STOPWORD */
  {   35,   -3 }, /* (47) tag_list ::= tag_list OR termlist */
  {   35,   -2 }, /* (48) tag_list ::= tag_list RB */
  {   28,   -3 }, /* (49) expr ::= modifier COLON numeric_range */
  {   40,   -4 }, /* (50) numeric_range ::= LSQB value_ref value_ref RSQB */
  {   28,   -3 }, /* (51) expr ::= modifier COLON geo_filter */
  {   36,   -6 }, /* (52) geo_filter ::= LSQB value_ref value_ref value_ref value_ref RSQB */
  {   28,   -3 }, /* (53) expr ::= modifier COLON vector_filter */
  {   37,   -5 }, /* (54) vector_filter ::= LSQB value_ref value_ref value_ref RSQB */
  {   39,   -1 }, /* (55) num ::= NUMBER */
  {   39,   -2 }, /* (56) num ::= LP num */
  {   39,   -2 }, /* (57) num ::= MINUS num */
  {   43,   -1 }, /* (58) term ::= TERM */
  {   43,   -1 }, /* (59) term ::= NUMBER */
  {   44,   -1 }, /* (60) value_ref ::= TERM */
  {   44,   -1 }, /* (61) value_ref ::= num */
  {   44,   -1 }, /* (62) value_ref ::= ATTRIBUTE */
  {   44,   -2 }, /* (63) value_ref ::= LP ATTRIBUTE */
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
    ctx->root = yymsp[0].minor.yy17;
 
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
    int rv = one_not_null(yymsp[-1].minor.yy17, yymsp[0].minor.yy17, (void**)&yylhsminor.yy17);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy17 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- `out` is already assigned
    } else {
        if (yymsp[-1].minor.yy17 && yymsp[-1].minor.yy17->type == QN_PHRASE && yymsp[-1].minor.yy17->pn.exact == 0 && 
            yymsp[-1].minor.yy17->opts.fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy17 = yymsp[-1].minor.yy17;
        } else {     
            yylhsminor.yy17 = NewPhraseNode(0);
            QueryNode_AddChild(yylhsminor.yy17, yymsp[-1].minor.yy17);
        }
        QueryNode_AddChild(yylhsminor.yy17, yymsp[0].minor.yy17);
    }
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 4: /* expr ::= union */
      case 21: /* expr ::= prefix */ yytestcase(yyruleno==21);
{
    yylhsminor.yy17 = yymsp[0].minor.yy17;
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 5: /* union ::= expr OR expr */
{
    int rv = one_not_null(yymsp[-2].minor.yy17, yymsp[0].minor.yy17, (void**)&yylhsminor.yy17);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy17 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (yymsp[-2].minor.yy17->type == QN_UNION && yymsp[-2].minor.yy17->opts.fieldMask == RS_FIELDMASK_ALL) {
            yylhsminor.yy17 = yymsp[-2].minor.yy17;
        } else {
            yylhsminor.yy17 = NewUnionNode();
            QueryNode_AddChild(yylhsminor.yy17, yymsp[-2].minor.yy17);
            yylhsminor.yy17->opts.fieldMask |= yymsp[-2].minor.yy17->opts.fieldMask;
        }

        // Handle yymsp[0].minor.yy17
        QueryNode_AddChild(yylhsminor.yy17, yymsp[0].minor.yy17);
        yylhsminor.yy17->opts.fieldMask |= yymsp[0].minor.yy17->opts.fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy17, yylhsminor.yy17->opts.fieldMask);
    }
    
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 6: /* union ::= union OR expr */
{
    yylhsminor.yy17 = yymsp[-2].minor.yy17;
    if (yymsp[0].minor.yy17) {
        QueryNode_AddChild(yylhsminor.yy17, yymsp[0].minor.yy17);
        yylhsminor.yy17->opts.fieldMask |= yymsp[0].minor.yy17->opts.fieldMask;
        QueryNode_SetFieldMask(yymsp[0].minor.yy17, yylhsminor.yy17->opts.fieldMask);
    }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 7: /* expr ::= modifier COLON expr */
{
    if (yymsp[0].minor.yy17 == NULL) {
        yylhsminor.yy17 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy17, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy17 = yymsp[0].minor.yy17; 
    }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
{
    
    if (yymsp[0].minor.yy17 == NULL) {
        yylhsminor.yy17 = NULL;
    } else {
        //yymsp[0].minor.yy17->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy66); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy66, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                rm_free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy17, mask);
        Vector_Free(yymsp[-2].minor.yy66);
        yylhsminor.yy17=yymsp[0].minor.yy17;
    }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 9: /* expr ::= LP expr RP */
{
    yymsp[-2].minor.yy17 = yymsp[-1].minor.yy17;
}
        break;
      case 10: /* attribute ::= ATTRIBUTE COLON term */
{
    
    yylhsminor.yy55 = (QueryAttribute){ .name = yymsp[-2].minor.yy0.s, .namelen = yymsp[-2].minor.yy0.len, .value = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), .vallen = yymsp[0].minor.yy0.len };
}
  yymsp[-2].minor.yy55 = yylhsminor.yy55;
        break;
      case 11: /* attribute_list ::= attribute */
{
    yylhsminor.yy63 = array_new(QueryAttribute, 2);
    yylhsminor.yy63 = array_append(yylhsminor.yy63, yymsp[0].minor.yy55);
}
  yymsp[0].minor.yy63 = yylhsminor.yy63;
        break;
      case 12: /* attribute_list ::= attribute_list SEMICOLON attribute */
{
    yylhsminor.yy63 = array_append(yymsp[-2].minor.yy63, yymsp[0].minor.yy55);
}
  yymsp[-2].minor.yy63 = yylhsminor.yy63;
        break;
      case 13: /* attribute_list ::= attribute_list SEMICOLON */
{
    yylhsminor.yy63 = yymsp[-1].minor.yy63;
}
  yymsp[-1].minor.yy63 = yylhsminor.yy63;
        break;
      case 14: /* attribute_list ::= */
{
    yymsp[1].minor.yy63 = NULL;
}
        break;
      case 15: /* expr ::= expr ARROW LB attribute_list RB */
{

    if (yymsp[-4].minor.yy17 && yymsp[-1].minor.yy63) {
        QueryNode_ApplyAttributes(yymsp[-4].minor.yy17, yymsp[-1].minor.yy63, array_len(yymsp[-1].minor.yy63), ctx->status);
    }
    array_free_ex(yymsp[-1].minor.yy63, rm_free((char*)((QueryAttribute*)ptr )->value));
    yylhsminor.yy17 = yymsp[-4].minor.yy17;
}
  yymsp[-4].minor.yy17 = yylhsminor.yy17;
        break;
      case 16: /* expr ::= QUOTE termlist QUOTE */
{
    yymsp[-1].minor.yy17->pn.exact =1;
    yymsp[-1].minor.yy17->opts.flags |= QueryNode_Verbatim;

    yymsp[-2].minor.yy17 = yymsp[-1].minor.yy17;
}
        break;
      case 17: /* expr ::= QUOTE term QUOTE */
{
  yymsp[-2].minor.yy17 = NewTokenNode(ctx, rm_strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
  yymsp[-2].minor.yy17->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 18: /* expr ::= QUOTE value_ref QUOTE */
{
  yymsp[-2].minor.yy17 = NewTokenNode_WithParam(ctx, &yymsp[-1].minor.yy0);
  yymsp[-2].minor.yy17->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 19: /* expr ::= term */
{
  yylhsminor.yy17 = NewTokenNode(ctx, rm_strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1);
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 20: /* expr ::= value_ref */
{
  yylhsminor.yy17 = NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 22: /* expr ::= termlist */
{
        yylhsminor.yy17 = yymsp[0].minor.yy17;
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 23: /* expr ::= STOPWORD */
{
    yymsp[0].minor.yy17 = NULL;
}
        break;
      case 24: /* termlist ::= value_ref value_ref */
{
  yylhsminor.yy17 = NewPhraseNode(0);
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParam(ctx, &yymsp[-1].minor.yy0));
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 25: /* termlist ::= termlist term */
{
    yylhsminor.yy17 = yymsp[-1].minor.yy17;
    QueryNode_AddChild(yylhsminor.yy17, NewTokenNode(ctx, rm_strdupcase(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 26: /* termlist ::= termlist value_ref */
{
  yylhsminor.yy17 = yymsp[-1].minor.yy17;
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 27: /* termlist ::= termlist STOPWORD */
      case 48: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==48);
{
    yylhsminor.yy17 = yymsp[-1].minor.yy17;
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 28: /* expr ::= MINUS expr */
{ 
    if (yymsp[0].minor.yy17) {
        yymsp[-1].minor.yy17 = NewNotNode(yymsp[0].minor.yy17);
    } else {
        yymsp[-1].minor.yy17 = NULL;
    }
}
        break;
      case 29: /* expr ::= TILDE expr */
{ 
    if (yymsp[0].minor.yy17) {
        yymsp[-1].minor.yy17 = NewOptionalNode(yymsp[0].minor.yy17);
    } else {
        yymsp[-1].minor.yy17 = NULL;
    }
}
        break;
      case 30: /* prefix ::= PREFIX */
{
    yylhsminor.yy17 = NewPrefixNode_WithParam(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 31: /* expr ::= PERCENT term PERCENT */
      case 34: /* expr ::= PERCENT STOPWORD PERCENT */ yytestcase(yyruleno==34);
{
  yymsp[-1].minor.yy0.s = rm_strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
    yymsp[-2].minor.yy17 = NewFuzzyNode(ctx, yymsp[-1].minor.yy0.s, strlen(yymsp[-1].minor.yy0.s), 1);
}
        break;
      case 32: /* expr ::= PERCENT PERCENT term PERCENT PERCENT */
      case 35: /* expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */ yytestcase(yyruleno==35);
{
  yymsp[-2].minor.yy0.s = rm_strdupcase(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yymsp[-4].minor.yy17 = NewFuzzyNode(ctx, yymsp[-2].minor.yy0.s, strlen(yymsp[-2].minor.yy0.s), 2);
}
        break;
      case 33: /* expr ::= PERCENT PERCENT PERCENT term PERCENT PERCENT PERCENT */
      case 36: /* expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */ yytestcase(yyruleno==36);
{
  yymsp[-3].minor.yy0.s = rm_strdupcase(yymsp[-3].minor.yy0.s, yymsp[-3].minor.yy0.len);
    yymsp[-6].minor.yy17 = NewFuzzyNode(ctx, yymsp[-3].minor.yy0.s, strlen(yymsp[-3].minor.yy0.s), 3);
}
        break;
      case 37: /* modifier ::= MODIFIER */
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 38: /* modifierlist ::= modifier OR term */
{
    yylhsminor.yy66 = NewVector(char *, 2);
    char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
    s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 39: /* modifierlist ::= modifierlist OR term */
{
    char *s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy66, s);
    yylhsminor.yy66 = yymsp[-2].minor.yy66;
}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 40: /* expr ::= modifier COLON tag_list */
{
    if (!yymsp[0].minor.yy17) {
        yylhsminor.yy17= NULL;
    } else {
      // Tag field names must be case sensitive, we can't do rm_strdupcase
        char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        size_t slen = unescapen((char*)s, yymsp[-2].minor.yy0.len);

        yylhsminor.yy17 = NewTagNode(s, slen);
        QueryNode_AddChildren(yylhsminor.yy17, yymsp[0].minor.yy17->children, QueryNode_NumChildren(yymsp[0].minor.yy17));
        
        // Set the children count on yymsp[0].minor.yy17 to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(yymsp[0].minor.yy17, 0);
        QueryNode_Free(yymsp[0].minor.yy17);
    }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 41: /* tag_list ::= LB value_ref */
{
  yymsp[-1].minor.yy17 = NewPhraseNode(0);
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-1].minor.yy17, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
}
        break;
      case 42: /* tag_list ::= LB STOPWORD */
{
    yymsp[-1].minor.yy17 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy17, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
        break;
      case 43: /* tag_list ::= LB prefix */
      case 44: /* tag_list ::= LB termlist */ yytestcase(yyruleno==44);
{
    yymsp[-1].minor.yy17 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy17, yymsp[0].minor.yy17);
}
        break;
      case 45: /* tag_list ::= tag_list OR value_ref */
{
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-2].minor.yy17, NewTokenNode_WithParam(ctx, &yymsp[0].minor.yy0));
  yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 46: /* tag_list ::= tag_list OR STOPWORD */
{
    QueryNode_AddChild(yymsp[-2].minor.yy17, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 47: /* tag_list ::= tag_list OR termlist */
{
    QueryNode_AddChild(yymsp[-2].minor.yy17, yymsp[0].minor.yy17);
    yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 49: /* expr ::= modifier COLON numeric_range */
{
    // we keep the capitalization as is
    yymsp[0].minor.yy86->nf->fieldName = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy17 = NewNumericNode(yymsp[0].minor.yy86);
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 50: /* numeric_range ::= LSQB value_ref value_ref RSQB */
{
  // Update token types to be more specific if possible
  if (yymsp[-2].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-2].minor.yy0.type = QT_PARAM_NUMERIC_MIN_RANGE;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-1].minor.yy0.type = QT_PARAM_NUMERIC_MAX_RANGE;
  yymsp[-3].minor.yy86 = NewNumericFilterQueryParam_WithParams(ctx, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, yymsp[-2].minor.yy0.inclusive, yymsp[-1].minor.yy0.inclusive);
}
        break;
      case 51: /* expr ::= modifier COLON geo_filter */
{
    // we keep the capitalization as is
    yymsp[0].minor.yy86->gf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy17 = NewGeofilterNode(yymsp[0].minor.yy86);
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 52: /* geo_filter ::= LSQB value_ref value_ref value_ref value_ref RSQB */
{
  // Update token types to be more specific if possible
  if (yymsp[-4].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-4].minor.yy0.type = QT_PARAM_GEO_COORD;
  if (yymsp[-3].minor.yy0.type == QT_PARAM_NUMERIC)
    yymsp[-3].minor.yy0.type = QT_PARAM_GEO_COORD;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_TERM)
    yymsp[-1].minor.yy0.type = QT_PARAM_GEO_UNIT;
  yymsp[-5].minor.yy86 = NewGeoFilterQueryParam_WithParams(ctx, &yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
}
        break;
      case 53: /* expr ::= modifier COLON vector_filter */
{
    // we keep the capitalization as is
    if (yymsp[0].minor.yy86) {
        yymsp[0].minor.yy86->vf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        yylhsminor.yy17 = NewVectorNode(yymsp[0].minor.yy86);
    } else {
        yylhsminor.yy17 = NewQueryNode(QN_NULL);
    }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 54: /* vector_filter ::= LSQB value_ref value_ref value_ref RSQB */
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
      case 55: /* num ::= NUMBER */
{
    yylhsminor.yy29.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy29.inclusive = 1;
}
  yymsp[0].minor.yy29 = yylhsminor.yy29;
        break;
      case 56: /* num ::= LP num */
{
    yymsp[-1].minor.yy29=yymsp[0].minor.yy29;
    yymsp[-1].minor.yy29.inclusive = 0;
}
        break;
      case 57: /* num ::= MINUS num */
{
    yymsp[0].minor.yy29.num = -yymsp[0].minor.yy29.num;
    yymsp[-1].minor.yy29 = yymsp[0].minor.yy29;
}
        break;
      case 58: /* term ::= TERM */
      case 59: /* term ::= NUMBER */ yytestcase(yyruleno==59);
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 60: /* value_ref ::= TERM */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 61: /* value_ref ::= num */
{
  yylhsminor.yy0.numval = yymsp[0].minor.yy29.num;
  yylhsminor.yy0.inclusive = yymsp[0].minor.yy29.inclusive;
  yylhsminor.yy0.type = QT_NUMERIC;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 62: /* value_ref ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_ANY;
  yylhsminor.yy0.inclusive = 1;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 63: /* value_ref ::= LP ATTRIBUTE */
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
