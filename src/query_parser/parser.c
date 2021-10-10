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

void setup_trace(QueryParseCtx *ctx) {
  #ifdef PARSER_DEBUG
  void RSQueryParser_Trace(FILE*, char*);
  ctx->trace_log = fopen("/tmp/lemon_query.log", "w");
  RSQueryParser_Trace(ctx->trace_log, "tr: ");
  #endif
}


void reportSyntaxError(QueryError *status, QueryToken* tok, const char *msg) {
  if (tok->type == QT_TERM || tok->type == QT_TERM_CASE) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %.*s",
      msg, tok->pos, tok->len, tok->s);
  } else if (tok->type == QT_NUMERIC) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %f",
      msg, tok->pos, tok->numval);
  } else {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "%s at offset %d", msg, tok->pos);
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
#define YYNSTATE             67
#define YYNRULE              66
#define YYNTOKEN             27
#define YY_MAX_SHIFT         66
#define YY_MIN_SHIFTREDUCE   114
#define YY_MAX_SHIFTREDUCE   179
#define YY_ERROR_ACTION      180
#define YY_ACCEPT_ACTION     181
#define YY_NO_ACTION         182
#define YY_MIN_REDUCE        183
#define YY_MAX_REDUCE        248
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
#define YY_ACTTAB_COUNT (289)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   194,   44,    5,  248,   19,   59,    6,  174,  136,   48,
 /*    10 */   173,  142,   26,  175,    7,  123,  149,  183,    9,    5,
 /*    20 */    66,   19,   66,    6,  174,  136,   65,  173,  142,   26,
 /*    30 */   175,    7,  184,  149,    5,    9,   19,   66,    6,  174,
 /*    40 */   136,   60,  173,  142,   26,  175,    7,    9,  149,   66,
 /*    50 */     8,  174,  154,   47,  173,  142,    5,  175,   19,  207,
 /*    60 */     6,  174,  136,   39,  173,  142,   26,  175,    7,   27,
 /*    70 */   149,  173,  174,  158,  175,  173,  142,   17,  175,   23,
 /*    80 */    64,   37,   19,   57,    6,  174,  136,  248,  173,  142,
 /*    90 */    26,  175,    7,   20,  149,  238,    9,   61,   66,  206,
 /*   100 */    62,   32,  168,   63,   42,  178,  129,   13,  176,   28,
 /*   110 */   203,   40,  187,  239,   43,  233,  235,   45,  219,  231,
 /*   120 */     5,   46,   19,   36,    6,  174,  136,  248,  173,  142,
 /*   130 */    26,  175,    7,   21,  149,  172,  174,  136,  171,  173,
 /*   140 */   142,   26,  175,    7,  248,  149,  220,    9,    3,   66,
 /*   150 */    22,  203,   40,  187,  193,   32,  168,  185,   45,   32,
 /*   160 */   168,  181,   46,   33,   36,   14,  177,   33,  203,   40,
 /*   170 */   187,  224,   29,    4,   18,   45,  203,   40,  187,   46,
 /*   180 */   161,   36,  130,   45,   34,  174,  139,   46,  173,   36,
 /*   190 */    11,  175,    1,  203,   40,  187,  228,   30,    2,  165,
 /*   200 */    45,  203,   40,  187,   46,   38,   36,  195,   45,   35,
 /*   210 */   174,  139,   46,  173,   36,   12,  175,  146,  203,   40,
 /*   220 */   187,  147,   50,   15,  148,   45,  203,   40,  187,   46,
 /*   230 */    52,   36,   53,   45,  145,  174,   54,   46,  173,   36,
 /*   240 */    16,  175,   55,  203,   40,  187,   56,  144,   58,  143,
 /*   250 */    45,   32,  168,  241,   46,  178,   36,   41,  176,   28,
 /*   260 */   174,   32,  168,  173,  240,  178,  175,  132,  176,   28,
 /*   270 */    24,  167,  131,  174,   51,  182,  173,  182,   31,  175,
 /*   280 */   182,  163,  174,   49,  182,  173,  182,   25,  175,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    28,   29,    2,   38,    4,   43,    6,    7,    8,   44,
 /*    10 */    10,   11,   12,   13,   14,   15,   16,    0,   18,    2,
 /*    20 */    20,    4,   20,    6,    7,    8,   13,   10,   11,   12,
 /*    30 */    13,   14,    0,   16,    2,   18,    4,   20,    6,    7,
 /*    40 */     8,   43,   10,   11,   12,   13,   14,   18,   16,   20,
 /*    50 */     5,    7,    8,   21,   10,   11,    2,   13,    4,   43,
 /*    60 */     6,    7,    8,   18,   10,   11,   12,   13,   14,   31,
 /*    70 */    16,   10,    7,    8,   13,   10,   11,   23,   13,   25,
 /*    80 */    42,   43,    4,   43,    6,    7,    8,   38,   10,   11,
 /*    90 */    12,   13,   14,   44,   16,   38,   18,    7,   20,   43,
 /*   100 */    10,    6,    7,   13,   22,   10,   24,   27,   13,   14,
 /*   110 */    30,   31,   32,   38,   34,   35,   36,   37,   42,   39,
 /*   120 */     2,   41,    4,   43,    6,    7,    8,   38,   10,   11,
 /*   130 */    12,   13,   14,   44,   16,    7,    7,    8,   10,   10,
 /*   140 */    11,   12,   13,   14,   38,   16,   42,   18,   27,   20,
 /*   150 */    44,   30,   31,   32,   42,    6,    7,    0,   37,    6,
 /*   160 */     7,   40,   41,   14,   43,   27,   13,   14,   30,   31,
 /*   170 */    32,   30,   31,   27,   18,   37,   30,   31,   32,   41,
 /*   180 */    24,   43,    4,   37,   43,    7,    8,   41,   10,   43,
 /*   190 */    27,   13,    5,   30,   31,   32,   30,   31,   27,   26,
 /*   200 */    37,   30,   31,   32,   41,   18,   43,   28,   37,   43,
 /*   210 */     7,    8,   41,   10,   43,   27,   13,   12,   30,   31,
 /*   220 */    32,   12,   12,   27,   12,   37,   30,   31,   32,   41,
 /*   230 */    12,   43,   12,   37,   12,    7,    8,   41,   10,   43,
 /*   240 */    27,   13,   12,   30,   31,   32,   12,   12,   12,   12,
 /*   250 */    37,    6,    7,    4,   41,   10,   43,    5,   13,   14,
 /*   260 */     7,    6,    7,   10,    4,   10,   13,    4,   13,   14,
 /*   270 */    23,   26,    4,    7,    8,   45,   10,   45,   12,   13,
 /*   280 */    45,   26,    7,    8,   45,   10,   45,   12,   13,   45,
 /*   290 */    45,   45,   45,   45,   45,   45,   45,   45,   45,   45,
 /*   300 */    45,   45,   45,   45,   45,   45,   45,   45,   45,   45,
};
#define YY_SHIFT_COUNT    (66)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (275)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    32,   54,    0,   17,   78,  118,  118,  118,  118,  118,
 /*    10 */   118,  129,   29,   29,   29,    2,    2,   44,   65,   90,
 /*    20 */   245,  255,   95,   95,   13,  266,  275,  178,  153,  203,
 /*    30 */   203,  228,  149,  149,  253,  253,  253,  253,  128,  128,
 /*    40 */    61,  128,   13,  156,   82,   45,  187,  157,  173,  205,
 /*    50 */   209,  210,  212,  218,  220,  222,  230,  234,  235,  236,
 /*    60 */   237,  249,  260,  263,  268,  252,  247,
};
#define YY_REDUCE_COUNT (42)
#define YY_REDUCE_MIN   (-38)
#define YY_REDUCE_MAX   (213)
static const short yy_reduce_ofst[] = {
 /*     0 */   121,   80,  138,  138,  138,  146,  163,  171,  188,  196,
 /*    10 */   213,  138,  138,  138,  138,  138,  138,  141,  166,   38,
 /*    20 */   -35,   49,   89,  106,  -28,  -38,   -2,   16,   57,   16,
 /*    30 */    16,   40,   75,   57,   56,   56,   56,   56,   76,  104,
 /*    40 */    16,  112,  179,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   180,  180,  180,  180,  210,  180,  180,  180,  180,  180,
 /*    10 */   180,  209,  191,  190,  186,  188,  189,  180,  180,  180,
 /*    20 */   180,  180,  180,  180,  197,  180,  180,  180,  180,  225,
 /*    30 */   229,  180,  180,  180,  222,  226,  202,  180,  180,  180,
 /*    40 */   204,  180,  196,  221,  180,  180,  180,  180,  180,  180,
 /*    50 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*    60 */   180,  243,  242,  244,  180,  180,  180,
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
  /*   13 */ "ATTRIBUTE",
  /*   14 */ "LP",
  /*   15 */ "RP",
  /*   16 */ "MODIFIER",
  /*   17 */ "AND",
  /*   18 */ "OR",
  /*   19 */ "ORX",
  /*   20 */ "ARROW",
  /*   21 */ "STAR",
  /*   22 */ "SEMICOLON",
  /*   23 */ "LB",
  /*   24 */ "RB",
  /*   25 */ "LSQB",
  /*   26 */ "RSQB",
  /*   27 */ "expr",
  /*   28 */ "attribute",
  /*   29 */ "attribute_list",
  /*   30 */ "prefix",
  /*   31 */ "termlist",
  /*   32 */ "union",
  /*   33 */ "fuzzy",
  /*   34 */ "tag_list",
  /*   35 */ "geo_filter",
  /*   36 */ "vector_filter",
  /*   37 */ "modifierlist",
  /*   38 */ "num",
  /*   39 */ "numeric_range",
  /*   40 */ "query",
  /*   41 */ "modifier",
  /*   42 */ "term",
  /*   43 */ "param_term",
  /*   44 */ "param_any",
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
 /*  18 */ "expr ::= QUOTE ATTRIBUTE QUOTE",
 /*  19 */ "expr ::= param_term",
 /*  20 */ "expr ::= prefix",
 /*  21 */ "expr ::= termlist",
 /*  22 */ "expr ::= STOPWORD",
 /*  23 */ "termlist ::= param_term param_term",
 /*  24 */ "termlist ::= termlist param_term",
 /*  25 */ "termlist ::= termlist STOPWORD",
 /*  26 */ "expr ::= MINUS expr",
 /*  27 */ "expr ::= TILDE expr",
 /*  28 */ "prefix ::= PREFIX",
 /*  29 */ "expr ::= PERCENT param_term PERCENT",
 /*  30 */ "expr ::= PERCENT PERCENT param_term PERCENT PERCENT",
 /*  31 */ "expr ::= PERCENT PERCENT PERCENT param_term PERCENT PERCENT PERCENT",
 /*  32 */ "expr ::= PERCENT STOPWORD PERCENT",
 /*  33 */ "expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT",
 /*  34 */ "expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT",
 /*  35 */ "modifier ::= MODIFIER",
 /*  36 */ "modifierlist ::= modifier OR term",
 /*  37 */ "modifierlist ::= modifierlist OR term",
 /*  38 */ "expr ::= modifier COLON tag_list",
 /*  39 */ "tag_list ::= LB param_term",
 /*  40 */ "tag_list ::= LB STOPWORD",
 /*  41 */ "tag_list ::= LB prefix",
 /*  42 */ "tag_list ::= LB termlist",
 /*  43 */ "tag_list ::= tag_list OR param_term",
 /*  44 */ "tag_list ::= tag_list OR STOPWORD",
 /*  45 */ "tag_list ::= tag_list OR prefix",
 /*  46 */ "tag_list ::= tag_list OR termlist",
 /*  47 */ "tag_list ::= tag_list RB",
 /*  48 */ "expr ::= modifier COLON numeric_range",
 /*  49 */ "numeric_range ::= LSQB param_any param_any RSQB",
 /*  50 */ "expr ::= modifier COLON geo_filter",
 /*  51 */ "geo_filter ::= LSQB param_any param_any param_any param_any RSQB",
 /*  52 */ "expr ::= modifier COLON vector_filter",
 /*  53 */ "vector_filter ::= LSQB param_any param_any param_any RSQB",
 /*  54 */ "num ::= NUMBER",
 /*  55 */ "num ::= LP num",
 /*  56 */ "num ::= MINUS num",
 /*  57 */ "term ::= TERM",
 /*  58 */ "term ::= NUMBER",
 /*  59 */ "param_term ::= TERM",
 /*  60 */ "param_term ::= NUMBER",
 /*  61 */ "param_term ::= ATTRIBUTE",
 /*  62 */ "param_any ::= ATTRIBUTE",
 /*  63 */ "param_any ::= LP ATTRIBUTE",
 /*  64 */ "param_any ::= TERM",
 /*  65 */ "param_any ::= num",
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
    case 38: /* num */
    case 40: /* query */
    case 41: /* modifier */
    case 42: /* term */
    case 43: /* param_term */
    case 44: /* param_any */
{
 
}
      break;
    case 27: /* expr */
    case 30: /* prefix */
    case 31: /* termlist */
    case 32: /* union */
    case 33: /* fuzzy */
    case 34: /* tag_list */
{
 QueryNode_Free((yypminor->yy17)); 
}
      break;
    case 28: /* attribute */
{
 rm_free((char*)(yypminor->yy55).value); 
}
      break;
    case 29: /* attribute_list */
{
 array_free_ex((yypminor->yy63), rm_free((char*)((QueryAttribute*)ptr )->value)); 
}
      break;
    case 35: /* geo_filter */
    case 36: /* vector_filter */
{
 QueryParam_Free((yypminor->yy86)); 
}
      break;
    case 37: /* modifierlist */
{
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy66)); i++) {
        char *s;
        Vector_Get((yypminor->yy66), i, &s);
        rm_free(s);
    }
    Vector_Free((yypminor->yy66)); 

}
      break;
    case 39: /* numeric_range */
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
  {   40,   -1 }, /* (0) query ::= expr */
  {   40,    0 }, /* (1) query ::= */
  {   40,   -1 }, /* (2) query ::= STAR */
  {   27,   -2 }, /* (3) expr ::= expr expr */
  {   27,   -1 }, /* (4) expr ::= union */
  {   32,   -3 }, /* (5) union ::= expr OR expr */
  {   32,   -3 }, /* (6) union ::= union OR expr */
  {   27,   -3 }, /* (7) expr ::= modifier COLON expr */
  {   27,   -3 }, /* (8) expr ::= modifierlist COLON expr */
  {   27,   -3 }, /* (9) expr ::= LP expr RP */
  {   28,   -3 }, /* (10) attribute ::= ATTRIBUTE COLON term */
  {   29,   -1 }, /* (11) attribute_list ::= attribute */
  {   29,   -3 }, /* (12) attribute_list ::= attribute_list SEMICOLON attribute */
  {   29,   -2 }, /* (13) attribute_list ::= attribute_list SEMICOLON */
  {   29,    0 }, /* (14) attribute_list ::= */
  {   27,   -5 }, /* (15) expr ::= expr ARROW LB attribute_list RB */
  {   27,   -3 }, /* (16) expr ::= QUOTE termlist QUOTE */
  {   27,   -3 }, /* (17) expr ::= QUOTE term QUOTE */
  {   27,   -3 }, /* (18) expr ::= QUOTE ATTRIBUTE QUOTE */
  {   27,   -1 }, /* (19) expr ::= param_term */
  {   27,   -1 }, /* (20) expr ::= prefix */
  {   27,   -1 }, /* (21) expr ::= termlist */
  {   27,   -1 }, /* (22) expr ::= STOPWORD */
  {   31,   -2 }, /* (23) termlist ::= param_term param_term */
  {   31,   -2 }, /* (24) termlist ::= termlist param_term */
  {   31,   -2 }, /* (25) termlist ::= termlist STOPWORD */
  {   27,   -2 }, /* (26) expr ::= MINUS expr */
  {   27,   -2 }, /* (27) expr ::= TILDE expr */
  {   30,   -1 }, /* (28) prefix ::= PREFIX */
  {   27,   -3 }, /* (29) expr ::= PERCENT param_term PERCENT */
  {   27,   -5 }, /* (30) expr ::= PERCENT PERCENT param_term PERCENT PERCENT */
  {   27,   -7 }, /* (31) expr ::= PERCENT PERCENT PERCENT param_term PERCENT PERCENT PERCENT */
  {   27,   -3 }, /* (32) expr ::= PERCENT STOPWORD PERCENT */
  {   27,   -5 }, /* (33) expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */
  {   27,   -7 }, /* (34) expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */
  {   41,   -1 }, /* (35) modifier ::= MODIFIER */
  {   37,   -3 }, /* (36) modifierlist ::= modifier OR term */
  {   37,   -3 }, /* (37) modifierlist ::= modifierlist OR term */
  {   27,   -3 }, /* (38) expr ::= modifier COLON tag_list */
  {   34,   -2 }, /* (39) tag_list ::= LB param_term */
  {   34,   -2 }, /* (40) tag_list ::= LB STOPWORD */
  {   34,   -2 }, /* (41) tag_list ::= LB prefix */
  {   34,   -2 }, /* (42) tag_list ::= LB termlist */
  {   34,   -3 }, /* (43) tag_list ::= tag_list OR param_term */
  {   34,   -3 }, /* (44) tag_list ::= tag_list OR STOPWORD */
  {   34,   -3 }, /* (45) tag_list ::= tag_list OR prefix */
  {   34,   -3 }, /* (46) tag_list ::= tag_list OR termlist */
  {   34,   -2 }, /* (47) tag_list ::= tag_list RB */
  {   27,   -3 }, /* (48) expr ::= modifier COLON numeric_range */
  {   39,   -4 }, /* (49) numeric_range ::= LSQB param_any param_any RSQB */
  {   27,   -3 }, /* (50) expr ::= modifier COLON geo_filter */
  {   35,   -6 }, /* (51) geo_filter ::= LSQB param_any param_any param_any param_any RSQB */
  {   27,   -3 }, /* (52) expr ::= modifier COLON vector_filter */
  {   36,   -5 }, /* (53) vector_filter ::= LSQB param_any param_any param_any RSQB */
  {   38,   -1 }, /* (54) num ::= NUMBER */
  {   38,   -2 }, /* (55) num ::= LP num */
  {   38,   -2 }, /* (56) num ::= MINUS num */
  {   42,   -1 }, /* (57) term ::= TERM */
  {   42,   -1 }, /* (58) term ::= NUMBER */
  {   43,   -1 }, /* (59) param_term ::= TERM */
  {   43,   -1 }, /* (60) param_term ::= NUMBER */
  {   43,   -1 }, /* (61) param_term ::= ATTRIBUTE */
  {   44,   -1 }, /* (62) param_any ::= ATTRIBUTE */
  {   44,   -2 }, /* (63) param_any ::= LP ATTRIBUTE */
  {   44,   -1 }, /* (64) param_any ::= TERM */
  {   44,   -1 }, /* (65) param_any ::= num */
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
  setup_trace(ctx);
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
  setup_trace(ctx);
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
      case 20: /* expr ::= prefix */ yytestcase(yyruleno==20);
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
  // TODO: Quoted/verbatim string in termlist should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  yymsp[-1].minor.yy17->pn.exact = 1;
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
      case 18: /* expr ::= QUOTE ATTRIBUTE QUOTE */
{
  // Quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(yymsp[-1].minor.yy0.len + 1);
  *s = '$';
  memcpy(s + 1, yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
  yymsp[-2].minor.yy17 = NewTokenNode(ctx, rm_strdupcase(s, yymsp[-1].minor.yy0.len + 1), -1);
  rm_free(s);
  yymsp[-2].minor.yy17->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 19: /* expr ::= param_term */
{
  yylhsminor.yy17 = NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 21: /* expr ::= termlist */
{
        yylhsminor.yy17 = yymsp[0].minor.yy17;
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 22: /* expr ::= STOPWORD */
{
    yymsp[0].minor.yy17 = NULL;
}
        break;
      case 23: /* termlist ::= param_term param_term */
{
  yylhsminor.yy17 = NewPhraseNode(0);
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParams(ctx, &yymsp[-1].minor.yy0));
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 24: /* termlist ::= termlist param_term */
{
  yylhsminor.yy17 = yymsp[-1].minor.yy17;
  QueryNode_AddChild(yylhsminor.yy17, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 25: /* termlist ::= termlist STOPWORD */
      case 47: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==47);
{
    yylhsminor.yy17 = yymsp[-1].minor.yy17;
}
  yymsp[-1].minor.yy17 = yylhsminor.yy17;
        break;
      case 26: /* expr ::= MINUS expr */
{ 
    if (yymsp[0].minor.yy17) {
        yymsp[-1].minor.yy17 = NewNotNode(yymsp[0].minor.yy17);
    } else {
        yymsp[-1].minor.yy17 = NULL;
    }
}
        break;
      case 27: /* expr ::= TILDE expr */
{ 
    if (yymsp[0].minor.yy17) {
        yymsp[-1].minor.yy17 = NewOptionalNode(yymsp[0].minor.yy17);
    } else {
        yymsp[-1].minor.yy17 = NULL;
    }
}
        break;
      case 28: /* prefix ::= PREFIX */
{
    yylhsminor.yy17 = NewPrefixNode_WithParams(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy17 = yylhsminor.yy17;
        break;
      case 29: /* expr ::= PERCENT param_term PERCENT */
      case 32: /* expr ::= PERCENT STOPWORD PERCENT */ yytestcase(yyruleno==32);
{
  yymsp[-2].minor.yy17 = NewFuzzyNode_WithParams(ctx, &yymsp[-1].minor.yy0, 1);
}
        break;
      case 30: /* expr ::= PERCENT PERCENT param_term PERCENT PERCENT */
      case 33: /* expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */ yytestcase(yyruleno==33);
{
  yymsp[-4].minor.yy17 = NewFuzzyNode_WithParams(ctx, &yymsp[-2].minor.yy0, 2);
}
        break;
      case 31: /* expr ::= PERCENT PERCENT PERCENT param_term PERCENT PERCENT PERCENT */
      case 34: /* expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */ yytestcase(yyruleno==34);
{
  yymsp[-6].minor.yy17 = NewFuzzyNode_WithParams(ctx, &yymsp[-3].minor.yy0, 3);
}
        break;
      case 35: /* modifier ::= MODIFIER */
{
    yymsp[0].minor.yy0.len = unescapen((char*)yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    yylhsminor.yy0 = yymsp[0].minor.yy0;
 }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 36: /* modifierlist ::= modifier OR term */
{
    yylhsminor.yy66 = NewVector(char *, 2);
    char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
    s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy66, s);
}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 37: /* modifierlist ::= modifierlist OR term */
{
    char *s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy66, s);
    yylhsminor.yy66 = yymsp[-2].minor.yy66;
}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 38: /* expr ::= modifier COLON tag_list */
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
      case 39: /* tag_list ::= LB param_term */
{
  yymsp[-1].minor.yy17 = NewPhraseNode(0);
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-1].minor.yy17, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
        break;
      case 40: /* tag_list ::= LB STOPWORD */
{
    yymsp[-1].minor.yy17 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy17, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
        break;
      case 41: /* tag_list ::= LB prefix */
      case 42: /* tag_list ::= LB termlist */ yytestcase(yyruleno==42);
{
    yymsp[-1].minor.yy17 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy17, yymsp[0].minor.yy17);
}
        break;
      case 43: /* tag_list ::= tag_list OR param_term */
{
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-2].minor.yy17, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
  yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 44: /* tag_list ::= tag_list OR STOPWORD */
{
    QueryNode_AddChild(yymsp[-2].minor.yy17, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 45: /* tag_list ::= tag_list OR prefix */
      case 46: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==46);
{
    QueryNode_AddChild(yymsp[-2].minor.yy17, yymsp[0].minor.yy17);
    yylhsminor.yy17 = yymsp[-2].minor.yy17;
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 48: /* expr ::= modifier COLON numeric_range */
{
  if (yymsp[0].minor.yy86) {
    // we keep the capitalization as is
    yymsp[0].minor.yy86->nf->fieldName = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy17 = NewNumericNode(yymsp[0].minor.yy86);
  } else {
    yylhsminor.yy17 = NewQueryNode(QN_NULL);
  }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 49: /* numeric_range ::= LSQB param_any param_any RSQB */
{
  // Update token type to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;
  if (yymsp[-2].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-2].minor.yy0.type = QT_PARAM_NUMERIC_MIN_RANGE;
  else if (yymsp[-2].minor.yy0.type != QT_NUMERIC)
    badToken = &yymsp[-2].minor.yy0;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-1].minor.yy0.type = QT_PARAM_NUMERIC_MAX_RANGE;
  else if (!badToken && yymsp[-1].minor.yy0.type != QT_NUMERIC)
    badToken = &yymsp[-1].minor.yy0;

  if (!badToken) {
    yymsp[-3].minor.yy86 = NewNumericFilterQueryParam_WithParams(ctx, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, yymsp[-2].minor.yy0.inclusive, yymsp[-1].minor.yy0.inclusive);
  } else {
    reportSyntaxError(ctx->status, badToken, "Expecting numeric or parameter");
    yymsp[-3].minor.yy86 = NULL;
  }
}
        break;
      case 50: /* expr ::= modifier COLON geo_filter */
{
  if (yymsp[0].minor.yy86) {
    // we keep the capitalization as is
    yymsp[0].minor.yy86->gf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy17 = NewGeofilterNode(yymsp[0].minor.yy86);
  } else {
    yylhsminor.yy17 = NewQueryNode(QN_NULL);
  }
}
  yymsp[-2].minor.yy17 = yylhsminor.yy17;
        break;
      case 51: /* geo_filter ::= LSQB param_any param_any param_any param_any RSQB */
{
  // Update token type to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;

  if (yymsp[-4].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-4].minor.yy0.type = QT_PARAM_GEO_COORD;
  else if (yymsp[-4].minor.yy0.type != QT_NUMERIC)
    badToken = &yymsp[-4].minor.yy0;
  if (yymsp[-3].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-3].minor.yy0.type = QT_PARAM_GEO_COORD;
  else if (!badToken && yymsp[-3].minor.yy0.type != QT_NUMERIC)
    badToken = &yymsp[-3].minor.yy0;
  if (yymsp[-2].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-2].minor.yy0.type = QT_PARAM_NUMERIC;
  else if (!badToken && yymsp[-2].minor.yy0.type != QT_NUMERIC)
    badToken = &yymsp[-2].minor.yy0;
  if (yymsp[-1].minor.yy0.type == QT_PARAM_ANY)
    yymsp[-1].minor.yy0.type = QT_PARAM_GEO_UNIT;
  else if (!badToken && yymsp[-1].minor.yy0.type != QT_TERM)
    badToken = &yymsp[-1].minor.yy0;

  if (!badToken) {
    yymsp[-5].minor.yy86 = NewGeoFilterQueryParam_WithParams(ctx, &yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
  } else {
    reportSyntaxError(ctx->status, badToken, "Syntax error");
    yymsp[-5].minor.yy86 = NULL;
  }
}
        break;
      case 52: /* expr ::= modifier COLON vector_filter */
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
      case 53: /* vector_filter ::= LSQB param_any param_any param_any RSQB */
{
  // Update token types to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;
  if (yymsp[-3].minor.yy0.type == QT_TERM) {
    yymsp[-3].minor.yy0.type = QT_VEC;
    // FIXME: Remove hack for handling lexer/scanner of terms with trailing equal signs.
    //  Equal signs are currently considered as punct (punctuation) and are not included in a
    //  term, But in base64 encoding, it is used as padding to extend the string to a length
    //  which is a multiple of 3.
    size_t len = yymsp[-3].minor.yy0.len;
    int remainder = len % 3;
    if (remainder == 1 && *((yymsp[-3].minor.yy0.s) + len) == '=' && *((yymsp[-3].minor.yy0.s) + len + 1) == '=')
      yymsp[-3].minor.yy0.len = len + 2;
    else if (remainder == 2 && *((yymsp[-3].minor.yy0.s) + len) == '=')
      yymsp[-3].minor.yy0.len = len + 1;
  } else if (yymsp[-3].minor.yy0.type == QT_PARAM_ANY) {
    yymsp[-3].minor.yy0.type = QT_PARAM_VEC;
  } else {
    badToken = &yymsp[-3].minor.yy0;
  }

  if (yymsp[-2].minor.yy0.type == QT_PARAM_ANY) {
    yymsp[-2].minor.yy0.type = QT_PARAM_VEC_SIM_TYPE;
  } else if (!badToken && yymsp[-2].minor.yy0.type != QT_TERM) {
    badToken = &yymsp[-2].minor.yy0;
  }

  if (yymsp[-1].minor.yy0.type == QT_PARAM_ANY) {
    yymsp[-1].minor.yy0.type = QT_PARAM_NUMERIC;
  } else if (!badToken && yymsp[-1].minor.yy0.type != QT_NUMERIC) {
    badToken = &yymsp[-1].minor.yy0;
  }

  if (!badToken) {
    yymsp[-4].minor.yy86 = NewVectorFilterQueryParam_WithParams(ctx, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
  } else {
    reportSyntaxError(ctx->status, badToken, "Syntax error");
    yymsp[-4].minor.yy86 = NULL;
  }
}
        break;
      case 54: /* num ::= NUMBER */
{
    yylhsminor.yy29.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy29.inclusive = 1;
}
  yymsp[0].minor.yy29 = yylhsminor.yy29;
        break;
      case 55: /* num ::= LP num */
{
    yymsp[-1].minor.yy29=yymsp[0].minor.yy29;
    yymsp[-1].minor.yy29.inclusive = 0;
}
        break;
      case 56: /* num ::= MINUS num */
{
    yymsp[0].minor.yy29.num = -yymsp[0].minor.yy29.num;
    yymsp[-1].minor.yy29 = yymsp[0].minor.yy29;
}
        break;
      case 57: /* term ::= TERM */
      case 58: /* term ::= NUMBER */ yytestcase(yyruleno==58);
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 59: /* param_term ::= TERM */
      case 64: /* param_any ::= TERM */ yytestcase(yyruleno==64);
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 60: /* param_term ::= NUMBER */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  // Number is treated as a term here
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 61: /* param_term ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 62: /* param_any ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_ANY;
  yylhsminor.yy0.inclusive = 1;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 63: /* param_any ::= LP ATTRIBUTE */
{
  yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;
  yymsp[-1].minor.yy0.type = QT_PARAM_ANY;
  yymsp[-1].minor.yy0.inclusive = 0; // Could be relevant if type is refined
}
        break;
      case 65: /* param_any ::= num */
{
  yylhsminor.yy0.numval = yymsp[0].minor.yy29.num;
  yylhsminor.yy0.inclusive = yymsp[0].minor.yy29.inclusive;
  yylhsminor.yy0.type = QT_NUMERIC;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
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
