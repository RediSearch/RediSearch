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
#include "util/arr.h"
#include "rmutil/vector.h"
#include "query_node.h"
#include "vector_index.h"
#include "query_param.h"
#include "query_internal.h"
#include "util/strconv.h"

// strndup + lowercase in one pass!
char *strdupcase(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
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
      "%s at offset %d near %.*s", msg, tok->pos, tok->len, tok->s);
  } else if (tok->type == QT_NUMERIC) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %f", msg, tok->pos, tok->numval);
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
#define YYNOCODE 53
#define YYACTIONTYPE unsigned short int
#define RSQueryParser_TOKENTYPE QueryToken
typedef union {
  int yyinit;
  RSQueryParser_TOKENTYPE yy0;
  QueryNode * yy31;
  VectorQueryParams yy34;
  QueryAttribute yy35;
  RangeNumber yy43;
  SingleVectorQueryParam yy63;
  Vector* yy76;
  QueryParam * yy104;
  QueryAttribute * yy105;
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
#define YYNSTATE             86
#define YYNRULE              81
#define YYNTOKEN             30
#define YY_MAX_SHIFT         85
#define YY_MIN_SHIFTREDUCE   144
#define YY_MAX_SHIFTREDUCE   224
#define YY_ERROR_ACTION      225
#define YY_ACCEPT_ACTION     226
#define YY_NO_ACTION         227
#define YY_MIN_REDUCE        228
#define YY_MAX_REDUCE        308
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
#define YY_ACTTAB_COUNT (363)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    61,   25,    6,  306,   20,   77,    7,  214,  215,  166,
 /*    10 */    22,   78,  213,  172,   30,  216,    8,  153,  179,  228,
 /*    20 */    10,    6,   85,   20,   85,    7,  214,  215,  166,  289,
 /*    30 */    49,  213,  172,   30,  216,    8,  229,  179,    6,   10,
 /*    40 */    20,   54,    7,  214,  215,  166,  223,    9,  213,  172,
 /*    50 */    30,  216,    2,    6,  179,   20,   31,    7,  214,  215,
 /*    60 */   166,  306,   47,  213,  172,   30,  216,    8,   66,  179,
 /*    70 */   160,   44,   83,  214,  215,  169,  306,   18,  213,   24,
 /*    80 */     6,  216,   20,   21,    7,  214,  215,  166,  223,   84,
 /*    90 */   213,  172,   30,  216,    2,   20,  179,    7,  214,  215,
 /*   100 */   166,  306,   48,  213,  172,   30,  216,    8,   23,  179,
 /*   110 */     6,   10,   20,   85,    7,  214,  215,  166,  252,   37,
 /*   120 */   213,  172,   30,  216,    8,   10,  179,   85,  214,  215,
 /*   130 */   166,   36,   37,  213,  172,   30,  216,    8,   52,  179,
 /*   140 */   159,   10,   14,   85,   38,  248,   50,  232,  292,   55,
 /*   150 */   278,  269,   33,  211,  212,   57,  264,  276,  210,    4,
 /*   160 */    58,   43,  248,   50,  232,  213,    3,   41,  216,  248,
 /*   170 */    50,  232,   57,   65,   25,  226,   53,   58,   43,   57,
 /*   180 */   217,  239,   56,   59,   58,   43,   15,  218,   19,  248,
 /*   190 */    50,  232,   28,    5,   27,  191,  248,   50,  232,   57,
 /*   200 */   273,   34,  265,    1,   58,   43,   57,   75,  283,  287,
 /*   210 */    12,   58,   43,  248,   50,  232,   42,    3,   46,  282,
 /*   220 */   248,   50,  232,   57,   39,  207,  206,  293,   58,   43,
 /*   230 */    57,  251,   63,  220,   40,   58,   43,   13,  230,  238,
 /*   240 */   248,   50,  232,   51,   16,  288,  179,  248,   50,  232,
 /*   250 */    57,  240,  224,  281,   60,   58,   43,   57,   26,  202,
 /*   260 */    62,   17,   58,   43,  248,   50,  232,  280,   64,  195,
 /*   270 */   214,  215,  184,  176,   57,  213,  172,  296,  216,   58,
 /*   280 */    43,   39,  207,  206,  214,  215,  169,  221,  177,  213,
 /*   290 */   219,   32,  216,   68,  178,  214,  215,  188,   70,   71,
 /*   300 */   213,  172,  295,  216,  193,  175,   73,   39,  207,  206,
 /*   310 */    39,  207,  206,  221,   74,   28,  219,   32,   45,  174,
 /*   320 */    40,  214,  215,   69,   76,  173,  213,  227,   35,  216,
 /*   330 */   214,  215,   67,  294,  162,  213,  227,   29,  216,  214,
 /*   340 */   215,   72,  227,  161,  213,   80,   79,  216,  227,  227,
 /*   350 */    81,  227,  227,   82,  214,  215,  227,  227,  227,  213,
 /*   360 */   227,  227,  216,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    39,   40,    2,   44,    4,   49,    6,    7,    8,    9,
 /*    10 */    51,   49,   12,   13,   14,   15,   16,   17,   18,    0,
 /*    20 */    20,    2,   22,    4,   22,    6,    7,    8,    9,   41,
 /*    30 */    42,   12,   13,   14,   15,   16,    0,   18,    2,   20,
 /*    40 */     4,   22,    6,    7,    8,    9,   10,    5,   12,   13,
 /*    50 */    14,   15,   16,    2,   18,    4,   34,    6,    7,    8,
 /*    60 */     9,   44,   20,   12,   13,   14,   15,   16,   51,   18,
 /*    70 */     4,   49,   50,    7,    8,    9,   44,   26,   12,   28,
 /*    80 */     2,   15,    4,   51,    6,    7,    8,    9,   10,   15,
 /*    90 */    12,   13,   14,   15,   16,    4,   18,    6,    7,    8,
 /*   100 */     9,   44,   23,   12,   13,   14,   15,   16,   51,   18,
 /*   110 */     2,   20,    4,   22,    6,    7,    8,    9,   49,   12,
 /*   120 */    12,   13,   14,   15,   16,   20,   18,   22,    7,    8,
 /*   130 */     9,   24,   12,   12,   13,   14,   15,   16,   25,   18,
 /*   140 */    27,   20,   30,   22,   24,   33,   34,   35,   44,   37,
 /*   150 */    38,   33,   34,    7,    8,   43,   50,   45,   12,   30,
 /*   160 */    48,   49,   33,   34,   35,   12,   30,   49,   15,   33,
 /*   170 */    34,   35,   43,   39,   40,   46,   47,   48,   49,   43,
 /*   180 */     8,   31,   32,   47,   48,   49,   30,   15,   20,   33,
 /*   190 */    34,   35,   26,   30,   28,   27,   33,   34,   35,   43,
 /*   200 */    33,   34,   50,    5,   48,   49,   43,   49,   49,   49,
 /*   210 */    30,   48,   49,   33,   34,   35,   49,   30,   20,   49,
 /*   220 */    33,   34,   35,   43,    6,    7,    8,   44,   48,   49,
 /*   230 */    43,   49,   48,   15,   16,   48,   49,   30,    0,   49,
 /*   240 */    33,   34,   35,   52,   30,   41,   18,   33,   34,   35,
 /*   250 */    43,   31,   17,    0,   29,   48,   49,   43,   28,   15,
 /*   260 */    22,   30,   48,   49,   33,   34,   35,    0,   29,   29,
 /*   270 */     7,    8,    9,   14,   43,   12,   13,    4,   15,   48,
 /*   280 */    49,    6,    7,    8,    7,    8,    9,   12,   14,   12,
 /*   290 */    15,   16,   15,   14,   14,    7,    8,    9,   14,   14,
 /*   300 */    12,   13,    4,   15,   29,   14,   14,    6,    7,    8,
 /*   310 */     6,    7,    8,   12,   14,   26,   15,   16,    5,   14,
 /*   320 */    16,    7,    8,    9,   14,   14,   12,   53,   14,   15,
 /*   330 */     7,    8,    9,    4,    4,   12,   53,   14,   15,    7,
 /*   340 */     8,    9,   53,    4,   12,    7,    8,   15,   53,   53,
 /*   350 */    12,   53,   53,   15,    7,    8,   53,   53,   53,   12,
 /*   360 */    53,   53,   15,   53,   53,   53,   53,   53,   53,   53,
 /*   370 */    53,   53,   53,   53,   53,   53,   53,   53,   53,   53,
 /*   380 */    53,   53,   53,   53,   53,
};
#define YY_SHIFT_COUNT    (85)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (347)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    36,   51,   78,    0,   19,   91,  108,  108,  108,  108,
 /*    10 */   108,  108,  121,  105,  105,  105,    2,    2,  263,  288,
 /*    20 */   338,  275,  301,  301,  301,  107,   79,   79,   74,  314,
 /*    30 */   323,   66,  218,  277,  277,  332,  347,  347,  347,  304,
 /*    40 */   304,  347,  347,  347,  347,  347,  146,  146,  172,  120,
 /*    50 */   153,  228,   74,  238,  166,  168,  113,   42,  198,  235,
 /*    60 */   253,  225,  230,  244,  267,  239,  240,  259,  274,  279,
 /*    70 */   280,  284,  285,  291,  292,  300,  305,  310,  311,  273,
 /*    80 */   298,  329,  330,  339,  313,  289,
};
#define YY_REDUCE_COUNT (52)
#define YY_REDUCE_MIN   (-44)
#define YY_REDUCE_MAX   (231)
static const short yy_reduce_ofst[] = {
 /*     0 */   129,  112,  136,  156,  156,  156,  163,  180,  187,  207,
 /*    10 */   214,  231,  156,  156,  156,  156,  156,  156,  118,  167,
 /*    20 */    22,  -41,   17,   32,   57,  -12,  -39,  134,  150,  -44,
 /*    30 */   -38,   69,  104,   69,   69,  158,  159,  160,  170,  183,
 /*    40 */   104,  182,  182,  182,  182,  190,  106,  152,  191,  204,
 /*    50 */    69,  184,  220,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   225,  225,  225,  225,  225,  255,  225,  225,  225,  225,
 /*    10 */   225,  225,  254,  236,  235,  231,  233,  234,  225,  225,
 /*    20 */   225,  225,  225,  225,  225,  285,  225,  225,  242,  225,
 /*    30 */   225,  225,  225,  270,  274,  225,  225,  225,  225,  225,
 /*    40 */   225,  267,  271,  247,  225,  225,  225,  225,  225,  284,
 /*    50 */   249,  225,  241,  225,  225,  266,  225,  225,  225,  225,
 /*    60 */   225,  225,  225,  225,  225,  225,  225,  225,  225,  225,
 /*    70 */   225,  225,  225,  225,  225,  225,  225,  225,  225,  299,
 /*    80 */   298,  297,  300,  225,  225,  225,
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
  /*    8 */ "SIZE",
  /*    9 */ "STOPWORD",
  /*   10 */ "STAR",
  /*   11 */ "TERMLIST",
  /*   12 */ "TERM",
  /*   13 */ "PREFIX",
  /*   14 */ "PERCENT",
  /*   15 */ "ATTRIBUTE",
  /*   16 */ "LP",
  /*   17 */ "RP",
  /*   18 */ "MODIFIER",
  /*   19 */ "AND",
  /*   20 */ "OR",
  /*   21 */ "ORX",
  /*   22 */ "ARROW",
  /*   23 */ "TOP_K",
  /*   24 */ "AS",
  /*   25 */ "SEMICOLON",
  /*   26 */ "LB",
  /*   27 */ "RB",
  /*   28 */ "LSQB",
  /*   29 */ "RSQB",
  /*   30 */ "expr",
  /*   31 */ "attribute",
  /*   32 */ "attribute_list",
  /*   33 */ "prefix",
  /*   34 */ "termlist",
  /*   35 */ "union",
  /*   36 */ "fuzzy",
  /*   37 */ "tag_list",
  /*   38 */ "geo_filter",
  /*   39 */ "vector_query",
  /*   40 */ "vector_command",
  /*   41 */ "vector_attribute",
  /*   42 */ "vector_attribute_list",
  /*   43 */ "modifierlist",
  /*   44 */ "num",
  /*   45 */ "numeric_range",
  /*   46 */ "query",
  /*   47 */ "star",
  /*   48 */ "modifier",
  /*   49 */ "param_term",
  /*   50 */ "term",
  /*   51 */ "param_any",
  /*   52 */ "param_size",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "query ::= expr",
 /*   1 */ "query ::=",
 /*   2 */ "query ::= star",
 /*   3 */ "expr ::= expr expr",
 /*   4 */ "expr ::= union",
 /*   5 */ "union ::= expr OR expr",
 /*   6 */ "union ::= union OR expr",
 /*   7 */ "expr ::= modifier COLON expr",
 /*   8 */ "expr ::= modifierlist COLON expr",
 /*   9 */ "expr ::= LP expr RP",
 /*  10 */ "attribute ::= ATTRIBUTE COLON param_term",
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
 /*  52 */ "query ::= expr ARROW LSQB vector_query RSQB",
 /*  53 */ "query ::= star ARROW LSQB vector_query RSQB",
 /*  54 */ "vector_query ::= vector_command vector_attribute_list AS param_term",
 /*  55 */ "vector_query ::= vector_command AS param_term",
 /*  56 */ "vector_query ::= vector_command vector_attribute_list",
 /*  57 */ "vector_query ::= vector_command",
 /*  58 */ "vector_command ::= TOP_K param_size modifier ATTRIBUTE",
 /*  59 */ "vector_attribute ::= TERM param_term",
 /*  60 */ "vector_attribute_list ::= vector_attribute_list vector_attribute",
 /*  61 */ "vector_attribute_list ::= vector_attribute",
 /*  62 */ "num ::= SIZE",
 /*  63 */ "num ::= NUMBER",
 /*  64 */ "num ::= LP num",
 /*  65 */ "num ::= MINUS num",
 /*  66 */ "term ::= TERM",
 /*  67 */ "term ::= NUMBER",
 /*  68 */ "term ::= SIZE",
 /*  69 */ "param_term ::= TERM",
 /*  70 */ "param_term ::= NUMBER",
 /*  71 */ "param_term ::= SIZE",
 /*  72 */ "param_term ::= ATTRIBUTE",
 /*  73 */ "param_size ::= SIZE",
 /*  74 */ "param_size ::= ATTRIBUTE",
 /*  75 */ "param_any ::= ATTRIBUTE",
 /*  76 */ "param_any ::= LP ATTRIBUTE",
 /*  77 */ "param_any ::= TERM",
 /*  78 */ "param_any ::= num",
 /*  79 */ "star ::= STAR",
 /*  80 */ "star ::= LP star RP",
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
    case 41: /* vector_attribute */
    case 44: /* num */
    case 46: /* query */
    case 47: /* star */
    case 48: /* modifier */
    case 49: /* param_term */
    case 50: /* term */
    case 51: /* param_any */
    case 52: /* param_size */
{
 
}
      break;
    case 30: /* expr */
    case 33: /* prefix */
    case 34: /* termlist */
    case 35: /* union */
    case 36: /* fuzzy */
    case 37: /* tag_list */
    case 39: /* vector_query */
    case 40: /* vector_command */
{
 QueryNode_Free((yypminor->yy31)); 
}
      break;
    case 31: /* attribute */
{
 rm_free((char*)(yypminor->yy35).value); 
}
      break;
    case 32: /* attribute_list */
{
 array_free_ex((yypminor->yy105), rm_free((char*)((QueryAttribute*)ptr )->value)); 
}
      break;
    case 38: /* geo_filter */
{
 QueryParam_Free((yypminor->yy104)); 
}
      break;
    case 42: /* vector_attribute_list */
{

  array_free((yypminor->yy34).needResolve);
  array_free_ex((yypminor->yy34).params, {
    rm_free((char*)((VecSimRawParam*)ptr)->value);
    rm_free((char*)((VecSimRawParam*)ptr)->name);
  });

}
      break;
    case 43: /* modifierlist */
{
 
    for (size_t i = 0; i < Vector_Size((yypminor->yy76)); i++) {
        char *s;
        Vector_Get((yypminor->yy76), i, &s);
        rm_free(s);
    }
    Vector_Free((yypminor->yy76)); 

}
      break;
    case 45: /* numeric_range */
{

  QueryParam_Free((yypminor->yy104));

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
  {   46,   -1 }, /* (0) query ::= expr */
  {   46,    0 }, /* (1) query ::= */
  {   46,   -1 }, /* (2) query ::= star */
  {   30,   -2 }, /* (3) expr ::= expr expr */
  {   30,   -1 }, /* (4) expr ::= union */
  {   35,   -3 }, /* (5) union ::= expr OR expr */
  {   35,   -3 }, /* (6) union ::= union OR expr */
  {   30,   -3 }, /* (7) expr ::= modifier COLON expr */
  {   30,   -3 }, /* (8) expr ::= modifierlist COLON expr */
  {   30,   -3 }, /* (9) expr ::= LP expr RP */
  {   31,   -3 }, /* (10) attribute ::= ATTRIBUTE COLON param_term */
  {   32,   -1 }, /* (11) attribute_list ::= attribute */
  {   32,   -3 }, /* (12) attribute_list ::= attribute_list SEMICOLON attribute */
  {   32,   -2 }, /* (13) attribute_list ::= attribute_list SEMICOLON */
  {   32,    0 }, /* (14) attribute_list ::= */
  {   30,   -5 }, /* (15) expr ::= expr ARROW LB attribute_list RB */
  {   30,   -3 }, /* (16) expr ::= QUOTE termlist QUOTE */
  {   30,   -3 }, /* (17) expr ::= QUOTE term QUOTE */
  {   30,   -3 }, /* (18) expr ::= QUOTE ATTRIBUTE QUOTE */
  {   30,   -1 }, /* (19) expr ::= param_term */
  {   30,   -1 }, /* (20) expr ::= prefix */
  {   30,   -1 }, /* (21) expr ::= termlist */
  {   30,   -1 }, /* (22) expr ::= STOPWORD */
  {   34,   -2 }, /* (23) termlist ::= param_term param_term */
  {   34,   -2 }, /* (24) termlist ::= termlist param_term */
  {   34,   -2 }, /* (25) termlist ::= termlist STOPWORD */
  {   30,   -2 }, /* (26) expr ::= MINUS expr */
  {   30,   -2 }, /* (27) expr ::= TILDE expr */
  {   33,   -1 }, /* (28) prefix ::= PREFIX */
  {   30,   -3 }, /* (29) expr ::= PERCENT param_term PERCENT */
  {   30,   -5 }, /* (30) expr ::= PERCENT PERCENT param_term PERCENT PERCENT */
  {   30,   -7 }, /* (31) expr ::= PERCENT PERCENT PERCENT param_term PERCENT PERCENT PERCENT */
  {   30,   -3 }, /* (32) expr ::= PERCENT STOPWORD PERCENT */
  {   30,   -5 }, /* (33) expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */
  {   30,   -7 }, /* (34) expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */
  {   48,   -1 }, /* (35) modifier ::= MODIFIER */
  {   43,   -3 }, /* (36) modifierlist ::= modifier OR term */
  {   43,   -3 }, /* (37) modifierlist ::= modifierlist OR term */
  {   30,   -3 }, /* (38) expr ::= modifier COLON tag_list */
  {   37,   -2 }, /* (39) tag_list ::= LB param_term */
  {   37,   -2 }, /* (40) tag_list ::= LB STOPWORD */
  {   37,   -2 }, /* (41) tag_list ::= LB prefix */
  {   37,   -2 }, /* (42) tag_list ::= LB termlist */
  {   37,   -3 }, /* (43) tag_list ::= tag_list OR param_term */
  {   37,   -3 }, /* (44) tag_list ::= tag_list OR STOPWORD */
  {   37,   -3 }, /* (45) tag_list ::= tag_list OR prefix */
  {   37,   -3 }, /* (46) tag_list ::= tag_list OR termlist */
  {   37,   -2 }, /* (47) tag_list ::= tag_list RB */
  {   30,   -3 }, /* (48) expr ::= modifier COLON numeric_range */
  {   45,   -4 }, /* (49) numeric_range ::= LSQB param_any param_any RSQB */
  {   30,   -3 }, /* (50) expr ::= modifier COLON geo_filter */
  {   38,   -6 }, /* (51) geo_filter ::= LSQB param_any param_any param_any param_any RSQB */
  {   46,   -5 }, /* (52) query ::= expr ARROW LSQB vector_query RSQB */
  {   46,   -5 }, /* (53) query ::= star ARROW LSQB vector_query RSQB */
  {   39,   -4 }, /* (54) vector_query ::= vector_command vector_attribute_list AS param_term */
  {   39,   -3 }, /* (55) vector_query ::= vector_command AS param_term */
  {   39,   -2 }, /* (56) vector_query ::= vector_command vector_attribute_list */
  {   39,   -1 }, /* (57) vector_query ::= vector_command */
  {   40,   -4 }, /* (58) vector_command ::= TOP_K param_size modifier ATTRIBUTE */
  {   41,   -2 }, /* (59) vector_attribute ::= TERM param_term */
  {   42,   -2 }, /* (60) vector_attribute_list ::= vector_attribute_list vector_attribute */
  {   42,   -1 }, /* (61) vector_attribute_list ::= vector_attribute */
  {   44,   -1 }, /* (62) num ::= SIZE */
  {   44,   -1 }, /* (63) num ::= NUMBER */
  {   44,   -2 }, /* (64) num ::= LP num */
  {   44,   -2 }, /* (65) num ::= MINUS num */
  {   50,   -1 }, /* (66) term ::= TERM */
  {   50,   -1 }, /* (67) term ::= NUMBER */
  {   50,   -1 }, /* (68) term ::= SIZE */
  {   49,   -1 }, /* (69) param_term ::= TERM */
  {   49,   -1 }, /* (70) param_term ::= NUMBER */
  {   49,   -1 }, /* (71) param_term ::= SIZE */
  {   49,   -1 }, /* (72) param_term ::= ATTRIBUTE */
  {   52,   -1 }, /* (73) param_size ::= SIZE */
  {   52,   -1 }, /* (74) param_size ::= ATTRIBUTE */
  {   51,   -1 }, /* (75) param_any ::= ATTRIBUTE */
  {   51,   -2 }, /* (76) param_any ::= LP ATTRIBUTE */
  {   51,   -1 }, /* (77) param_any ::= TERM */
  {   51,   -1 }, /* (78) param_any ::= num */
  {   47,   -1 }, /* (79) star ::= STAR */
  {   47,   -3 }, /* (80) star ::= LP star RP */
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
  ctx->root = yymsp[0].minor.yy31;
}
        break;
      case 1: /* query ::= */
{
  ctx->root = NULL;
}
        break;
      case 2: /* query ::= star */
{  yy_destructor(yypParser,47,&yymsp[0].minor);
{
  setup_trace(ctx);
  ctx->root = NewWildcardNode();
}
}
        break;
      case 3: /* expr ::= expr expr */
{
    int rv = one_not_null(yymsp[-1].minor.yy31, yymsp[0].minor.yy31, (void**)&yylhsminor.yy31);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy31 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- `out` is already assigned
    } else {
        if (yymsp[-1].minor.yy31 && yymsp[-1].minor.yy31->type == QN_PHRASE && yymsp[-1].minor.yy31->pn.exact == 0 && 
            yymsp[-1].minor.yy31->opts.fieldMask == RS_FIELDMASK_ALL ) {
            yylhsminor.yy31 = yymsp[-1].minor.yy31;
        } else {     
            yylhsminor.yy31 = NewPhraseNode(0);
            QueryNode_AddChild(yylhsminor.yy31, yymsp[-1].minor.yy31);
        }
        QueryNode_AddChild(yylhsminor.yy31, yymsp[0].minor.yy31);
    }
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 4: /* expr ::= union */
      case 20: /* expr ::= prefix */ yytestcase(yyruleno==20);
{
    yylhsminor.yy31 = yymsp[0].minor.yy31;
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 5: /* union ::= expr OR expr */
{
    int rv = one_not_null(yymsp[-2].minor.yy31, yymsp[0].minor.yy31, (void**)&yylhsminor.yy31);
    if (rv == NODENN_BOTH_INVALID) {
        yylhsminor.yy31 = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (yymsp[-2].minor.yy31->type == QN_UNION && yymsp[-2].minor.yy31->opts.fieldMask == RS_FIELDMASK_ALL) {
            yylhsminor.yy31 = yymsp[-2].minor.yy31;
        } else {
            yylhsminor.yy31 = NewUnionNode();
            QueryNode_AddChild(yylhsminor.yy31, yymsp[-2].minor.yy31);
            yylhsminor.yy31->opts.fieldMask |= yymsp[-2].minor.yy31->opts.fieldMask;
        }

        // Handle yymsp[0].minor.yy31
        QueryNode_AddChild(yylhsminor.yy31, yymsp[0].minor.yy31);
        yylhsminor.yy31->opts.fieldMask |= yymsp[0].minor.yy31->opts.fieldMask;
        QueryNode_SetFieldMask(yylhsminor.yy31, yylhsminor.yy31->opts.fieldMask);
    }
    
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 6: /* union ::= union OR expr */
{
    yylhsminor.yy31 = yymsp[-2].minor.yy31;
    if (yymsp[0].minor.yy31) {
        QueryNode_AddChild(yylhsminor.yy31, yymsp[0].minor.yy31);
        yylhsminor.yy31->opts.fieldMask |= yymsp[0].minor.yy31->opts.fieldMask;
        QueryNode_SetFieldMask(yymsp[0].minor.yy31, yylhsminor.yy31->opts.fieldMask);
    }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 7: /* expr ::= modifier COLON expr */
{
    if (yymsp[0].minor.yy31 == NULL) {
        yylhsminor.yy31 = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(yymsp[0].minor.yy31, IndexSpec_GetFieldBit(ctx->sctx->spec, yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len));
        }
        yylhsminor.yy31 = yymsp[0].minor.yy31; 
    }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 8: /* expr ::= modifierlist COLON expr */
{
    
    if (yymsp[0].minor.yy31 == NULL) {
        yylhsminor.yy31 = NULL;
    } else {
        //yymsp[0].minor.yy31->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(yymsp[-2].minor.yy76); i++) {
                char *p;
                Vector_Get(yymsp[-2].minor.yy76, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                rm_free(p);
            }
        }
        QueryNode_SetFieldMask(yymsp[0].minor.yy31, mask);
        Vector_Free(yymsp[-2].minor.yy76);
        yylhsminor.yy31=yymsp[0].minor.yy31;
    }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 9: /* expr ::= LP expr RP */
{
    yymsp[-2].minor.yy31 = yymsp[-1].minor.yy31;
}
        break;
      case 10: /* attribute ::= ATTRIBUTE COLON param_term */
{
  const char *value = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
  size_t value_len = yymsp[0].minor.yy0.len;
  if (yymsp[0].minor.yy0.type == QT_PARAM_TERM) {
    size_t found_value_len;
    const char *found_value = Param_DictGet(ctx->opts->params, value, &found_value_len, ctx->status);
    if (found_value) {
      rm_free((char*)value);
      value = rm_strndup(found_value, found_value_len);
      value_len = found_value_len;
    }
  }
  yylhsminor.yy35 = (QueryAttribute){ .name = yymsp[-2].minor.yy0.s, .namelen = yymsp[-2].minor.yy0.len, .value = value, .vallen = value_len };
}
  yymsp[-2].minor.yy35 = yylhsminor.yy35;
        break;
      case 11: /* attribute_list ::= attribute */
{
  yylhsminor.yy105 = array_new(QueryAttribute, 2);
  yylhsminor.yy105 = array_append(yylhsminor.yy105, yymsp[0].minor.yy35);
}
  yymsp[0].minor.yy105 = yylhsminor.yy105;
        break;
      case 12: /* attribute_list ::= attribute_list SEMICOLON attribute */
{
  yylhsminor.yy105 = array_append(yymsp[-2].minor.yy105, yymsp[0].minor.yy35);
}
  yymsp[-2].minor.yy105 = yylhsminor.yy105;
        break;
      case 13: /* attribute_list ::= attribute_list SEMICOLON */
{
    yylhsminor.yy105 = yymsp[-1].minor.yy105;
}
  yymsp[-1].minor.yy105 = yylhsminor.yy105;
        break;
      case 14: /* attribute_list ::= */
{
    yymsp[1].minor.yy105 = NULL;
}
        break;
      case 15: /* expr ::= expr ARROW LB attribute_list RB */
{

    if (yymsp[-4].minor.yy31 && yymsp[-1].minor.yy105) {
        QueryNode_ApplyAttributes(yymsp[-4].minor.yy31, yymsp[-1].minor.yy105, array_len(yymsp[-1].minor.yy105), ctx->status);
    }
    array_free_ex(yymsp[-1].minor.yy105, rm_free((char*)((QueryAttribute*)ptr )->value));
    yylhsminor.yy31 = yymsp[-4].minor.yy31;
}
  yymsp[-4].minor.yy31 = yylhsminor.yy31;
        break;
      case 16: /* expr ::= QUOTE termlist QUOTE */
{
  // TODO: Quoted/verbatim string in termlist should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  yymsp[-1].minor.yy31->pn.exact = 1;
  yymsp[-1].minor.yy31->opts.flags |= QueryNode_Verbatim;

  yymsp[-2].minor.yy31 = yymsp[-1].minor.yy31;
}
        break;
      case 17: /* expr ::= QUOTE term QUOTE */
{
  yymsp[-2].minor.yy31 = NewTokenNode(ctx, rm_strdupcase(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len), -1);
  yymsp[-2].minor.yy31->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 18: /* expr ::= QUOTE ATTRIBUTE QUOTE */
{
  // Quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(yymsp[-1].minor.yy0.len + 1);
  *s = '$';
  memcpy(s + 1, yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
  yymsp[-2].minor.yy31 = NewTokenNode(ctx, rm_strdupcase(s, yymsp[-1].minor.yy0.len + 1), -1);
  rm_free(s);
  yymsp[-2].minor.yy31->opts.flags |= QueryNode_Verbatim;
}
        break;
      case 19: /* expr ::= param_term */
{
  yylhsminor.yy31 = NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 21: /* expr ::= termlist */
{
        yylhsminor.yy31 = yymsp[0].minor.yy31;
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 22: /* expr ::= STOPWORD */
{
    yymsp[0].minor.yy31 = NULL;
}
        break;
      case 23: /* termlist ::= param_term param_term */
{
  yylhsminor.yy31 = NewPhraseNode(0);
  QueryNode_AddChild(yylhsminor.yy31, NewTokenNode_WithParams(ctx, &yymsp[-1].minor.yy0));
  QueryNode_AddChild(yylhsminor.yy31, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 24: /* termlist ::= termlist param_term */
{
  yylhsminor.yy31 = yymsp[-1].minor.yy31;
  QueryNode_AddChild(yylhsminor.yy31, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 25: /* termlist ::= termlist STOPWORD */
      case 47: /* tag_list ::= tag_list RB */ yytestcase(yyruleno==47);
{
    yylhsminor.yy31 = yymsp[-1].minor.yy31;
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 26: /* expr ::= MINUS expr */
{ 
    if (yymsp[0].minor.yy31) {
        yymsp[-1].minor.yy31 = NewNotNode(yymsp[0].minor.yy31);
    } else {
        yymsp[-1].minor.yy31 = NULL;
    }
}
        break;
      case 27: /* expr ::= TILDE expr */
{ 
    if (yymsp[0].minor.yy31) {
        yymsp[-1].minor.yy31 = NewOptionalNode(yymsp[0].minor.yy31);
    } else {
        yymsp[-1].minor.yy31 = NULL;
    }
}
        break;
      case 28: /* prefix ::= PREFIX */
{
    yylhsminor.yy31 = NewPrefixNode_WithParams(ctx, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 29: /* expr ::= PERCENT param_term PERCENT */
      case 32: /* expr ::= PERCENT STOPWORD PERCENT */ yytestcase(yyruleno==32);
{
  yymsp[-2].minor.yy31 = NewFuzzyNode_WithParams(ctx, &yymsp[-1].minor.yy0, 1);
}
        break;
      case 30: /* expr ::= PERCENT PERCENT param_term PERCENT PERCENT */
      case 33: /* expr ::= PERCENT PERCENT STOPWORD PERCENT PERCENT */ yytestcase(yyruleno==33);
{
  yymsp[-4].minor.yy31 = NewFuzzyNode_WithParams(ctx, &yymsp[-2].minor.yy0, 2);
}
        break;
      case 31: /* expr ::= PERCENT PERCENT PERCENT param_term PERCENT PERCENT PERCENT */
      case 34: /* expr ::= PERCENT PERCENT PERCENT STOPWORD PERCENT PERCENT PERCENT */ yytestcase(yyruleno==34);
{
  yymsp[-6].minor.yy31 = NewFuzzyNode_WithParams(ctx, &yymsp[-3].minor.yy0, 3);
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
    yylhsminor.yy76 = NewVector(char *, 2);
    char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    Vector_Push(yylhsminor.yy76, s);
    s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yylhsminor.yy76, s);
}
  yymsp[-2].minor.yy76 = yylhsminor.yy76;
        break;
      case 37: /* modifierlist ::= modifierlist OR term */
{
    char *s = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
    Vector_Push(yymsp[-2].minor.yy76, s);
    yylhsminor.yy76 = yymsp[-2].minor.yy76;
}
  yymsp[-2].minor.yy76 = yylhsminor.yy76;
        break;
      case 38: /* expr ::= modifier COLON tag_list */
{
    if (!yymsp[0].minor.yy31) {
        yylhsminor.yy31= NULL;
    } else {
      // Tag field names must be case sensitive, we can't do rm_strdupcase
        char *s = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
        size_t slen = unescapen((char*)s, yymsp[-2].minor.yy0.len);

        yylhsminor.yy31 = NewTagNode(s, slen);
        QueryNode_AddChildren(yylhsminor.yy31, yymsp[0].minor.yy31->children, QueryNode_NumChildren(yymsp[0].minor.yy31));
        
        // Set the children count on yymsp[0].minor.yy31 to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(yymsp[0].minor.yy31, 0);
        QueryNode_Free(yymsp[0].minor.yy31);
    }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 39: /* tag_list ::= LB param_term */
{
  yymsp[-1].minor.yy31 = NewPhraseNode(0);
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-1].minor.yy31, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
}
        break;
      case 40: /* tag_list ::= LB STOPWORD */
{
    yymsp[-1].minor.yy31 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy31, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
}
        break;
      case 41: /* tag_list ::= LB prefix */
      case 42: /* tag_list ::= LB termlist */ yytestcase(yyruleno==42);
{
    yymsp[-1].minor.yy31 = NewPhraseNode(0);
    QueryNode_AddChild(yymsp[-1].minor.yy31, yymsp[0].minor.yy31);
}
        break;
      case 43: /* tag_list ::= tag_list OR param_term */
{
  if (yymsp[0].minor.yy0.type == QT_TERM)
    yymsp[0].minor.yy0.type = QT_TERM_CASE;
  else if (yymsp[0].minor.yy0.type == QT_PARAM_TERM)
    yymsp[0].minor.yy0.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(yymsp[-2].minor.yy31, NewTokenNode_WithParams(ctx, &yymsp[0].minor.yy0));
  yylhsminor.yy31 = yymsp[-2].minor.yy31;
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 44: /* tag_list ::= tag_list OR STOPWORD */
{
    QueryNode_AddChild(yymsp[-2].minor.yy31, NewTokenNode(ctx, rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len), -1));
    yylhsminor.yy31 = yymsp[-2].minor.yy31;
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 45: /* tag_list ::= tag_list OR prefix */
      case 46: /* tag_list ::= tag_list OR termlist */ yytestcase(yyruleno==46);
{
    QueryNode_AddChild(yymsp[-2].minor.yy31, yymsp[0].minor.yy31);
    yylhsminor.yy31 = yymsp[-2].minor.yy31;
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 48: /* expr ::= modifier COLON numeric_range */
{
  if (yymsp[0].minor.yy104) {
    // we keep the capitalization as is
    yymsp[0].minor.yy104->nf->fieldName = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy31 = NewNumericNode(yymsp[0].minor.yy104);
  } else {
    yylhsminor.yy31 = NewQueryNode(QN_NULL);
  }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
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
    yymsp[-3].minor.yy104 = NewNumericFilterQueryParam_WithParams(ctx, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, yymsp[-2].minor.yy0.inclusive, yymsp[-1].minor.yy0.inclusive);
  } else {
    reportSyntaxError(ctx->status, badToken, "Expecting numeric or parameter");
    yymsp[-3].minor.yy104 = NULL;
  }
}
        break;
      case 50: /* expr ::= modifier COLON geo_filter */
{
  if (yymsp[0].minor.yy104) {
    // we keep the capitalization as is
    yymsp[0].minor.yy104->gf->property = rm_strndup(yymsp[-2].minor.yy0.s, yymsp[-2].minor.yy0.len);
    yylhsminor.yy31 = NewGeofilterNode(yymsp[0].minor.yy104);
  } else {
    yylhsminor.yy31 = NewQueryNode(QN_NULL);
  }
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
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
    yymsp[-5].minor.yy104 = NewGeoFilterQueryParam_WithParams(ctx, &yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0);
  } else {
    reportSyntaxError(ctx->status, badToken, "Syntax error");
    yymsp[-5].minor.yy104 = NULL;
  }
}
        break;
      case 52: /* query ::= expr ARROW LSQB vector_query RSQB */
{ // main parse, hybrid query as entire query case.
  setup_trace(ctx);
  switch (yymsp[-1].minor.yy31->vn.vq->type) {
    case VECSIM_QT_TOPK:
      yymsp[-1].minor.yy31->vn.vq->topk.order = BY_SCORE;
      break;
  }
  ctx->root = yymsp[-1].minor.yy31;
  QueryNode_AddChild(yymsp[-1].minor.yy31, yymsp[-4].minor.yy31);
}
        break;
      case 53: /* query ::= star ARROW LSQB vector_query RSQB */
{  yy_destructor(yypParser,47,&yymsp[-4].minor);
{ // main parse, simple vecsim search as entire query case.
  setup_trace(ctx);
  switch (yymsp[-1].minor.yy31->vn.vq->type) {
    case VECSIM_QT_TOPK:
      yymsp[-1].minor.yy31->vn.vq->topk.order = BY_SCORE;
      break;
  }
  ctx->root = yymsp[-1].minor.yy31;
}
}
        break;
      case 54: /* vector_query ::= vector_command vector_attribute_list AS param_term */
{
  if (yymsp[-3].minor.yy31->vn.vq->scoreField) {
    rm_free(yymsp[-3].minor.yy31->vn.vq->scoreField);
    yymsp[-3].minor.yy31->vn.vq->scoreField = NULL;
  }
  yymsp[-3].minor.yy31->params = array_grow(yymsp[-3].minor.yy31->params, 1);
  memset(&array_tail(yymsp[-3].minor.yy31->params), 0, sizeof(*yymsp[-3].minor.yy31->params));
  QueryNode_SetParam(ctx, &(array_tail(yymsp[-3].minor.yy31->params)), &(yymsp[-3].minor.yy31->vn.vq->scoreField), NULL, &yymsp[0].minor.yy0);
  yymsp[-3].minor.yy31->vn.vq->params = yymsp[-2].minor.yy34;
  yylhsminor.yy31 = yymsp[-3].minor.yy31;
}
  yymsp[-3].minor.yy31 = yylhsminor.yy31;
        break;
      case 55: /* vector_query ::= vector_command AS param_term */
{
  if (yymsp[-2].minor.yy31->vn.vq->scoreField) {
    rm_free(yymsp[-2].minor.yy31->vn.vq->scoreField);
    yymsp[-2].minor.yy31->vn.vq->scoreField = NULL;
  }
  yymsp[-2].minor.yy31->params = array_grow(yymsp[-2].minor.yy31->params, 1);
  memset(&array_tail(yymsp[-2].minor.yy31->params), 0, sizeof(*yymsp[-2].minor.yy31->params));
  QueryNode_SetParam(ctx, &(array_tail(yymsp[-2].minor.yy31->params)), &(yymsp[-2].minor.yy31->vn.vq->scoreField), NULL, &yymsp[0].minor.yy0);
  yylhsminor.yy31 = yymsp[-2].minor.yy31;
}
  yymsp[-2].minor.yy31 = yylhsminor.yy31;
        break;
      case 56: /* vector_query ::= vector_command vector_attribute_list */
{
  yymsp[-1].minor.yy31->vn.vq->params = yymsp[0].minor.yy34;
  yylhsminor.yy31 = yymsp[-1].minor.yy31;
}
  yymsp[-1].minor.yy31 = yylhsminor.yy31;
        break;
      case 57: /* vector_query ::= vector_command */
{
  yylhsminor.yy31 = yymsp[0].minor.yy31;
}
  yymsp[0].minor.yy31 = yylhsminor.yy31;
        break;
      case 58: /* vector_command ::= TOP_K param_size modifier ATTRIBUTE */
{
  yymsp[0].minor.yy0.type = QT_PARAM_VEC;
  yymsp[-3].minor.yy31 = NewVectorNode_WithParams(ctx, VECSIM_QT_TOPK, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
  yymsp[-3].minor.yy31->vn.vq->property = rm_strndup(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
  RedisModule_Assert(-1 != (rm_asprintf(&yymsp[-3].minor.yy31->vn.vq->scoreField, "__%.*s_score", yymsp[-1].minor.yy0.len, yymsp[-1].minor.yy0.s)));
}
        break;
      case 59: /* vector_attribute ::= TERM param_term */
{
  const char *value = rm_strndup(yymsp[0].minor.yy0.s, yymsp[0].minor.yy0.len);
  const char *name = rm_strndup(yymsp[-1].minor.yy0.s, yymsp[-1].minor.yy0.len);
  yylhsminor.yy63.param = (VecSimRawParam){ .name = name, .nameLen = yymsp[-1].minor.yy0.len, .value = value, .valLen = yymsp[0].minor.yy0.len };
  if (yymsp[0].minor.yy0.type == QT_PARAM_TERM) {
    yylhsminor.yy63.needResolve = true;
  }
  else { // if yymsp[0].minor.yy0.type == QT_TERM
    yylhsminor.yy63.needResolve = false;
  }
}
  yymsp[-1].minor.yy63 = yylhsminor.yy63;
        break;
      case 60: /* vector_attribute_list ::= vector_attribute_list vector_attribute */
{
  yylhsminor.yy34.params = array_append(yymsp[-1].minor.yy34.params, yymsp[0].minor.yy63.param);
  yylhsminor.yy34.needResolve = array_append(yymsp[-1].minor.yy34.needResolve, yymsp[0].minor.yy63.needResolve);
}
  yymsp[-1].minor.yy34 = yylhsminor.yy34;
        break;
      case 61: /* vector_attribute_list ::= vector_attribute */
{
  yylhsminor.yy34.params = array_new(VecSimRawParam, 1);
  yylhsminor.yy34.needResolve = array_new(bool, 1);
  yylhsminor.yy34.params = array_append(yylhsminor.yy34.params, yymsp[0].minor.yy63.param);
  yylhsminor.yy34.needResolve = array_append(yylhsminor.yy34.needResolve, yymsp[0].minor.yy63.needResolve);
}
  yymsp[0].minor.yy34 = yylhsminor.yy34;
        break;
      case 62: /* num ::= SIZE */
      case 63: /* num ::= NUMBER */ yytestcase(yyruleno==63);
{
    yylhsminor.yy43.num = yymsp[0].minor.yy0.numval;
    yylhsminor.yy43.inclusive = 1;
}
  yymsp[0].minor.yy43 = yylhsminor.yy43;
        break;
      case 64: /* num ::= LP num */
{
    yymsp[-1].minor.yy43=yymsp[0].minor.yy43;
    yymsp[-1].minor.yy43.inclusive = 0;
}
        break;
      case 65: /* num ::= MINUS num */
{
    yymsp[0].minor.yy43.num = -yymsp[0].minor.yy43.num;
    yymsp[-1].minor.yy43 = yymsp[0].minor.yy43;
}
        break;
      case 66: /* term ::= TERM */
      case 67: /* term ::= NUMBER */ yytestcase(yyruleno==67);
      case 68: /* term ::= SIZE */ yytestcase(yyruleno==68);
{
    yylhsminor.yy0 = yymsp[0].minor.yy0; 
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 69: /* param_term ::= TERM */
      case 77: /* param_any ::= TERM */ yytestcase(yyruleno==77);
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 70: /* param_term ::= NUMBER */
      case 71: /* param_term ::= SIZE */ yytestcase(yyruleno==71);
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  // Number is treated as a term here
  yylhsminor.yy0.type = QT_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 72: /* param_term ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_TERM;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 73: /* param_size ::= SIZE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_SIZE;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 74: /* param_size ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_SIZE;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 75: /* param_any ::= ATTRIBUTE */
{
  yylhsminor.yy0 = yymsp[0].minor.yy0;
  yylhsminor.yy0.type = QT_PARAM_ANY;
  yylhsminor.yy0.inclusive = 1;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 76: /* param_any ::= LP ATTRIBUTE */
{
  yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;
  yymsp[-1].minor.yy0.type = QT_PARAM_ANY;
  yymsp[-1].minor.yy0.inclusive = 0; // Could be relevant if type is refined
}
        break;
      case 78: /* param_any ::= num */
{
  yylhsminor.yy0.numval = yymsp[0].minor.yy43.num;
  yylhsminor.yy0.inclusive = yymsp[0].minor.yy43.inclusive;
  yylhsminor.yy0.type = QT_NUMERIC;
}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 80: /* star ::= LP star RP */
{
}
  yy_destructor(yypParser,47,&yymsp[-1].minor);
        break;
      default:
      /* (79) star ::= STAR */ yytestcase(yyruleno==79);
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
