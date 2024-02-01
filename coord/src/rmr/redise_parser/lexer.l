%{

#include "grammar.h"
#include "token.h"
#include "rmalloc.h"
#include <string.h>
#include <math.h>

Token tok = { };

/* handle locations */
int yycolumn = 1;

#define YY_USER_ACTION yycolumn += yyleng; tok.pos = yycolumn; tok.s = yytext; tok.len = yyleng; tok.strval = NULL;
%}

%%

"MYID"                  { return MYID; }
"HASREPLICATION"        { return HASREPLICATION; }
"RANGES"                { return RANGES; }
"SHARD"                 { return SHARD; }
"SLOTRANGE"             { return SLOTRANGE; }
"ADDR"                  { return ADDR; }
"UNIXADDR"              { return UNIXADDR; }
"MASTER"                { return MASTER; }
"HASHFUNC"              { return HASHFUNC; }
"NUMSLOTS"              { return NUMSLOTS; }

[0-9]+    {
  tok.intval = atoi(yytext);
  return INTEGER;
}

[A-Za-z0-9_\.\:\@\-\/\[\]]+ {
  tok.strval = rm_strdup(yytext);
  return STRING;
}

(\"(\\.|[^\"])*\")|('(\\.|[^'])*')    {
  /* String literals, with escape sequences - enclosed by "" or '' */
  tok.strval = rm_strndup(yytext+1, yyleng-1);
  return STRING;
}


[ \t]+ /* ignore whitespace */
\n    { yycolumn = 1; } /* ignore whitespace */

%%

int yywrap(void) {
  return 1;
}
