%{

#include "grammar.h"
#include "token.h"
#include <string.h>
#include <math.h>

Token tok = { };

/* handle locations */
int yycolumn = 1;

#define YY_USER_ACTION yycolumn += yyleng; tok.pos = yycolumn;  tok.s = yytext; tok.len = strlen(yytext);
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
  tok.strval = yytext;
  return STRING;
}

(\"(\\.|[^\"])*\")|('(\\.|[^'])*')    {
  /* String literals, with escape sequences - enclosed by "" or '' */
  tok.strval = yytext+1;
  tok.strval[tok.len-1] = '\0';
  return STRING;
}


[ \t]+ /* ignore whitespace */
\n    { yycolumn = 1; } /* ignore whitespace */

%%

int yywrap(void) {
  return 1;
}
