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
"NUMSLOTS"                 { return NUMSLOTS; }

[\-\+]?[0-9]+    {   
  tok.intval = atoi(yytext); 
  return INTEGER;
}

[0-9A-Za-z][A-Za-z0-9_\.\:\@\-\/\[\]]* {
    *(yytext+strlen(yytext)) = '\0';
  	tok.strval = yytext;
  	return STRING;
}

(\"(\\.|[^\"])*\")|('(\\.|[^'])*')    {
  /* String literals, with escape sequences - enclosed by "" or '' */
  *(yytext+strlen(yytext)-1) = '\0';
  tok.strval = yytext+1;
  return STRING;
}




[ \t]+ /* ignore whitespace */
\n    { yycolumn = 1; } /* ignore whitespace */

%%

/**
 * yyerror() is invoked when the lexer or the parser encounter
 * an error. The error message is passed via *s
 *
 *
 */
void yyerror(char *s) {
	printf("error: %s at line: %d\n",s,yylineno);
}

int yywrap(void) {
	return 1;
}