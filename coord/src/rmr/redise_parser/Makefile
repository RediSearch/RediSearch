SRCUTIL := $(abspath ../../../../srcutil)
include $(SRCUTIL)/make-parser.mk

lex.yy.c:
	flex --header-file=lexer.h -o lex.yy.c -i lexer.l

grammar.c:
	$(LEMON) -l -s -T$(TEMPLATE) grammar.y

generate: lex.yy.c grammar.c
