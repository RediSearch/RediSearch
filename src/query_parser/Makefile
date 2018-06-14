LEMON := ../../srcutil/lemon
TEMPLATE := ../../srcutil/lempar.c
RAGEL := ragel

all: lexer.c parser.c.inc

lexer.c: lexer.rl
	$(RAGEL) -s lexer.rl -o $@

parser.c: parser.y
	$(LEMON) -s -T$(TEMPLATE) parser.y
	mv parser.c parser.c.inc

