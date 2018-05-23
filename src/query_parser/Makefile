LEMON := lemon
RAGEL := ragel

%.c: %.y
parser:
	$(RAGEL) -s lexer.rl -o lexer.c
	$(LEMON) -s -Ttemplate.c.tpl parser.y

.PHONY: parser
all: parser
