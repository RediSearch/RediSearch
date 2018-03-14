%.c: %.y
parser:
	ragel -s lexer.rl -o lexer.c
	lemon -s -Tlempar.c.tpl `pwd`/parser.y

.PHONY: parser
all: parser