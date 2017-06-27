parser:
	ragel -s lexer.rl -o lexer.c
	lemon -s -Ttemplate.c.tpl parser.y

.PHONY: parser
all: parser