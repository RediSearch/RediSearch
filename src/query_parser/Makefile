parser:
	ragel -s lexer.rl -o lexer.c
	lemon -s parser.y

.PHONY: parser
all: parser