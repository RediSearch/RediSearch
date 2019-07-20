LEMON := $(SRCUTIL)/lemon
TEMPLATE := $(SRCUTIL)/lempar.c
RAGEL := ragel

all: lexer.c parser.c

lexer.c: lexer.rl
	$(RAGEL) -s lexer.rl -o $@

parser.c: parser.y
	$(LEMON) -s -T$(TEMPLATE) parser.y

clean:
	rm -f lexer.c parser.c
