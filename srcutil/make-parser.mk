LEMON := $(SRCUTIL)/lemon
TEMPLATE := $(SRCUTIL)/lempar.c
RAGEL := ragel

all: $(LEMON) lexer.c parser.c

lexer.c: lexer.rl
	$(RAGEL) -L -s lexer.rl -o $@

parser.c: parser.y
	$(LEMON) -l -s -T$(TEMPLATE) parser.y

clean:
	rm -f lexer.c parser.c

$(LEMON): lemon.c lempar.c
	gcc -o $(LEMON) lemon.c
