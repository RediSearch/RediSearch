SRCUTIL ?= .
LEMON := $(SRCUTIL)/lemon
TEMPLATE := $(SRCUTIL)/lempar.c
RAGEL := ragel

all: $(LEMON) lexer.c parser.c

lexer.c: lexer.rl
	$(RAGEL) -L -s lexer.rl -o $@

parser.c: parser.y
	$(LEMON) -l -s -T$(TEMPLATE) parser.y

clean:
	rm -f  $(SRCUTIL)/lexer.c  $(SRCUTIL)/parser.c

$(LEMON):  $(SRCUTIL)/lemon.c  $(SRCUTIL)/lempar.c
	gcc -o $(LEMON) $(SRCUTIL)/lemon.c
