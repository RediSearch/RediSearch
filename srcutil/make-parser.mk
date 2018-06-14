LEMON := $(SRCUTIL)/lemon
TEMPLATE := $(SRCUTIL)/lempar.c
RAGEL := ragel

all: lexer.c parser-toplevel.c parser.c.inc

lexer.c: lexer.rl
	$(RAGEL) -s lexer.rl -o $@

parser.c.inc: parser.y
	$(LEMON) -s -T$(TEMPLATE) parser.y
	mv parser.c parser.c.inc

parser-toplevel.c: $(SRCUTIL)/gen_parser_toplevel.py
	$(SRCUTIL)/gen_parser_toplevel.py -p $(PARSER_SYMBOL_PREFIX) -i parser.c.inc > $@

clean:
	rm -f lexer.c parser.c parser.c.inc parser-toplevel.c
