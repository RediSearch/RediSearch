SRCUTIL := $(abspath ../../../srcutil)
PARSER_SYMBOL_PREFIX := QueryParseCtx
include $(SRCUTIL)/make-parser.mk

lexer.c: lexer.rl
	$(RAGEL) -L -s -m lexer.rl -o $@