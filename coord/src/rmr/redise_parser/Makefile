
RM_INCLUDE_DIR ?= ../../../../deps/  
SOURCEDIR=$(shell pwd -P)
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) $(wildcard $(SOURCEDIR)/dep/*.c) 
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))
CPPFLAGS = -I$(RM_INCLUDE_DIR) -I../../../../src/ -I../../../../deps/RedisModulesSDK/ 
CFLAGS = -w $(DEBUGFLAGS) -fPIC -std=gnu99  
#CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
CC:=gcc

%.c: %.y
	
libparser.a:  $(CC_OBJECTS)
	echo $(CC)
	ar rcs $@ $(CC_OBJECTS)

generate:
	flex  --header-file=lexer.h -o lex.yy.c -i lexer.l 
	lemon -s -Ttemplate.c.tpl grammar.y
	
.PHONY: libparser.a
all: libparser.a