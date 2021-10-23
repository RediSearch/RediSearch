
ROOT=../..
include $(ROOT)/deps/readies/mk/main

define HELP
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/hiredis
SRCDIR=$(ROOT)/deps/hiredis

TARGET=$(BINDIR)/libhiredis.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

CC=gcc

_SOURCES=alloc.c async.c dict.c hiredis.c net.c read.c sds.c sockcompat.c ssl.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	$(CC_FLAGS.coverage)

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(OS),macos)
LD_FLAGS += -undefined dynamic_lookup
endif

LD_FLAGS += $(LD_FLAGS.coverage)

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -fvisibility=hidden -c $< -o $@

$(SRCDIR)/.patched:
	cd $(ROOT)/deps/hiredis ;\
	patch -p1 -i $(ROOT)/build/hiredis/hiredis_patch
	touch $@

$(TARGET): $(SRCDIR)/.patched $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(OBJECTS)

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif
