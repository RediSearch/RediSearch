
ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK.configure=1

define HELP
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
make source   # fetch sources and generate configure script
endef

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/libuv
SRCDIR=$(ROOT)/deps/libuv

TARGET=$(BINDIR)/.libs/libevent.a

# export CFLAGS += -fPIC

#----------------------------------------------------------------------------------------------

include $(MK)/defs
include $(MK)/rules

CONFIGURE_FLAGS += \
	--with-pic \
	--disable-shared \
	--enable-static \
	--disable-silent-rules

#----------------------------------------------------------------------------------------------

source: $(SRCDIR)/configure

.PHONY: source

$(SRCDIR)/configure:
	$(SHOW)cd $(SRCDIR); ./autogen.sh
