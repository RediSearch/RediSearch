include mkinc.mak
CFLAGS=-O2
CPPFLAGS=-Iinclude
all: libstemmer.o stemwords
libstemmer.o: $(snowball_sources:.c=.o)
	$(AR) -cru $@ $^
stemwords: examples/stemwords.o libstemmer.o
	$(CC) $(CFLAGS) -o $@ $^
clean:
	rm -f stemwords *.o src_c/*.o examples/*.o runtime/*.o libstemmer/*.o
