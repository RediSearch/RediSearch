all: libbloom.a

libbloom.a: sb.o contrib/MurmurHash2.o
	$(AR) -rcs $@ $^
