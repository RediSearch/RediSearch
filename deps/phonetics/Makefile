default: dmtest

CXXFLAGS=-fPIC -fno-rtti -fno-exceptions

double_metaphone.o: double_metaphone.cpp
dmtest: dmtest.cpp double_metaphone.o 

clean:
	rm dmtest dmtest.o double_metaphone.o 
