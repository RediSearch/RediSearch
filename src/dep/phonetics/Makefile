default: dmtest

double_metaphone.o: double_metaphone.cpp
	g++ double_metaphone.cpp -c -g -o double_metaphone.o -Wl -Wall -Werror -fPIC

dmtest: dmtest.cpp double_metaphone.o 
	g++ dmtest.cpp -c -g -o dmtest.o -Wall -Werror
	g++ dmtest.o double_metaphone.o -g -lstdc++ -o dmtest

clean:
	rm dmtest dmtest.o double_metaphone.o 