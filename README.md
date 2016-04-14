streamvbyte
===========
[![Build Status](https://travis-ci.org/lemire/streamvbyte.png)](https://travis-ci.org/lemire/streamvbyte)

Fast integer codec in C.

It includes fast differential coding.

It assumes a recent Intel processor (e.g., haswell or better) .

The code should build using most C compilers. The provided makefile
expects a Linux-like system.


Usage:

      make
      ./unit

See example.c for an example.

Short code sample:

        size_t compsize = streamvbyte_encode(datain, N, compressedbuffer); // encoding
        // here the result is stored in compressedbuffer using compsize bytes
        streamvbyte_decode(compressedbuffer, recovdata, N); // decoding (fast)
