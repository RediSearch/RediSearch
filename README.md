streamvbyte
===========
[![Build Status](https://travis-ci.org/lemire/streamvbyte.png)](https://travis-ci.org/lemire/streamvbyte)

StreamVByte is a new integer compression technique that applies SIMD instructions (vectorization) to
Google's Group Varint approach. The net result is faster than other byte-oriented compression
techniques.

The approach is patent-free, the code is available under the Apache License.


It includes fast differential coding.

It assumes a recent Intel processor (e.g., haswell or better) .

The code should build using most C compilers. The provided makefile
expects a Linux-like system.


Usage:

      make
      ./unit

See example.c for an example.

Short code sample:
```C
size_t compsize = streamvbyte_encode(datain, N, compressedbuffer); // encoding
// here the result is stored in compressedbuffer using compsize bytes
streamvbyte_decode(compressedbuffer, recovdata, N); // decoding (fast)
```

See also
--------

https://github.com/lemire/MaskedVByte

https://github.com/lemire/simdcomp

