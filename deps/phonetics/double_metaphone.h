/* COPYRIGHT NOTICE
 *
 * This code was pulled directly from the Text-DoubleMetaphone perl package,
 * version 0.07
 *
 * The README mentions that the copyright is:
 *
 *  Copyright 2000, Maurice Aubrey <maurice@hevanet.com>.
 *  All rights reserved.

 *  This code is based heavily on the C++ implementation by
 *  Lawrence Philips and incorporates several bug fixes courtesy
 *  of Kevin Atkinson <kevina@users.sourceforge.net>.
 *
 *  This module is free software; you may redistribute it and/or
 *  modify it under the same terms as Perl itself.
 */

#ifndef DOUBLE_METAPHONE__H
#define DOUBLE_METAPHONE__H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char *str;
  int length;
  int bufsize;
  int free_string_on_destroy;
} metastring;

void DoubleMetaphone(const char *str, char **primary_pp, char **secondary_pp);

#ifdef __cplusplus
}
#endif
#endif /* DOUBLE_METAPHONE__H */