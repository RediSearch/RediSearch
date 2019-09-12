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

#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include "double_metaphone.h"

#include "rmalloc.h"

/*
 * * If META_USE_PERL_MALLOC is defined we use Perl's memory routines.
 * */
#ifdef META_USE_PERL_MALLOC

#include "EXTERN.h"
#include "perl.h"
#define META_MALLOC(v, n, t) New(1, v, n, t)
#define META_REALLOC(v, n, t) Renew(v, n, t)
#define META_FREE(x) Safefree((x))

#else

#define META_MALLOC(v, n, t) (v = (t *)rm_malloc(((n) * sizeof(t))))
#define META_REALLOC(v, n, t) (v = (t *)rm_realloc((v), ((n) * sizeof(t))))
#define META_FREE(x) rm_free((x))

#endif /* META_USE_PERL_MALLOC */

static metastring *NewMetaString(const char *init_str) {
  metastring *s;
  char empty_string[] = "";

  META_MALLOC(s, 1, metastring);
  assert(s != NULL);

  if (init_str == NULL) init_str = empty_string;
  s->length = strlen(init_str);
  /* preallocate a bit more for potential growth */
  s->bufsize = s->length + 7;

  META_MALLOC(s->str, s->bufsize, char);
  assert(s->str != NULL);

  strncpy(s->str, init_str, s->length + 1);
  s->free_string_on_destroy = 1;

  return s;
}

static void DestroyMetaString(metastring *s) {
  if (s == NULL) return;

  if (s->free_string_on_destroy && (s->str != NULL)) META_FREE(s->str);

  META_FREE(s);
}

static void IncreaseBuffer(metastring *s, int chars_needed) {
  META_REALLOC(s->str, (s->bufsize + chars_needed + 10), char);
  assert(s->str != NULL);
  s->bufsize = s->bufsize + chars_needed + 10;
}

static void MakeUpper(metastring *s) {
  char *i;

  for (i = s->str; *i; i++) {
    *i = toupper(*i);
  }
}

static int IsVowel(metastring *s, int pos) {
  char c;

  if ((pos < 0) || (pos >= s->length)) return 0;

  c = *(s->str + pos);
  if ((c == 'A') || (c == 'E') || (c == 'I') || (c == 'O') || (c == 'U') || (c == 'Y')) return 1;

  return 0;
}

static int SlavoGermanic(metastring *s) {
  if ((char *)strstr(s->str, "W"))
    return 1;
  else if ((char *)strstr(s->str, "K"))
    return 1;
  else if ((char *)strstr(s->str, "CZ"))
    return 1;
  else if ((char *)strstr(s->str, "WITZ"))
    return 1;
  else
    return 0;
}

static int GetLength(metastring *s) {
  return s->length;
}

static char GetAt(metastring *s, int pos) {
  if ((pos < 0) || (pos >= s->length)) return '\0';

  return ((char)*(s->str + pos));
}

static void SetAt(metastring *s, int pos, char c) {
  if ((pos < 0) || (pos >= s->length)) return;

  *(s->str + pos) = c;
}

/*
   Caveats: the START value is 0 based
*/
static int StringAt(metastring *s, int start, int length, ...) {
  char *test;
  char *pos;
  va_list ap;

  if ((start < 0) || (start >= s->length)) return 0;

  pos = (s->str + start);
  va_start(ap, length);

  do {
    test = va_arg(ap, char *);
    if (*test && (strncmp(pos, test, length) == 0)) return 1;
  } while (strcmp(test, ""));

  va_end(ap);

  return 0;
}

static void MetaphAdd(metastring *s, const char *new_str) {
  int add_length;

  if (new_str == NULL) return;

  add_length = strlen(new_str);
  if ((s->length + add_length) > (s->bufsize - 1)) {
    IncreaseBuffer(s, add_length);
  }

  strcat(s->str, new_str);
  s->length += add_length;
}

void DoubleMetaphone(const char *str, char **primary_pp, char **secondary_pp) {
  int length;
  metastring *original;
  metastring *primary;
  metastring *secondary;
  int current;
  int last;

  current = 0;
  /* we need the real length and last prior to padding */
  length = strlen(str);
  last = length - 1;
  original = NewMetaString(str);
  /* Pad original so we can index beyond end */
  MetaphAdd(original, "     ");

  primary = NewMetaString("");
  secondary = NewMetaString("");

  MakeUpper(original);

  /* skip these when at start of word */
  if (StringAt(original, 0, 2, "GN", "KN", "PN", "WR", "PS", "")) current += 1;

  /* Initial 'X' is pronounced 'Z' e.g. 'Xavier' */
  if (GetAt(original, 0) == 'X') {
    MetaphAdd(primary, "S"); /* 'Z' maps to 'S' */
    MetaphAdd(secondary, "S");
    current += 1;
  }

  /* main loop */
  while ((primary->length < 4) || (secondary->length < 4)) {
    if (current >= length) break;

    switch (GetAt(original, current)) {
      case 'A':
      case 'E':
      case 'I':
      case 'O':
      case 'U':
      case 'Y':
        if (current == 0) {
          /* all init vowels now map to 'A' */
          MetaphAdd(primary, "A");
          MetaphAdd(secondary, "A");
        }
        current += 1;
        break;

      case 'B':

        /* "-mb", e.g", "dumb", already skipped over... */
        MetaphAdd(primary, "P");
        MetaphAdd(secondary, "P");

        if (GetAt(original, current + 1) == 'B')
          current += 2;
        else
          current += 1;
        break;

#if 0  // This is 2018 and nobody is using Latin1
      case 'Ç':
        MetaphAdd(primary, "S");
        MetaphAdd(secondary, "S");
        current += 1;
        break;
#endif

      case 'C':
        /* various germanic */
        if ((current > 1) && !IsVowel(original, current - 2) &&
            StringAt(original, (current - 1), 3, "ACH", "") &&
            ((GetAt(original, current + 2) != 'I') &&
             ((GetAt(original, current + 2) != 'E') ||
              StringAt(original, (current - 2), 6, "BACHER", "MACHER", "")))) {
          MetaphAdd(primary, "K");
          MetaphAdd(secondary, "K");
          current += 2;
          break;
        }

        /* special case 'caesar' */
        if ((current == 0) && StringAt(original, current, 6, "CAESAR", "")) {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "S");
          current += 2;
          break;
        }

        /* italian 'chianti' */
        if (StringAt(original, current, 4, "CHIA", "")) {
          MetaphAdd(primary, "K");
          MetaphAdd(secondary, "K");
          current += 2;
          break;
        }

        if (StringAt(original, current, 2, "CH", "")) {
          /* find 'michael' */
          if ((current > 0) && StringAt(original, current, 4, "CHAE", "")) {
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "X");
            current += 2;
            break;
          }

          /* greek roots e.g. 'chemistry', 'chorus' */
          if ((current == 0) &&
              (StringAt(original, (current + 1), 5, "HARAC", "HARIS", "") ||
               StringAt(original, (current + 1), 3, "HOR", "HYM", "HIA", "HEM", "")) &&
              !StringAt(original, 0, 5, "CHORE", "")) {
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "K");
            current += 2;
            break;
          }

          /* germanic, greek, or otherwise 'ch' for 'kh' sound */
          if ((StringAt(original, 0, 4, "VAN ", "VON ", "") || StringAt(original, 0, 3, "SCH", ""))
              /*  'architect but not 'arch', 'orchestra', 'orchid' */
              || StringAt(original, (current - 2), 6, "ORCHES", "ARCHIT", "ORCHID", "") ||
              StringAt(original, (current + 2), 1, "T", "S", "") ||
              ((StringAt(original, (current - 1), 1, "A", "O", "U", "E", "") || (current == 0))
               /* e.g., 'wachtler', 'wechsler', but not 'tichner' */
               && StringAt(original, (current + 2), 1, "L", "R", "N", "M", "B", "H", "F", "V", "W",
                           " ", ""))) {
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "K");
          } else {
            if (current > 0) {
              if (StringAt(original, 0, 2, "MC", "")) {
                /* e.g., "McHugh" */
                MetaphAdd(primary, "K");
                MetaphAdd(secondary, "K");
              } else {
                MetaphAdd(primary, "X");
                MetaphAdd(secondary, "K");
              }
            } else {
              MetaphAdd(primary, "X");
              MetaphAdd(secondary, "X");
            }
          }
          current += 2;
          break;
        }
        /* e.g, 'czerny' */
        if (StringAt(original, current, 2, "CZ", "") &&
            !StringAt(original, (current - 2), 4, "WICZ", "")) {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "X");
          current += 2;
          break;
        }

        /* e.g., 'focaccia' */
        if (StringAt(original, (current + 1), 3, "CIA", "")) {
          MetaphAdd(primary, "X");
          MetaphAdd(secondary, "X");
          current += 3;
          break;
        }

        /* double 'C', but not if e.g. 'McClellan' */
        if (StringAt(original, current, 2, "CC", "") &&
            !((current == 1) && (GetAt(original, 0) == 'M'))) {
          /* 'bellocchio' but not 'bacchus' */
          if (StringAt(original, (current + 2), 1, "I", "E", "H", "") &&
              !StringAt(original, (current + 2), 2, "HU", "")) {
            /* 'accident', 'accede' 'succeed' */
            if (((current == 1) && (GetAt(original, current - 1) == 'A')) ||
                StringAt(original, (current - 1), 5, "UCCEE", "UCCES", "")) {
              MetaphAdd(primary, "KS");
              MetaphAdd(secondary, "KS");
              /* 'bacci', 'bertucci', other italian */
            } else {
              MetaphAdd(primary, "X");
              MetaphAdd(secondary, "X");
            }
            current += 3;
            break;
          } else { /* Pierce's rule */
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "K");
            current += 2;
            break;
          }
        }

        if (StringAt(original, current, 2, "CK", "CG", "CQ", "")) {
          MetaphAdd(primary, "K");
          MetaphAdd(secondary, "K");
          current += 2;
          break;
        }

        if (StringAt(original, current, 2, "CI", "CE", "CY", "")) {
          /* italian vs. english */
          if (StringAt(original, current, 3, "CIO", "CIE", "CIA", "")) {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "X");
          } else {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "S");
          }
          current += 2;
          break;
        }

        /* else */
        MetaphAdd(primary, "K");
        MetaphAdd(secondary, "K");

        /* name sent in 'mac caffrey', 'mac gregor */
        if (StringAt(original, (current + 1), 2, " C", " Q", " G", ""))
          current += 3;
        else if (StringAt(original, (current + 1), 1, "C", "K", "Q", "") &&
                 !StringAt(original, (current + 1), 2, "CE", "CI", ""))
          current += 2;
        else
          current += 1;
        break;

      case 'D':
        if (StringAt(original, current, 2, "DG", "")) {
          if (StringAt(original, (current + 2), 1, "I", "E", "Y", "")) {
            /* e.g. 'edge' */
            MetaphAdd(primary, "J");
            MetaphAdd(secondary, "J");
            current += 3;
            break;
          } else {
            /* e.g. 'edgar' */
            MetaphAdd(primary, "TK");
            MetaphAdd(secondary, "TK");
            current += 2;
            break;
          }
        }

        if (StringAt(original, current, 2, "DT", "DD", "")) {
          MetaphAdd(primary, "T");
          MetaphAdd(secondary, "T");
          current += 2;
          break;
        }

        /* else */
        MetaphAdd(primary, "T");
        MetaphAdd(secondary, "T");
        current += 1;
        break;

      case 'F':
        if (GetAt(original, current + 1) == 'F')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "F");
        MetaphAdd(secondary, "F");
        break;

      case 'G':
        if (GetAt(original, current + 1) == 'H') {
          if ((current > 0) && !IsVowel(original, current - 1)) {
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "K");
            current += 2;
            break;
          }

          if (current < 3) {
            /* 'ghislane', ghiradelli */
            if (current == 0) {
              if (GetAt(original, current + 2) == 'I') {
                MetaphAdd(primary, "J");
                MetaphAdd(secondary, "J");
              } else {
                MetaphAdd(primary, "K");
                MetaphAdd(secondary, "K");
              }
              current += 2;
              break;
            }
          }
          /* Parker's rule (with some further refinements) - e.g., 'hugh' */
          if (((current > 1) && StringAt(original, (current - 2), 1, "B", "H", "D", ""))
              /* e.g., 'bough' */
              || ((current > 2) && StringAt(original, (current - 3), 1, "B", "H", "D", ""))
              /* e.g., 'broughton' */
              || ((current > 3) && StringAt(original, (current - 4), 1, "B", "H", ""))) {
            current += 2;
            break;
          } else {
            /* e.g., 'laugh', 'McLaughlin', 'cough', 'gough', 'rough', 'tough' */
            if ((current > 2) && (GetAt(original, current - 1) == 'U') &&
                StringAt(original, (current - 3), 1, "C", "G", "L", "R", "T", "")) {
              MetaphAdd(primary, "F");
              MetaphAdd(secondary, "F");
            } else if ((current > 0) && GetAt(original, current - 1) != 'I') {

              MetaphAdd(primary, "K");
              MetaphAdd(secondary, "K");
            }

            current += 2;
            break;
          }
        }

        if (GetAt(original, current + 1) == 'N') {
          if ((current == 1) && IsVowel(original, 0) && !SlavoGermanic(original)) {
            MetaphAdd(primary, "KN");
            MetaphAdd(secondary, "N");
          } else
              /* not e.g. 'cagney' */
              if (!StringAt(original, (current + 2), 2, "EY", "") &&
                  (GetAt(original, current + 1) != 'Y') && !SlavoGermanic(original)) {
            MetaphAdd(primary, "N");
            MetaphAdd(secondary, "KN");
          } else {
            MetaphAdd(primary, "KN");
            MetaphAdd(secondary, "KN");
          }
          current += 2;
          break;
        }

        /* 'tagliaro' */
        if (StringAt(original, (current + 1), 2, "LI", "") && !SlavoGermanic(original)) {
          MetaphAdd(primary, "KL");
          MetaphAdd(secondary, "L");
          current += 2;
          break;
        }

        /* -ges-,-gep-,-gel-, -gie- at beginning */
        if ((current == 0) && ((GetAt(original, current + 1) == 'Y') ||
                               StringAt(original, (current + 1), 2, "ES", "EP", "EB", "EL", "EY",
                                        "IB", "IL", "IN", "IE", "EI", "ER", ""))) {
          MetaphAdd(primary, "K");
          MetaphAdd(secondary, "J");
          current += 2;
          break;
        }

        /*  -ger-,  -gy- */
        if ((StringAt(original, (current + 1), 2, "ER", "") ||
             (GetAt(original, current + 1) == 'Y')) &&
            !StringAt(original, 0, 6, "DANGER", "RANGER", "MANGER", "") &&
            !StringAt(original, (current - 1), 1, "E", "I", "") &&
            !StringAt(original, (current - 1), 3, "RGY", "OGY", "")) {
          MetaphAdd(primary, "K");
          MetaphAdd(secondary, "J");
          current += 2;
          break;
        }

        /*  italian e.g, 'biaggi' */
        if (StringAt(original, (current + 1), 1, "E", "I", "Y", "") ||
            StringAt(original, (current - 1), 4, "AGGI", "OGGI", "")) {
          /* obvious germanic */
          if ((StringAt(original, 0, 4, "VAN ", "VON ", "") ||
               StringAt(original, 0, 3, "SCH", "")) ||
              StringAt(original, (current + 1), 2, "ET", "")) {
            MetaphAdd(primary, "K");
            MetaphAdd(secondary, "K");
          } else {
            /* always soft if french ending */
            if (StringAt(original, (current + 1), 4, "IER ", "")) {
              MetaphAdd(primary, "J");
              MetaphAdd(secondary, "J");
            } else {
              MetaphAdd(primary, "J");
              MetaphAdd(secondary, "K");
            }
          }
          current += 2;
          break;
        }

        if (GetAt(original, current + 1) == 'G')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "K");
        MetaphAdd(secondary, "K");
        break;

      case 'H':
        /* only keep if first & before vowel or btw. 2 vowels */
        if (((current == 0) || IsVowel(original, current - 1)) && IsVowel(original, current + 1)) {
          MetaphAdd(primary, "H");
          MetaphAdd(secondary, "H");
          current += 2;
        } else /* also takes care of 'HH' */
          current += 1;
        break;

      case 'J':
        /* obvious spanish, 'jose', 'san jacinto' */
        if (StringAt(original, current, 4, "JOSE", "") || StringAt(original, 0, 4, "SAN ", "")) {
          if (((current == 0) && (GetAt(original, current + 4) == ' ')) ||
              StringAt(original, 0, 4, "SAN ", "")) {
            MetaphAdd(primary, "H");
            MetaphAdd(secondary, "H");
          } else {
            MetaphAdd(primary, "J");
            MetaphAdd(secondary, "H");
          }
          current += 1;
          break;
        }

        if ((current == 0) && !StringAt(original, current, 4, "JOSE", "")) {
          MetaphAdd(primary, "J"); /* Yankelovich/Jankelowicz */
          MetaphAdd(secondary, "A");
        } else {
          /* spanish pron. of e.g. 'bajador' */
          if (IsVowel(original, current - 1) && !SlavoGermanic(original) &&
              ((GetAt(original, current + 1) == 'A') || (GetAt(original, current + 1) == 'O'))) {
            MetaphAdd(primary, "J");
            MetaphAdd(secondary, "H");
          } else {
            if (current == last) {
              MetaphAdd(primary, "J");
              MetaphAdd(secondary, "");
            } else {
              if (!StringAt(original, (current + 1), 1, "L", "T", "K", "S", "N", "M", "B", "Z",
                            "") &&
                  !StringAt(original, (current - 1), 1, "S", "K", "L", "")) {
                MetaphAdd(primary, "J");
                MetaphAdd(secondary, "J");
              }
            }
          }
        }

        if (GetAt(original, current + 1) == 'J') /* it could happen! */
          current += 2;
        else
          current += 1;
        break;

      case 'K':
        if (GetAt(original, current + 1) == 'K')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "K");
        MetaphAdd(secondary, "K");
        break;

      case 'L':
        if (GetAt(original, current + 1) == 'L') {
          /* spanish e.g. 'cabrillo', 'gallegos' */
          if (((current == (length - 3)) &&
               StringAt(original, (current - 1), 4, "ILLO", "ILLA", "ALLE", "")) ||
              ((StringAt(original, (last - 1), 2, "AS", "OS", "") ||
                StringAt(original, last, 1, "A", "O", "")) &&
               StringAt(original, (current - 1), 4, "ALLE", ""))) {
            MetaphAdd(primary, "L");
            MetaphAdd(secondary, "");
            current += 2;
            break;
          }
          current += 2;
        } else
          current += 1;
        MetaphAdd(primary, "L");
        MetaphAdd(secondary, "L");
        break;

      case 'M':
        if ((StringAt(original, (current - 1), 3, "UMB", "") &&
             (((current + 1) == last) || StringAt(original, (current + 2), 2, "ER", "")))
            /* 'dumb','thumb' */
            || (GetAt(original, current + 1) == 'M'))
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "M");
        MetaphAdd(secondary, "M");
        break;

      case 'N':
        if (GetAt(original, current + 1) == 'N')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "N");
        MetaphAdd(secondary, "N");
        break;

#if 0  // UTF8, not Latin1
      case 'Ñ':
        current += 1;
        MetaphAdd(primary, "N");
        MetaphAdd(secondary, "N");
        break;
#endif

      case 'P':
        if (GetAt(original, current + 1) == 'H') {
          MetaphAdd(primary, "F");
          MetaphAdd(secondary, "F");
          current += 2;
          break;
        }

        /* also account for "campbell", "raspberry" */
        if (StringAt(original, (current + 1), 1, "P", "B", ""))
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "P");
        MetaphAdd(secondary, "P");
        break;

      case 'Q':
        if (GetAt(original, current + 1) == 'Q')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "K");
        MetaphAdd(secondary, "K");
        break;

      case 'R':
        /* french e.g. 'rogier', but exclude 'hochmeier' */
        if ((current == last) && !SlavoGermanic(original) &&
            StringAt(original, (current - 2), 2, "IE", "") &&
            !StringAt(original, (current - 4), 2, "ME", "MA", "")) {
          MetaphAdd(primary, "");
          MetaphAdd(secondary, "R");
        } else {
          MetaphAdd(primary, "R");
          MetaphAdd(secondary, "R");
        }

        if (GetAt(original, current + 1) == 'R')
          current += 2;
        else
          current += 1;
        break;

      case 'S':
        /* special cases 'island', 'isle', 'carlisle', 'carlysle' */
        if (StringAt(original, (current - 1), 3, "ISL", "YSL", "")) {
          current += 1;
          break;
        }

        /* special case 'sugar-' */
        if ((current == 0) && StringAt(original, current, 5, "SUGAR", "")) {
          MetaphAdd(primary, "X");
          MetaphAdd(secondary, "S");
          current += 1;
          break;
        }

        if (StringAt(original, current, 2, "SH", "")) {
          /* germanic */
          if (StringAt(original, (current + 1), 4, "HEIM", "HOEK", "HOLM", "HOLZ", "")) {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "S");
          } else {
            MetaphAdd(primary, "X");
            MetaphAdd(secondary, "X");
          }
          current += 2;
          break;
        }

        /* italian & armenian */
        if (StringAt(original, current, 3, "SIO", "SIA", "") ||
            StringAt(original, current, 4, "SIAN", "")) {
          if (!SlavoGermanic(original)) {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "X");
          } else {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "S");
          }
          current += 3;
          break;
        }

        /* german & anglicisations, e.g. 'smith' match 'schmidt', 'snider' match 'schneider'
           also, -sz- in slavic language altho in hungarian it is pronounced 's' */
        if (((current == 0) && StringAt(original, (current + 1), 1, "M", "N", "L", "W", "")) ||
            StringAt(original, (current + 1), 1, "Z", "")) {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "X");
          if (StringAt(original, (current + 1), 1, "Z", ""))
            current += 2;
          else
            current += 1;
          break;
        }

        if (StringAt(original, current, 2, "SC", "")) {
          /* Schlesinger's rule */
          if (GetAt(original, current + 2) == 'H') /* dutch origin, e.g. 'school', 'schooner' */ {
            if (StringAt(original, (current + 3), 2, "OO", "ER", "EN", "UY", "ED", "EM", "")) {
              /* 'schermerhorn', 'schenker' */
              if (StringAt(original, (current + 3), 2, "ER", "EN", "")) {
                MetaphAdd(primary, "X");
                MetaphAdd(secondary, "SK");
              } else {
                MetaphAdd(primary, "SK");
                MetaphAdd(secondary, "SK");
              }
              current += 3;
              break;
            } else {
              if ((current == 0) && !IsVowel(original, 3) && (GetAt(original, 3) != 'W')) {
                MetaphAdd(primary, "X");
                MetaphAdd(secondary, "S");
              } else {
                MetaphAdd(primary, "X");
                MetaphAdd(secondary, "X");
              }
              current += 3;
              break;
            }
          }

          if (StringAt(original, (current + 2), 1, "I", "E", "Y", "")) {
            MetaphAdd(primary, "S");
            MetaphAdd(secondary, "S");
            current += 3;
            break;
          }
          /* else */
          MetaphAdd(primary, "SK");
          MetaphAdd(secondary, "SK");
          current += 3;
          break;
        }

        /* french e.g. 'resnais', 'artois' */
        if ((current == last) && StringAt(original, (current - 2), 2, "AI", "OI", "")) {
          MetaphAdd(primary, "");
          MetaphAdd(secondary, "S");
        } else {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "S");
        }

        if (StringAt(original, (current + 1), 1, "S", "Z", ""))
          current += 2;
        else
          current += 1;
        break;

      case 'T':
        if (StringAt(original, current, 4, "TION", "")) {
          MetaphAdd(primary, "X");
          MetaphAdd(secondary, "X");
          current += 3;
          break;
        }

        if (StringAt(original, current, 3, "TIA", "TCH", "")) {
          MetaphAdd(primary, "X");
          MetaphAdd(secondary, "X");
          current += 3;
          break;
        }

        if (StringAt(original, current, 2, "TH", "") || StringAt(original, current, 3, "TTH", "")) {
          /* special case 'thomas', 'thames' or germanic */
          if (StringAt(original, (current + 2), 2, "OM", "AM", "") ||
              StringAt(original, 0, 4, "VAN ", "VON ", "") || StringAt(original, 0, 3, "SCH", "")) {
            MetaphAdd(primary, "T");
            MetaphAdd(secondary, "T");
          } else {
            MetaphAdd(primary, "0"); /* yes, zero */
            MetaphAdd(secondary, "T");
          }
          current += 2;
          break;
        }

        if (StringAt(original, (current + 1), 1, "T", "D", ""))
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "T");
        MetaphAdd(secondary, "T");
        break;

      case 'V':
        if (GetAt(original, current + 1) == 'V')
          current += 2;
        else
          current += 1;
        MetaphAdd(primary, "F");
        MetaphAdd(secondary, "F");
        break;

      case 'W':
        /* can also be in middle of word */
        if (StringAt(original, current, 2, "WR", "")) {
          MetaphAdd(primary, "R");
          MetaphAdd(secondary, "R");
          current += 2;
          break;
        }

        if ((current == 0) &&
            (IsVowel(original, current + 1) || StringAt(original, current, 2, "WH", ""))) {
          /* Wasserman should match Vasserman */
          if (IsVowel(original, current + 1)) {
            MetaphAdd(primary, "A");
            MetaphAdd(secondary, "F");
          } else {
            /* need Uomo to match Womo */
            MetaphAdd(primary, "A");
            MetaphAdd(secondary, "A");
          }
        }

        /* Arnow should match Arnoff */
        if (((current == last) && IsVowel(original, current - 1)) ||
            StringAt(original, (current - 1), 5, "EWSKI", "EWSKY", "OWSKI", "OWSKY", "") ||
            StringAt(original, 0, 3, "SCH", "")) {
          MetaphAdd(primary, "");
          MetaphAdd(secondary, "F");
          current += 1;
          break;
        }

        /* polish e.g. 'filipowicz' */
        if (StringAt(original, current, 4, "WICZ", "WITZ", "")) {
          MetaphAdd(primary, "TS");
          MetaphAdd(secondary, "FX");
          current += 4;
          break;
        }

        /* else skip it */
        current += 1;
        break;

      case 'X':
        /* french e.g. breaux */
        if (!((current == last) && (StringAt(original, (current - 3), 3, "IAU", "EAU", "") ||
                                    StringAt(original, (current - 2), 2, "AU", "OU", "")))) {
          MetaphAdd(primary, "KS");
          MetaphAdd(secondary, "KS");
        }

        if (StringAt(original, (current + 1), 1, "C", "X", ""))
          current += 2;
        else
          current += 1;
        break;

      case 'Z':
        /* chinese pinyin e.g. 'zhao' */
        if (GetAt(original, current + 1) == 'H') {
          MetaphAdd(primary, "J");
          MetaphAdd(secondary, "J");
          current += 2;
          break;
        } else if (StringAt(original, (current + 1), 2, "ZO", "ZI", "ZA", "") ||
                   (SlavoGermanic(original) &&
                    ((current > 0) && GetAt(original, current - 1) != 'T'))) {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "TS");
        } else {
          MetaphAdd(primary, "S");
          MetaphAdd(secondary, "S");
        }

        if (GetAt(original, current + 1) == 'Z')
          current += 2;
        else
          current += 1;
        break;

      default:
        current += 1;
    }
    /* printf("PRIMARY: %s\n", primary->str);
    printf("SECONDARY: %s\n", secondary->str);  */
  }

  if (primary->length > 4) SetAt(primary, 4, '\0');

  if (secondary->length > 4) SetAt(secondary, 4, '\0');
  if (primary_pp) {
    if (*primary_pp) {
      rm_free(*primary_pp);
      *primary_pp = NULL;
    }
    if (primary->length > 0) {
      *primary_pp = primary->str;
      primary->free_string_on_destroy = 0;
    }
  }
  if (secondary_pp) {
    if (*secondary_pp) {
      rm_free(*secondary_pp);
      *secondary_pp = NULL;
    }
    if (secondary->length > 0) {
      *secondary_pp = secondary->str;
      secondary->free_string_on_destroy = 0;
    }
  }

  DestroyMetaString(original);
  DestroyMetaString(primary);
  DestroyMetaString(secondary);
}
