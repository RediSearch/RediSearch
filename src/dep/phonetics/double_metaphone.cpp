#include "double_metaphone.h"

#include <vector>
#include <string>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>

#include "double_metaphone_capi.h"

const unsigned int max_length = 32;

void MakeUpper(string &s) {
  for (unsigned int i = 0; i < s.length(); i++) {
    s[i] = toupper(s[i]);
  }
}

int IsVowel(string &s, unsigned int pos)
{
  char c;

  if ((pos < 0) || (pos >= s.length()))
    return 0;

  c = s[pos];
  if ((c == 'A') || (c == 'E') || (c == 'I') || (c =='O') ||
      (c =='U')  || (c == 'Y')) {
    return 1;
  }

  return 0;
}


int SlavoGermanic(string &s)
{
  if ((char *) strstr(s.c_str(), "W"))
    return 1;
  else if ((char *) strstr(s.c_str(), "K"))
    return 1;
  else if ((char *) strstr(s.c_str(), "CZ"))
    return 1;
  else if ((char *) strstr(s.c_str(), "WITZ"))
    return 1;
  else
    return 0;
}


char GetAt(string &s, unsigned int pos)
{
  if ((pos < 0) || (pos >= s.length())) {
    return '\0';
  }

  return s[pos];
}


void SetAt(string &s, unsigned int pos, char c)
{
  if ((pos < 0) || (pos >= s.length())) {
    return;
  }

  s[pos] = c;
}


/*
  Caveats: the START value is 0 based
*/
int StringAt(string &s, unsigned int start, unsigned int length, ...)
{
  char *test;
  const char *pos;
  va_list ap;

  if ((start < 0) || (start >= s.length())) {
    return 0;
  }

  pos = (s.c_str() + start);
  va_start(ap, length);

  do {
    test = va_arg(ap, char *);
    if (*test && (strncmp(pos, test, length) == 0)) {
      return 1;
    }
  } while (strcmp(test, ""));

  va_end(ap);

  return 0;
}


void DoubleMetaphone(const string &str, vector<string> *codes)
{
  int        length;
  string original;
  string primary;
  string secondary;
  int        current;
  int        last;

  current = 0;
  /* we need the real length and last prior to padding */
  length  = str.length();
  last    = length - 1;
  original = str; // make a copy
  /* Pad original so we can index beyond end */
  original += "     ";

  primary = "";
  secondary = "";

  MakeUpper(original);

  /* skip these when at start of word */
  if (StringAt(original, 0, 2, "GN", "KN", "PN", "WR", "PS", "")) {
    current += 1;
  }

  /* Initial 'X' is pronounced 'Z' e.g. 'Xavier' */
  if (GetAt(original, 0) == 'X') {
    primary += "S";	/* 'Z' maps to 'S' */
    secondary += "S";
    current += 1;
  }

  /* main loop */
  while ((primary.length() < max_length) || (secondary.length() < max_length)) {
    if (current >= length) {
      break;
    }

    switch (GetAt(original, current)) {
    case 'A':
    case 'E':
    case 'I':
    case 'O':
    case 'U':
    case 'Y':
      if (current == 0) {
        /* all init vowels now map to 'A' */
        primary += "A";
        secondary += "A";
      }
      current += 1;
      break;

    case 'B':
      /* "-mb", e.g", "dumb", already skipped over... */
      primary += "P";
      secondary += "P";

      if (GetAt(original, current + 1) == 'B')
        current += 2;
      else
        current += 1;
      break;

    case 'Ç':
      primary += "S";
      secondary += "S";
      current += 1;
      break;

    case 'C':
      /* various germanic */
      if ((current > 1) &&
          !IsVowel(original, current - 2) &&
          StringAt(original, (current - 1), 3, "ACH", "") &&
          ((GetAt(original, current + 2) != 'I') &&
           ((GetAt(original, current + 2) != 'E') ||
            StringAt(original, (current - 2), 6, "BACHER", "MACHER", "")))) {
        primary += "K";
        secondary += "K";
        current += 2;
        break;
      }

      /* special case 'caesar' */
      if ((current == 0) && StringAt(original, current, 6, "CAESAR", "")) {
        primary += "S";
        secondary += "S";
        current += 2;
        break;
      }

      /* italian 'chianti' */
      if (StringAt(original, current, 4, "CHIA", "")) {
        primary += "K";
        secondary += "K";
        current += 2;
        break;
      }

      if (StringAt(original, current, 2, "CH", "")) {
        /* find 'michael' */
        if ((current > 0) && StringAt(original, current, 4, "CHAE", "")) {
          primary += "K";
          secondary += "X";
          current += 2;
          break;
        }

        /* greek roots e.g. 'chemistry', 'chorus' */
        if ((current == 0) &&
            (StringAt(original, (current + 1), 5,
                      "HARAC", "HARIS", "") ||
             StringAt(original, (current + 1), 3,
                      "HOR", "HYM", "HIA", "HEM", "")) &&
            !StringAt(original, 0, 5, "CHORE", "")) {
          primary += "K";
          secondary += "K";
          current += 2;
          break;
        }

        /* germanic, greek, or otherwise 'ch' for 'kh' sound */
        if ((StringAt(original, 0, 4, "VAN ", "VON ", "") ||
             StringAt(original, 0, 3, "SCH", "")) ||
            /*  'architect but not 'arch', 'orchestra', 'orchid' */
            StringAt(original, (current - 2), 6,
                     "ORCHES", "ARCHIT", "ORCHID", "") ||
            StringAt(original, (current + 2), 1,
                     "T", "S", "") ||
            ((StringAt(original, (current - 1), 1,
                       "A", "O", "U", "E", "") ||
              (current == 0)) &&
             /* e.g., 'wachtler', 'wechsler', but not 'tichner' */
             StringAt(original, (current + 2), 1, "L", "R",
                      "N", "M", "B", "H", "F", "V", "W", " ", ""))) {
          primary += "K";
          secondary += "K";
        } else {
          if (current > 0) {
            if (StringAt(original, 0, 2, "MC", "")) {
              /* e.g., "McHugh" */
              primary += "K";
              secondary += "K";
            } else {
              primary += "X";
              secondary += "K";
            }
          } else {
            primary += "X";
            secondary += "X";
          }
        }
        current += 2;
        break;
      }
      /* e.g, 'czerny' */
      if (StringAt(original, current, 2, "CZ", "") &&
          !StringAt(original, (current - 2), 4, "WICZ", "")) {
        primary += "S";
        secondary += "X";
        current += 2;
        break;
      }

      /* e.g., 'focaccia' */
      if (StringAt(original, (current + 1), 3, "CIA", "")) {
        primary += "X";
        secondary += "X";
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
            primary += "KS";
            secondary += "KS";
            /* 'bacci', 'bertucci', other italian */
          } else {
            primary += "X";
            secondary += "X";
          }
          current += 3;
          break;
        } else {  /* Pierce's rule */
          primary += "K";
          secondary += "K";
          current += 2;
          break;
        }
      }

      if (StringAt(original, current, 2, "CK", "CG", "CQ", "")) {
        primary += "K";
        secondary += "K";
        current += 2;
        break;
      }

      if (StringAt(original, current, 2, "CI", "CE", "CY", "")) {
        /* italian vs. english */
        if (StringAt(original, current, 3, "CIO", "CIE", "CIA", "")) {
          primary += "S";
          secondary += "X";
        } else {
          primary += "S";
          secondary += "S";
        }
        current += 2;
        break;
      }

      /* else */
      primary += "K";
      secondary += "K";

      /* name sent in 'mac caffrey', 'mac gregor */
      if (StringAt(original, (current + 1), 2, " C", " Q", " G", ""))
        current += 3;
      else
        if (StringAt(original, (current + 1), 1, "C", "K", "Q", "") &&
            !StringAt(original, (current + 1), 2, "CE", "CI", ""))
          current += 2;
        else
          current += 1;
      break;

    case 'D':
      if (StringAt(original, current, 2, "DG", "")) {
        if (StringAt(original, (current + 2), 1, "I", "E", "Y", "")) {
          /* e.g. 'edge' */
          primary += "J";
          secondary += "J";
          current += 3;
          break;
        } else {
          /* e.g. 'edgar' */
          primary += "TK";
          secondary += "TK";
          current += 2;
          break;
        }
      }

      if (StringAt(original, current, 2, "DT", "DD", "")) {
        primary += "T";
        secondary += "T";
        current += 2;
        break;
      }

      /* else */
      primary += "T";
      secondary += "T";
      current += 1;
      break;

    case 'F':
      if (GetAt(original, current + 1) == 'F')
        current += 2;
      else
        current += 1;
      primary += "F";
      secondary += "F";
      break;

    case 'G':
      if (GetAt(original, current + 1) == 'H') {
        if ((current > 0) && !IsVowel(original, current - 1)) {
          primary += "K";
          secondary += "K";
          current += 2;
          break;
        }

        if (current < 3) {
          /* 'ghislane', ghiradelli */
          if (current == 0) {
            if (GetAt(original, current + 2) == 'I') {
              primary += "J";
              secondary += "J";
            } else {
              primary += "K";
              secondary += "K";
            }
            current += 2;
            break;
          }
        }
        /* Parker's rule (with some further refinements) - e.g., 'hugh' */
        if (((current > 1) &&
             StringAt(original, (current - 2), 1, "B", "H", "D", "")) ||
            /* e.g., 'bough' */
            ((current > 2) &&
             StringAt(original, (current - 3), 1, "B", "H", "D", "")) ||
            /* e.g., 'broughton' */
            ((current > 3) &&
             StringAt(original, (current - 4), 1, "B", "H", ""))) {
          current += 2;
          break;
        } else {
          /* e.g., 'laugh', 'McLaughlin', 'cough', 'gough', 'rough', 'tough' */
          if ((current > 2) &&
              (GetAt(original, current - 1) == 'U') &&
              StringAt(original, (current - 3), 1, "C",
                       "G", "L", "R", "T", "")) {
            primary += "F";
            secondary += "F";
          } else if ((current > 0) &&
                     GetAt(original, current - 1) != 'I') {
            primary += "K";
            secondary += "K";
          }

          current += 2;
          break;
        }
      }

      if (GetAt(original, current + 1) == 'N') {
        if ((current == 1) &&
            IsVowel(original, 0) &&
            !SlavoGermanic(original)) {
          primary += "KN";
          secondary += "N";
        } else
          /* not e.g. 'cagney' */
          if (!StringAt(original, (current + 2), 2, "EY", "") &&
              (GetAt(original, current + 1) != 'Y') &&
              !SlavoGermanic(original)) {
            primary += "N";
            secondary += "KN";
          } else {
            primary += "KN";
            secondary += "KN";
          }
        current += 2;
        break;
      }

      /* 'tagliaro' */
      if (StringAt(original, (current + 1), 2, "LI", "") &&
          !SlavoGermanic(original)) {
        primary += "KL";
        secondary += "L";
        current += 2;
        break;
      }

      /* -ges-,-gep-,-gel-, -gie- at beginning */
      if ((current == 0) &&
          ((GetAt(original, current + 1) == 'Y') ||
           StringAt(original, (current + 1), 2, "ES", "EP",
                    "EB", "EL", "EY", "IB", "IL", "IN", "IE",
                    "EI", "ER", ""))) {
        primary += "K";
        secondary += "J";
        current += 2;
        break;
      }

      /*  -ger-,  -gy- */
      if ((StringAt(original, (current + 1), 2, "ER", "") ||
           (GetAt(original, current + 1) == 'Y')) &&
          !StringAt(original, 0, 6, "DANGER", "RANGER", "MANGER", "") &&
          !StringAt(original, (current - 1), 1, "E", "I", "") &&
          !StringAt(original, (current - 1), 3, "RGY", "OGY", "")) {
        primary += "K";
        secondary += "J";
        current += 2;
        break;
      }

      /*  italian e.g, 'biaggi' */
      if (StringAt(original, (current + 1), 1, "E", "I", "Y", "") ||
          StringAt(original, (current - 1), 4, "AGGI", "OGGI", "")) {
        /* obvious germanic */
        if ((StringAt(original, 0, 4, "VAN ", "VON ", "") ||
             StringAt(original, 0, 3, "SCH", "")) ||
            StringAt(original, (current + 1), 2, "ET", ""))
          {
            primary += "K";
            secondary += "K";
          } else {
          /* always soft if french ending */
          if (StringAt(original, (current + 1), 4, "IER ", "")) {
            primary += "J";
            secondary += "J";
          } else {
            primary += "J";
            secondary += "K";
          }
        }
        current += 2;
        break;
      }

      if (GetAt(original, current + 1) == 'G')
        current += 2;
      else
        current += 1;
      primary += "K";
      secondary += "K";
      break;

    case 'H':
      /* only keep if first & before vowel or btw. 2 vowels */
      if (((current == 0) ||
           IsVowel(original, current - 1)) &&
          IsVowel(original, current + 1)) {
        primary += "H";
        secondary += "H";
        current += 2;
      }
      else		/* also takes care of 'HH' */
        current += 1;
      break;

    case 'J':
      /* obvious spanish, 'jose', 'san jacinto' */
      if (StringAt(original, current, 4, "JOSE", "") ||
          StringAt(original, 0, 4, "SAN ", "")) {
        if (((current == 0) && (GetAt(original, current + 4) == ' ')) ||
            StringAt(original, 0, 4, "SAN ", "")) {
          primary += "H";
          secondary += "H";
        } else {
          primary += "J";
          secondary += "H";
        }
        current += 1;
        break;
      }

      if ((current == 0) && !StringAt(original, current, 4, "JOSE", "")) {
        primary += "J";	/* Yankelovich/Jankelowicz */
        secondary += "A";
      } else {
        /* spanish pron. of e.g. 'bajador' */
        if (IsVowel(original, current - 1) &&
            !SlavoGermanic(original) &&
            ((GetAt(original, current + 1) == 'A') ||
             (GetAt(original, current + 1) == 'O'))) {
          primary += "J";
          secondary += "H";
        } else {
          if (current == last) {
            primary += "J";
            secondary += "";
          } else {
            if (!StringAt(original, (current + 1), 1,
                          "L", "T", "K", "S", "N", "M", "B", "Z", "") &&
                !StringAt(original, (current - 1), 1, "S", "K", "L", "")) {
              primary += "J";
              secondary += "J";
            }
          }
        }
      }

      if (GetAt(original, current + 1) == 'J')	/* it could happen! */
        current += 2;
      else
        current += 1;
      break;

    case 'K':
      if (GetAt(original, current + 1) == 'K')
        current += 2;
      else
        current += 1;
      primary += "K";
      secondary += "K";
      break;

    case 'L':
      if (GetAt(original, current + 1) == 'L') {
        /* spanish e.g. 'cabrillo', 'gallegos' */
        if (((current == (length - 3)) &&
             StringAt(original, (current - 1), 4,
                      "ILLO", "ILLA", "ALLE", "")) ||
            ((StringAt(original, (last - 1), 2, "AS", "OS", "") ||
              StringAt(original, last, 1, "A", "O", "")) &&
             StringAt(original, (current - 1), 4, "ALLE", ""))) {
          primary += "L";
          secondary += "";
          current += 2;
          break;
        }
        current += 2;
      }
      else
        current += 1;
      primary += "L";
      secondary += "L";
      break;

    case 'M':
      if ((StringAt(original, (current - 1), 3, "UMB", "") &&
           (((current + 1) == last) ||
            StringAt(original, (current + 2), 2, "ER", ""))) ||
          /* 'dumb','thumb' */
          (GetAt(original, current + 1) == 'M')) {
        current += 2;
      } else {
        current += 1;
      }
      primary += "M";
      secondary += "M";
      break;

    case 'N':
      if (GetAt(original, current + 1) == 'N') {
        current += 2;
      } else {
        current += 1;
      }
      primary += "N";
      secondary += "N";
      break;

    case 'Ñ':
      current += 1;
      primary += "N";
      secondary += "N";
      break;

    case 'P':
      if (GetAt(original, current + 1) == 'H') {
        primary += "F";
        secondary += "F";
        current += 2;
        break;
      }

      /* also account for "campbell", "raspberry" */
      if (StringAt(original, (current + 1), 1, "P", "B", ""))
        current += 2;
      else
        current += 1;
      primary += "P";
      secondary += "P";
      break;

    case 'Q':
      if (GetAt(original, current + 1) == 'Q')
        current += 2;
      else
        current += 1;
      primary += "K";
      secondary += "K";
      break;

    case 'R':
      /* french e.g. 'rogier', but exclude 'hochmeier' */
      if ((current == last) &&
          !SlavoGermanic(original) &&
          StringAt(original, (current - 2), 2, "IE", "") &&
          !StringAt(original, (current - 4), 2, "ME", "MA", "")) {
        primary += "";
        secondary += "R";
      } else {
        primary += "R";
        secondary += "R";
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
        primary += "X";
        secondary += "S";
        current += 1;
        break;
      }

      if (StringAt(original, current, 2, "SH", "")) {
        /* germanic */
        if (StringAt(original, (current + 1), 4,
                     "HEIM", "HOEK", "HOLM", "HOLZ", "")) {
          primary += "S";
          secondary += "S";
        } else {
          primary += "X";
          secondary += "X";
        }
        current += 2;
        break;
      }

      /* italian & armenian */
      if (StringAt(original, current, 3, "SIO", "SIA", "") ||
          StringAt(original, current, 4, "SIAN", "")) {
        if (!SlavoGermanic(original)) {
          primary += "S";
          secondary += "X";
        } else {
          primary += "S";
          secondary += "S";
        }
        current += 3;
        break;
      }

      /* german & anglicisations, e.g. 'smith' match 'schmidt', 'snider' match 'schneider'
         also, -sz- in slavic language altho in hungarian it is pronounced 's' */
      if (((current == 0) &&
           StringAt(original, (current + 1), 1, "M", "N", "L", "W", "")) ||
          StringAt(original, (current + 1), 1, "Z", "")) {
        primary += "S";
        secondary += "X";
        if (StringAt(original, (current + 1), 1, "Z", ""))
          current += 2;
        else
          current += 1;
        break;
      }

      if (StringAt(original, current, 2, "SC", "")) {
        /* Schlesinger's rule */
        if (GetAt(original, current + 2) == 'H') {
          /* dutch origin, e.g. 'school', 'schooner' */
          if (StringAt(original, (current + 3), 2,
                       "OO", "ER", "EN", "UY", "ED", "EM", "")) {
            /* 'schermerhorn', 'schenker' */
            if (StringAt(original, (current + 3), 2, "ER", "EN", "")) {
              primary += "X";
              secondary += "SK";
            } else {
              primary += "SK";
              secondary += "SK";
            }
            current += 3;
            break;
          } else {
            if ((current == 0) && !IsVowel(original, 3) &&
                (GetAt(original, 3) != 'W')) {
              primary += "X";
              secondary += "S";
            } else {
              primary += "X";
              secondary += "X";
            }
            current += 3;
            break;
          }
        }

        if (StringAt(original, (current + 2), 1, "I", "E", "Y", "")) {
          primary += "S";
          secondary += "S";
          current += 3;
          break;
        }
        /* else */
        primary += "SK";
        secondary += "SK";
        current += 3;
        break;
      }

      /* french e.g. 'resnais', 'artois' */
      if ((current == last) &&
          StringAt(original, (current - 2), 2, "AI", "OI", "")) {
        primary += "";
        secondary += "S";
      } else {
        primary += "S";
        secondary += "S";
      }

      if (StringAt(original, (current + 1), 1, "S", "Z", ""))
        current += 2;
      else
        current += 1;
      break;

    case 'T':
      if (StringAt(original, current, 4, "TION", "")) {
        primary += "X";
        secondary += "X";
        current += 3;
        break;
      }

      if (StringAt(original, current, 3, "TIA", "TCH", "")) {
        primary += "X";
        secondary += "X";
        current += 3;
        break;
      }

      if (StringAt(original, current, 2, "TH", "") ||
          StringAt(original, current, 3, "TTH", "")) {
        /* special case 'thomas', 'thames' or germanic */
        if (StringAt(original, (current + 2), 2, "OM", "AM", "") ||
            StringAt(original, 0, 4, "VAN ", "VON ", "") ||
            StringAt(original, 0, 3, "SCH", "")) {
          primary += "T";
          secondary += "T";
        } else {
          primary += "0"; /* yes, zero */
          secondary += "T";
        }
        current += 2;
        break;
      }

      if (StringAt(original, (current + 1), 1, "T", "D", "")) {
        current += 2;
      } else {
        current += 1;
      }
      primary += "T";
      secondary += "T";
      break;

    case 'V':
      if (GetAt(original, current + 1) == 'V') {
        current += 2;
      } else {
        current += 1;
      }
      primary += "F";
      secondary += "F";
      break;

    case 'W':
      /* can also be in middle of word */
      if (StringAt(original, current, 2, "WR", "")) {
        primary += "R";
        secondary += "R";
        current += 2;
        break;
      }

      if ((current == 0) &&
          (IsVowel(original, current + 1) ||
           StringAt(original, current, 2, "WH", ""))) {
        /* Wasserman should match Vasserman */
        if (IsVowel(original, current + 1)) {
          primary += "A";
          secondary += "F";
        } else {
          /* need Uomo to match Womo */
          primary += "A";
          secondary += "A";
        }
      }

      /* Arnow should match Arnoff */
      if (((current == last) && IsVowel(original, current - 1)) ||
          StringAt(original, (current - 1), 5,
                   "EWSKI", "EWSKY", "OWSKI", "OWSKY", "") ||
          StringAt(original, 0, 3, "SCH", "")) {
        primary += "";
        secondary += "F";
        current += 1;
        break;
      }

      /* polish e.g. 'filipowicz' */
      if (StringAt(original, current, 4, "WICZ", "WITZ", "")) {
        primary += "TS";
        secondary += "FX";
        current += 4;
        break;
      }

      /* else skip it */
      current += 1;
      break;

    case 'X':
      /* french e.g. breaux */
      if (!((current == last) &&
            (StringAt(original, (current - 3), 3, "IAU", "EAU", "") ||
             StringAt(original, (current - 2), 2, "AU", "OU", "")))) {
        primary += "KS";
        secondary += "KS";
      }


      if (StringAt(original, (current + 1), 1, "C", "X", ""))
        current += 2;
      else
        current += 1;
      break;

    case 'Z':
      /* chinese pinyin e.g. 'zhao' */
      if (GetAt(original, current + 1) == 'H') {
        primary += "J";
        secondary += "J";
        current += 2;
        break;
      } else if (StringAt(original, (current + 1), 2, "ZO", "ZI", "ZA", "") ||
                 (SlavoGermanic(original) &&
                  ((current > 0) &&
                   GetAt(original, current - 1) != 'T'))) {
        primary += "S";
        secondary += "TS";
      } else {
        primary += "S";
        secondary += "S";
      }

      if (GetAt(original, current + 1) == 'Z')
        current += 2;
      else
        current += 1;
      break;

    default:
      current += 1;
    }
    /* printf("PRIMARY: %s\n", primary.str);
       printf("SECONDARY: %s\n", secondary.str);  */
  }


  if (primary.length() > max_length)
    SetAt(primary, max_length, '\0');

  if (secondary.length() > max_length)
    SetAt(secondary, max_length, '\0');

  codes->push_back(primary);
  codes->push_back(secondary);
}


extern "C" {
void DoubleMetaphone_c(const char* str, size_t len, char** primary, char** secondary){
  vector<string> codes;
  string s = string(str, len);
  DoubleMetaphone(s, &codes);
  if(primary != NULL){
    *primary = strdup(codes[0].c_str());
  }
  if(secondary != NULL){
    *secondary = strdup(codes[1].c_str());
  }
}
}
