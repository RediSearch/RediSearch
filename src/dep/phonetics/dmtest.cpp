#include <vector>
#include <string>
#include "stdio.h"
#include <stdlib.h>
#include <string.h>

#include "../phonetics/double_metaphone.h"

int main(int argc, char *argv[]) {
  char line[512];
  char *word;

  if (argc != 2) {
    printf(
        "Usage: dmtest <filename>\n"
        "  where <filename> contains one word per line, will print\n"
        "  each word with its 2 double metaphone values.\n");
    exit(1);
  }

  FILE *f = fopen(argv[1], "r");

  while (!feof(f)) {
    word = fgets(line, sizeof(line), f);
    if (word && strlen(word)) {
      vector<string> codes;
      if (word[strlen(word) - 1] == '\n') {
        word[strlen(word) - 1] = '\0';
      }
      string s = string(word);
      DoubleMetaphone(s, &codes);
      printf("%s,%s,%s\n", word, codes[0].c_str(), codes[1].c_str());
    }
  }
}
