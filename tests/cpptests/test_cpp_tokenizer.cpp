/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "stemmer.h"
#include "tokenize.h"

#include <set>
#include <vector>

extern "C" {
#include "toksep.h"
}

class TokenizerTest : public ::testing::Test {};

TEST_F(TokenizerTest, testTokenize) {
  Stemmer *st = NewStemmer(SnowballStemmer, RS_LANG_ENGLISH);
  RSTokenizer *tk = GetSimpleTokenizer(st, DefaultStopWordList());
  char *txt = strdup("hello worlds    - - -,,, . . . -=- hello\\-world to be שלום עולם");
  const char *expected[] = {"hello", "worlds", "hello-world", "שלום", "עולם"};
  const char *stems[] = {NULL, "+world", NULL, NULL, NULL, NULL, NULL};
  tk->Start(tk, txt, strlen(txt), TOKENIZE_DEFAULT_OPTIONS);
  Token tok;
  size_t i = 0;
  while (tk->Next(tk, &tok)) {
    ASSERT_EQ(i + 1, tok.pos);
    ASSERT_EQ(tok.tokLen, strlen(expected[i]));
    std::string got(tok.tok, tok.tokLen);
    ASSERT_STREQ(got.c_str(), expected[i]);
    if (!stems[i]) {
      ASSERT_TRUE(tok.stem == NULL);
    } else {
      std::string gotStem(tok.stem, tok.stemLen);
      ASSERT_STREQ(gotStem.c_str(), stems[i]);
    }
    i++;
  }
  free(txt);
  st->Free(st);
  tk->Free(tk);
}

struct MyToken {
  std::string token;
  std::string stem;
  std::string raw;

  MyToken(const Token &t) {
    if (t.raw) {
      raw.assign(t.raw, t.rawLen);
    }
    if (t.tok) {
      token.assign(t.tok, t.tok);
    }
    if (t.stem) {
      stem.assign(t.stem, t.stemLen);
    }
  }
};

TEST_F(TokenizerTest, testChineseMixed) {
  auto tk = NewChineseTokenizer(NULL, NULL, 0);
  std::string tokstr(
      "同时支持对 UTF-8/GBK \\\\ 编码的切分，hello-world hello\\-world \\:\\:world \\:\\:支持 php5 "
      "trailing\\-backslash\\- hi "
      "和 "
      "world\\- "
      "multiple\\ words\\ with\\ spaces "
      "multiple\\-words\\-with\\-hyphens "
      "php7 扩展和 sphinx token 插件 ");

  // append a very large token, too
  for (size_t ii = 0; ii < 20; ++ii) {
    tokstr.append(20, 'a');
    tokstr.append(1, '\\');
    tokstr.append(1, ' ');
  }
  tokstr += " trailing trailing2";
  // printf("tokstr: %s\n", tokstr.c_str());

  char *txt = strdup(tokstr.c_str());
  tk->Start(tk, txt, strlen(txt), 0);
  Token t = {0};
  size_t pos = 1;
  std::set<std::string> tokens;
  while (tk->Next(tk, &t)) {
    ASSERT_EQ(t.pos, pos);
    std::string tok(t.tok, t.tokLen);
    tokens.insert(tok);
    // printf("inserted %s (n=%d)\n", tok.c_str(), tok.size());
    pos++;
  }
  ASSERT_NE(tokens.end(), tokens.find("::支持"));
  ASSERT_NE(tokens.end(), tokens.find("hello-world"));
  ASSERT_NE(tokens.end(), tokens.find("::world"));
  ASSERT_NE(tokens.end(), tokens.find("trailing2"));
  ASSERT_NE(tokens.end(), tokens.find("trailing"));
  ASSERT_NE(tokens.end(), tokens.find("world-"));
  ASSERT_NE(
      tokens.end(),
      tokens.find(
          " aaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaa "
          "aaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaa "));
  ASSERT_NE(tokens.end(), tokens.find("multiple words with spaces"));
  ASSERT_NE(tokens.end(), tokens.find("multiple-words-with-hyphens"));
  // FIXME: Current parsing behavior makes this really odd..
  //   ASSERT_NE(tokens.end(), tokens.find("\\"));
  tk->Free(tk);
  free(txt);
}

TEST_F(TokenizerTest, testTrailingEscapes) {
  auto tk = NewChineseTokenizer(NULL, NULL, 0);
  char *txt = strdup("hello world\\ ");
  tk->Start(tk, txt, strlen(txt), 0);

  std::set<std::string> tokens;
  Token t;
  size_t pos = 1;
  while (tk->Next(tk, &t)) {
    ASSERT_EQ(t.pos, pos);
    std::string tok(t.tok, t.tokLen);
    tokens.insert(tok);
    // printf("inserted %s (n=%d)\n", tok.c_str(), tok.size());
    pos++;
  }
  ASSERT_NE(tokens.end(), tokens.find("hello"));
  ASSERT_NE(tokens.end(), tokens.find("world "));  // note the space
  tk->Free(tk);
  free(txt);
}

TEST_F(TokenizerTest, testEscapedSeparator) {
  auto tk = NewSimpleTokenizer(NULL, NULL, 0);
  char *txt = strdup("hello\\\\ world");
  const char *expected[] = {"hello\\", "world"}; // note it is normalized
  tk->Start(tk, txt, strlen(txt), 0);

  Token tok;
  size_t i = 0;
  while (tk->Next(tk, &tok)) {
    ASSERT_EQ(i + 1, tok.pos);
    ASSERT_EQ(tok.tokLen, strlen(expected[i]));
    std::string got(tok.tok, tok.tokLen);
    ASSERT_STREQ(got.c_str(), expected[i]);
    i++;
  }
  free(txt);
  tk->Free(tk);
}

TEST_F(TokenizerTest, testRawTokenSpans) {
  const struct {
    const char *name;
    std::string input;
    std::vector<size_t> lengths;
  } cases[] = {
      {"empty", "", {0}},
      {"consecutive and trailing separators", " ,ab\\ cd,,ef ", {0, 0, 6, 0, 2}},
      {"trailing backslash", "b\\", {2}},
  };
  for (const auto &test : cases) {
    SCOPED_TRACE(test.name);
    std::string buffer = test.input;
    char *cursor = buffer.data();
    size_t offset = 0;
    for (size_t expected : test.lengths) {
      ASSERT_NE(cursor, nullptr);
      char *start = cursor;
      size_t len = SIZE_MAX;
      char *token = toksep(&cursor, &len);
      ASSERT_EQ(token, start);
      ASSERT_EQ(token, buffer.data() + offset);
      ASSERT_EQ(len, expected);
      ASSERT_EQ(buffer, test.input);
      offset += len;
      if (buffer[offset] == '\0') {
        ASSERT_EQ(cursor, nullptr);
      } else {
        ++offset;
        ASSERT_EQ(cursor, buffer[offset] ? buffer.data() + offset : nullptr);
      }
    }
    ASSERT_EQ(cursor, nullptr);
  }
}
