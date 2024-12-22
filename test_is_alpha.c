#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>


bool isAlphabetic(const char *str, size_t len) {
    for (size_t i = 0; i < len;) {
        unsigned char c = (unsigned char)str[i];
        int bytes;
        uint32_t code_point;

        // UTF-8 Decoding with validation
        if (c <= 0x7F) {
            code_point = c;
            bytes = 1;
        } else if ((c & 0xE0) == 0xC0) {
            if (i + 1 >= len || (str[i + 1] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x1F) << 6) | (str[i + 1] & 0x3F);
            bytes = 2;
        } else if ((c & 0xF0) == 0xE0) {
            if (i + 2 >= len || (str[i + 1] & 0xC0) != 0x80 || (str[i + 2] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x0F) << 12) | ((str[i + 1] & 0x3F) << 6) | (str[i + 2] & 0x3F);
            bytes = 3;
        } else if ((c & 0xF8) == 0xF0) {
            if (i + 3 >= len || (str[i + 1] & 0xC0) != 0x80 || (str[i + 2] & 0xC0) != 0x80 || (str[i + 3] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x07) << 18) | ((str[i + 1] & 0x3F) << 12) | ((str[i + 2] & 0x3F) << 6) | (str[i + 3] & 0x3F);
            bytes = 4;
        } else {
            return false;
        }

        // Check if code point is in any letter range
        bool isLetter = 
            // Latin scripts
            (code_point >= 0x0041 && code_point <= 0x005A) ||  // Basic Latin (A-Z)
            (code_point >= 0x0061 && code_point <= 0x007A) ||  // Basic Latin (a-z)
            (code_point >= 0x00C0 && code_point <= 0x00FF) ||  // Latin-1 Supplement
            (code_point >= 0x0100 && code_point <= 0x017F) ||  // Latin Extended-A
            (code_point >= 0x0180 && code_point <= 0x024F) ||  // Latin Extended-B
            (code_point >= 0x1E00 && code_point <= 0x1EFF) ||  // Latin Extended Additional
            // Other scripts
            (code_point >= 0x0370 && code_point <= 0x03FF) ||  // Greek and Coptic
            (code_point >= 0x0400 && code_point <= 0x04FF) ||  // Cyrillic
            (code_point >= 0x0500 && code_point <= 0x052F) ||  // Cyrillic Supplement
            (code_point >= 0x0531 && code_point <= 0x0556) ||  // Armenian
            (code_point >= 0x0561 && code_point <= 0x0587) ||  // Armenian
            // CJK scripts
            (code_point >= 0x3040 && code_point <= 0x309F) ||  // Hiragana
            (code_point >= 0x30A0 && code_point <= 0x30FF) ||  // Katakana
            (code_point >= 0x3400 && code_point <= 0x4DBF) ||  // CJK Extension A
            (code_point >= 0x4E00 && code_point <= 0x9FFF) ||  // CJK Unified Ideographs
            (code_point >= 0xF900 && code_point <= 0xFAFF) ||  // CJK Compatibility Ideographs
            (code_point >= 0x20000 && code_point <= 0x2A6DF) || // CJK Extension B
            (code_point >= 0x2A700 && code_point <= 0x2B73F) || // CJK Extension C
            (code_point >= 0x2B740 && code_point <= 0x2B81F) || // CJK Extension D
            (code_point >= 0x2B820 && code_point <= 0x2CEAF) || // CJK Extension E
            (code_point >= 0x2CEB0 && code_point <= 0x2EBEF);   // CJK Extension F

        if (!isLetter) {
            return false;
        }
        i += bytes;
    }
    return true;
}
// Test function for isAlphabetic
void test_isAlphabetic() {
    // Test case 1: All alphabetic characters
    assert(isAlphabetic("HelloWorld", strlen("HelloWorld")) == true);

    // Test case 2: Contains non-alphabetic characters
    assert(isAlphabetic("Hello123", strlen("Hello123")) == false);

    // Test case 3: Empty string
    assert(isAlphabetic("", 0) == true);

    // Test case 4: All alphabetic characters with mixed case
    assert(isAlphabetic("HelloWorld", strlen("HelloWorld")) == true);

    // Test case 5: String with spaces
    assert(isAlphabetic("Hello World", strlen("Hello World")) == false);

    // Test case 6: String with special characters
    assert(isAlphabetic("Hello@World", strlen("Hello@World")) == false);

    // Test case 7: Spanish alphabetic characters
    assert(isAlphabetic("HolaMundo", strlen("HolaMundo")) == true);

    // Test case 8: Spanish characters with accents
    assert(isAlphabetic("Canción", strlen("Canción")) == true);

    // Test case 9: Chinese characters
    assert(isAlphabetic("你好世界", strlen("你好世界")) == true);

    // Test case 10: Mixed Chinese and English characters
    assert(isAlphabetic("你好World", strlen("你好World")) == true);

    // Test case 11: Spanish characters with numbers
    assert(isAlphabetic("Hola123", strlen("Hola123")) == false);

    // Test case 12: Chinese characters with numbers
    assert(isAlphabetic("你好123", strlen("你好123")) == false);


    printf("All tests passed!\n");
}

int main() {
    test_isAlphabetic();
    return 0;
}