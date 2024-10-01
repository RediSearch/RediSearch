#include "test_util.h"
#include "src/query_node.h"
#include "src/obfuscation/obfuscation_api.h"

#include <stdlib.h>
#include <string.h>

struct RSQueryNode;
char *Obfuscate_QueryNode(struct RSQueryNode *node);

enum {
    IndexSize = MAX_OBFUSCATED_INDEX_NAME,
    FieldSize = MAX_OBFUSCATED_FIELD_NAME,
    DocumentSize = MAX_OBFUSCATED_DOCUMENT_NAME
};

#define DEFINE_OBJECT_OBFUSCATION_TESTS(name)                      \
int testSimple ## name ## Obfuscation() {                          \
    char obfuscated[name##Size];                                   \
    Obfuscate_##name(1, obfuscated);                               \
    return strcmp(obfuscated, #name"@1");                          \
}                                                                  \
int testMax ## name ## Obfuscation() {                             \
    char obfuscated[name##Size];                                   \
    Obfuscate_##name(UINT64_MAX, obfuscated);                      \
    return strcmp(obfuscated, #name"@18446744073709551615");       \
}

DEFINE_OBJECT_OBFUSCATION_TESTS(Index)
DEFINE_OBJECT_OBFUSCATION_TESTS(Field)
DEFINE_OBJECT_OBFUSCATION_TESTS(Document)

int testTextObfuscation() {
    char *obfuscated = Obfuscate_Text("hello");
    return strcmp(obfuscated, "Text");
}

int testNumberObfuscation() {
    char *obfuscated = Obfuscate_Number(rand());
    return strcmp(obfuscated, "Number");
}

int testVectorObfuscation() {
    char *obfuscated = Obfuscate_Vector("hello", 5);
    return strcmp(obfuscated, "Vector");
}

int testTagObfuscation() {
    char *obfuscated = Obfuscate_Tag("hello");
    return strcmp(obfuscated, "Tag");
}

int testGeoObfuscation() {
    char *obfuscated = Obfuscate_Geo(1, 2);
    return strcmp(obfuscated, "Geo");
}

int testGeoShapeObfuscation() {
    char *obfuscated = Obfuscate_GeoShape();
    return strcmp(obfuscated, "GeoShape");
}

int testQueryNodeObfuscation() {
    const char* expected[] = {
        "Phrase",
        "Union",
        "Token",
        "Numeric",
        "Not",
        "Optional",
        "Geo",
        "Geometry",
        "Prefix",
        "Ids",
        "Wildcard",
        "Tag",
        "Fuzzy",
        "Lexrange",
        "Vector",
        "WildcardQuery",
        "Null",
        "Missing"
    };
    for (int i = QN_PHRASE; i < QN_MAX; ++i) {
        struct RSQueryNode node = {
            .type = i,
        };
        char *obfuscated = Obfuscate_QueryNode(&node);
        ASSERT(strcmp(obfuscated, expected[i - 1]) == 0);
    }
    return 0;
}

TEST_MAIN({
    TESTFUNC(testSimpleIndexObfuscation);
    TESTFUNC(testMaxIndexObfuscation);
    TESTFUNC(testSimpleFieldObfuscation);
    TESTFUNC(testMaxFieldObfuscation);
    TESTFUNC(testSimpleDocumentObfuscation);
    TESTFUNC(testMaxDocumentObfuscation);
    TESTFUNC(testTextObfuscation);
    TESTFUNC(testNumberObfuscation);
    TESTFUNC(testVectorObfuscation);
    TESTFUNC(testTagObfuscation);
    TESTFUNC(testGeoObfuscation);
    TESTFUNC(testGeoShapeObfuscation);
    TESTFUNC(testQueryNodeObfuscation);
})
