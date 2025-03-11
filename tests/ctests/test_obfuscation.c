#include "test_util.h"
#include "src/query_node.h"
#include "src/obfuscation/obfuscation_api.h"

#include <stdlib.h>
#include <string.h>

struct RSQueryNode;
const char *Obfuscate_QueryNode(struct RSQueryNode *node);

enum {
    IndexSize = MAX_OBFUSCATED_INDEX_NAME,
    FieldSize = MAX_OBFUSCATED_FIELD_NAME,
    FieldPathSize = MAX_OBFUSCATED_PATH_NAME,
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

DEFINE_OBJECT_OBFUSCATION_TESTS(Field)
DEFINE_OBJECT_OBFUSCATION_TESTS(FieldPath)
DEFINE_OBJECT_OBFUSCATION_TESTS(Document)

int testSimpleIndexObfuscation() {
  Sha1 sha;
  Sha1_Compute("idx", 3, &sha);
  char buffer[MAX_OBFUSCATED_INDEX_NAME];
  Obfuscate_Index(&sha, buffer);
  return strcmp(buffer, "Index@4e7f626df794f6491574a236f22c100c34ed804f");
}

int testTextObfuscation() {
    const char *obfuscated = Obfuscate_Text("hello");
    return strcmp(obfuscated, "Text");
}

int testNumberObfuscation() {
    const char *obfuscated = Obfuscate_Number(rand());
    return strcmp(obfuscated, "Number");
}

int testVectorObfuscation() {
    const char *obfuscated = Obfuscate_Vector("hello", 5);
    return strcmp(obfuscated, "Vector");
}

int testTagObfuscation() {
    const char *obfuscated = Obfuscate_Tag("hello");
    return strcmp(obfuscated, "Tag");
}

int testGeoObfuscation() {
    const char *obfuscated = Obfuscate_Geo(1, 2);
    return strcmp(obfuscated, "Geo");
}

int testGeoShapeObfuscation() {
    const char *obfuscated = Obfuscate_GeoShape();
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
        "LexRange",
        "Vector",
        "WildcardQuery",
        "Null",
        "Missing"
    };
    for (int i = QN_PHRASE; i < QN_MAX; ++i) {
        struct RSQueryNode node = {
            .type = i,
        };
        const char *obfuscated = Obfuscate_QueryNode(&node);
        ASSERT(strcmp(obfuscated, expected[i - 1]) == 0);
    }
    return 0;
}

TEST_MAIN({
    TESTFUNC(testSimpleIndexObfuscation);
    TESTFUNC(testSimpleFieldObfuscation);
    TESTFUNC(testMaxFieldObfuscation);
    TESTFUNC(testSimpleFieldPathObfuscation);
    TESTFUNC(testMaxFieldPathObfuscation);
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
