
#include "test_util.h"

#include <stdlib.h>
#include <string.h>
#include <cstdint>

struct QueryNode;
char *Obfuscate_QueryNode(struct QueryNode *node);

#define DEFINE_OBJECT_OBFUSCATION_TESTS(name)                      \
int testSimple ## name ## Obfuscation() {                          \
    char *obfuscated = Obfuscate_##name(1);                        \
    int result = strcmp(obfuscated, #name"@1");                    \
    rm_free(obfuscated);                                           \
    return result;                                                 \
}                                                                  \
int testMax ## name ## Obfuscation() {                             \
    char *obfuscated = Obfuscate_##name(UINT64_MAX);               \
    int result = strcmp(obfuscated, #name"@18446744073709551615"); \
    rm_free(obfuscated);                                           \
    return result;                                                 \
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
})
