#include "test_util.h"
#include <string.h>
#include "../stemmer.h"
#include "../tokenize.h"

int testStemmer() {
    
    Stemmer *s = NewStemmer(SnowballStemmer, "en");
    ASSERT( s != NULL )
    
    size_t sl;
    const char *stem = s->Stem(s->ctx, "arbitrary",(size_t)strlen("arbitrary"), &sl);
    ASSERT( stem != NULL)
    ASSERT (!strcasecmp(stem, "arbitrari"));
    ASSERT ( sl == strlen(stem));
    printf("stem: %s\n", stem);
    
    //free((void*)stem);
    s->Free(s);
    return 0; 
}

typedef struct{
    int num;
    char **expected;
    
} tokenContext;

int tokenFunc(void *ctx, Token t) {
    //printf("%s %d\n", t.s, t.type);
    
    tokenContext *tx = ctx;
    
    assert( strcmp(t.s, tx->expected[tx->num++]) == 0);
    assert(t.len == strlen(t.s));
    assert(t.fieldId == 1);
    assert(t.pos > 0);
    assert(t.score == 1);
    if (t.type == DT_STEM) {
        //printf("%s -> %s\n",t.s, tx->expected[tx->num-2]);
        assert( strcmp(t.s, tx->expected[tx->num-2]) != 0);
    }
    return   0;    
}


int testTokenize() {
    
    char *txt = strdup("Hello? world... worlds going ? __WAZZ@UP? שלום");
    tokenContext ctx = {0};
    const char *expected[] = {"hello", "world", "worlds", "world", "going", "go", "wazz", "up", "שלום"};
    ctx.expected = (char **)expected;
    
    tokenize(txt, 1, 1, &ctx, tokenFunc, 1);
    ASSERT(ctx.num == 9);
    
    free(txt);
    
    return 0;
    
}

int main(int argc, char**argv) {
    
    TEST_START();
    TESTFUNC(testStemmer);
    TESTFUNC(testTokenize);
    
}