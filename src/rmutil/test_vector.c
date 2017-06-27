#include "vector.h"
#include <stdio.h>
#include "assert.h"

int main(int argc, char **argv) {
    
    
    Vector *v = NewVector(int, 1);
    int N = 10;

    for (int i = 0; i < N/2; i++) {
        Vector_Put(v, i, i);
    }

    for (int  i = N/2; i < N; i++) {
        Vector_Push(v, i);    
    }
    assert(Vector_Size(v) == N);
    assert(Vector_Cap(v) >= N);
    
    for (int i = 0; i < Vector_Size(v); i++) {
        int n;
        int rc = Vector_Get(v, i, &n);
        printf("%d %d\n", rc, n);
        assert ( 1== rc );
        assert (n == i);
    }
    
    Vector_Free(v);
    
    v = NewVector(char *, 0);
    N = 4;
    char *strings[4] = {"hello", "world", "foo", "bar"};

    for (int i = 0; i < N/2; i++) {
        Vector_Put(v, i, strings[i]);
    }
    
    for (int i = N/2; i < N; i++) {
        Vector_Push(v, strings[i]);
    }
    assert(Vector_Size(v) == N);
    assert(Vector_Cap(v) >= N);
    
    for (size_t i = 0; i < Vector_Size(v); i++) {
        char *x;
        int rc = Vector_Get(v, i, &x);
        assert (rc == 1);
        assert (!strcmp(x, strings[i]));
    }
    
    int rc = Vector_Get(v, 100, NULL);
    assert (rc == 0);
    
    Vector_Free(v);
    printf("PASS!\n");
    
    return 0;
    //Vector_Push(v, "hello");
    //Vector_Push(v, "world");
    // char *x = NULL;
    // int rc = Vector_Getx(v, 0, &x);
    // printf("rc: %d got %s\n", rc, x);
    
}
