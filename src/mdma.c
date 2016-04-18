#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#include "index.h"
#include "varint.h"

typedef struct {
    const char *name;
    const char *text;
} DocumentField;

typedef struct {
    t_docId docId;
    DocumentField *fields;
    int numFiels;    
} Document;

typedef struct {
    t_docId *docIds;
    size_t totalResults;
    t_offset limit;
} Result;

int AddDocument(Document *doc) {
    
    return 0;
}

Result *Search(const char *query) {
    
}