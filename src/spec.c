#include "spec.h"
#include <math.h>

/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len) {
    
    for (int i = 0; i < spec->numFields; i++) {
        if (!strncmp(spec->fields[i].name, name, len)) {
            return &spec->fields[i];
        }
    }
    
    return NULL;
};

/* 
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
* 
* The format currently is <field> <weight>, <field> <weight> ... 
*/
int IndexSpec_ParseRedisArgs(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    if (argc % 2) {
        return RedisModule_WrongArity(ctx);
    }
    
    const char *args[argc];
    for (int i = 0; i < argc; i++) {
        args[i] = RedisModule_StringPtrLen(argv[i], NULL);
    }
    
    return IndexSpec_Parse(spec, args, argc);
}

int IndexSpec_Parse(IndexSpec *spec, const char **argv, int argc) {
    if (argc % 2) {
        return REDISMODULE_ERR;
    }
    
    spec->numFields = 0;
    spec->fields = calloc(argc/2, sizeof(FieldSpec));
    int n = 0;
    for (int i = 0; i < argc; i+=2) {
        //size_t sz;
        spec->fields[n].name = argv[i];
        //spec->fields[n].name[sz] = '\0';
        double d = strtod(argv[i+1], NULL);
        if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL) {
            goto failure;
        }
        spec->fields[n].weight = d;
        spec->numFields++;
        // /printf("Parsed field '%s' with weight %f\n", spec->fields[n].name, spec->fields[n].weight);
        n++;
        
    }
    
    return REDISMODULE_OK;
    
failure: //on failure free the spec fields array and return an error
    
    free(spec->fields);
    spec->fields =  NULL;
    return REDISMODULE_ERR;
    
}

void IndexSpec_Free(IndexSpec *spec) {
    if (spec->fields != NULL) {
        free(spec->fields);
    }
}