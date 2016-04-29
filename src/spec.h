#ifndef __SPEC_H__
#define __SPEC_H__
#include <stdlib.h>
#include <string.h>
#include "redismodule.h"


typedef struct {
    const char *name;
    double weight;
    // TODO: More options here..
} FieldSpec;

typedef struct {
    FieldSpec *fields;
    int numFields;
        
} IndexSpec;


/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len);

/* 
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
* 
* The format currently is <field> <weight>, <field> <weight> ... 
*/
int IndexSpec_ParseRedisArgs(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* Same as above but with ordinary strings, to allow unit testing */
int IndexSpec_Parse(IndexSpec *s, const char **argv, int argc);

/*
* Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
* and should be on the request's stack
*/
void IndexSpec_Free(IndexSpec *spec);


int IndexSpec_Load(RedisModuleCtx *ctx, IndexSpec *sp, const char *name);
int IndexSpec_Save(RedisModuleCtx *ctx, IndexSpec *sp, const char *name);

#endif