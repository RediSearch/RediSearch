#include "spec.h"
#include "rmutil/strings.h"
#include "util/logging.h"

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
* The format currently is <field> <NUMERIC|weight>, <field> <NUMERIC|weight> ...
*/
int IndexSpec_ParseRedisArgs(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
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

    int id = 1;
    spec->numFields = 0;
    spec->fields = calloc(argc / 2, sizeof(FieldSpec));
    int n = 0;
    for (int i = 0; i < argc; i += 2, id *= 2) {
        // size_t sz;
        spec->fields[n].name = argv[i];
        double d = 0;
        FieldType t = F_FULLTEXT;
        // printf("%s %s\n", argv[i], argv[i+1])
        if (!strncasecmp(argv[i + 1], NUMERIC_STR, strlen(NUMERIC_STR))) {
            t = F_NUMERIC;
        } else {
            d = strtod(argv[i + 1], NULL);
            if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL) {
                goto failure;
            }
        }

        // spec->fields[n].name[sz] = '\0';

        spec->fields[n].weight = d;
        spec->fields[n].type = t;
        spec->fields[n].id = id;
        spec->numFields++;
        // printf("loaded field %s id %d\n", argv[i], id);
        n++;
    }

    return REDISMODULE_OK;

failure:  // on failure free the spec fields array and return an error

    free(spec->fields);
    spec->fields = NULL;
    return REDISMODULE_ERR;
}

void IndexSpec_Free(IndexSpec *spec) {
    if (spec->fields != NULL) {
        free(spec->fields);
    }
}

/* Saves the spec as a LIST, containing basically the arguments needed to recreate the spec */
int IndexSpec_Save(RedisModuleCtx *ctx, IndexSpec *sp) {
    RedisModuleKey *k =
        RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, "idx:%s", sp->name),
                            REDISMODULE_READ | REDISMODULE_WRITE);
    if (k == NULL) {
        return REDISMODULE_ERR;
    }
    // reset the list we'll be writing into
    if (RedisModule_DeleteKey(k) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    for (int i = 0; i < sp->numFields; i++) {
        RedisModule_ListPush(
            k, REDISMODULE_LIST_TAIL,
            RedisModule_CreateString(ctx, sp->fields[i].name, strlen(sp->fields[i].name)));
        if (sp->fields[i].type == F_FULLTEXT) {
            RedisModule_ListPush(k, REDISMODULE_LIST_TAIL,
                                 RedisModule_CreateStringPrintf(ctx, "%f", sp->fields[i].weight));
        } else {
            RedisModule_ListPush(k, REDISMODULE_LIST_TAIL,
                                 RedisModule_CreateString(ctx, NUMERIC_STR, strlen(NUMERIC_STR)));
        }
    }

    return REDISMODULE_OK;
}

/* Load the spec from the saved version, which uses a redis list that basically
mimics the CREATE comand that was used to create it */
int IndexSpec_Load(RedisModuleCtx *ctx, IndexSpec *sp, const char *name) {
    sp->name = name;

    RedisModuleCallReply *resp = RedisModule_Call(
        ctx, "LRANGE", "scc", RedisModule_CreateStringPrintf(ctx, "idx:%s", sp->name), "0", "-1");
    if (resp == NULL || RedisModule_CallReplyType(resp) != REDISMODULE_REPLY_ARRAY) {
        return REDISMODULE_ERR;
    }

    size_t arrlen = RedisModule_CallReplyLength(resp);
    RedisModuleString *arr[arrlen];

    for (size_t i = 0; i < arrlen; i++) {
        arr[i] = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(resp, i));
    }
    return IndexSpec_ParseRedisArgs(sp, ctx, arr, arrlen);
}

u_char IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc) {
    u_char ret = 0;

    for (int i = 0; i < argc; i++) {
        size_t len;
        const char *p = RedisModule_StringPtrLen(argv[i], &len);

        FieldSpec *fs = IndexSpec_GetField(sp, p, len);
        if (fs != NULL) {
            LG_DEBUG("Found mask for %s: %d\n", p, fs->id);
            ret |= (fs->id & 0xff);
        }
    }

    return ret;
}