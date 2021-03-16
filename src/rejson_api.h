#ifndef SRC_REJSON_API_H_
#define SRC_REJSON_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum JSONType {
    JSONType_String = 0,
    JSONType_Int = 1,
    JSONType_Float = 2,
    JSONType_Bool = 3,
    JSONType_Object = 4,
    JSONType_Array = 5,
    JSONType_Null = 6,
    JSONType_Err = 7,
    JSONType__EOF
} JSONType;

typedef void *RedisJSON;

// API Signatures
typedef RedisModuleString * (*get_json_path)(struct RedisModuleCtx* ctx, RedisModuleString* key_name, const char* path);
//typedef int (*get_json_path)(RedisModuleKey* key, const char* path);

// API "Bundle"
typedef struct RedisJSONAPI_V1 {
    const RedisJSON* (*getPath)(struct RedisModuleCtx* ctx, RedisModuleString* key_name, const char* path);
//    const RedisJSON* (*getAt)(const RedisJSON *json, size_t at);
    int (*getInfo)(const RedisJSON *json, const char **name, int *type, size_t *items);
//    int (*getString)(const RedisJSON *json, const char** str, size_t *size);
//    int (*getInt)(const RedisJSON *json, int *num);
//    int (*getDouble)(const RedisJSON *json, double* num);
//
    void (*free)(const RedisJSON*);
    //
    //
    //

    /*
    // For Object and Array
    // (for Array - question if internal Vec<Value> can be returned (it does not have attribute #[repr(C)])
    // (for Object - question if whether we should flat down the map into an array)
    // Can use callbacks instead
    int (*JSON_GetAllChildren)(const RedisJSON *data, RedisJSON **dataResult, size_t *count);

    // For Array
    int (*JSON_GetAtPos)(const RedisJSON *data, size_t indexStart, size_t indexEnd, const RedisJSON *data_result);
    // For Object
    int (*JSON_GetAtField)(const RedisJSON *data, const char *fieldName, RedisJSON *dataResult);

    // Free memory which was specifically allocated for API for multi
    int (*JSON_FreeMulti)(const RedisJSON *data);

    // TODO: either isJSON or share pointer for RedisModuleType
    int (*JSON_IsJSON)(RedisModuleCtx *ctx, const char *key);
    // TODO: extern RedisModuleType *JSONType;

    // Alternative naming
    const RedisJSON *(*getRoot)(struct RedisModuleCtx* ctx, RedisModuleString* key_name, const char* path);
    void (*freeJSON)(const RedisJSON *json);
    int (*sayWhat)(const RedisJSON *json, const char **name, JsonValueType *type, size_t *items);
    const RedisJSON *(*getAt)(const RedisJSON *json, size_t at); // allocates RedisJSON
    int (*getString)(const RedisJSON *json, const char** str, size_t *size);
    int (*getInt)(const RedisJSON *json, int *num);
    int (*getDouble)(const RedisJSON *json, double* num);
    int (*getBool)(const RedisJSON *json, int* num); // int?
    int (*replyWith)(struct RedisModuleCtx* ctx, const RedisJSON *json);
    int (*replyWiths)(struct RedisModuleCtx* ctx, const RedisJSON *jsons, size_t len);
     */
} RedisJSONAPI_V1;

// REJSON APIs
RedisJSONAPI_V1 *japi;

//Demo use case 1
// FT.CREATE idx on json...
// FT.SEARCH * .. ==> 0 results
// JSON.SET searchToken '{}'...
// FT.SEARCH * ==> 1 result

// Open questions for phase 1:
// Should Array with size > 1 be supported in phase1?

//Path "...A" => Object '{.., "A": [ "C","D", {"E":"F"} ] , ...}'  => not supported since one of the elements is an Object
//Path "...A" => Object '{.., "A": [ "C","D", true, 10.12 ] , ...}' => Not supported since array size is > 1

// Ingest - get all values of fields in index
// Load - get all values of requested fields
// Printout - Reply With




#ifdef __cplusplus
}
#endif
#endif /* SRC_REJSON_API_H_ */
