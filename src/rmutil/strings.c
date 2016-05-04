#include <string.h>
#include <sys/param.h>
#include <ctype.h>
#include "strings.h"


#include "sds.h"

RedisModuleString *RMUtil_CreateFormattedString(RedisModuleCtx *ctx, const char *fmt, ...) {
    sds s = sdsempty();
    
    va_list ap;
    va_start(ap, fmt);
    s = sdscatvprintf(s, fmt, ap);
    va_end(ap);
    
    RedisModuleString *ret = RedisModule_CreateString(ctx, (const char *)s, sdslen(s));
    sdsfree(s);
    return ret;
}

int RMUtil_StringEquals(RedisModuleString *s1, RedisModuleString *s2) {
    
    
    const char *c1, *c2;
    size_t l1, l2;
    c1 = RedisModule_StringPtrLen(s1, &l1);
    c2 = RedisModule_StringPtrLen(s2, &l2);
    
    return strncasecmp(c1, c2, MIN(l1,l2)) == 0;
}

void RMUtil_StringToLower(RedisModuleString *s) {
    
    size_t l;
    char *c = (char *)RedisModule_StringPtrLen(s, &l);
    size_t i;
    for (i = 0; i < l; i++) {
        *c = tolower(*c);
        ++c;
    }
    
}

void RMUtil_StringToUpper(RedisModuleString *s) {
    size_t l;
    char *c = (char *)RedisModule_StringPtrLen(s, &l);
    size_t i;
    for (i = 0; i < l; i++) {
        *c = toupper(*c);
        ++c;
    }
    
    
}
