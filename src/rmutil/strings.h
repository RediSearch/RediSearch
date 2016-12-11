#ifndef __RMUTIL_STRINGS_H__
#define __RMUTIL_STRINGS_H__

#include <redismodule.h>

/*
* Create a new RedisModuleString object from a printf-style format and arguments.
* Note that RedisModuleString objects CANNOT be used as formatting arguments.
*/
// DEPRECATED since it was added to the RedisModule API. Replaced with a macro below
//RedisModuleString *RMUtil_CreateFormattedString(RedisModuleCtx *ctx, const char *fmt, ...);
#define RMUtil_CreateFormattedString RedisModule_CreateStringPrintf


/* Return 1 if the two strings are equal. Case *sensitive* */
int RMUtil_StringEquals(RedisModuleString *s1, RedisModuleString *s2);

/* Return 1 if the string is equal to a C NULL terminated string. Case *sensitive* */
int RMUtil_StringEqualsC(RedisModuleString *s1, const char *s2);

/* Converts a redis string to lowercase in place without reallocating anything */
void RMUtil_StringToLower(RedisModuleString *s);

/* Converts a redis string to uppercase in place without reallocating anything */
void RMUtil_StringToUpper(RedisModuleString *s);
#endif
