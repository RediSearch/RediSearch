#ifndef SRC_REDIS_VERSION_H_
#define SRC_REDIS_VERSION_H_

extern int redisMajorVersion;
extern int redisMinorVersion;
extern int redisPatchVersion;

extern int rlecMajorVersion;
extern int rlecMinorVersion;
extern int rlecPatchVersion;
extern int rlecBuild;

void getRedisVersion();

static inline int IsEnterprise() {
  return rlecMajorVersion != -1;
}

#endif /* SRC_REDIS_VERSION_H_ */
