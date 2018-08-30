#ifndef __MDMA_LOGGING__
#define __MDMA_LOGGING__

#define L_DEBUG 1
#define L_INFO 2
#define L_WARN 4
#define L_ERROR 8
#define L_TRACE 16

extern int RSLoggingLevel;
// L_DEBUG | L_INFO

void RSLoggingInit(int level);

#define LG_MSG(...) fprintf(stdout, __VA_ARGS__);
#define LG_DEBUG(...)                                              \
  if (RSLoggingLevel & L_DEBUG) {                                  \
    LG_MSG("[DEBUG %s:%d@%s] ", __FILE__, __LINE__, __FUNCTION__); \
    LG_MSG(__VA_ARGS__);                                           \
    LG_MSG("\n");                                                  \
  }
#define LG_INFO(...)                             \
  if (RSLoggingLevel & L_INFO) {                 \
    LG_MSG("[INFO %s:%d] ", __FILE__, __LINE__); \
    LG_MSG(__VA_ARGS__);                         \
  }
#define LG_WARN(...)                                \
  if (RSLoggingLevel & L_WARN) {                    \
    LG_MSG("[WARNING %s:%d] ", __FILE__, __LINE__); \
    LG_MSG(__VA_ARGS__);                            \
  }
#define LG_ERROR(...)                             \
  if (RSLoggingLevel & L_ERROR) {                 \
    LG_MSG("[ERROR %s:%d] ", __FILE__, __LINE__); \
    LG_MSG(__VA_ARGS__);                          \
  }

#endif