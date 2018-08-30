#include "logging.h"

int RSLoggingLevel = 0;
// L_DEBUG | L_INFO

void RSLoggingInit(int level) {
  RSLoggingLevel = level;
}
