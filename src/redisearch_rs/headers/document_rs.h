#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * The various document types supported by RediSearch.
 *
 * cbindgen:prefix-with-name
 */
typedef enum DocumentType {
  DocumentType_Hash = 0,
  DocumentType_Json = 1,
  DocumentType_Unsupported = 2,
} DocumentType;
