/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include <stdlib.h>
#include "vec_sim_common.h"
#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief A struct to hold an index information for generic purposes. Each information field is of
 * the type VecSim_InfoFieldType. This struct exposes an iterator-like API to iterate over the
 * information fields.
 */
typedef struct VecSimDebugInfoIterator VecSimDebugInfoIterator;

typedef enum {
    INFOFIELD_STRING,
    INFOFIELD_INT64,
    INFOFIELD_UINT64,
    INFOFIELD_FLOAT64,
    INFOFIELD_ITERATOR
} VecSim_InfoFieldType;

typedef union {
    double floatingPointValue;              // Floating point value. 64 bits float.
    int64_t integerValue;                   // Integer value. Signed 64 bits integer.
    uint64_t uintegerValue;                 // Unsigned value. Unsigned 64 bits integer.
    const char *stringValue;                // String value.
    VecSimDebugInfoIterator *iteratorValue; // Iterator value.
} FieldValue;

/**
 * @brief A struct to hold field information. This struct contains three members:
 *  fieldType - Enum describing the content of the value.
 *  fieldName - Field name.
 *  fieldValue - A union of string/integer/float values.
 */
typedef struct {
    const char *fieldName;          // Field name.
    VecSim_InfoFieldType fieldType; // Field type (in {STR, INT64, FLOAT64})
    FieldValue fieldValue;
} VecSim_InfoField;

/**
 * @brief Returns the number of fields in the info iterator.
 *
 * @param infoIterator Given info iterator.
 * @return size_t Number of fields.
 */
size_t VecSimDebugInfoIterator_NumberOfFields(VecSimDebugInfoIterator *infoIterator);

/**
 * @brief Returns if the fields iterator is depleted.
 *
 * @param infoIterator Given info iterator.
 * @return true Iterator is not depleted.
 * @return false Otherwise.
 */
bool VecSimDebugInfoIterator_HasNextField(VecSimDebugInfoIterator *infoIterator);

/**
 * @brief Returns a pointer to the next info field.
 *
 * @param infoIterator Given info iterator.
 * @return VecSim_InfoField* A pointer to the next info field.
 */
VecSim_InfoField *VecSimDebugInfoIterator_NextField(VecSimDebugInfoIterator *infoIterator);

/**
 * @brief Free an info iterator.
 *
 * @param infoIterator Given info iterator.
 */
void VecSimDebugInfoIterator_Free(VecSimDebugInfoIterator *infoIterator);

#ifdef __cplusplus
}
#endif
