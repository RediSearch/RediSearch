#ifndef RS_FIELDSSTATS_H_
#define RS_FIELDSSTATS_H_

#include "spec.h"

typedef struct {
  size_t numTextFields;
  size_t numTextFieldsSortable;
  size_t numTextFieldsNoIndex;
  size_t numNumericFields;
  size_t numNumericFieldsSortable;
  size_t numNumericFieldsNoIndex;
  size_t numGeoFields;
  size_t numGeoFieldsSortable;
  size_t numGeoFieldsNoIndex;
  size_t numTagFields;
  size_t numTagFieldsSortable;
  size_t numTagFieldsNoIndex;
  size_t numTagFieldsCaseSensitive;
  size_t numVectorFields;
  size_t numVectorFieldsFlat;
  size_t numVectorFieldsHSNW;
} FieldsGlobalStats;

/**
 * Check the type of the the given field and update RSGlobalConfig.fieldsStats
 * according to the givan toAdd value.
 */
void FieldsGlobalStats_UpdateStats(FieldSpec *fs, int toAdd);

/**
 * Exposing all the fields that > 0 to INFO command.
 */
void FieldsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx);

#endif
