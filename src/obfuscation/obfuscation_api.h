//
// Created by jonathan on 9/18/24.
//

#ifndef OBFUSCATION_API_H
#define OBFUSCATION_API_H
#include "redisearch.h"

char *Obfuscate_Index(t_uniqueId indexId);
char *Obfuscate_Field(t_uniqueId fieldId);
char *Obfuscate_Document(t_uniqueId docId);

char *Obfuscate_Text(const char* text);
char *Obfuscate_Number(size_t number);
char *Obfuscate_Vector(const char* vector, size_t dim);
char *Obfuscate_Tag(const char* tag);
char *Obfuscate_Geo(uint16_t longitude, uint16_t latitude);
char *Obfuscate_GeoShape();

struct QueryNode;
char *Obfuscate_QueryNode(struct QueryNode *node);

#endif //OBFUSCATION_API_H
