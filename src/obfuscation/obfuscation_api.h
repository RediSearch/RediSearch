/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef OBFUSCATION_API_H
#define OBFUSCATION_API_H
#include "redisearch.h"

#define MAX_OBFUSCATED_INDEX_NAME 6/*strlen("Index@")*/ + MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND + 1/*null terminator*/
#define MAX_OBFUSCATED_FIELD_NAME 6/*strlen("Field@")*/ + MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND + 1/*null terminator*/
#define MAX_OBFUSCATED_PATH_NAME MAX_OBFUSCATED_FIELD_NAME
#define MAX_OBFUSCATED_DOCUMENT_NAME 9/*strlen("Document@")*/ + MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND + 1/*null terminator*/

void Obfuscate_Index(t_uniqueId indexId, char* buffer);
void Obfuscate_Field(t_uniqueId fieldId, char* buffer);
void Obfuscate_FieldPath(t_uniqueId fieldId, char* buffer);
void Obfuscate_Document(t_uniqueId docId, char* buffer);

char *Obfuscate_Text(const char* text);
char *Obfuscate_Number(double number);
char *Obfuscate_Vector(const char* vector, size_t dim);
char *Obfuscate_Tag(const char* tag);
char *Obfuscate_Geo(uint16_t longitude, uint16_t latitude);
char *Obfuscate_GeoShape();

struct RSQueryNode;
char *Obfuscate_QueryNode(struct RSQueryNode *node);

#endif //OBFUSCATION_API_H
