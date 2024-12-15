/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef OBFUSCATION_API_H
#define OBFUSCATION_API_H
#include "redisearch.h"
#include "hash/hash.h"

// Length definitions fo the required buffer sizes for obfuscation
#define MAX_OBFUSCATED_INDEX_NAME 6/*strlen("Index@")*/ + SHA1_TEXT_MAX_LENGTH + 1/*null terminator*/
#define MAX_OBFUSCATED_FIELD_NAME 6/*strlen("Field@")*/ + MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND + 1/*null terminator*/
#define MAX_OBFUSCATED_PATH_NAME MAX_OBFUSCATED_FIELD_NAME
#define MAX_OBFUSCATED_DOCUMENT_NAME 9/*strlen("Document@")*/ + MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND + 1/*null terminator*/
#define MAX_OBFUSCATED_KEY_NAME MAX_OBFUSCATED_DOCUMENT_NAME

// Writes into buffer the obfuscated name of the index, based on the sha input.
// Assumes buffer size is at least MAX_OBFUSCATED_INDEX_NAME
void Obfuscate_Index(const Sha1 *sha, char *buffer);
// Writes into buffer the obfuscated name of the field, based on the field id.
// Assumes buffer size is at least MAX_OBFUSCATED_FIELD_NAME
void Obfuscate_Field(t_uniqueId fieldId, char *buffer);
// Writes into buffer the obfuscated name of the field path, based on the field id.
// Assumes buffer size is at least MAX_OBFUSCATED_PATH_NAME
void Obfuscate_FieldPath(t_uniqueId fieldId, char *buffer);
// Writes into buffer the obfuscated name of the document, based on the doc id.
// Assumes buffer size is at least MAX_OBFUSCATED_DOCUMENT_NAME
void Obfuscate_Document(t_uniqueId docId, char *buffer);
// The main difference between a document key and a document is that a document was assigned a unique document id
// Writes into buffer the obfuscated name of the key, based on the timespec(currently the indexing failure time)
// Assumes buffer size is at least MAX_OBFUSCATED_KEY_NAME
void Obfuscate_KeyWithTime(struct timespec spec, char *buffer);

// Set of functions to obfuscate types of data we index
// Currently done in a very simplified way
// the returned pointer needs to be freed using rm_free
char *Obfuscate_Text(const char *text);
char *Obfuscate_Number(size_t number);
char *Obfuscate_Vector(const char *vector, size_t dim);
char *Obfuscate_Tag(const char *tag);
char *Obfuscate_Geo(uint16_t longitude, uint16_t latitude);
char *Obfuscate_GeoShape();

struct RSQueryNode;
// Obfuscate a query node based on its type
// the returned pointer needs to be freed using rm_free
char *Obfuscate_QueryNode(struct RSQueryNode *node);

#endif //OBFUSCATION_API_H
