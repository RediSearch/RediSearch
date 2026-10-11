/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef INTERNAL_RESP_SCHEMA_H__
#define INTERNAL_RESP_SCHEMA_H__

// Internal aggregate results: [tag, rows, schema]. RESP2 rows retain their count prefix.
// Each row is [presence, values]. A null presence denotes a dense
// schema prefix; otherwise an ASCII 0/1 string identifies the present columns.
// Schema is a trailer so LOAD * can append columns without buffering streaming rows.
#define INTERNAL_RESP_SCHEMA_TAG "__resp_schema_v1"

#endif
