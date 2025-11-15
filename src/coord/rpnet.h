/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "module.h"
#include "config.h"
#include "result_processor.h"
#include "rmr/rmr.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_cursor_mappings.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  ResultProcessor base;
  struct {
    MRReply *root;  // Root reply. We need to free this when done with the rows
    MRReply *rows;  // Array containing reply rows for quick access
    MRReply *meta;  // Metadata for the current reply, if any (RESP3)
  } current;
  // Lookup - the rows are written in here
  RLookup *lookup;
  size_t curIdx;
  MRIterator *it;
  MRCommand cmd;
  AREQ *areq;

  // NEW: Direct cursor mappings (no more dispatcher context)
  StrongRef mappings;  // Single mapping array per RPNet

  // profile vars
  arrayof(MRReply *) shardsProfile;
} RPNet;


void rpnetFree(ResultProcessor *rp);
RPNet *RPNet_New(const MRCommand *cmd, int (*nextFunc)(ResultProcessor *, SearchResult *));
void RPNet_resetCurrent(RPNet *nc);
int rpnetNext(ResultProcessor *self, SearchResult *r);
int rpnetNext_EOF(ResultProcessor *self, SearchResult *r);
int rpnetNext_StartWithMappings(ResultProcessor *rp, SearchResult *r);
int getNextReply(RPNet *nc);

#ifdef __cplusplus
}
#endif
