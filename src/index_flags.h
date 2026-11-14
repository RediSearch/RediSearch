/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

// Split out of spec.h so headers spec.h itself includes (e.g. search_disk_api.h)
// can name the type.
typedef enum {
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,
  Index_HasMultiValue = 0x04,
  Index_HasCustomStopwords = 0x08,
  Index_StoreFreqs = 0x010,
  Index_StoreNumeric = 0x020,
  Index_StoreByteOffsets = 0x40,
  Index_WideSchema = 0x080,
  Index_HasSmap = 0x100,
  Index_Temporary = 0x200,
  Index_DocIdsOnly = 0x00,

  // If any of the fields has phonetics. This is just a cache for quick lookup
  Index_HasPhonetic = 0x400,
  Index_Async = 0x800,
  Index_SkipInitialScan = 0x1000,
  Index_HasFieldAlias = 0x4000,
  Index_HasVecSim = 0x8000,
  Index_HasSuffixTrie = 0x10000,
  // If any of the fields has undefined order. This is just a cache for quick lookup
  Index_HasUndefinedOrder = 0x20000,

  Index_HasGeometry = 0x40000,

  Index_HasNonEmpty = 0x80000,  // Index has at least one field that does not indexes empty values
} IndexFlags;
