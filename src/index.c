/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include "spec.h"
#include <math.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/heap.h"
#include "profile.h"
#include "hybrid_reader.h"
#include "metric_iterator.h"
#include "optimizer_reader.h"
#include "util/units.h"
#include "iterators/inverted_index_iterator.h"
#include "iterators/profile_iterator.h"


static inline int UI_ReadUnsorted(void *ctx, RSIndexResult **hit) {
  UnionIterator *ui = ctx;
  int rc = INDEXREAD_OK;
  RSIndexResult *res = NULL;
  while (ui->currIt < ui->num) {
    rc = ui->origits[ui->currIt]->Read(ui->origits[ui->currIt]->ctx, &res);
    if (rc == INDEXREAD_OK) {
      *hit = res;
      return rc;
    }
    ++ui->currIt;
  }
  return INDEXREAD_EOF;
}

void trimUnionIterator(IndexIterator *iter, size_t offset, size_t limit, bool asc) {
  RS_LOG_ASSERT(iter->type == UNION_ITERATOR, "trim applies to union iterators only");
  UnionIterator *ui = (UnionIterator *)iter;
  if (ui->norig <= 2) { // nothing to trim
    return;
  }

  size_t curTotal = 0;
  int i;
  if (offset == 0) {
    if (asc) {
      for (i = 1; i < ui->num; ++i) {
        IndexIterator *it = ui->origits[i];
        curTotal += it->NumEstimated(it->ctx);
        if (curTotal > limit) {
          ui->num = i + 1;
          memset(ui->its + ui->num, 0, ui->norig - ui->num);
          break;
        }
      }
    } else {  //desc
      for (i = ui->num - 2; i > 0; --i) {
        IndexIterator *it = ui->origits[i];
        curTotal += it->NumEstimated(it->ctx);
        if (curTotal > limit) {
          ui->num -= i;
          memmove(ui->its, ui->its + i, ui->num);
          memset(ui->its + ui->num, 0, ui->norig - ui->num);
          break;
        }
      }
    }
  } else {
    UI_SyncIterList(ui);
  }
  iter->Read = UI_ReadUnsorted;
}
