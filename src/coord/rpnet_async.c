/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rpnet_async.h"
#include "rpnet.h"
#include "rmr/rmr.h"
#include "rmr/chan.h"
#include "rmr/reply.h"
#include "concurrent_ctx.h"
#include "config.h"
#include "module.h"
#include "util/timeout.h"
#include "aggregate/aggregate.h"
#include "search_result.h"

#include <stdatomic.h>

#define CURSOR_EOF 0

// --- Helper: deserialize a single row from the reply into a new SearchResult ---

static SearchResult *deserializeRow(RPNetAsync *self, MRReply *rows, size_t idx) {
  const bool resp3 = (self->rpnet->cmd.protocol == 3);

  MRReply *fields = MRReply_ArrayElement(rows, idx);
  size_t fields_length = 0;
  MRReply *score = NULL;

  if (resp3) {
    RS_LOG_ASSERT(fields && MRReply_Type(fields) == MR_REPLY_MAP, "invalid result record");
    score = MRReply_MapElement(fields, "score");
    fields = MRReply_MapElement(fields, "extra_attributes");
    fields_length = fields && MRReply_Type(fields) == MR_REPLY_MAP ? MRReply_Length(fields) : 0;
  } else {
    fields_length = fields && MRReply_Type(fields) == MR_REPLY_ARRAY ? MRReply_Length(fields) : 0;
    RS_LOG_ASSERT(fields_length % 2 == 0, "invalid fields record");
  }

  SearchResult result = SearchResult_New();

  if (score) {
    RS_LOG_ASSERT(MRReply_Type(score) == MR_REPLY_DOUBLE, "invalid score record");
    SearchResult_SetScore(&result, MRReply_Double(score));
  }

  for (size_t i = 0; i < fields_length; i += 2) {
    size_t len;
    const char *field = MRReply_String(MRReply_ArrayElement(fields, i), &len);
    MRReply *val = MRReply_ArrayElement(fields, i + 1);
    RSValue *v = MRReply_ToValue(val);
    RLookupRow_WriteByNameOwned(self->lookup, field, len, SearchResult_GetRowDataMut(&result), v);
  }

  return SearchResult_AllocateMove(&result);
}

// --- Helper: process a single reply (extract rows, profile, accumulate totals) ---
// This mirrors the reply-processing logic in rpnetNext() (rpnet.c), adapted for
// buffered async draining. The core parsing (error handling, profile extraction,
// RESP2/RESP3 row extraction, warning processing) should be factored into shared
// helpers — see rpnet.c:getNextReply / rpnetNext for the synchronous counterpart.
// Returns RS_RESULT_OK on success, RS_RESULT_TIMEDOUT on timeout warning,
// RS_RESULT_ERROR on error reply.

static int processReplyIntoBuffer(RPNetAsync *self, MRReply *root) {
  RPNet *nc = self->rpnet;
  const bool resp3 = (nc->cmd.protocol == 3);

  // Check if an error was returned
  if (MRReply_Type(root) == MR_REPLY_ERROR) {
    if (nc->cmd.forProfiling) {
      MRReply *error = MRReply_Clone(root);
      array_append(self->shardsProfile, error);
    }

    QueryErrorCode errCode = QueryError_GetCodeFromMessage(MRReply_String(root, NULL));
    if (errCode == QUERY_ERROR_CODE_GENERIC ||
        errCode == QUERY_ERROR_CODE_UNAVAILABLE_SLOTS ||
        ((errCode == QUERY_ERROR_CODE_TIMED_OUT) && nc->areq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) ||
        ((errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) && nc->areq->reqConfig.oomPolicy == OomPolicy_Fail)) {
      QueryError_SetCode(&self->error, errCode);
      QueryError_SetDetail(&self->error, MRReply_String(root, NULL));
      MRReply_Free(root);
      return RS_RESULT_ERROR;
    }
    // Non-fatal error: skip
    MRReply_Free(root);
    return RS_RESULT_OK;
  }

  // For profile command, extract the profile data
  if (nc->cmd.forProfiling) {
    if (CURSOR_EOF == MRReply_Integer(MRReply_ArrayElement(root, 1))) {
      MRReply *profile_data;
      if (nc->cmd.protocol == 3) {
        MRReply *data = MRReply_ArrayElement(root, 0);
        profile_data = MRReply_TakeMapElement(data, "profile");
      } else {
        RS_ASSERT(nc->cmd.protocol == 2);
        RS_ASSERT(MRReply_Length(root) == 3);
        profile_data = MRReply_TakeArrayElement(root, 2);
      }
      array_append(self->shardsProfile, profile_data);
    }
  }

  // Extract rows and meta from reply
  MRReply *rows = NULL, *meta = NULL;
  if (resp3) {
    meta = MRReply_ArrayElement(root, 0);
    if (nc->cmd.forProfiling) {
      meta = MRReply_MapElement(meta, "results");
    }
    rows = MRReply_MapElement(meta, "results");
  } else {
    rows = MRReply_ArrayElement(root, 0);
  }

  const size_t empty_rows_len = resp3 ? 0 : 1;
  RS_LOG_ASSERT(rows && MRReply_Type(rows) == MR_REPLY_ARRAY, rows ? "rows is not an array" : "rows is NULL");

  if (MRReply_Length(rows) <= empty_rows_len) {
    // Empty reply — check for timeout warning
    bool timed_out = false;
    if (resp3) {
      RS_ASSERT(meta);
      MRReply *warning = MRReply_MapElement(meta, "warning");
      size_t num_warnings = MRReply_Length(warning);
      for (size_t i = 0; i < num_warnings; i++) {
        const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, i), NULL);
        if (!strcmp(warning_str, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT))) {
          timed_out = true;
        }
      }
    }
    MRReply_Free(root);
    return timed_out ? RS_RESULT_TIMEDOUT : RS_RESULT_OK;
  }

  // Accumulate total_results
  size_t startIdx;
  if (resp3) {
    startIdx = 0;
    self->totalResults += MRReply_Length(rows);
    processResultFormat(&nc->areq->reqflags, meta);
  } else {
    startIdx = 1;
    self->totalResults += MRReply_Integer(MRReply_ArrayElement(rows, 0));
  }

  // Deserialize all rows into buffer
  size_t len = MRReply_Length(rows);
  for (size_t i = startIdx; i < len; i++) {
    SearchResult *result = deserializeRow(self, rows, i);
    array_append(self->buffer, result);
  }

  // Check for warnings in the last batch
  if (resp3 && meta) {
    MRReply *warning = MRReply_MapElement(meta, "warning");
    size_t num_warnings = MRReply_Length(warning);
    for (size_t i = 0; i < num_warnings; i++) {
      const char *warning_str = MRReply_String(MRReply_ArrayElement(warning, i), NULL);
      if (!strcmp(warning_str, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT))) {
        MRReply_Free(root);
        return RS_RESULT_TIMEDOUT;
      } else if (!strcmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS)) {
        QueryError_SetReachedMaxPrefixExpansionsWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
      } else if (!strcmp(warning_str, QUERY_WOOM_SHARD)) {
        QueryError_SetQueryOOMWarning(AREQ_QueryProcessingCtx(nc->areq)->err);
      } else if (!strcmp(warning_str, QUERY_WINDEXING_FAILURE)) {
        AREQ_QueryProcessingCtx(nc->areq)->bgScanOOM = true;
      } else if (!strcmp(warning_str, QUERY_ASM_INACCURATE_RESULTS)) {
        nc->areq->stateflags |= QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT;
      }
    }
  }

  MRReply_Free(root);
  return RS_RESULT_OK;
}

// --- Core async run function (submitted to thread pool) ---

static void RPNetAsync_Run(void *arg) {
  RPNetAsync *self = (RPNetAsync *)arg;
  RPNet *nc = self->rpnet;

  while (true) {
    // Check query timeout
    if (nc->areq && AREQ_ShouldCheckTimeout(nc->areq) &&
        TimedOut(&nc->areq->sctx->time.timeout)) {
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
      self->lastRc = RS_RESULT_TIMEDOUT;
      goto done;
    }

    // Check blocked-client timeout
    if (nc->areq && AREQ_TimedOut(nc->areq)) {
      self->lastRc = RS_RESULT_TIMEDOUT;
      goto done;
    }

    // Trigger next cursor read batch if needed
    if (nc->cmd.forCursor) {
      if (!MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
        // No more pending commands and channel is empty — we're done
        self->lastRc = RS_RESULT_EOF;
        goto done;
      }
    }

    MRReply *reply = MRChannel_TryPop(MRIterator_GetChannel(nc->it));

    if (reply == NULL) {
      // Check if all shards are done
      if (!MRIterator_GetPending(nc->it) && MRIterator_GetChannelSize(nc->it) == 0) {
        self->lastRc = RS_RESULT_EOF;
        goto done;
      }

      // Yield: mark as waiting and return thread to pool
      atomic_store_explicit(&self->waiting, true, memory_order_release);

      // Double-check to avoid missed notification
      reply = MRChannel_TryPop(MRIterator_GetChannel(nc->it));
      if (reply != NULL) {
        atomic_store_explicit(&self->waiting, false, memory_order_release);
        // Fall through to process
      } else {
        // Also check if ManuallyTriggerNext now says we're done
        if (!nc->cmd.forCursor || !MR_ManuallyTriggerNextIfNeeded(nc->it, clusterConfig.cursorReplyThreshold)) {
          if (!MRIterator_GetPending(nc->it) && MRIterator_GetChannelSize(nc->it) == 0) {
            atomic_store_explicit(&self->waiting, false, memory_order_release);
            self->lastRc = RS_RESULT_EOF;
            goto done;
          }
        }
        return;  // Thread returns to pool — will be re-dispatched on notify
      }
    }

    // Process reply: deserialize rows and buffer results
    int rc = processReplyIntoBuffer(self, reply);
    if (rc == RS_RESULT_ERROR) {
      self->lastRc = RS_RESULT_ERROR;
      goto done;
    } else if (rc == RS_RESULT_TIMEDOUT) {
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(nc->it));
      self->lastRc = RS_RESULT_TIMEDOUT;
      goto done;
    }
  }

done:
  pthread_mutex_lock(&self->mutex);
  self->complete = true;
  pthread_cond_signal(&self->done_cond);
  pthread_mutex_unlock(&self->mutex);
}

// --- RPNetAsync API ---

RPNetAsync *RPNetAsync_New(RPNet *rpnet, RLookup *lookup) {
  RPNetAsync *self = rm_calloc(1, sizeof(*self));
  self->rpnet = rpnet;
  self->lookup = lookup;
  self->buffer = array_new(SearchResult *, 64);
  self->shardsProfile = rpnet->cmd.forProfiling ? array_new(MRReply *, 2) : NULL;
  self->totalResults = 0;
  self->lastRc = RS_RESULT_OK;
  self->complete = false;
  atomic_init(&self->waiting, false);

  pthread_mutex_init(&self->mutex, NULL);
  pthread_cond_init(&self->done_cond, NULL);

  self->error = QueryError_Default();

  return self;
}

void RPNetAsync_Start(RPNetAsync *self) {
  ConcurrentSearch_ThreadPoolRun(RPNetAsync_Run, self, 0);
}

void RPNetAsync_WaitForCompletion(RPNetAsync *self) {
  pthread_mutex_lock(&self->mutex);
  while (!self->complete) {
    pthread_cond_wait(&self->done_cond, &self->mutex);
  }
  pthread_mutex_unlock(&self->mutex);
}

void RPNetAsync_NotifyDataAvailable(RPNetAsync *self) {
  if (atomic_exchange_explicit(&self->waiting, false, memory_order_acq_rel)) {
    ConcurrentSearch_ThreadPoolRun(RPNetAsync_Run, self, 0);
  }
}

void RPNetAsync_Free(RPNetAsync *self) {
  if (!self) return;

  // Free any remaining buffered results that weren't transferred out
  if (self->buffer) {
    for (size_t i = 0; i < array_len(self->buffer); i++) {
      if (self->buffer[i]) {
        SearchResult_DeallocateDestroy(self->buffer[i]);
      }
    }
    array_free(self->buffer);
  }

  if (self->shardsProfile) {
    array_foreach(self->shardsProfile, reply, MRReply_Free(reply));
    array_free(self->shardsProfile);
  }

  QueryError_ClearError(&self->error);
  pthread_mutex_destroy(&self->mutex);
  pthread_cond_destroy(&self->done_cond);

  rm_free(self);
}

// --- RPBufferedSource implementation ---

static int RPBufferedSource_Next(ResultProcessor *base, SearchResult *r) {
  RPBufferedSource *self = (RPBufferedSource *)base;
  if (self->curIdx >= self->bufferLen) {
    return self->lastRc;
  }
  SearchResult *current = self->buffer[self->curIdx];
  self->buffer[self->curIdx] = NULL;
  self->curIdx++;
  SearchResult_Override(r, current);
  rm_free(current);
  return RS_RESULT_OK;
}

static void RPBufferedSource_Free(ResultProcessor *base) {
  RPBufferedSource *self = (RPBufferedSource *)base;
  if (self->buffer) {
    for (size_t i = self->curIdx; i < self->bufferLen; i++) {
      if (self->buffer[i]) {
        SearchResult_DeallocateDestroy(self->buffer[i]);
      }
    }
    array_free(self->buffer);
  }
  rm_free(self);
}

RPBufferedSource *RPBufferedSource_New(SearchResult **buffer, size_t len, int lastRc) {
  RPBufferedSource *self = rm_calloc(1, sizeof(*self));
  self->base.Next = RPBufferedSource_Next;
  self->base.Free = RPBufferedSource_Free;
  self->base.type = RP_NETWORK;
  self->buffer = buffer;
  self->bufferLen = len;
  self->curIdx = 0;
  self->lastRc = lastRc;
  return self;
}
