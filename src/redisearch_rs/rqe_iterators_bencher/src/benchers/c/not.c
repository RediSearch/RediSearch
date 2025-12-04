/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <time.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

// Minimal declarations to avoid complex header dependencies
typedef uint64_t t_docId;

typedef enum {
    ITERATOR_OK,
    ITERATOR_NOTFOUND,
    ITERATOR_EOF,
    ITERATOR_TIMEOUT,
} IteratorStatus;

// Get high-resolution timestamp in nanoseconds
static uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// ===== Simple IdList Iterator (child) =====
typedef struct {
    t_docId *ids;
    size_t count;
    size_t index;
    t_docId last_doc_id;
    bool at_eof;
} SimpleIdListIterator;

static SimpleIdListIterator* NewSimpleIdListIterator(t_docId *ids, size_t count) {
    SimpleIdListIterator *it = malloc(sizeof(SimpleIdListIterator));
    it->ids = ids;
    it->count = count;
    it->index = 0;
    it->last_doc_id = 0;
    it->at_eof = (count == 0);
    return it;
}

static IteratorStatus IdList_Read(SimpleIdListIterator *it) {
    if (it->at_eof || it->index >= it->count) {
        it->at_eof = true;
        return ITERATOR_EOF;
    }
    it->last_doc_id = it->ids[it->index++];
    return ITERATOR_OK;
}

static IteratorStatus IdList_SkipTo(SimpleIdListIterator *it, t_docId doc_id) {
    while (it->index < it->count && it->ids[it->index] < doc_id) {
        it->index++;
    }
    if (it->index >= it->count) {
        it->at_eof = true;
        return ITERATOR_EOF;
    }
    it->last_doc_id = it->ids[it->index++];
    if (it->last_doc_id == doc_id) {
        return ITERATOR_OK;
    }
    return ITERATOR_NOTFOUND;
}

// ===== Simple NOT Iterator (non-optimized) =====
typedef struct {
    SimpleIdListIterator *child;
    t_docId max_doc_id;
    t_docId last_doc_id;
    bool at_eof;
} SimpleNotIterator;

static SimpleNotIterator* NewSimpleNotIterator(SimpleIdListIterator *child, t_docId max_doc_id) {
    SimpleNotIterator *it = malloc(sizeof(SimpleNotIterator));
    it->child = child;
    it->max_doc_id = max_doc_id;
    it->last_doc_id = 0;
    it->at_eof = false;
    return it;
}

// NI_Read_NotOptimized equivalent
static IteratorStatus Not_Read(SimpleNotIterator *it) {
    if (it->at_eof || it->last_doc_id >= it->max_doc_id) {
        it->at_eof = true;
        return ITERATOR_EOF;
    }

    if (it->last_doc_id == it->child->last_doc_id) {
        IdList_Read(it->child);
    }

    while (it->last_doc_id < it->max_doc_id) {
        it->last_doc_id++;
        if (it->last_doc_id < it->child->last_doc_id || it->child->at_eof) {
            return ITERATOR_OK;
        }
        IdList_Read(it->child);
    }
    it->at_eof = true;
    return ITERATOR_EOF;
}

// NI_SkipTo_NotOptimized equivalent
static IteratorStatus Not_SkipTo(SimpleNotIterator *it, t_docId doc_id) {
    if (it->at_eof) return ITERATOR_EOF;
    if (doc_id > it->max_doc_id) {
        it->at_eof = true;
        return ITERATOR_EOF;
    }

    // Case 1: Child is ahead or at EOF
    if (it->child->last_doc_id > doc_id || it->child->at_eof) {
        it->last_doc_id = doc_id;
        return ITERATOR_OK;
    }
    // Case 2: Child is behind
    if (it->child->last_doc_id < doc_id) {
        IteratorStatus rc = IdList_SkipTo(it->child, doc_id);
        if (rc != ITERATOR_OK) {
            it->last_doc_id = doc_id;
            return ITERATOR_OK;
        }
    }
    // Child has docId
    it->last_doc_id = doc_id;
    IteratorStatus rc = Not_Read(it);
    return rc == ITERATOR_OK ? ITERATOR_NOTFOUND : rc;
}

static void Not_Free(SimpleNotIterator *it) {
    free(it->child->ids);
    free(it->child);
    free(it);
}

// ===== Benchmark functions =====

// Create exclusion list (every `step`-th doc excluded)
static t_docId* create_exclusion_list(t_docId max_id, t_docId step, size_t *count_out) {
    size_t count = max_id / step;
    t_docId *ids = malloc(count * sizeof(t_docId));
    for (size_t i = 0; i < count; i++) {
        ids[i] = (i + 1) * step;
    }
    *count_out = count;
    return ids;
}

// Benchmark NOT iterator read - sparse exclusions (every 1000th doc excluded)
void benchmark_not_read_sparse_c(uint64_t max_id, uint64_t* iterations_out, uint64_t* time_ns_out) {
    size_t count;
    t_docId *ids = create_exclusion_list(max_id, 1000, &count);
    SimpleIdListIterator *child = NewSimpleIdListIterator(ids, count);
    IdList_Read(child);  // Prime the child
    SimpleNotIterator *it = NewSimpleNotIterator(child, max_id);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    while (Not_Read(it) == ITERATOR_OK) {
        volatile uint64_t doc_id = it->last_doc_id;
        (void)doc_id;
        iterations++;
    }

    uint64_t end_time = get_time_ns();
    Not_Free(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}

// Benchmark NOT iterator read - dense exclusions (every 10th doc kept, 90% excluded)
void benchmark_not_read_dense_c(uint64_t max_id, uint64_t* iterations_out, uint64_t* time_ns_out) {
    // Create exclusion list: all docs except every 10th
    size_t count = max_id - (max_id / 10);
    t_docId *ids = malloc(count * sizeof(t_docId));
    size_t idx = 0;
    for (t_docId i = 1; i <= max_id; i++) {
        if (i % 10 != 0) {
            ids[idx++] = i;
        }
    }
    SimpleIdListIterator *child = NewSimpleIdListIterator(ids, count);
    IdList_Read(child);
    SimpleNotIterator *it = NewSimpleNotIterator(child, max_id);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    while (Not_Read(it) == ITERATOR_OK) {
        volatile uint64_t doc_id = it->last_doc_id;
        (void)doc_id;
        iterations++;
    }

    uint64_t end_time = get_time_ns();
    Not_Free(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}

// Benchmark NOT iterator skip_to - sparse exclusions
void benchmark_not_skip_to_sparse_c(uint64_t max_id, uint64_t step, uint64_t* iterations_out, uint64_t* time_ns_out) {
    size_t count;
    t_docId *ids = create_exclusion_list(max_id, 1000, &count);
    SimpleIdListIterator *child = NewSimpleIdListIterator(ids, count);
    IdList_Read(child);
    SimpleNotIterator *it = NewSimpleNotIterator(child, max_id);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    while (Not_SkipTo(it, it->last_doc_id + step) != ITERATOR_EOF) {
        volatile uint64_t doc_id = it->last_doc_id;
        (void)doc_id;
        iterations++;
    }

    uint64_t end_time = get_time_ns();
    Not_Free(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}

// Benchmark NOT iterator skip_to - dense exclusions
void benchmark_not_skip_to_dense_c(uint64_t max_id, uint64_t step, uint64_t* iterations_out, uint64_t* time_ns_out) {
    size_t count = max_id - (max_id / 10);
    t_docId *ids = malloc(count * sizeof(t_docId));
    size_t idx = 0;
    for (t_docId i = 1; i <= max_id; i++) {
        if (i % 10 != 0) {
            ids[idx++] = i;
        }
    }
    SimpleIdListIterator *child = NewSimpleIdListIterator(ids, count);
    IdList_Read(child);
    SimpleNotIterator *it = NewSimpleNotIterator(child, max_id);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    while (Not_SkipTo(it, it->last_doc_id + step) != ITERATOR_EOF) {
        volatile uint64_t doc_id = it->last_doc_id;
        (void)doc_id;
        iterations++;
    }

    uint64_t end_time = get_time_ns();
    Not_Free(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}
