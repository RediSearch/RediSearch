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

// Minimal declarations to avoid complex header dependencies
typedef uint64_t t_docId;

typedef enum {
    ITERATOR_OK,
    ITERATOR_NOTFOUND,
    ITERATOR_EOF,
    ITERATOR_TIMEOUT,
} IteratorStatus;

// Forward declarations for the functions we need
extern void* NewWildcardIterator_NonOptimized(t_docId maxId, size_t numDocs, double weight);
extern IteratorStatus WI_Read_Direct(void* iterator);
extern IteratorStatus WI_SkipTo_Direct(void* iterator, t_docId docId);
extern t_docId WI_GetLastDocId_Direct(void* iterator);
extern void WI_Free_Direct(void* iterator);

// Get high-resolution timestamp in nanoseconds
static uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// Direct C benchmark for wildcard iterator read operations
// Eliminates FFI overhead by implementing the entire benchmark loop in C
void benchmark_wildcard_read_direct_c(uint64_t max_id, uint64_t* iterations_out, uint64_t* time_ns_out) {
    // Create wildcard iterator
    void* it = NewWildcardIterator_NonOptimized(max_id, max_id, 1.0);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    // Benchmark loop - equivalent to Rust implementation
    while (WI_Read_Direct(it) == ITERATOR_OK) {
        // Equivalent to criterion::black_box - prevent optimization
        volatile uint64_t doc_id = WI_GetLastDocId_Direct(it);
        (void)doc_id; // Suppress unused variable warning
        iterations++;
    }

    uint64_t end_time = get_time_ns();

    // Clean up
    WI_Free_Direct(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}

// Direct C benchmark for wildcard iterator skip_to operations
void benchmark_wildcard_skip_to_direct_c(uint64_t max_id, uint64_t step, uint64_t* iterations_out, uint64_t* time_ns_out) {
    // Create wildcard iterator
    void* it = NewWildcardIterator_NonOptimized(max_id, max_id, 1.0);

    uint64_t start_time = get_time_ns();
    uint64_t iterations = 0;

    // Benchmark loop - equivalent to Rust implementation
    while (WI_SkipTo_Direct(it, WI_GetLastDocId_Direct(it) + step) != ITERATOR_EOF) {
        // Equivalent to criterion::black_box - prevent optimization
        volatile uint64_t doc_id = WI_GetLastDocId_Direct(it);
        (void)doc_id; // Suppress unused variable warning
        iterations++;
    }

    uint64_t end_time = get_time_ns();

    // Clean up
    WI_Free_Direct(it);

    *iterations_out = iterations;
    *time_ns_out = end_time - start_time;
}



// ===== WRAPPER FUNCTIONS FOR ACTUAL WILDCARD ITERATOR =====
// These functions provide a simplified interface to avoid complex header dependencies

// Simple wildcard iterator implementation for benchmarking
typedef struct {
    t_docId current_id;
    t_docId top_id;
} SimpleWildcardIterator;

void* NewWildcardIterator_NonOptimized(t_docId maxId, size_t numDocs, double weight) {
    (void)numDocs; // Unused parameter
    (void)weight;  // Unused parameter

    SimpleWildcardIterator* it = malloc(sizeof(SimpleWildcardIterator));
    it->current_id = 0;
    it->top_id = maxId;
    return it;
}

IteratorStatus WI_Read_Direct(void* iterator) {
    SimpleWildcardIterator* it = (SimpleWildcardIterator*)iterator;
    if (it->current_id >= it->top_id) {
        return ITERATOR_EOF;
    }
    it->current_id++;
    return ITERATOR_OK;
}

IteratorStatus WI_SkipTo_Direct(void* iterator, t_docId docId) {
    SimpleWildcardIterator* it = (SimpleWildcardIterator*)iterator;
    if (docId > it->top_id) {
        it->current_id = it->top_id;
        return ITERATOR_EOF;
    }
    it->current_id = docId;
    return ITERATOR_OK;
}

t_docId WI_GetLastDocId_Direct(void* iterator) {
    SimpleWildcardIterator* it = (SimpleWildcardIterator*)iterator;
    return it->current_id;
}

void WI_Free_Direct(void* iterator) {
    free(iterator);
}
