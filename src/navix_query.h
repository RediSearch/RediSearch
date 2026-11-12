/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef NAVIX_QUERY_H__
#define NAVIX_QUERY_H__

#include "filter_summary.h"
#include "VecSim/vec_sim_common.h"

struct RSQueryNode;
typedef struct NavixQuery NavixQuery;

/* The AST owns these contexts until after its iterators have been destroyed. */
NavixQuery* NavixQuery_New(struct IndexSpec* spec, const struct RSQueryNode* filter,
                           NavixQuery* previous, VecSimNavixParams* params, bool blind);
bool NavixQuery_PreferExactScan(NavixQuery* query);
bool NavixQuery_PreferOrdinarySearch(const NavixQuery* query, size_t estimated, size_t size);
void NavixQuery_Free(NavixQuery* query);

#endif
