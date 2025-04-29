/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
/*
 * cluster_spell_check.h
 *
 *  Created on: Jul 29, 2018
 *      Author: meir
 */

#ifndef SRC_CLUSTER_SPELL_CHECK_H_
#define SRC_CLUSTER_SPELL_CHECK_H_

#include "rmr/reply.h"
#include "rmr/rmr.h"

int spellCheckReducer_resp2(struct MRCtx *mc, int count, MRReply **replies);
int spellCheckReducer_resp3(struct MRCtx* mc, int count, MRReply** replies);

#endif /* SRC_CLUSTER_SPELL_CHECK_H_ */
