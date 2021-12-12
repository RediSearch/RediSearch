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

int spellCheckReducer(struct MRCtx *mc, int count, MRReply **replies);


#endif /* SRC_CLUSTER_SPELL_CHECK_H_ */
