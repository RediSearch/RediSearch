/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "rmr/reply.h"
#include "value_ffi.h"

// Consumes the reply, transferring string buffers to the resulting value.
RSValue *MRReply_ToValue(MRReply *reply);
