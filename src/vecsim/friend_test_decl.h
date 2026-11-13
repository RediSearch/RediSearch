/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef INDEX_TEST_FRIEND_CLASS
#define INDEX_TEST_FRIEND_CLASS(class_name)                                                        \
    template <typename>                                                                            \
    friend class class_name;
#endif
