/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{VecSimQueryReply_Order, VecSimQueryReply_Order_BY_SCORE_THEN_ID};
use vecsim::ReplyOrder;

#[test]
fn reply_order_from_raw_rejects_orders_the_query_api_does_not_accept() {
    assert_eq!(
        ReplyOrder::from_raw(VecSimQueryReply_Order_BY_SCORE_THEN_ID),
        None
    );
    assert_eq!(ReplyOrder::from_raw(VecSimQueryReply_Order::MAX), None);
}
