/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use quantile_ffi::{NewQuantileStream, QS_Free, QS_GetCount, QS_Insert, QS_Query};
use std::ptr;

#[test]
fn construction() {
    // SAFETY: the target array is initialized and alive for construction. Each
    // non-null handle is exclusively used below and freed once, with no escaping borrows.
    #[expect(clippy::multiple_unsafe_ops_per_block)]
    unsafe {
        assert!(NewQuantileStream(ptr::null(), 0, 0).is_null());
        for mut targets in [vec![], vec![0.5, 0.9]] {
            let stream = NewQuantileStream(targets.as_ptr(), targets.len(), 2);
            assert!(!stream.is_null());
            targets.clear();
            drop(targets);
            assert!(QS_Query(stream, 0.5).is_nan());
            QS_Insert(stream, 3.0);
            assert_eq!(QS_GetCount(stream), 0);
            QS_Insert(stream, 7.0);
            assert_eq!(QS_GetCount(stream), 2);
            assert_eq!(QS_Query(stream, 1.0), 7.0);
            QS_Free(stream);
        }
        let stream = NewQuantileStream(ptr::null(), 0, 1);
        assert!(!stream.is_null());
        QS_Free(stream);
    }
}
