/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use slots_tracker::SlotRange;

#[test]
fn contains_is_inclusive() {
    let r = SlotRange {
        start: 100,
        end: 200,
    };
    assert!(r.contains(100) && r.contains(150) && r.contains(200));
    assert!(!r.contains(99) && !r.contains(201));
}
