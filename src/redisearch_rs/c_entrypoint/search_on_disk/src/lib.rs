/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::HashMap;

use ffi::{
    BasicDiskAPI, DocTableDiskAPI, IndexDiskAPI, RedisModuleCtx, RedisSearchDisk,
    RedisSearchDiskAPI,
};

#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_HasAPI() -> bool {
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn SearchDisk_GetAPI() -> *mut RedisSearchDiskAPI {
    let api = Box::new(RedisSearchDiskAPI {
        basic: BasicDiskAPI {
            open: Some(open),
            openIndexSpec: None,
            closeIndexSpec: None,
            close: None,
        },
        index: IndexDiskAPI {
            indexDocument: None,
            newTermIterator: None,
            newWildcardIterator: None,
        },
        docTable: DocTableDiskAPI {
            putDocument: None,
            isDocIdDeleted: None,
            getDocumentMetadata: None,
        },
    });

    Box::into_raw(api)
}

#[unsafe(no_mangle)]
extern "C" fn open(
    _ctx: *mut RedisModuleCtx,
    _path: *const ::std::os::raw::c_char,
) -> *mut RedisSearchDisk {
    let disk = Box::new(SearchDisk {
        indices: HashMap::new(),
    });

    Box::into_raw(disk) as *mut RedisSearchDisk
}

struct SearchDisk {
    indices: HashMap<String, ()>,
}
