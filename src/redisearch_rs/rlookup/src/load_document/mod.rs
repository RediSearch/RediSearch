use std::fmt::Display;
use std::ptr::NonNull;
use std::{error::Error, ffi::CStr};

use document::DocumentType;
use ffi::DocumentMetadata;
use query_error::QueryError;
use redis_module::RedisString;
use sorting_vector::RSSortingVector;
use value::RSValueFFI;

use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};

mod hash;
mod json;

const UNDERSCORE_KEY: &CStr = c"__key";

#[derive(Debug)]
pub struct LoadDocumentError {}

impl Display for LoadDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load document")
    }
}

impl Error for LoadDocumentError {}

//   // Load the document from the schema. This should be simple enough...
//   void *key = NULL;  // This is populated by getKeyCommon; we free it at the end
//   DocumentType type = options->dmd ? options->dmd->type : options->type;
//   GetKeyFunc getKey = (type == DocumentType_Hash) ? (GetKeyFunc)getKeyCommonHash :
//                                                     (GetKeyFunc)getKeyCommonJSON;
//   int rc = REDISMODULE_ERR;
//   // On error we silently skip the rest
//   // On success we continue
//   // (success could also be when no value is found and nothing is loaded into `dst`,
//   //  for example, with a JSONPath with no matches)
//   if (options->nkeys) {
//     for (size_t ii = 0; ii < options->nkeys; ++ii) {
//       const RLookupKey *kk = options->keys[ii];
//       if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
//         goto done;
//       }
//     }
//   } else { // If we called load to perform IF operation with FT.ADD command
//     for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
//       /* key is not part of document schema. no need/impossible to 'load' it */
//       if (!(kk->flags & RLOOKUP_F_SCHEMASRC)) {
//         continue;
//       }
//       if (!options->forceLoad) {
//         /* wanted a sort key, but field is not sortable */
//         if ((options->mode & RLOOKUP_LOAD_SVKEYS) && !(kk->flags & RLOOKUP_F_SVSRC)) {
//           continue;
//         }
//       }
//       if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
//         goto done;
//       }
//     }
//   }
//   rc = REDISMODULE_OK;

// done:
//   if (key) {
//     switch (type) {
//     case DocumentType_Hash: RedisModule_CloseKey(key); break;
//     case DocumentType_Json: break;
//     case DocumentType_Unsupported: RS_LOG_ASSERT(1, "placeholder");
//     }
//   }
//   return rc;

pub fn load_specific_keys<'a, 'k, I>(
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    dmd: &DocumentMetadata,
    keys_to_load: I,
    force_load: bool,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError>
where
    I: IntoIterator<Item = &'k RLookupKey<'a>>,
    'a: 'k,
{
    dst_row.set_sorting_vector(unsafe {
        dmd.sortVector
            .cast::<RSSortingVector<RSValueFFI>>()
            .as_ref()
    });

    // GetKeyFunc getKey = (type == DocumentType_Hash) ? (GetKeyFunc)getKeyCommonHash :
    //                                                   (GetKeyFunc)getKeyCommonJSON;

    let key_name_len = unsafe { ffi::sdslen__(dmd.keyPtr) };

    let key_name =
        unsafe { RedisString::from_raw_parts(Some(ctx.cast()), dmd.keyPtr, key_name_len) };

    // DocumentType type = options->dmd ? options->dmd->type : options->type;
    // TODO this does NOT fall back to looking up the type from the options since I think thats stupid.
    //  but we should assert that its not required.
    let load_key = match dmd.type_() {
        DocumentType::Hash => hash::load_key,
        DocumentType::Json => json::load_key,
        DocumentType::Unsupported => unimplemented!("unsupported document type"),
    };

    // // On error we silently skip the rest
    // // On success we continue
    // // (success could also be when no value is found and nothing is loaded into `dst`,
    // //  for example, with a JSONPath with no matches)
    //   for (size_t ii = 0; ii < options->nkeys; ++ii) {
    //     const RLookupKey *kk = options->keys[ii];
    //     if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
    //       goto done;
    //     }
    //   }
    for key_to_load in keys_to_load {
        if is_value_available(dst_row, key_to_load, force_load) {
            continue;
        }

        load_key(key_to_load, dst_row, ctx, &key_name, api_version, status)?;
    }

    Ok(())
}

// If we called load to perform IF operation with FT.ADD command
pub fn load_specific_keys_from_rlookup(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<RSValueFFI>,
    ctx: NonNull<ffi::RedisModuleCtx>,
    dmd: &DocumentMetadata,
    force_load: bool,
    need_sortable: bool,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    let keys_to_load = rlookup
        .iter()
        // include only keys that are part of the document schema. We cannot load other keys anyway.
        .filter(|key| key.flags.contains(RLookupKeyFlag::SchemaSrc));

    let keys_to_load: Vec<_> = if need_sortable && !force_load {
        // if we're told to load only sortable keys, we'll filter out all non-sortable ones
        // UNLESS force_load is given which overrides this filtering
        keys_to_load
            .filter(|key| key.flags.contains(RLookupKeyFlag::SvSrc))
            .collect()
    } else {
        // otherwise, just keep the list of keys as-is.
        keys_to_load.collect()
    };

    load_specific_keys(
        dst_row,
        ctx,
        dmd,
        keys_to_load,
        force_load,
        api_version,
        status,
    )
}

// returns true if the value of the key is already available
// avoids the need to call to redis api to get the value
// i.e we can use the sorting vector as a cache
fn is_value_available(
    dst_row: &RLookupRow<'_, RSValueFFI>,
    kk: &RLookupKey,
    force_load: bool,
) -> bool {
    !force_load
        && (
            // No need to "write" this key. It's always implicitly loaded!
            kk.flags.contains(RLookupKeyFlag::ValAvailable)
        ||
        // There is no value in the sorting vector, and we don't need to load it from the document.
        (kk.flags.contains(RLookupKeyFlag::SvSrc) && dst_row.get(kk).is_none())
        )
}

pub fn load_all_keys(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<RSValueFFI>,
    dmd: &DocumentMetadata,
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_string: bool,
    api_version: u32,
    status: &mut QueryError,
) -> Result<(), LoadDocumentError> {
    dst_row.set_sorting_vector(unsafe {
        dmd.sortVector
            .cast::<RSSortingVector<RSValueFFI>>()
            .as_ref()
    });

    let key_name_len = unsafe { ffi::sdslen__(dmd.keyPtr) };

    let key_name =
        unsafe { RedisString::from_raw_parts(Some(ctx.cast()), dmd.keyPtr, key_name_len) };

    match dmd.type_() {
        DocumentType::Hash => {
            hash::load_all_keys(rlookup, dst_row, ctx, &key_name, force_string, status)
        }
        DocumentType::Json => {
            json::load_all_keys(rlookup, dst_row, ctx, &key_name, api_version, status)
        }
        DocumentType::Unsupported => unimplemented!("unsupported document type"),
    }
}
