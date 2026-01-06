/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod hash;
mod json;

use document::{DocumentMetadata, DocumentType};
use enumflags2::{BitFlags, bitflags};
use sorting_vector::RSSortingVector;
use std::{ffi::c_char, marker::PhantomData, ptr::NonNull};
use value::RSValueFFI;

use crate::{RLookup, RLookupRow};

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
enum LoadDocumentFlag {
    /* Use keylist (keys/nkeys) for the fields to list */
    Keylist,
    /* Load only cached keys (don't open keys) */
    SvKrys,
    /* Load all keys in the document */
    AllKeys,
    /* Load all the keys in the RLookup object */
    LkKeys,
    /**
     * Don't use sortables when loading documents. This will enforce the loader to load
     * the fields from the document itself, even if they are sortables and un-normalized.
     */
    ForceLoad,
    /**
     * Force string return; don't coerce to native type
     */
    ForceString,
}

pub type LoadDocumentFlags = BitFlags<LoadDocumentFlag>;

pub struct LoadDocumentOptions<'a> {
    context: NonNull<redis_module::raw::RedisModuleCtx>,

    //   /** Needed for the key name, and perhaps the sortable */
    dmd: Option<DocumentMetadata>,
    //   /**
    //    * The following options control the loading of fields, in case non-SORTABLE
    //    * fields are desired.
    //    */
    flags: LoadDocumentFlags,

    //   /* Needed for rule filter where dmd does not exist */
    // used by hash::get_one
    key_ptr: *const c_char, // must be sds string???

    //   DocumentType type;
    //   /** Keys to load. If present, then loadNonCached and loadAllFields is ignored */
    //   const RLookupKey **keys;
    //   /** Number of keys in keys array */
    //   size_t nkeys;

    //   struct QueryError *status;
    _m: PhantomData<&'a ()>,
}

pub struct LoadDocumentError {}

// RLookup *it, RLookupRow *dst, RLookupLoadOptions *options
pub fn load_document(
    lookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    if let Some(dmd) = options.dmd.as_ref() {
        dst_row.set_sorting_vector(unsafe {
            dmd.sortVector
                .cast::<RSSortingVector<RSValueFFI>>()
                .as_ref()
        });
    }

    if options.flags.contains(LoadDocumentFlag::AllKeys) {
        let doc_ty = DocumentType::from(options.dmd.as_ref().unwrap().type_());

        match doc_ty {
            // NOTE: dmd MUST NOT be None
            DocumentType::Hash => hash::get_all(lookup, dst_row, options),
            DocumentType::Json => json::get_all(lookup, dst_row, options),
            DocumentType::Unsupported => panic!("unsupported document type"),
        }
    } else {
        // // Load the document from the schema. This should be simple enough...
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

        todo!()
    }
}
