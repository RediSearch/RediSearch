//   ┌───────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
//   │   Layer   │                                                                 Functions                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Core node │ radix node with str: SmallVec<u16>, children: Vec<RuneNode>, first_runes: Vec<u16> (parallel array for binary search by leading rune),    │
//   │           │ payload: Option<Box<[u8]>>, score: f32, num_docs: u64, flags (Terminal/Deleted)                                                           │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Trie      │ Trie { root, size, freecb } — no sort_mode field needed (Lex implicit)                                                                    │
//   │ wrapper   │                                                                                                                                           │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Insert    │ Trie_InsertRune, Trie_InsertStringBuffer, Trie_Insert, Trie_InsertRuneNoSize; both ADD_REPLACE + ADD_INCR                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Delete    │ Trie_Delete, Trie_DeleteRunes, Trie_DecrementNumDocs                                                                                      │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Lookup    │ Trie_GetNode(runes, exact, *offsetOut)                                                                                                    │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Iterate   │ Trie_IterateAll, Trie_Iterate (DFA), Trie_IterateRange, Trie_IterateContains, Trie_IterateWildcard                                        │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ Misc      │ Trie_Size, Trie_RandomKey                                                                                                                 │
//   ├───────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
//   │ RDB       │ TrieType_GenericSave / GenericLoad (call back into C for RedisModule_Save* — wrap as FFI fn pointers)                                     │
//   └───────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

use itertools::Itertools;

use crate::TrieMap;

pub type Rune = u16;
type Data = u8;

pub struct RuneTrieMap {
    byte_trie_map: TrieMap<Data>,
}

impl RuneTrieMap {
    pub fn new() -> Self {
        Self {
            byte_trie_map: TrieMap::new(),
        }
    }

    pub fn insert(&mut self, key: &[Rune], data: Data) {
        self.byte_trie_map.insert(&convert_key(key), data);
    }

    pub fn remove(&mut self, key: &[Rune]) {
        self.byte_trie_map.remove(&convert_key(key));
    }

    pub fn get(&mut self, key: &[Rune]) -> Option<&Data> {
        self.byte_trie_map.find(&convert_key(key))
    }
}

fn convert_key(key: &[u16]) -> Vec<u8> {
    // Consider: Endianess
    // Consider: let bytes: &[u8] = bytemuck::cast_slice(my_u16_slice);
    key.into_iter()
        .map(|x| x.to_be_bytes())
        .flatten()
        .collect_vec()
}
