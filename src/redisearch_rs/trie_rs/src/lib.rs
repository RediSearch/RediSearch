pub mod c_api;
pub mod low_memory_vec;
pub mod matches_prefixes_iterator;
pub mod ordered_u8_map;
pub mod range_trie_iterator;
pub mod sub_trie_iterator;
pub mod trie;
pub mod trie_iter;
pub mod wildcard_trie_iterator;

use std::alloc::{GlobalAlloc, Layout};
use std::os::raw::c_void;
extern "C" {
    static RedisModule_Alloc: Option<extern "C" fn(usize) -> *mut c_void>;
    static RedisModule_Free: Option<extern "C" fn(*mut c_void)>;
}

pub struct RedisAlloc;

unsafe impl GlobalAlloc for RedisAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = (layout.size() + layout.align() - 1) & (!(layout.align() - 1));
        RedisModule_Alloc.unwrap()(size).cast::<u8>()
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        RedisModule_Free.unwrap()(ptr.cast::<c_void>())
    }
}

#[cfg(feature = "redis_allocator")]
#[global_allocator]
pub static ALLOC: RedisAlloc = RedisAlloc;

#[cfg(test)]
mod trie_tests {
    use crate::trie::Trie;
    use crate::trie_iter::TrieIterator;
    use std::str;

    #[test]
    fn basic1() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo", 1);
        assert_eq!(*trie.get_str("foo").unwrap(), 1);
        assert_eq!(trie.len(), 1);
        assert_eq!(trie.n_nodes(), 1);
    }

    #[test]
    fn basic2() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.get_str("foo2123"), None);
        assert_eq!(trie.len(), 2);
        assert_eq!(trie.n_nodes(), 3);
    }

    #[test]
    fn test_del() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.len(), 2);
        assert_eq!(trie.n_nodes(), 3);

        trie.del_str("foo1");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.len(), 1);
        assert_eq!(trie.n_nodes(), 1);

        trie.del_str("foo2");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(trie.get_str("foo2"), None);
        assert_eq!(trie.len(), 0);
        assert_eq!(trie.n_nodes(), 0);
    }

    #[test]
    fn test_split() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo").unwrap(), 2);
        assert_eq!(trie.len(), 2);
        assert_eq!(trie.n_nodes(), 2);

        trie.del_str("foo");
        assert_eq!(trie.get_str("foo"), None);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(trie.len(), 1);
        assert_eq!(trie.n_nodes(), 1);

        trie.del_str("foo1");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(trie.get_str("foo2"), None);
        assert_eq!(trie.len(), 0);
        assert_eq!(trie.n_nodes(), 0);
    }

    #[test]
    fn test_find() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("fo1", 4);
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        trie.add_str("foo3", 3);
        assert_eq!(trie.len(), 4);
        assert_eq!(trie.n_nodes(), 6);

        assert_eq!(trie.find_str("bar").next(), None);

        let mut res: Vec<(String, i64)> = trie
            .find_str("foo")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(
            res,
            vec![
                ("foo1".to_string(), 1),
                ("foo2".to_string(), 2),
                ("foo3".to_string(), 3)
            ]
        );

        let mut res: Vec<(String, i64)> = trie
            .find_str("fo")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(
            res,
            vec![
                ("foo1".to_string(), 1),
                ("foo2".to_string(), 2),
                ("foo3".to_string(), 3),
                ("fo1".to_string(), 4)
            ]
        );

        let mut res: Vec<(String, i64)> = trie
            .find_str("")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(
            res,
            vec![
                ("foo1".to_string(), 1),
                ("foo2".to_string(), 2),
                ("foo3".to_string(), 3),
                ("fo1".to_string(), 4)
            ]
        );
    }

    #[test]
    fn test_find_matches_prefixes() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("fo", 4);
        trie.add_str("foo1", 1);
        trie.add_str("foo11", 2);
        trie.add_str("foo3", 3);
        assert_eq!(trie.len(), 4);
        assert_eq!(trie.n_nodes(), 5);

        let mut res: Vec<(String, i64)> = trie
            .find_matches_prefixes_str("foo112")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(
            res,
            vec![
                ("foo1".to_string(), 1),
                ("foo11".to_string(), 2),
                ("fo".to_string(), 4)
            ]
        );

        let mut res: Vec<(String, i64)> = trie
            .find_matches_prefixes_str("foo33")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(res, vec![("foo3".to_string(), 3), ("fo".to_string(), 4)]);
    }

    #[test]
    fn test_trie_into_iter() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("fo", 4);
        trie.add_str("foo1", 1);
        trie.add_str("foo11", 2);
        trie.add_str("foo3", 3);
        assert_eq!(trie.len(), 4);
        assert_eq!(trie.n_nodes(), 5);

        let mut data: Vec<i64> = trie.into_iter().collect();
        data.sort();
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_lots_of_values() {
        let mut trie: Trie<usize> = Trie::new();
        let docs: Vec<String> = (0..21).into_iter().map(|i| format!("doc{i}")).collect();
        for (index, doc) in docs.iter().enumerate() {
            trie.add_str(doc.as_str(), index);
        }
        assert_eq!(trie.len(), 21);
        assert_eq!(trie.n_nodes(), 22);

        for (index, doc) in docs.iter().enumerate() {
            assert_eq!(trie.del_str(doc.as_str()).unwrap(), index);
        }
    }

    #[test]
    fn test_lex_range() {
        let mut trie: Trie<usize> = Trie::new();
        trie.add_str("foo", 4);
        trie.add_str("foo1", 1);
        trie.add_str("bar", 2);
        trie.add_str("bar1", 3);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(None, true, None, true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(
            res,
            vec![
                ("bar".to_string(), 2),
                ("bar1".to_string(), 3),
                ("foo".to_string(), 4),
                ("foo1".to_string(), 1)
            ]
        );

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar"), true, Some("cc"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar".to_string(), 2), ("bar1".to_string(), 3)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar"), false, Some("cc"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar\0"), false, Some("cc"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar\0"), true, Some("cc"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar1"), true, Some("foo"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3), ("foo".to_string(), 4)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar1"), false, Some("foo"), false)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar1"), false, Some("foo"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("foo".to_string(), 4)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar"), false, Some("foo"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3), ("foo".to_string(), 4)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar"), false, Some("foo1"), false)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(res, vec![("bar1".to_string(), 3), ("foo".to_string(), 4)]);

        let res: Vec<(String, usize)> = trie
            .lex_range_str(Some("bar"), false, Some("foo1"), true)
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        assert_eq!(
            res,
            vec![
                ("bar1".to_string(), 3),
                ("foo".to_string(), 4),
                ("foo1".to_string(), 1)
            ]
        );
    }


    #[test]
    fn test_wildcard() {
        let mut trie: Trie<usize> = Trie::new();
        trie.add_str("afoo", 4);
        trie.add_str("afoo1", 1);
        trie.add_str("aboo", 2);
        trie.add_str("aboo1", 3);
        trie.add_str("bbbb", 3);

        let mut res: Vec<(String, usize)> = trie.wildcard_search_str("*oo*")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        assert_eq!(
            res,
            vec![
                ("afoo1".to_string(), 1),
                ("aboo".to_string(), 2),
                ("aboo1".to_string(), 3),
                ("afoo".to_string(), 4)
            ]
        );

        let mut res: Vec<(String, usize)> = trie.wildcard_search_str("*foo*")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        assert_eq!(
            res,
            vec![
                ("afoo1".to_string(), 1),
                ("afoo".to_string(), 4)
            ]
        );

        let mut res: Vec<(String, usize)> = trie.wildcard_search_str("*f?o*")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        assert_eq!(
            res,
            vec![
                ("afoo1".to_string(), 1),
                ("afoo".to_string(), 4)
            ]
        );

        let mut res: Vec<(String, usize)> = trie.wildcard_search_str("*??o*")
            .into_iter()
            .map(|(k, v)| (str::from_utf8(&k).unwrap().to_string(), *v))
            .collect();
        
        res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        assert_eq!(
            res,
            vec![
                ("afoo1".to_string(), 1),
                ("aboo".to_string(), 2),
                ("aboo1".to_string(), 3),
                ("afoo".to_string(), 4)
            ]
        );
    }
}
