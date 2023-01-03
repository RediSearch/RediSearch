pub mod c_api;
pub mod matches_prefixes_iterator;
pub mod sub_trie_iterator;
pub mod trie;
pub mod trie_iter;

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
    }

    #[test]
    fn basic2() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.len(), 2);
    }

    #[test]
    fn test_del() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.len(), 2);

        trie.del_str("foo1");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(*trie.get_str("foo2").unwrap(), 2);
        assert_eq!(trie.len(), 1);

        trie.del_str("foo2");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(trie.get_str("foo2"), None);
        assert_eq!(trie.len(), 0);
    }

    #[test]
    fn test_split() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("foo1", 1);
        trie.add_str("foo", 2);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(*trie.get_str("foo").unwrap(), 2);
        assert_eq!(trie.len(), 2);

        trie.del_str("foo");
        assert_eq!(trie.get_str("foo"), None);
        assert_eq!(*trie.get_str("foo1").unwrap(), 1);
        assert_eq!(trie.len(), 1);

        trie.del_str("foo1");
        assert_eq!(trie.get_str("foo1"), None);
        assert_eq!(trie.get_str("foo2"), None);
        assert_eq!(trie.len(), 0);
    }

    #[test]
    fn test_find() {
        let mut trie: Trie<i64> = Trie::new();
        trie.add_str("fo1", 4);
        trie.add_str("foo1", 1);
        trie.add_str("foo2", 2);
        trie.add_str("foo3", 3);

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

        let mut data: Vec<i64> = trie.into_iter().collect();
        data.sort();
        assert_eq!(data, vec![1, 2, 3, 4]);
    }
}
