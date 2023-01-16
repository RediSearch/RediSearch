use crate::low_memory_vec::LowMemoryVec;

pub trait FirstCharGetter {
    fn get_first_char(&self) -> u8;
}

#[derive(Debug)]
pub struct OrderedU8Map<T: FirstCharGetter> {
    // the Option<Box<...>> is used to improve memory usage, most of the data are leaves with no children so we only store 8 bytes
    // for those instead of 16 bytes if we use LowMemoryVec directly
    map: Option<Box<LowMemoryVec<(u8, T)>>>,
}

impl<T: FirstCharGetter> OrderedU8Map<T> {
    pub(crate) fn new() -> OrderedU8Map<T> {
        OrderedU8Map { map: None }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.as_ref().map(|e| e.len()).unwrap_or(0)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, key: u8) -> Option<&T> {
        let m = self.map.as_ref()?;
        let index = m.binary_search_by_key(&key, |e| e.0).ok()?;
        Some(&m.get(index as u32)?.1)
    }

    pub(crate) fn get_mut(&mut self, key: u8) -> Option<&mut T> {
        let m = self.map.as_mut()?;
        let index = m.binary_search_by_key(&key, |e| e.0).ok()?;
        Some(&mut m.get_mut(index as u32)?.1)
    }

    pub(crate) fn get_or_create<F: FnOnce() -> T>(&mut self, key: u8, create: F) -> &mut T {
        let m = self.map.get_or_insert(Box::new(LowMemoryVec::new()));
        let index = match m.binary_search_by_key(&key, |e| e.0) {
            Ok(index) => index,
            Err(index) => {
                let val = create();
                m.insert(index as u32, (key, val));
                index
            }
        };
        &mut m[index].1
    }

    pub(crate) fn insert(&mut self, key: u8, val: T) -> bool {
        let m = self.map.get_or_insert(Box::new(LowMemoryVec::new()));
        match m.binary_search_by_key(&key, |e| e.0) {
            Ok(_index) => false,
            Err(index) => {
                m.insert(index as u32, (key, val));
                true
            }
        }
    }

    pub(crate) fn remove(&mut self, key: u8) -> Option<T> {
        let m = self.map.as_mut()?;
        let index = m.binary_search_by_key(&key, |e| e.0).ok()?;
        let res = m.remove(index as u32);
        Some(res.1)
    }

    pub(crate) fn values(&self) -> OrderedU8ValuesIterator<'_, T> {
        OrderedU8ValuesIterator::new(self)
    }

    pub(crate) fn take(&mut self) -> OrderedU8Map<T> {
        OrderedU8Map {
            map: self.map.take()
        }
    }
}

pub struct OrderedU8MapIterator<T: FirstCharGetter> {
    map: OrderedU8Map<T>,
}

impl<T: FirstCharGetter> IntoIterator for OrderedU8Map<T> {
    type Item = (u8, T);
    type IntoIter = OrderedU8MapIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        OrderedU8MapIterator { map: self }
    }
}

impl<T: FirstCharGetter> Iterator for OrderedU8MapIterator<T> {
    type Item = (u8, T);

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.map.map.as_mut()?.pop()?;
        Some(res)
    }
}

pub struct OrderedU8ValuesIterator<'map, T: FirstCharGetter> {
    map: &'map OrderedU8Map<T>,
    index: usize,
}

impl<'map, T: FirstCharGetter> OrderedU8ValuesIterator<'map, T> {
    fn new(map: &'map OrderedU8Map<T>) -> OrderedU8ValuesIterator<'map, T> {
        OrderedU8ValuesIterator { map, index: 0 }
    }
}

impl<'map, T: FirstCharGetter> Iterator for OrderedU8ValuesIterator<'map, T> {
    type Item = &'map T;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.map.map.as_ref()?.get(self.index as u32)?;
        self.index += 1;
        Some(&res.1)
    }
}
