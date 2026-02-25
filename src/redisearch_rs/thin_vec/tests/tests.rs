use core::mem::size_of;
use core::usize;
use std::format;
use std::vec;
use thin_vec::{
    Header, MediumThinVec, SmallThinVec, ThinVec, TinyThinVec, VecCapacity, small_thin_vec,
};

#[test]
fn test_size_of() {
    use core::mem::size_of;
    assert_eq!(size_of::<SmallThinVec<u8>>(), size_of::<&u8>());

    assert_eq!(size_of::<Option<SmallThinVec<u8>>>(), size_of::<&u8>());
}

#[test]
fn test_drop_empty() {
    SmallThinVec::<u8>::new();
}

#[test]
fn test_alloc() {
    let mut v: SmallThinVec<i32> = SmallThinVec::new();
    assert!(!v.has_allocated());
    v.push(1);
    assert!(v.has_allocated());
    v.pop();
    assert!(v.has_allocated());
    v.shrink_to_fit();
    assert!(!v.has_allocated());
    v.reserve(64);
    assert!(v.has_allocated());
    v = SmallThinVec::with_capacity(64);
    assert!(v.has_allocated());
    v = SmallThinVec::with_capacity(0);
    assert!(!v.has_allocated());
}

#[test]
fn test_clone() {
    let mut v = SmallThinVec::<i32>::new();
    assert!(!v.has_allocated());
    v.push(0);
    v.pop();
    assert!(v.has_allocated());

    let v2 = v.clone();
    assert!(!v2.has_allocated());
}

#[test]
fn test_partial_eq() {
    let v1: SmallThinVec<i32> = small_thin_vec![0];
    let v2: SmallThinVec<i32> = small_thin_vec![0];
    let v3: SmallThinVec<i32> = small_thin_vec![1];
    assert_eq!(v1, v2);
    assert_ne!(v1, v3);
    let v4: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    assert_eq!(v4, vec![1, 2, 3]);
}

#[test]
fn test_clear() {
    let mut v = SmallThinVec::<i32>::new();
    assert_eq!(v.len(), 0);
    assert_eq!(v.capacity(), 0);
    assert_eq!(&v[..], &[]);

    v.clear();
    assert_eq!(v.len(), 0);
    assert_eq!(v.capacity(), 0);
    assert_eq!(&v[..], &[]);

    v.push(1);
    v.push(2);
    assert_eq!(v.len(), 2);
    assert!(v.capacity() >= 2);
    assert_eq!(&v[..], &[1, 2]);

    v.clear();
    assert_eq!(v.len(), 0);
    assert!(v.capacity() >= 2);
    assert_eq!(&v[..], &[]);

    v.push(3);
    v.push(4);
    assert_eq!(v.len(), 2);
    assert!(v.capacity() >= 2);
    assert_eq!(&v[..], &[3, 4]);

    v.clear();
    assert_eq!(v.len(), 0);
    assert!(v.capacity() >= 2);
    assert_eq!(&v[..], &[]);

    v.clear();
    assert_eq!(v.len(), 0);
    assert!(v.capacity() >= 2);
    assert_eq!(&v[..], &[]);
}

#[test]
fn test_empty_singleton_torture() {
    {
        let mut v = SmallThinVec::<i32>::new();
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert!(v.is_empty());
        assert_eq!(&v[..], &[]);
        assert_eq!(&mut v[..], &mut []);

        assert_eq!(v.pop(), None);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = SmallThinVec::<i32>::new();
        assert_eq!(v.into_iter().count(), 0);

        let v = SmallThinVec::<i32>::new();
        #[allow(clippy::never_loop)]
        for _ in v.into_iter() {
            unreachable!();
        }
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.truncate(1);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);

        v.truncate(0);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.shrink_to_fit();
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        let new = v.split_off(0);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);

        assert_eq!(new.len(), 0);
        assert_eq!(new.capacity(), 0);
        assert_eq!(&new[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.reserve(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.reserve_exact(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.reserve(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = SmallThinVec::<i32>::with_capacity(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = SmallThinVec::<i32>::default();

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.retain(|_| unreachable!());

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = SmallThinVec::<i32>::new();
        v.retain_mut(|_| unreachable!());

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = SmallThinVec::<i32>::new();
        let v = v.clone();

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }
}

struct DropCounter<'a> {
    count: &'a mut u32,
}

impl Drop for DropCounter<'_> {
    fn drop(&mut self) {
        *self.count += 1;
    }
}

/// A drop tracker that increments a shared counter when dropped.
/// Safe to use in parallel tests unlike `static mut`.
struct DropTracker(std::rc::Rc<std::cell::Cell<u32>>);

impl Drop for DropTracker {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn test_small_vec_struct() {
    assert!(size_of::<SmallThinVec<u8>>() == size_of::<usize>());
}

#[test]
fn test_double_drop() {
    struct TwoVec<T> {
        x: SmallThinVec<T>,
        y: SmallThinVec<T>,
    }

    let (mut count_x, mut count_y) = (0, 0);
    {
        let mut tv = TwoVec {
            x: SmallThinVec::new(),
            y: SmallThinVec::new(),
        };
        tv.x.push(DropCounter {
            count: &mut count_x,
        });
        tv.y.push(DropCounter {
            count: &mut count_y,
        });

        // If ThinVec had a drop flag, here is where it would be zeroed.
        // Instead, it should rely on its internal state to prevent
        // doing anything significant when dropped multiple times.
        drop(tv.x);

        // Here tv goes out of scope, tv.y should be dropped, but not tv.x.
    }

    assert_eq!(count_x, 1);
    assert_eq!(count_y, 1);
}

#[test]
fn test_reserve() {
    let mut v: SmallThinVec<i32> = SmallThinVec::new();
    assert_eq!(v.capacity(), 0);

    v.reserve(2);
    assert!(v.capacity() >= 2);

    for i in 0..16 {
        v.push(i);
    }

    assert!(v.capacity() >= 16);
    v.reserve(16);
    assert!(v.capacity() >= 32);

    v.push(16);

    v.reserve(16);
    assert!(v.capacity() >= 33)
}

#[test]
#[should_panic(expected = "ThinVec size may not exceed the capacity of an 8-bit sized int")]
fn test_reserve_beyond_max_capacity() {
    let mut v: TinyThinVec<i32> = TinyThinVec::new();
    assert_eq!(v.capacity(), 0);

    v.reserve(u8::MAX as usize + 1);
}

#[test]
#[should_panic(expected = "ThinVec size may not exceed the capacity of an 8-bit sized int")]
fn test_reserve_exact_beyond_max_capacity() {
    let mut v: TinyThinVec<i32> = TinyThinVec::new();
    assert_eq!(v.capacity(), 0);

    v.reserve_exact(u8::MAX as usize + 1);
}

#[test]
fn test_extend() {
    let mut v = SmallThinVec::<usize>::new();
    let mut w: SmallThinVec<usize> = SmallThinVec::new();
    v.extend(w.clone());
    assert_eq!(v, &[]);

    v.extend(0..3);
    for i in 0..3 {
        w.push(i)
    }

    assert_eq!(v, w);

    v.extend(3..10);
    for i in 3..10 {
        w.push(i)
    }

    assert_eq!(v, w);

    v.extend(w.clone()); // specializes to `append`
    assert!(v.iter().eq(w.iter().chain(w.iter())));

    // Zero sized types
    #[derive(PartialEq, Debug)]
    struct Foo;

    let mut a: SmallThinVec<Foo> = SmallThinVec::new();
    let b: SmallThinVec<Foo> = small_thin_vec![Foo, Foo];

    a.extend(b);
    assert_eq!(a, &[Foo, Foo]);

    // Double drop
    let mut count_x = 0;
    {
        let mut x: SmallThinVec<DropCounter> = SmallThinVec::new();
        let y: SmallThinVec<DropCounter> = small_thin_vec![DropCounter {
            count: &mut count_x
        }];
        x.extend(y);
    }

    assert_eq!(count_x, 1);
}

#[test]
fn test_slice_from_mut() {
    let mut values: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    {
        let slice = &mut values[2..];
        assert!(slice == [3, 4, 5]);
        for p in slice {
            *p += 2;
        }
    }

    assert!(values == [1, 2, 5, 6, 7]);
}

#[test]
fn test_slice_to_mut() {
    let mut values: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    {
        let slice = &mut values[..2];
        assert!(slice == [1, 2]);
        for p in slice {
            *p += 1;
        }
    }

    assert!(values == [2, 3, 3, 4, 5]);
}

#[test]
fn test_split_at_mut() {
    let mut values: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    {
        let (left, right) = values.split_at_mut(2);
        {
            let left: &[_] = left;
            assert!(left[..left.len()] == [1, 2]);
        }
        for p in left {
            *p += 1;
        }

        {
            let right: &[_] = right;
            assert!(right[..right.len()] == [3, 4, 5]);
        }
        for p in right {
            *p += 2;
        }
    }

    assert_eq!(values, [2, 3, 5, 6, 7]);
}

#[test]
fn test_clone_from() {
    let mut v: SmallThinVec<Box<i32>> = small_thin_vec![];
    let three: SmallThinVec<Box<i32>> = small_thin_vec![Box::new(1), Box::new(2), Box::new(3)];
    let two: SmallThinVec<Box<i32>> = small_thin_vec![Box::new(4), Box::new(5)];
    // zero, long
    v.clone_from(&three);
    assert_eq!(v, three);

    // equal
    v.clone_from(&three);
    assert_eq!(v, three);

    // long, short
    v.clone_from(&two);
    assert_eq!(v, two);

    // short, long
    v.clone_from(&three);
    assert_eq!(v, three)
}

#[test]
fn test_retain() {
    let mut vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4];
    vec.retain(|&x| x % 2 == 0);
    assert_eq!(vec, [2, 4]);
}

#[test]
fn test_retain_mut() {
    let mut vec: SmallThinVec<i32> = small_thin_vec![9, 9, 9, 9];
    let mut i = 0;
    vec.retain_mut(|x| {
        i += 1;
        *x = i;
        i != 4
    });
    assert_eq!(vec, [1, 2, 3]);
}

#[test]
fn zero_sized_values() {
    let mut v: SmallThinVec<()> = SmallThinVec::new();
    assert_eq!(v.len(), 0);
    v.push(());
    assert_eq!(v.len(), 1);
    v.push(());
    assert_eq!(v.len(), 2);
    assert_eq!(v.pop(), Some(()));
    assert_eq!(v.pop(), Some(()));
    assert_eq!(v.pop(), None);

    assert_eq!(v.iter().count(), 0);
    v.push(());
    assert_eq!(v.iter().count(), 1);
    v.push(());
    assert_eq!(v.iter().count(), 2);

    for &() in &v {}

    assert_eq!(v.iter_mut().count(), 2);
    v.push(());
    assert_eq!(v.iter_mut().count(), 3);
    v.push(());
    assert_eq!(v.iter_mut().count(), 4);

    for &mut () in &mut v {}
    unsafe {
        v.set_len(0);
    }
    assert_eq!(v.iter_mut().count(), 0);
}

#[test]
fn test_partition() {
    let empty: SmallThinVec<i32> = small_thin_vec![];
    let result: (SmallThinVec<i32>, SmallThinVec<i32>) =
        empty.into_iter().partition(|x: &i32| *x < 3);
    assert_eq!(result, (small_thin_vec![], small_thin_vec![]));

    let v1: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let result: (SmallThinVec<i32>, SmallThinVec<i32>) = v1.into_iter().partition(|x| *x < 4);
    assert_eq!(result, (small_thin_vec![1, 2, 3], small_thin_vec![]));

    let v2: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let result: (SmallThinVec<i32>, SmallThinVec<i32>) = v2.into_iter().partition(|x| *x < 2);
    assert_eq!(result, (small_thin_vec![1], small_thin_vec![2, 3]));

    let v3: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let result: (SmallThinVec<i32>, SmallThinVec<i32>) = v3.into_iter().partition(|x| *x < 0);
    assert_eq!(result, (small_thin_vec![], small_thin_vec![1, 2, 3]));
}

#[test]
fn test_zip_unzip() {
    let z1: SmallThinVec<(i32, i32)> = small_thin_vec![(1, 4), (2, 5), (3, 6)];

    let (left, right): (SmallThinVec<i32>, SmallThinVec<i32>) = z1.iter().cloned().unzip();

    assert_eq!((1, 4), (left[0], right[0]));
    assert_eq!((2, 5), (left[1], right[1]));
    assert_eq!((3, 6), (left[2], right[2]));
}

#[test]
fn test_remove() {
    let mut v: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let mut i = v.remove(1);
    assert_eq!(i, 2);
    let expected: SmallThinVec<i32> = small_thin_vec![1, 3];
    assert_eq!(v, expected);

    i = v.remove(0);
    assert_eq!(i, 1);
    let expected: SmallThinVec<i32> = small_thin_vec![3];
    assert_eq!(v, expected);
}

#[test]
#[should_panic]
fn test_remove_out_of_bounds() {
    let mut v: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    v.remove(3);
}

#[test]
fn test_vec_truncate_drop() {
    use std::cell::Cell;
    use std::rc::Rc;

    let drops = Rc::new(Cell::new(0u32));
    let mut v: SmallThinVec<DropTracker> = small_thin_vec![
        DropTracker(Rc::clone(&drops)),
        DropTracker(Rc::clone(&drops)),
        DropTracker(Rc::clone(&drops)),
        DropTracker(Rc::clone(&drops)),
        DropTracker(Rc::clone(&drops)),
    ];
    assert_eq!(drops.get(), 0);
    v.truncate(3);
    assert_eq!(drops.get(), 2);
    v.truncate(0);
    assert_eq!(drops.get(), 5);
}

#[test]
#[should_panic]
fn test_vec_truncate_fail() {
    struct BadElem(i32);
    impl Drop for BadElem {
        fn drop(&mut self) {
            let BadElem(ref mut x) = *self;
            if *x == 0xbadbeef {
                panic!("BadElem panic: 0xbadbeef")
            }
        }
    }

    let mut v: SmallThinVec<BadElem> =
        small_thin_vec![BadElem(1), BadElem(2), BadElem(0xbadbeef), BadElem(4)];
    v.truncate(0);
}

#[test]
#[should_panic]
fn test_vec_clear_fail() {
    struct BadElem(i32);
    impl Drop for BadElem {
        fn drop(&mut self) {
            let BadElem(ref mut x) = *self;
            if *x == 0xbadbeef {
                panic!("BadElem panic: 0xbadbeef")
            }
        }
    }

    let mut v: SmallThinVec<BadElem> =
        small_thin_vec![BadElem(1), BadElem(2), BadElem(0xbadbeef), BadElem(4)];
    v.clear();
}

#[test]
fn test_index() {
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    assert!(vec[1] == 2);
}

#[test]
#[should_panic]
fn test_index_out_of_bounds() {
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let _ = vec[3];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_1() {
    let x: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[!0..];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_2() {
    let x: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[..6];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_3() {
    let x: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[!0..4];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_4() {
    let x: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[1..6];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_5() {
    let x: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[3..2];
}

#[test]
#[should_panic]
fn test_swap_remove_empty() {
    let mut vec = SmallThinVec::<i32>::new();
    vec.swap_remove(0);
}

#[test]
fn test_move_items() {
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let mut vec2: SmallThinVec<i32> = small_thin_vec![];
    for i in vec {
        vec2.push(i);
    }
    assert_eq!(vec2, [1, 2, 3]);
}

#[test]
fn test_move_items_reverse() {
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let mut vec2: SmallThinVec<i32> = small_thin_vec![];
    for i in vec.into_iter().rev() {
        vec2.push(i);
    }
    assert_eq!(vec2, [3, 2, 1]);
}

#[test]
fn test_move_items_zero_sized() {
    let vec: SmallThinVec<()> = small_thin_vec![(), (), ()];
    let mut vec2: SmallThinVec<()> = small_thin_vec![];
    for i in vec {
        vec2.push(i);
    }
    assert_eq!(vec2, [(), (), ()]);
}

#[test]
fn test_split_off() {
    let mut vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3, 4, 5, 6];
    let vec2 = vec.split_off(4);
    assert_eq!(vec, [1, 2, 3, 4]);
    assert_eq!(vec2, [5, 6]);
}

#[test]
fn test_into_iter_as_slice() {
    let vec: SmallThinVec<char> = small_thin_vec!['a', 'b', 'c'];
    let mut into_iter = vec.into_iter();
    assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    let _ = into_iter.next().unwrap();
    assert_eq!(into_iter.as_slice(), &['b', 'c']);
    let _ = into_iter.next().unwrap();
    let _ = into_iter.next().unwrap();
    assert_eq!(into_iter.as_slice(), &[]);
}

#[test]
fn test_into_iter_as_mut_slice() {
    let vec: SmallThinVec<char> = small_thin_vec!['a', 'b', 'c'];
    let mut into_iter = vec.into_iter();
    assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    into_iter.as_mut_slice()[0] = 'x';
    into_iter.as_mut_slice()[1] = 'y';
    assert_eq!(into_iter.next().unwrap(), 'x');
    assert_eq!(into_iter.as_slice(), &['y', 'c']);
}

#[test]
fn test_into_iter_debug() {
    let vec: SmallThinVec<char> = small_thin_vec!['a', 'b', 'c'];
    let into_iter = vec.into_iter();
    let debug = format!("{into_iter:?}");
    assert_eq!(debug, "IntoIter(['a', 'b', 'c'])");
}

#[test]
fn test_into_iter_count() {
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    assert_eq!(vec.into_iter().count(), 3);
}

#[test]
fn test_into_iter_clone() {
    fn iter_equal<I: Iterator<Item = i32>>(it: I, slice: &[i32]) {
        let v: SmallThinVec<i32> = it.collect();
        assert_eq!(&v[..], slice);
    }
    let vec: SmallThinVec<i32> = small_thin_vec![1, 2, 3];
    let mut it = vec.into_iter();
    iter_equal(it.clone(), &[1, 2, 3]);
    assert_eq!(it.next(), Some(1));
    let mut it = it.rev();
    iter_equal(it.clone(), &[3, 2]);
    assert_eq!(it.next(), Some(3));
    iter_equal(it.clone(), &[2]);
    assert_eq!(it.next(), Some(2));
    iter_equal(it.clone(), &[]);
    assert_eq!(it.next(), None);
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn overaligned_allocations() {
    #[repr(align(256))]
    struct Foo(usize);
    let mut v: SmallThinVec<Foo> = small_thin_vec![Foo(273)];
    for i in 0..0x1000 {
        v.reserve_exact(i);
        assert!(v[0].0 == 273);
        assert!(v.as_ptr() as usize & 0xff == 0);
        v.shrink_to_fit();
        assert!(v[0].0 == 273);
        assert!(v.as_ptr() as usize & 0xff == 0);
    }
}

#[test]
fn test_reserve_exact() {
    // This is all the same as test_reserve

    let mut v: SmallThinVec<i32> = SmallThinVec::new();
    assert_eq!(v.capacity(), 0);

    v.reserve_exact(2);
    assert!(v.capacity() >= 2);

    for i in 0..16 {
        v.push(i);
    }

    assert!(v.capacity() >= 16);
    v.reserve_exact(16);
    assert!(v.capacity() >= 32);

    v.push(16);

    v.reserve_exact(16);
    assert!(v.capacity() >= 33)
}

#[test]
fn test_set_len() {
    let mut vec: SmallThinVec<u32> = small_thin_vec![];
    unsafe {
        vec.set_len(0); // at one point this caused a crash
    }
}

#[test]
// The `debug_assert!` in `set_len` only fires if debug assertions are enabled.
#[cfg(debug_assertions)]
#[should_panic(expected = "invalid set_len(1) on empty ThinVec")]
fn test_set_len_invalid() {
    let mut vec: SmallThinVec<u32> = small_thin_vec![];
    unsafe {
        vec.set_len(1);
    }
}

#[test]
#[should_panic(
    expected = "The size of the allocated buffer for `ThinVec` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_header_too_big() {
    let vec: ThinVec<u8> = ThinVec::with_capacity(isize::MAX as usize - 2);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `ThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_cap_too_big() {
    let vec: ThinVec<u8> = ThinVec::with_capacity(isize::MAX as usize + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `ThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_size_mul1() {
    let vec: ThinVec<u16> = ThinVec::with_capacity(isize::MAX as usize + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `ThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_size_mul2() {
    let vec: ThinVec<u16> = ThinVec::with_capacity(isize::MAX as usize / 2 + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the allocated buffer for `ThinVec` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_cap_really_isnt_isize() {
    let vec: ThinVec<u8> = ThinVec::with_capacity(isize::MAX as usize);
    assert!(vec.capacity() > 0);
}

// ============================================================================
// Tests for generic size types
// ============================================================================

#[test]
fn test_header_sizes() {
    assert_eq!(size_of::<Header<u8>>(), 2); // 1 + 1
    assert_eq!(size_of::<Header<u16>>(), 4); // 2 + 2
    assert_eq!(size_of::<Header<u32>>(), 8); // 4 + 4
    assert_eq!(size_of::<Header<u64>>(), 16); // 8 + 8
}

/// Generic test helper that runs basic operations for any size type
fn test_basic_operations_generic<S: VecCapacity>() {
    // Test new and has_allocated
    let mut v: ThinVec<i32, S> = ThinVec::new();
    assert!(!v.has_allocated());
    assert_eq!(v.len(), 0);
    assert!(v.is_empty());

    // Test push
    v.push(1);
    assert!(v.has_allocated());
    assert_eq!(v.len(), 1);
    assert!(!v.is_empty());
    assert_eq!(v[0], 1);

    v.push(2);
    v.push(3);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0], 1);
    assert_eq!(v[1], 2);
    assert_eq!(v[2], 3);

    // Test pop
    assert_eq!(v.pop(), Some(3));
    assert_eq!(v.len(), 2);
    assert_eq!(v.pop(), Some(2));
    assert_eq!(v.pop(), Some(1));
    assert_eq!(v.pop(), None);
    assert!(v.is_empty());

    // Test with_capacity
    let v2: ThinVec<i32, S> = ThinVec::with_capacity(10);
    assert!(v2.capacity() >= 10);
    assert_eq!(v2.len(), 0);

    // Test shrink_to_fit
    let mut v3: ThinVec<i32, S> = ThinVec::with_capacity(100);
    v3.push(1);
    v3.push(2);
    v3.shrink_to_fit();
    assert!(v3.capacity() >= 2);
    assert_eq!(v3.len(), 2);
}

#[test]
fn test_basic_operations_u8() {
    test_basic_operations_generic::<u8>();
}

#[test]
fn test_basic_operations_u16() {
    test_basic_operations_generic::<u16>();
}

#[test]
fn test_basic_operations_u32() {
    test_basic_operations_generic::<u32>();
}

#[test]
fn test_basic_operations_u64() {
    test_basic_operations_generic::<u64>();
}

/// Test that each size type's empty header singleton works correctly
fn test_empty_singleton_generic<S: VecCapacity>() {
    let v1: ThinVec<i32, S> = ThinVec::new();
    let v2: ThinVec<i32, S> = ThinVec::new();

    // Both should use the same singleton
    assert!(!v1.has_allocated());
    assert!(!v2.has_allocated());
    assert_eq!(v1.len(), 0);
    assert_eq!(v2.len(), 0);
    assert_eq!(v1.capacity(), 0);
    assert_eq!(v2.capacity(), 0);
}

#[test]
fn test_empty_singleton_u8() {
    test_empty_singleton_generic::<u8>();
}

#[test]
fn test_empty_singleton_u16() {
    test_empty_singleton_generic::<u16>();
}

#[test]
fn test_empty_singleton_u32() {
    test_empty_singleton_generic::<u32>();
}

#[test]
fn test_empty_singleton_u64() {
    test_empty_singleton_generic::<u64>();
}

/// Test iterator operations for all size types
fn test_iterator_generic<S: VecCapacity>() {
    let mut v: ThinVec<i32, S> = ThinVec::new();
    v.push(1);
    v.push(2);
    v.push(3);

    // Test into_iter
    let collected: Vec<i32> = v.into_iter().collect();
    assert_eq!(collected, vec![1, 2, 3]);

    // Test iter
    let mut v: ThinVec<i32, S> = ThinVec::new();
    v.push(4);
    v.push(5);
    let sum: i32 = v.iter().sum();
    assert_eq!(sum, 9);

    // Test iter_mut
    for x in v.iter_mut() {
        *x *= 2;
    }
    assert_eq!(v[0], 8);
    assert_eq!(v[1], 10);
}

#[test]
fn test_iterator_u8() {
    test_iterator_generic::<u8>();
}

#[test]
fn test_iterator_u16() {
    test_iterator_generic::<u16>();
}

#[test]
fn test_iterator_u32() {
    test_iterator_generic::<u32>();
}

#[test]
fn test_iterator_u64() {
    test_iterator_generic::<u64>();
}

/// Test clone for all size types
fn test_clone_generic<S: VecCapacity>() {
    let mut v: ThinVec<i32, S> = ThinVec::new();
    v.push(1);
    v.push(2);
    v.push(3);

    let v2 = v.clone();
    assert_eq!(v, v2);
    assert_eq!(v2.len(), 3);

    // Empty clone
    let empty: ThinVec<i32, S> = ThinVec::new();
    let empty2 = empty.clone();
    assert!(!empty2.has_allocated());
}

#[test]
fn test_clone_u8() {
    test_clone_generic::<u8>();
}

#[test]
fn test_clone_u16() {
    test_clone_generic::<u16>();
}

#[test]
fn test_clone_u32() {
    test_clone_generic::<u32>();
}

#[test]
fn test_clone_u64() {
    test_clone_generic::<u64>();
}

// Test capacity overflow for u8 size type
#[test]
#[should_panic(expected = "8-bit")]
fn test_u8_capacity_overflow() {
    let _: TinyThinVec<u8> = TinyThinVec::with_capacity(256);
}

#[test]
fn test_u8_max_capacity() {
    let v: TinyThinVec<u8> = TinyThinVec::with_capacity(255);
    assert!(v.capacity() >= 255);
}

// Test capacity overflow for u16 size type
#[test]
#[should_panic(expected = "16-bit")]
fn test_u16_capacity_overflow() {
    let _: SmallThinVec<u8> = SmallThinVec::with_capacity(65536);
}

#[test]
fn test_u16_max_capacity() {
    let v: SmallThinVec<u8> = SmallThinVec::with_capacity(65535);
    assert!(v.capacity() >= 65535);
}

// Test that u32 can hold more than u16
#[test]
fn test_u32_large_capacity() {
    // This would panic with u16, but works with u32
    let v: MediumThinVec<u8> = MediumThinVec::with_capacity(100_000);
    assert!(v.capacity() >= 100_000);
}

// Test drop behavior with different size types
fn test_drop_generic<S: VecCapacity>() {
    use std::cell::Cell;
    use std::rc::Rc;

    let drops = Rc::new(Cell::new(0u32));

    {
        let mut v: ThinVec<DropTracker, S> = ThinVec::new();
        v.push(DropTracker(Rc::clone(&drops)));
        v.push(DropTracker(Rc::clone(&drops)));
        v.push(DropTracker(Rc::clone(&drops)));
    }

    assert_eq!(drops.get(), 3);
}

#[test]
fn test_drop_u8() {
    test_drop_generic::<u8>();
}

#[test]
fn test_drop_u16() {
    test_drop_generic::<u16>();
}

#[test]
fn test_drop_u32() {
    test_drop_generic::<u32>();
}

#[test]
fn test_drop_u64() {
    test_drop_generic::<u64>();
}

// Test From and Into conversions for different size types
#[test]
fn test_from_vec_different_sizes() {
    let std_vec = vec![1, 2, 3, 4, 5];

    let v8: TinyThinVec<i32> = TinyThinVec::from(std_vec.clone());
    assert_eq!(v8.len(), 5);

    let v16: SmallThinVec<i32> = SmallThinVec::from(std_vec.clone());
    assert_eq!(v16.len(), 5);

    let v32: MediumThinVec<i32> = MediumThinVec::from(std_vec.clone());
    assert_eq!(v32.len(), 5);

    let v64: ThinVec<i32> = ThinVec::from(std_vec);
    assert_eq!(v64.len(), 5);
}

// Test extend for different size types
fn test_extend_generic<S: VecCapacity>() {
    let mut v: ThinVec<i32, S> = ThinVec::new();
    v.extend(vec![1, 2, 3]);
    v.extend(4..7);
    assert_eq!(v.len(), 6);
    assert_eq!(&v[..], &[1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_extend_u8() {
    test_extend_generic::<u8>();
}

#[test]
fn test_extend_u16() {
    test_extend_generic::<u16>();
}

#[test]
fn test_extend_u32() {
    test_extend_generic::<u32>();
}

#[test]
fn test_extend_u64() {
    test_extend_generic::<u64>();
}

// Test split_off for different size types
fn test_split_off_generic<S: VecCapacity>() {
    let mut v: ThinVec<i32, S> = ThinVec::new();
    v.extend(1..=6);

    let v2 = v.split_off(3);
    assert_eq!(&v[..], &[1, 2, 3]);
    assert_eq!(&v2[..], &[4, 5, 6]);
}

#[test]
fn test_split_off_u8() {
    test_split_off_generic::<u8>();
}

#[test]
fn test_split_off_u16() {
    test_split_off_generic::<u16>();
}

#[test]
fn test_split_off_u32() {
    test_split_off_generic::<u32>();
}

#[test]
fn test_split_off_u64() {
    test_split_off_generic::<u64>();
}

// Test that vec size remains pointer-sized regardless of S
#[test]
fn test_vec_size_is_pointer_sized() {
    assert_eq!(size_of::<TinyThinVec<u8>>(), size_of::<usize>());
    assert_eq!(size_of::<SmallThinVec<u8>>(), size_of::<usize>());
    assert_eq!(size_of::<MediumThinVec<u8>>(), size_of::<usize>());
    assert_eq!(size_of::<ThinVec<u8>>(), size_of::<usize>());

    // Option should also be pointer-sized
    assert_eq!(size_of::<Option<TinyThinVec<u8>>>(), size_of::<usize>());
    assert_eq!(size_of::<Option<SmallThinVec<u8>>>(), size_of::<usize>());
    assert_eq!(size_of::<Option<MediumThinVec<u8>>>(), size_of::<usize>());
    assert_eq!(size_of::<Option<ThinVec<u8>>>(), size_of::<usize>());
}

// Test mem_usage returns correct sizes for different size types
#[test]
fn test_mem_usage_different_sizes() {
    // For u8 elements with capacity 10:
    // - u8 header: 2 bytes + padding to align u8 (0) + 10 bytes = 12 bytes
    // - u16 header: 4 bytes + padding to align u8 (0) + 10 bytes = 14 bytes (padded to 16)
    // - u32 header: 8 bytes + padding to align u8 (0) + 10 bytes = 18 bytes (padded to 24)
    // - u64 header: 16 bytes + padding to align u8 (0) + 10 bytes = 26 bytes (padded to 32)

    let v8: TinyThinVec<u8> = TinyThinVec::with_capacity(10);
    let v16: SmallThinVec<u8> = SmallThinVec::with_capacity(10);
    let v32: MediumThinVec<u8> = MediumThinVec::with_capacity(10);
    let v64: ThinVec<u8> = ThinVec::with_capacity(10);

    // Smaller size types should use less memory
    assert!(v8.mem_usage() <= v16.mem_usage());
    assert!(v16.mem_usage() <= v32.mem_usage());
    assert!(v32.mem_usage() <= v64.mem_usage());
}
