use core::mem::size_of;
use core::usize;
use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};
use std::format;
use std::vec;

#[test]
fn test_size_of() {
    use core::mem::size_of;
    assert_eq!(size_of::<LowMemoryThinVec<u8>>(), size_of::<&u8>());

    assert_eq!(size_of::<Option<LowMemoryThinVec<u8>>>(), size_of::<&u8>());
}

#[test]
fn test_drop_empty() {
    LowMemoryThinVec::<u8>::new();
}

#[test]
fn test_clone() {
    let mut v = LowMemoryThinVec::<i32>::new();
    assert!(!v.has_capacity());
    v.push(0);
    v.pop();
    assert!(v.has_capacity());

    let v2 = v.clone();
    assert!(!v2.has_capacity());
}

#[test]
fn test_partial_eq() {
    assert_eq!(low_memory_thin_vec![0], low_memory_thin_vec![0]);
    assert_ne!(low_memory_thin_vec![0], low_memory_thin_vec![1]);
    assert_eq!(low_memory_thin_vec![1, 2, 3], vec![1, 2, 3]);
}

#[test]
fn test_clear() {
    let mut v = LowMemoryThinVec::<i32>::new();
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
        let mut v = LowMemoryThinVec::<i32>::new();
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
        let v = LowMemoryThinVec::<i32>::new();
        assert_eq!(v.into_iter().count(), 0);

        let v = LowMemoryThinVec::<i32>::new();
        #[allow(clippy::never_loop)]
        for _ in v.into_iter() {
            unreachable!();
        }
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
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
        let mut v = LowMemoryThinVec::<i32>::new();
        v.shrink_to_fit();
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        let new = v.split_off(0);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);

        assert_eq!(new.len(), 0);
        assert_eq!(new.capacity(), 0);
        assert_eq!(&new[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        v.reserve(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        v.reserve_exact(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        v.reserve(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = LowMemoryThinVec::<i32>::with_capacity(0);

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = LowMemoryThinVec::<i32>::default();

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        v.retain(|_| unreachable!());

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let mut v = LowMemoryThinVec::<i32>::new();
        v.retain_mut(|_| unreachable!());

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }

    {
        let v = LowMemoryThinVec::<i32>::new();
        let v = v.clone();

        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 0);
        assert_eq!(&v[..], &[]);
    }
}

struct DropCounter<'a> {
    count: &'a mut u32,
}

impl<'a> Drop for DropCounter<'a> {
    fn drop(&mut self) {
        *self.count += 1;
    }
}

#[test]
fn test_small_vec_struct() {
    assert!(size_of::<LowMemoryThinVec<u8>>() == size_of::<usize>());
}

#[test]
fn test_double_drop() {
    struct TwoVec<T> {
        x: LowMemoryThinVec<T>,
        y: LowMemoryThinVec<T>,
    }

    let (mut count_x, mut count_y) = (0, 0);
    {
        let mut tv = TwoVec {
            x: LowMemoryThinVec::new(),
            y: LowMemoryThinVec::new(),
        };
        tv.x.push(DropCounter {
            count: &mut count_x,
        });
        tv.y.push(DropCounter {
            count: &mut count_y,
        });

        // If LowMemoryThinVec had a drop flag, here is where it would be zeroed.
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
    let mut v = LowMemoryThinVec::new();
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
fn test_extend() {
    let mut v = LowMemoryThinVec::<usize>::new();
    let mut w = LowMemoryThinVec::new();
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

    let mut a = LowMemoryThinVec::new();
    let b = low_memory_thin_vec![Foo, Foo];

    a.extend(b);
    assert_eq!(a, &[Foo, Foo]);

    // Double drop
    let mut count_x = 0;
    {
        let mut x = LowMemoryThinVec::new();
        let y = low_memory_thin_vec![DropCounter {
            count: &mut count_x
        }];
        x.extend(y);
    }

    assert_eq!(count_x, 1);
}

/* TODO: implement extend for Iter<&Copy>
    #[test]
    fn test_extend_ref() {
        let mut v = low_memory_thin_vec![1, 2];
        v.extend(&[3, 4, 5]);

        assert_eq!(v.len(), 5);
        assert_eq!(v, [1, 2, 3, 4, 5]);

        let w = low_memory_thin_vec![6, 7];
        v.extend(&w);

        assert_eq!(v.len(), 7);
        assert_eq!(v, [1, 2, 3, 4, 5, 6, 7]);
    }
*/

#[test]
fn test_slice_from_mut() {
    let mut values = low_memory_thin_vec![1, 2, 3, 4, 5];
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
    let mut values = low_memory_thin_vec![1, 2, 3, 4, 5];
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
    let mut values = low_memory_thin_vec![1, 2, 3, 4, 5];
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
    let mut v = low_memory_thin_vec![];
    let three: LowMemoryThinVec<Box<_>> =
        low_memory_thin_vec![Box::new(1), Box::new(2), Box::new(3)];
    let two: LowMemoryThinVec<Box<_>> = low_memory_thin_vec![Box::new(4), Box::new(5)];
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
    let mut vec = low_memory_thin_vec![1, 2, 3, 4];
    vec.retain(|&x| x % 2 == 0);
    assert_eq!(vec, [2, 4]);
}

#[test]
fn test_retain_mut() {
    let mut vec = low_memory_thin_vec![9, 9, 9, 9];
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
    let mut v = LowMemoryThinVec::new();
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
    assert_eq!(
        low_memory_thin_vec![]
            .into_iter()
            .partition(|x: &i32| *x < 3),
        (low_memory_thin_vec![], low_memory_thin_vec![])
    );
    assert_eq!(
        low_memory_thin_vec![1, 2, 3]
            .into_iter()
            .partition(|x| *x < 4),
        (low_memory_thin_vec![1, 2, 3], low_memory_thin_vec![])
    );
    assert_eq!(
        low_memory_thin_vec![1, 2, 3]
            .into_iter()
            .partition(|x| *x < 2),
        (low_memory_thin_vec![1], low_memory_thin_vec![2, 3])
    );
    assert_eq!(
        low_memory_thin_vec![1, 2, 3]
            .into_iter()
            .partition(|x| *x < 0),
        (low_memory_thin_vec![], low_memory_thin_vec![1, 2, 3])
    );
}

#[test]
fn test_zip_unzip() {
    let z1 = low_memory_thin_vec![(1, 4), (2, 5), (3, 6)];

    let (left, right): (LowMemoryThinVec<_>, LowMemoryThinVec<_>) = z1.iter().cloned().unzip();

    assert_eq!((1, 4), (left[0], right[0]));
    assert_eq!((2, 5), (left[1], right[1]));
    assert_eq!((3, 6), (left[2], right[2]));
}

#[test]
fn test_vec_truncate_drop() {
    static mut DROPS: u32 = 0;
    struct Elem(#[allow(dead_code)] i32);
    impl Drop for Elem {
        fn drop(&mut self) {
            unsafe {
                DROPS += 1;
            }
        }
    }

    let mut v = low_memory_thin_vec![Elem(1), Elem(2), Elem(3), Elem(4), Elem(5)];
    assert_eq!(unsafe { DROPS }, 0);
    v.truncate(3);
    assert_eq!(unsafe { DROPS }, 2);
    v.truncate(0);
    assert_eq!(unsafe { DROPS }, 5);
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

    let mut v = low_memory_thin_vec![BadElem(1), BadElem(2), BadElem(0xbadbeef), BadElem(4)];
    v.truncate(0);
}

#[test]
fn test_index() {
    let vec = low_memory_thin_vec![1, 2, 3];
    assert!(vec[1] == 2);
}

#[test]
#[should_panic]
fn test_index_out_of_bounds() {
    let vec = low_memory_thin_vec![1, 2, 3];
    let _ = vec[3];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_1() {
    let x = low_memory_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[!0..];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_2() {
    let x = low_memory_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[..6];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_3() {
    let x = low_memory_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[!0..4];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_4() {
    let x = low_memory_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[1..6];
}

#[test]
#[should_panic]
fn test_slice_out_of_bounds_5() {
    let x = low_memory_thin_vec![1, 2, 3, 4, 5];
    let _ = &x[3..2];
}

#[test]
#[should_panic]
fn test_swap_remove_empty() {
    let mut vec = LowMemoryThinVec::<i32>::new();
    vec.swap_remove(0);
}

#[test]
fn test_move_items() {
    let vec = low_memory_thin_vec![1, 2, 3];
    let mut vec2 = low_memory_thin_vec![];
    for i in vec {
        vec2.push(i);
    }
    assert_eq!(vec2, [1, 2, 3]);
}

#[test]
fn test_move_items_reverse() {
    let vec = low_memory_thin_vec![1, 2, 3];
    let mut vec2 = low_memory_thin_vec![];
    for i in vec.into_iter().rev() {
        vec2.push(i);
    }
    assert_eq!(vec2, [3, 2, 1]);
}

#[test]
fn test_move_items_zero_sized() {
    let vec = low_memory_thin_vec![(), (), ()];
    let mut vec2 = low_memory_thin_vec![];
    for i in vec {
        vec2.push(i);
    }
    assert_eq!(vec2, [(), (), ()]);
}

#[test]
fn test_split_off() {
    let mut vec = low_memory_thin_vec![1, 2, 3, 4, 5, 6];
    let vec2 = vec.split_off(4);
    assert_eq!(vec, [1, 2, 3, 4]);
    assert_eq!(vec2, [5, 6]);
}

#[test]
fn test_into_iter_as_slice() {
    let vec = low_memory_thin_vec!['a', 'b', 'c'];
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
    let vec = low_memory_thin_vec!['a', 'b', 'c'];
    let mut into_iter = vec.into_iter();
    assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);
    into_iter.as_mut_slice()[0] = 'x';
    into_iter.as_mut_slice()[1] = 'y';
    assert_eq!(into_iter.next().unwrap(), 'x');
    assert_eq!(into_iter.as_slice(), &['y', 'c']);
}

#[test]
fn test_into_iter_debug() {
    let vec = low_memory_thin_vec!['a', 'b', 'c'];
    let into_iter = vec.into_iter();
    let debug = format!("{:?}", into_iter);
    assert_eq!(debug, "IntoIter(['a', 'b', 'c'])");
}

#[test]
fn test_into_iter_count() {
    assert_eq!(low_memory_thin_vec![1, 2, 3].into_iter().count(), 3);
}

#[test]
fn test_into_iter_clone() {
    fn iter_equal<I: Iterator<Item = i32>>(it: I, slice: &[i32]) {
        let v: LowMemoryThinVec<i32> = it.collect();
        assert_eq!(&v[..], slice);
    }
    let mut it = low_memory_thin_vec![1, 2, 3].into_iter();
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
fn overaligned_allocations() {
    #[repr(align(256))]
    struct Foo(usize);
    let mut v = low_memory_thin_vec![Foo(273)];
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

    let mut v = LowMemoryThinVec::new();
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
    let mut vec: LowMemoryThinVec<u32> = low_memory_thin_vec![];
    unsafe {
        vec.set_len(0); // at one point this caused a crash
    }
}

#[test]
// The `debug_assert!` in `set_len` only fires if debug assertions are enabled.
#[cfg(debug_assertions)]
#[should_panic(expected = "invalid set_len(1) on empty LowMemoryThinVec")]
fn test_set_len_invalid() {
    let mut vec: LowMemoryThinVec<u32> = low_memory_thin_vec![];
    unsafe {
        vec.set_len(1);
    }
}

#[test]
#[should_panic(
    expected = "The size of the allocated buffer for `LowMemoryThinVec` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_header_too_big() {
    let vec: LowMemoryThinVec<u8> = LowMemoryThinVec::with_capacity(isize::MAX as usize - 2);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `LowMemoryThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_cap_too_big() {
    let vec: LowMemoryThinVec<u8> = LowMemoryThinVec::with_capacity(isize::MAX as usize + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `LowMemoryThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_size_mul1() {
    let vec: LowMemoryThinVec<u16> = LowMemoryThinVec::with_capacity(isize::MAX as usize + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the array of elements within `LowMemoryThinVec<T>` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_size_mul2() {
    let vec: LowMemoryThinVec<u16> = LowMemoryThinVec::with_capacity(isize::MAX as usize / 2 + 1);
    assert!(vec.capacity() > 0);
}

#[test]
#[should_panic(
    expected = "The size of the allocated buffer for `LowMemoryThinVec` would exceed `isize::MAX`, which is the maximum size that can be allocated."
)]
fn test_capacity_overflow_cap_really_isnt_isize() {
    let vec: LowMemoryThinVec<u8> = LowMemoryThinVec::with_capacity(isize::MAX as usize);
    assert!(vec.capacity() > 0);
}
