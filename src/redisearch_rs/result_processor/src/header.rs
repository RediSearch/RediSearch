use crate::{ResultProcessor, ResultProcessorType, SearchResult, upstream::Upstream};
use std::{ffi::c_void, marker::PhantomPinned, mem::MaybeUninit, ptr::NonNull, time::Duration};

pub type FFIResultProcessorNext =
    unsafe extern "C" fn(NonNull<Header>, res: NonNull<MaybeUninit<SearchResult>>) -> i32;

pub type FFIResultProcessorFree = unsafe extern "C" fn(NonNull<Header>);

#[repr(C)]
pub struct ResultProcessorWrapper<T> {
    pub base: Header,
    pub payload: T,
}

impl<T> ResultProcessorWrapper<T> {
    pub fn into_ptr(self) -> *mut Header {
        Box::into_raw(Box::new(self)).cast()
    }

    pub fn from_ptr(ptr: *mut Header) -> Box<Self> {
        // MUST BE KEPT IN SYNC WITH `into_ptr` above
        unsafe { Box::from_raw(ptr.cast()) }
    }
}

#[repr(C)]
pub struct Header {
    /// Reference to the parent structure
    /// TODO Check if Option needed
    pub parent: Option<NonNull<c_void>>, // QueryIterator*

    /// Previous result processor in the chain
    pub upstream: Option<NonNull<Self>>,

    /// Type of result processor
    pub ty: ResultProcessorType,

    /// time measurements
    /// TODO find ffi type
    pub timespec: Duration,

    pub next: FFIResultProcessorNext,
    pub free: FFIResultProcessorFree,

    // TODO check if we need to worry about <https://github.com/rust-lang/rust/issues/63818>
    _unpin: PhantomPinned,
}

impl Header {
    pub fn for_processor<T: ResultProcessor>() -> Self {
        Self {
            parent: None,
            upstream: None,
            ty: T::type_(),
            timespec: Duration::ZERO,
            next: result_processor_next::<T>,
            free: result_processor_free::<T>,
            _unpin: PhantomPinned,
        }
    }

    pub fn upstream(&mut self) -> Option<Upstream> {
        Some(Upstream {
            hdr: self.upstream?,
        })
    }
}

/// Utility function that implement the `ResultProcessorNext` FFI function
/// for any type that implements `ResultProcessor`
unsafe extern "C" fn result_processor_next<T: ResultProcessor>(
    ptr: NonNull<Header>,
    mut out: NonNull<MaybeUninit<SearchResult>>,
) -> i32 {
    let mut me = ptr.cast::<ResultProcessorWrapper<T>>();

    let ResultProcessorWrapper { base, payload } = unsafe { me.as_mut() };
    let upstream = base.upstream();

    match payload.next(upstream) {
        Ok(Some(res)) => {
            unsafe { out.as_mut() }.write(res);
            return 0;
        }
        Ok(None) => {
            // TODO convert into ret code
            return 1;
        }
        Err(_) => {
            // TODO convert rust error into ret code
            return 1;
        }
    }
}

/// Utility function that implement the `ResultProcessorFree` FFI function
/// MUST BE KEPT IN SYNC WITH `result_processor_alloc` below
unsafe extern "C" fn result_processor_free<T>(ptr: NonNull<Header>) {
    let me = ResultProcessorWrapper::<T>::from_ptr(ptr.as_ptr());
    // Safety: TODO
    drop(me);
}
