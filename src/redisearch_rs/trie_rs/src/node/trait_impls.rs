//! Implementation of various standard traits for [`Node`].
use crate::node::Node;
use std::{alloc::dealloc, ffi::c_char, fmt};

/// Convenience method to convert a `c_char` array into a `String`,
/// replacing non-UTF-8 characters with `�` along the way.
pub(crate) fn to_string_lossy(label: &[c_char]) -> String {
    let slice = label.iter().map(|&c| c as u8).collect::<Vec<_>>();
    String::from_utf8_lossy(&slice).into_owned()
}

impl<Data: fmt::Debug> fmt::Debug for Node<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stack = vec![(0, self, 0, 0)];

        while let Some((first_byte, next, white_indentation, line_indentation)) = stack.pop() {
            let label_repr = to_string_lossy(next.label());
            let data_repr = next
                .data()
                .as_ref()
                .map_or("(-)".to_string(), |data| format!("({:?})", data));

            let prefix = if white_indentation == 0 && line_indentation == 0 {
                "".to_string()
            } else {
                let whitespace = " ".repeat(white_indentation);
                let line = "–".repeat(line_indentation - 1);
                let first_byte = to_string_lossy(&[first_byte]);
                format!("{whitespace}↳{first_byte}{line}")
            };

            writeln!(f, "{prefix}\"{label_repr}\" {data_repr}")?;

            for (child, first_byte) in next
                .children()
                .iter()
                .zip(next.children_first_bytes())
                .rev()
            {
                let new_line_indentation = 4;
                let white_indentation = white_indentation + line_indentation + 2;
                stack.push((*first_byte, child, white_indentation, new_line_indentation));
            }
        }
        Ok(())
    }
}

impl<Data: PartialEq> PartialEq for Node<Data> {
    fn eq(&self, other: &Self) -> bool {
        self.label() == other.label()
            && self.children_first_bytes() == other.children_first_bytes()
            && self.data() == other.data()
            && self.children() == other.children()
    }
}

impl<Data: Eq> Eq for Node<Data> {}

// SAFETY:
// `Node` is semantically equivalent to a `HashMap<Vec<c_char>, Data>`, which is `Send`
// if whatever it contains is `Send`.
unsafe impl<Data: Send> Send for Node<Data> {}
// SAFETY:
// `Node` is semantically equivalent to a `HashMap<Vec<c_char>, Data>`, which is `Sync`
// if whatever it contains is `Sync`.
unsafe impl<Data: Sync> Sync for Node<Data> {}

impl<Data: Clone> Clone for Node<Data> {
    fn clone(&self) -> Self {
        // Allocate a new buffer with the same layout of the node we're cloning.
        let mut new_ptr = self.metadata().allocate();

        // SAFETY:
        // - We have exclusive access to the buffer that `new_ptr` points to,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_header(*self.header()) };

        // Copy the label.
        //
        // SAFETY:
        // 1. The capacity of the label buffer matches the length of the label, since
        //    we used the same header to compute the required layout.
        // 2. The two buffers don't overlap. The destination buffer was freshly allocated
        //    earlier in this function.
        // 3. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        unsafe { new_ptr.label().copy_from_slice_nonoverlapping(self.label()) };

        // Copy the children first bytes.
        //
        // SAFETY:
        // 1. The capacity of the destination buffer matches the length of the source, since
        //    we used the same header to compute the required layout.
        // 2. The two buffers don't overlap. The destination buffer was freshly allocated
        //    earlier in this function.
        // 3. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        unsafe {
            new_ptr
                .children_first_bytes()
                .copy_from_slice_nonoverlapping(self.children_first_bytes())
        };

        // Clone the children
        {
            let mut next_ptr = new_ptr.children().ptr();
            for child in self.children() {
                // SAFETY:
                // - The destination data is all contained within a single allocation.
                // - We have exclusive access to the destination buffer,
                //   since it was allocated earlier in this function.
                // - The destination pointer is well aligned, see 1. in [`PtrMetadata::child_ptr`]
                unsafe { next_ptr.write(child.clone()) };
                // SAFETY:
                // - The offsetted pointer doesn't overflow `isize`, since it is within the bounds
                //   of an allocation for a well-formed `Layout` instance.
                // - The offsetted pointer is within the bounds of the allocation, thanks to
                //   layout we used for the buffer.
                unsafe { next_ptr = next_ptr.add(1) };
            }
        }

        // Clone the value if present
        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_value(self.data().cloned()) };

        // SAFETY:
        // - All fields have been initialized.
        unsafe { new_ptr.assume_init() }
    }
}

impl<Data> Drop for Node<Data> {
    fn drop(&mut self) {
        let layout = self.metadata().layout();
        // SAFETY:
        // - We have exclusive access to buffer.
        // - The field is correctly initialized (see invariant 2. in [`Self::ptr`])
        // - The pointer is valid since it comes from a reference.
        unsafe { std::ptr::drop_in_place(self.data_mut()) };
        // SAFETY:
        // - We have exclusive access to buffer.
        // - The field is correctly initialized (see invariant 2. in [`Self::ptr`])
        // - The pointer is valid since it comes from a reference.
        unsafe { std::ptr::drop_in_place(self.children_mut()) };

        // SAFETY:
        // - The pointer was allocated via the same global allocator
        //    we are invoking via `dealloc` (see invariant 3. in [`Self::ptr`])
        // - `layout` is the same layout that was used
        //   to allocate the buffer (see invariant 1. in [`Self::ptr`])
        unsafe { dealloc(self.ptr.as_ptr().cast(), layout) };
    }
}
