//! Implementation of various standard traits for [`Node`].
use crate::node::Node;
use std::{ffi::c_char, fmt};

/// Convenience method to convert a `c_char` array into a `String`,
/// dropping non-UTF-8 characters along the way.
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
            && self.children() == other.children()
            && self.data() == other.data()
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
