/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use buffer::{Buffer, BufferReader, BufferWriter};
use std::alloc::{Layout, alloc};
use std::ffi::c_char;
use std::io::{Read, Write};
use std::ptr::{NonNull, copy_nonoverlapping};

#[test]
fn buffer_creation() {
    let capacity = 100;
    let buffer = create_test_buffer(capacity);

    assert_eq!(buffer.capacity(), capacity);
    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.remaining_capacity(), capacity);
    assert!(buffer.is_empty());
}

#[test]
#[should_panic]
fn reader_position_must_be_in_bounds() {
    let buffer = buffer_from_array([1u8, 2, 3, 4, 5]);
    BufferReader::new_at(&buffer, 6);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic = "The requested buffer capacity would overflow usize::MAX"]
fn cannot_overflow_usize() {
    let mut buffer = buffer_from_array([1u8, 2, 3, 4, 5]);
    buffer.reserve(usize::MAX - 3);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic = "The requested buffer capacity would overflow isize::MAX"]
fn cannot_overflow_isize() {
    let mut buffer = buffer_from_array([1u8, 2, 3, 4, 5]);
    buffer.reserve(isize::MAX as usize);
}

#[test]
fn read_from_arbitrary_position() {
    let buffer = buffer_from_array([1u8, 2, 3, 4, 5]);
    let initial_position = 2;
    let mut reader = BufferReader::new_at(&buffer, initial_position);

    let mut bytes = Vec::new();
    let n_bytes_read = reader.read_to_end(&mut bytes).unwrap();

    assert_eq!(n_bytes_read, buffer.len() - initial_position);
    assert_eq!(bytes, [3, 4, 5]);
}

#[test]
#[should_panic]
fn writer_position_must_be_in_bounds() {
    let mut buffer = buffer_from_array([1u8, 2, 3, 4, 5]);
    BufferWriter::new_at(&mut buffer, 6);
}

#[test]
fn buffer_as_slice() {
    let test_data = [1u8, 2, 3, 4, 5];
    let buffer = buffer_from_array(test_data);

    // Check slice access
    let slice = buffer.as_slice();
    assert_eq!(slice, &test_data);
}

#[test]
fn buffer_as_mut_slice() {
    unsafe {
        let capacity = 100;
        let mut buffer = create_test_buffer(capacity);

        // Initialize memory before setting length
        for i in 0..5 {
            *buffer.0.data.add(i) = 0; // Zero-initialize
        }
        // Now it's safe to set the length
        buffer.advance(5);

        // Modify via mut slice
        let mut_slice = buffer.as_mut_slice();
        for (i, item) in mut_slice.iter_mut().enumerate() {
            *item = (i + 1) as u8;
        }

        // Verify changes
        let expected = [1, 2, 3, 4, 5];
        assert_eq!(buffer.as_slice(), &expected);
    }
}

#[test]
fn buffer_advance() {
    unsafe {
        let mut buffer = create_test_buffer(100);

        assert_eq!(buffer.len(), 0);

        // Initialize first 10 bytes before advancing
        for i in 0..10 {
            *(buffer.0.data.add(i) as *mut u8) = i as u8;
        }
        buffer.advance(10);
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.remaining_capacity(), 90);

        // Initialize next 20 bytes before advancing
        for i in 0..20 {
            *(buffer.0.data.add(10 + i) as *mut u8) = i as u8;
        }
        buffer.advance(20);
        assert_eq!(buffer.len(), 30);
        assert_eq!(buffer.remaining_capacity(), 70);
    }
}

#[test]
fn buffer_advance_overflow() {
    use std::panic;

    unsafe {
        let capacity = 100;
        let mut buffer = create_test_buffer(capacity);

        // Use catch_unwind to handle the panic and clean up memory
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            buffer.advance(capacity + 1);
        }));

        // Assert that the panic occurred with the expected message
        match result {
            Err(panic_payload) => {
                match (
                    panic_payload.downcast_ref::<&str>(),
                    panic_payload.downcast_ref::<String>(),
                ) {
                    (Some(message), _) => {
                        assert!(
                            message.contains("n <= self.remaining_capacity()"),
                            "Expected panic message to contain 'n <= self.remaining_capacity()',\
                            got: {message}"
                        );
                    }
                    (None, Some(message)) => {
                        assert!(
                            message.contains("n <= self.remaining_capacity()"),
                            "Expected panic message to contain 'n <= self.remaining_capacity()',\
                            got: {message}"
                        );
                    }
                    (None, None) => {
                        panic!("Expected a string panic message");
                    }
                }
            }
            Ok(_) => panic!("Expected buffer.advance to panic, but it didn't"),
        }
    }
}

#[test]
fn buffer_reader() {
    unsafe {
        let mut buffer = create_test_buffer(100);

        // Fill buffer with test data
        let test_data = b"Hello, world!";
        copy_nonoverlapping(
            test_data.as_ptr(),
            buffer.0.data as *mut u8,
            test_data.len(),
        );
        buffer.advance(test_data.len());

        // Create reader
        let mut reader = BufferReader::new(&buffer);

        // Read data
        let mut dest = [0u8; 5];
        assert_eq!(reader.read(&mut dest).unwrap(), 5);
        assert_eq!(dest, b"Hello"[..]);
        assert_eq!(reader.position(), 5);

        // Read more data
        let mut dest = [0u8; 8];
        assert_eq!(reader.read(&mut dest).unwrap(), 8);
        assert_eq!(dest, b", world!"[..]);
        assert_eq!(reader.position(), 13);

        // Try to read more than available (should just give us 0)
        let mut dest = [0u8; 1];
        assert_eq!(reader.read(&mut dest).unwrap(), 0);
    }
}

#[test]
fn buffer_writer() {
    let mut buffer = create_test_buffer(100);

    // Create writer
    let mut writer = BufferWriter::new_at(&mut buffer, 0);

    // Write data
    let test_data = b"Hello";
    assert_eq!(writer.write(test_data).unwrap(), 5);
    assert_eq!(writer.buffer().len(), 5);

    // Write more data
    let test_data = b", world!";
    assert_eq!(writer.write(test_data).unwrap(), 8);
    assert_eq!(writer.buffer().len(), 13);

    // Check the written data
    let expected = b"Hello, world!";
    assert_eq!(writer.buffer().as_slice(), expected);
}

#[test]
fn buffer_writer_grow() {
    let initial_capacity = 10;
    let mut buffer = create_test_buffer(initial_capacity);

    // Create writer
    let mut writer = BufferWriter::new_at(&mut buffer, 0);

    // Write data that fits within initial capacity
    let test_data = b"HelloWorld";
    assert_eq!(writer.write(test_data).unwrap(), 10);
    assert_eq!(writer.buffer().len(), 10);

    // Write more data that will require growing the buffer
    let test_data = b"MoreData";
    assert_eq!(writer.write(test_data).unwrap(), 8);

    // Buffer should have grown
    assert!(writer.buffer().capacity() > initial_capacity);
    assert_eq!(writer.buffer().len(), 18);

    // Check the written data
    let expected = b"HelloWorldMoreData";
    assert_eq!(&writer.buffer().as_slice()[..18], expected);

    // Verify that the position is at the end of the written data.
    assert_eq!(writer.position(), 18);
}

#[test]
fn buffer_grow_edge_cases() {
    unsafe {
        // Test growing a buffer that's at capacity
        let initial_capacity = 10;
        let mut buffer = create_test_buffer(initial_capacity);

        // Fill buffer to capacity
        for i in 0..initial_capacity {
            *(buffer.0.data.add(i) as *mut u8) = (i % 255) as u8;
        }
        buffer.advance(initial_capacity);

        // Create writer at exact end of buffer
        let mut writer = BufferWriter::new(&mut buffer);

        // Write 1 more byte - should trigger grow
        let test_data = b"!";
        assert_eq!(writer.write(test_data).unwrap(), 1);

        // Buffer should have grown
        assert!(writer.buffer().capacity() > initial_capacity);
        assert_eq!(writer.buffer().len(), initial_capacity + 1);

        // Verify all data is preserved
        for i in 0..initial_capacity {
            assert_eq!(writer.buffer().as_slice()[i], (i % 255) as u8);
        }
        assert_eq!(writer.buffer().as_slice()[initial_capacity], b'!');

        // Test growing by large amount
        let large_data = vec![b'x'; 100];
        assert_eq!(writer.write(&large_data).unwrap(), 100);

        // Buffer capacity should accommodate all data
        assert!(writer.buffer().capacity() >= initial_capacity + 1 + 100);
        assert_eq!(writer.buffer().len(), initial_capacity + 1 + 100);

        // Verify the large data was written correctly
        for i in 0..100 {
            assert_eq!(writer.buffer().as_slice()[initial_capacity + 1 + i], b'x');
        }

        // Verify that the position matches the end of the written data.
        assert_eq!(writer.position(), initial_capacity + 1 + 100);
    }
}

// Helper function to create a new buffer for testing,
// with a predetermined capacity and no initialized entries.
fn create_test_buffer(capacity: usize) -> Buffer {
    let layout = Layout::array::<u8>(capacity).unwrap();
    let data = unsafe { alloc(layout) };
    unsafe { Buffer::new(NonNull::new(data).unwrap(), 0, capacity) }
}

// Helper function to create a new buffer for testing,
// with a predetermined capacity and no initialized entries.
fn buffer_from_array<const N: usize>(a: [u8; N]) -> Buffer {
    let ptr = Box::into_raw(Box::new(a)).cast::<u8>();
    let ptr = NonNull::new(ptr).unwrap();
    unsafe { Buffer::new(ptr, N, N) }
}

/// Mock implementation of Buffer_Grow for tests
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn Buffer_Grow(buffer: *mut ffi::Buffer, extra_len: usize) -> usize {
    // Safety: buffer is a valid pointer to a Buffer.
    let buffer = unsafe { &mut *buffer };
    let old_capacity = buffer.cap;

    // Double the capacity or add extra_len, whichever is greater
    let new_capacity = std::cmp::max(buffer.cap * 2, buffer.cap + extra_len);

    let layout = Layout::array::<c_char>(old_capacity).unwrap();
    let new_data = unsafe { std::alloc::realloc(buffer.data as *mut _, layout, new_capacity) };
    buffer.data = new_data as *mut c_char;
    buffer.cap = new_capacity;

    // Return bytes added
    new_capacity - old_capacity
}

/// Mock implementation of BufferFree for tests
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "C" fn Buffer_Free(buffer: *mut ffi::Buffer) -> usize {
    if buffer.is_null() {
        return 0;
    }

    // Safety: buffer is a valid pointer to a Buffer.
    let buffer = unsafe { &mut *buffer };

    let layout = Layout::array::<c_char>(buffer.cap).unwrap();
    let size = layout.size();
    unsafe {
        std::alloc::dealloc(buffer.data as *mut u8, layout);
    }

    buffer.data = std::ptr::null_mut();
    buffer.cap = 0;
    size
}
